from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from functools import wraps
from types import ModuleType
from typing import Concatenate, ParamSpec, TypeVar, cast

from dagster_postgres.auth import PgTokenProvider
from dagster_postgres.event_log import event_log
from dagster_postgres.run_storage import run_storage
from dagster_postgres.schedule_storage import schedule_storage
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.pool import NullPool, QueuePool

from statsig_dagster_integration.compat import require_supported_versions

P = ParamSpec("P")
PostgresStorage = (
    event_log.PostgresEventLogStorage
    | run_storage.PostgresRunStorage
    | schedule_storage.PostgresScheduleStorage
)
S = TypeVar("S", bound=PostgresStorage)


@dataclass(frozen=True)
class PoolSettings:
    pool_size: int = 1
    pool_recycle: int = 3600
    max_overflow: int = 10
    pool_timeout: float = 30.0
    statement_timeout: int = 600000

    def __post_init__(self) -> None:
        if self.pool_size != 1 or self.pool_recycle <= 0 or self.max_overflow < 0:
            raise ValueError("Pool requires size 1, positive recycle, and bounded overflow >=0")
        if self.pool_timeout <= 0 or self.statement_timeout <= 0:
            raise ValueError("Pool acquisition and statement timeouts must be positive")


DEFAULT_POOL_SETTINGS = PoolSettings()
_ACTIVE_SETTINGS: PoolSettings | None = None


def engine_options(
    postgres_url: str, options: dict[str, object], settings: PoolSettings
) -> dict[str, object]:
    result = dict(options)
    result.update(
        poolclass=QueuePool,
        pool_size=settings.pool_size,
        pool_recycle=result.get("pool_recycle", settings.pool_recycle),
        max_overflow=result.get("max_overflow", settings.max_overflow),
        pool_timeout=settings.pool_timeout,
    )
    connect_args = dict(cast("Mapping[str, object]", result.get("connect_args", {})))
    existing = connect_args.get("options", make_url(postgres_url).query.get("options", ""))
    connect_args["options"] = (
        f"{existing} -c statement_timeout={settings.statement_timeout}".strip()
    )
    result["connect_args"] = connect_args
    return result


@contextmanager
def replace_attribute(owner: ModuleType | type, name: str, replacement: object) -> Iterator[None]:
    original = getattr(owner, name)
    setattr(owner, name, replacement)
    try:
        yield
    finally:
        setattr(owner, name, original)


def constructor(
    original: Callable[Concatenate[S, P], None],
) -> Callable[Concatenate[S, P], None]:
    @wraps(original)
    def initialize(self: S, *args: P.args, **kwargs: P.kwargs) -> None:
        try:
            original(self, *args, **kwargs)
        except BaseException:
            engine = getattr(self, "_engine", None)
            if engine is not None:
                engine.dispose()
            raise

    return initialize


def optimization(
    original: Callable[[S, int, int, int], None],
) -> Callable[[S, int, int, int], None]:
    @wraps(original)
    def optimize(
        self: S,
        statement_timeout: int,
        pool_recycle: int,
        max_overflow: int,
    ) -> None:
        if max_overflow < 0:
            raise ValueError("Daemon pool overflow must be bounded")
        old_engine = self._engine
        try:
            original(self, statement_timeout, pool_recycle, max_overflow)
        except BaseException:
            if self._engine is not old_engine:
                self._engine.dispose()
                self._engine = old_engine
            raise
        if self._engine is not old_engine:
            old_engine.dispose()

    return optimize


def disposal(
    original: Callable[[S], None],
) -> Callable[[S], None]:
    @wraps(original)
    def dispose(self: S) -> None:
        try:
            original(self)
        finally:
            self._engine.dispose()

    return dispose


@contextmanager
def daemon_pools(settings: PoolSettings = DEFAULT_POOL_SETTINGS) -> Iterator[None]:
    """Process-local adapters preserve constructor timeout and dispose replaced engines."""
    require_supported_versions()
    global _ACTIVE_SETTINGS  # noqa: PLW0603 -- nested contexts must share the same process policy.
    if _ACTIVE_SETTINGS is not None:
        if _ACTIVE_SETTINGS != settings:
            raise RuntimeError("Daemon pool adapters already use different settings")
        yield
        return
    with ExitStack() as stack:
        _ACTIVE_SETTINGS = settings

        def reset_settings() -> None:
            global _ACTIVE_SETTINGS  # noqa: PLW0603 -- restore state when the owning context exits.
            _ACTIVE_SETTINGS = None

        stack.callback(reset_settings)
        for module, cls in (
            (event_log, event_log.PostgresEventLogStorage),
            (run_storage, run_storage.PostgresRunStorage),
            (schedule_storage, schedule_storage.PostgresScheduleStorage),
        ):
            original_create = module.create_pg_engine
            original_clean_create = module.create_engine

            def pooled_engine(
                postgres_url: str,
                token_provider: PgTokenProvider | None = None,
                _create: Callable[..., Engine] = original_create,
                **kwargs: object,
            ) -> Engine:
                return _create(
                    postgres_url,
                    token_provider,
                    **engine_options(postgres_url, kwargs, settings),
                )

            def clean_engine(
                postgres_url: str,
                _create: Callable[..., Engine] = original_clean_create,
                **kwargs: object,
            ) -> Engine:
                # The one-shot drop engine stays NullPool and is disposed by upstream finally.
                options = engine_options(postgres_url, kwargs, settings)
                for key in (
                    "pool_size",
                    "pool_recycle",
                    "max_overflow",
                    "pool_timeout",
                ):
                    options.pop(key)
                options["poolclass"] = NullPool
                return _create(postgres_url, **options)

            stack.enter_context(replace_attribute(module, "create_pg_engine", pooled_engine))
            stack.enter_context(replace_attribute(module, "create_engine", clean_engine))
            stack.enter_context(replace_attribute(cls, "__init__", constructor(cls.__init__)))
            stack.enter_context(
                replace_attribute(
                    cls,
                    "optimize_for_webserver",
                    optimization(cls.optimize_for_webserver),
                )
            )
            stack.enter_context(replace_attribute(cls, "dispose", disposal(cls.dispose)))
        yield
