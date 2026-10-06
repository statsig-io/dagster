import os
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from functools import wraps

from dagster._daemon.cli import (
    main as daemon_main,
    run_command,
)
from dagster_webserver.cli import main as webserver_main

from statsig_dagster_integration.compat import require_supported_versions
from statsig_dagster_integration.retries import workspace_retries
from statsig_dagster_integration.storage import DEFAULT_POOL_SETTINGS, PoolSettings, daemon_pools
from statsig_dagster_integration.telemetry import daemon_telemetry


@contextmanager
def run_telemetry() -> Iterator[None]:
    require_supported_versions()
    original = run_command.callback
    if original is None:
        raise RuntimeError("Pinned daemon run callback is missing")

    @wraps(original)
    def run(*args: object, **kwargs: object) -> object:
        with daemon_telemetry(
            profiler_enabled=os.getenv("STATSIG_DAGSTER_PROFILER_ENABLED", "1") != "0",
            tracemalloc_enabled=os.getenv("STATSIG_DAGSTER_TRACEMALLOC_ENABLED", "1") != "0",
            deployment=os.getenv("DAGSTER_DEPLOYMENT", "UNSET"),
        ):
            return original(*args, **kwargs)

    run_command.callback = run
    try:
        yield
    finally:
        run_command.callback = original


def main(
    arguments: Sequence[str] | None = None, *, pool_settings: PoolSettings = DEFAULT_POOL_SETTINGS
) -> None:
    args = list(sys.argv[1:] if arguments is None else arguments)
    if not args or args[0] not in {"daemon", "webserver"}:
        raise SystemExit(
            "Usage: python -m statsig_dagster_integration.cli {daemon|webserver} [args...]"
        )
    command = args.pop(0)
    sys.argv = [f"dagster-{command}", *args]
    with workspace_retries(), daemon_pools(pool_settings):
        if command == "webserver":
            webserver_main()
        else:
            with run_telemetry():
                daemon_main()


if __name__ == "__main__":
    main()
