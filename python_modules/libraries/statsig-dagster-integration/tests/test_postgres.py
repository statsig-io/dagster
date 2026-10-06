import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing
from types import ModuleType
from unittest.mock import patch

import psycopg2
import pytest
from dagster_postgres.run_storage import PostgresRunStorage
from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import DBAPIError, TimeoutError

from statsig_dagster_integration.storage import PoolSettings, PostgresStorage, daemon_pools
from tests.test_runtime import STORAGES


def connection_count(url: str) -> int:
    with closing(psycopg2.connect(url)) as observer, observer.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'daemon_pool_fixture'"
        )
        return cursor.fetchone()[0]


def named_url(url: str) -> str:
    return (
        make_url(url)
        .update_query_dict({"options": "-capplication_name=daemon_pool_fixture"})
        .render_as_string(hide_password=False)
    )


def test_real_constructor_storage_roundtrip_and_disposal(postgres_url: str) -> None:
    with daemon_pools():
        stores = [cls(named_url(postgres_url)) for _, cls in STORAGES]
        try:
            for store in stores:
                with store._engine.connect() as connection:
                    assert connection.execute(text("SHOW statement_timeout")).scalar() == "10min"
            assert connection_count(postgres_url) == 3
            run_store = stores[1]
            assert isinstance(run_store, PostgresRunStorage)
            run_store.set_cursor_values({"daemon-fixture": "persisted"})
            assert run_store.get_cursor_values({"daemon-fixture"}) == {
                "daemon-fixture": "persisted"
            }
        finally:
            for store in stores:
                store.dispose()
        assert connection_count(postgres_url) == 0


def test_concurrent_three_pool_budget_and_checkout_timeout(postgres_url: str) -> None:
    ready = threading.Barrier(34, timeout=10)
    release = threading.Event()
    settings = PoolSettings(pool_timeout=0.1)
    with daemon_pools(settings):
        stores = [
            cls(named_url(postgres_url), should_autocreate_tables=False) for _, cls in STORAGES
        ]

        def hold(store_index: int) -> int:
            with stores[store_index]._engine.connect() as connection:
                pid = connection.execute(text("SELECT pg_backend_pid()")).scalar()
                ready.wait()
                assert release.wait(10), "fixture release deadline"
                return pid

        try:
            with ThreadPoolExecutor(max_workers=33) as executor:
                futures = [executor.submit(hold, index) for index in range(3) for _ in range(11)]
                try:
                    ready.wait()
                    assert connection_count(postgres_url) == 33
                    started = time.monotonic()
                    with pytest.raises(TimeoutError):
                        stores[0]._engine.connect()
                    assert time.monotonic() - started < 1.0
                finally:
                    release.set()
                assert len({future.result(timeout=10) for future in futures}) == 33
            assert connection_count(postgres_url) == 3
        finally:
            release.set()
            for store in stores:
                store.dispose()
        assert connection_count(postgres_url) == 0


@pytest.mark.parametrize("module,cls", STORAGES)
def test_real_recycle_optimization_timeout_and_disposal(
    postgres_url: str, module: ModuleType, cls: type[PostgresStorage]
) -> None:
    with daemon_pools(PoolSettings(pool_recycle=1)):
        store = cls(named_url(postgres_url), should_autocreate_tables=False)
        try:
            with store._engine.connect() as connection:
                first = connection.execute(text("SELECT pg_backend_pid()")).scalar()
            time.sleep(1.1)
            with store._engine.connect() as connection:
                assert connection.execute(text("SELECT pg_backend_pid()")).scalar() != first
            store.optimize_for_webserver(30, 3600, 10)
            assert connection_count(postgres_url) == 0
            with store._engine.connect() as connection:
                assert connection.execute(text("SHOW statement_timeout")).scalar() == "30ms"
                with pytest.raises(DBAPIError):
                    connection.execute(text("SELECT pg_sleep(0.2)"))
            assert connection_count(postgres_url) == 1
        finally:
            store.dispose()
        assert connection_count(postgres_url) == 0


def test_two_instance_pools_dispose_independently(postgres_url: str) -> None:
    with daemon_pools(), ExitStack() as cleanup:
        stores = [
            cls(named_url(postgres_url), should_autocreate_tables=False)
            for _ in range(2)
            for _, cls in STORAGES
        ]
        for store in stores:
            cleanup.callback(store.dispose)
            with store._engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        assert connection_count(postgres_url) == 6
        for store in stores[:3]:
            store.dispose()
        assert connection_count(postgres_url) == 3
    assert connection_count(postgres_url) == 0


@pytest.mark.parametrize("module,cls", STORAGES)
def test_real_constructor_failure_releases_opened_connections(
    postgres_url: str, module: object, cls: type
) -> None:
    def connection_then_fail(operation: Callable[[], object]) -> None:
        operation()
        assert connection_count(postgres_url) == 1
        raise ValueError("synthetic failure after actual database inspection")

    with (
        daemon_pools(),
        patch.object(module, "retry_pg_connection_fn", side_effect=connection_then_fail),
        pytest.raises(ValueError),
    ):
        cls(named_url(postgres_url))
    assert connection_count(postgres_url) == 0


@pytest.mark.parametrize("module,cls", STORAGES)
def test_real_clean_storage_failure_closes_temporary_connection(
    postgres_url: str, module: ModuleType, cls: type[PostgresStorage]
) -> None:
    metadata = next(
        getattr(module, name)
        for name in (
            "SqlEventLogStorageMetadata",
            "RunStorageSqlMetadata",
            "ScheduleStorageSqlMetadata",
        )
        if hasattr(module, name)
    )

    def fail_drop(engine: Engine) -> None:
        with engine.connect() as connection:
            assert connection.execute(text("SHOW statement_timeout")).scalar() == "10min"
            assert connection_count(postgres_url) == 1
            raise ValueError("synthetic failure during clean storage")

    with (
        daemon_pools(),
        patch.object(metadata, "drop_all", side_effect=fail_drop),
        pytest.raises(ValueError),
    ):
        cls.create_clean_storage(named_url(postgres_url))
    assert connection_count(postgres_url) == 0


def test_actual_webserver_configured_statement_timeout(postgres_url: str) -> None:
    with daemon_pools():
        for _, cls in STORAGES:
            store = cls(named_url(postgres_url), should_autocreate_tables=False)
            try:
                store.optimize_for_webserver(30000, 3600, 10)
                with store._engine.connect() as connection:
                    assert connection.execute(text("SHOW statement_timeout")).scalar() == "30s"
            finally:
                store.dispose()
    assert connection_count(postgres_url) == 0
