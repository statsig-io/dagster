import subprocess
import sys
from contextlib import nullcontext
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from dagster._daemon import cli as upstream_cli
from dagster._grpc.client import DagsterGrpcClient
from dagster_postgres.run_storage import PostgresRunStorage
from sqlalchemy.pool import QueuePool

from statsig_dagster_integration import cli, telemetry
from statsig_dagster_integration.storage import DEFAULT_POOL_SETTINGS, PoolSettings


@pytest.mark.parametrize("command", ["daemon", "webserver"])
@pytest.mark.parametrize(
    "settings", [DEFAULT_POOL_SETTINGS, PoolSettings(pool_timeout=0.25, max_overflow=2)]
)
def test_both_image_components_use_adapters_without_emission(
    command: str, settings: PoolSettings
) -> None:
    original_query = DagsterGrpcClient._query

    def official_main() -> None:
        assert DagsterGrpcClient._query is not original_query
        store = PostgresRunStorage(
            "postgresql://synthetic@/fixture", should_autocreate_tables=False
        )
        try:
            assert isinstance(store._engine.pool, QueuePool)
            assert store._engine.pool.timeout() == settings.pool_timeout
            assert store._engine.pool._max_overflow == settings.max_overflow
            assert sys.argv == [f"dagster-{command}", "--help"]
        finally:
            store.dispose()

    with (
        patch.object(cli, f"{command}_main", side_effect=official_main) as main,
        patch.object(telemetry.googlecloudprofiler, "start") as profiler,
        patch.dict(
            "os.environ",
            {
                "STATSIG_DAGSTER_PROFILER_ENABLED": "0",
                "STATSIG_DAGSTER_TRACEMALLOC_ENABLED": "0",
            },
        ),
        patch.object(sys, "argv", []),
    ):
        cli.main([command, "--help"], pool_settings=settings)
        main.assert_called_once()
        profiler.assert_not_called()
    assert DagsterGrpcClient._query is original_query


def test_fresh_import_does_not_activate_framework_adapters() -> None:
    code = """
from dagster._grpc.client import DagsterGrpcClient
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._daemon.cli import run_command
from dagster_postgres.run_storage import run_storage
original = (DagsterGrpcClient._query, WorkspaceProcessContext._load_location,
            run_command.callback, run_storage.create_pg_engine)
import statsig_dagster_integration
from statsig_dagster_integration import cli, retries, storage, telemetry
assert original == (DagsterGrpcClient._query, WorkspaceProcessContext._load_location,
                    run_command.callback, run_storage.create_pg_engine)
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=20, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("enabled", [None, "0", "1"])
def test_actual_run_callback_preserves_default_telemetry_and_opt_out(
    enabled: str | None,
) -> None:
    environment = (
        {}
        if enabled is None
        else {
            "STATSIG_DAGSTER_PROFILER_ENABLED": enabled,
            "STATSIG_DAGSTER_TRACEMALLOC_ENABLED": enabled,
        }
    )
    with (
        patch.dict("os.environ", environment, clear=True),
        patch.object(upstream_cli.run_command, "callback") as callback,
        patch.object(cli, "daemon_telemetry", return_value=nullcontext()) as lifecycle,
        patch.object(sys, "argv", []),
    ):
        original = upstream_cli.run_command.callback

        def main() -> None:
            result = CliRunner().invoke(upstream_cli.cli, ["run"])
            assert result.exit_code == 0, result.output

        with patch.object(cli, "daemon_main", side_effect=main):
            cli.main(["daemon", "run"])
        callback.assert_called_once()
        lifecycle.assert_called_once_with(
            profiler_enabled=enabled != "0",
            tracemalloc_enabled=enabled != "0",
            deployment="UNSET",
        )
        assert upstream_cli.run_command.callback is original


@pytest.mark.parametrize(
    "arguments",
    [
        ["--help"],
        ["--version"],
        ["run", "--help"],
        ["debug", "--help"],
        ["liveness-check", "--help"],
    ],
)
def test_actual_non_run_click_invocations_never_start_telemetry(
    arguments: list[str],
) -> None:
    with patch.object(cli, "daemon_telemetry") as lifecycle, cli.run_telemetry():
        result = CliRunner().invoke(upstream_cli.cli, arguments)
        assert result.exit_code == 0, result.output
    lifecycle.assert_not_called()
