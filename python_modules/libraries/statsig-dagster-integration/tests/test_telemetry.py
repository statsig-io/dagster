import threading
import tracemalloc
from unittest.mock import Mock, patch

import pytest

from statsig_dagster_integration import telemetry


def test_disabled_telemetry_has_no_emission() -> None:
    with (
        patch.object(telemetry.googlecloudprofiler, "start") as start,
        telemetry.daemon_telemetry(),
    ):
        pass
    start.assert_not_called()


@pytest.mark.parametrize("exit_error", [None, ValueError, KeyboardInterrupt])
def test_thread_and_tracing_cleanup(exit_error: type[BaseException] | None) -> None:
    snapshot = Mock()
    with (
        patch.object(telemetry.googlecloudprofiler, "start") as start,
        patch.object(telemetry.tracemalloc, "take_snapshot", return_value=snapshot),
    ):
        if exit_error is None:
            with telemetry.daemon_telemetry(tracemalloc_enabled=True, interval_seconds=0.001):
                assert tracemalloc.is_tracing()
        else:
            with (
                pytest.raises(exit_error),
                telemetry.daemon_telemetry(tracemalloc_enabled=True, interval_seconds=0.001),
            ):
                raise exit_error
    assert not tracemalloc.is_tracing()
    assert not any(thread.name == "dagster-tracemalloc" for thread in threading.enumerate())
    start.assert_not_called()


def test_profiler_failure_and_existing_tracing() -> None:
    tracemalloc.start()
    try:
        with (
            patch.object(
                telemetry.googlecloudprofiler,
                "start",
                side_effect=ValueError("synthetic"),
            ) as start,
            telemetry.daemon_telemetry(
                profiler_enabled=True, tracemalloc_enabled=True, deployment="fixture"
            ),
        ):
            assert tracemalloc.is_tracing()
        assert tracemalloc.is_tracing()
        start.assert_called_once_with(
            service="dagster-daemon", service_version="fixture", verbose=3
        )
    finally:
        tracemalloc.stop()
