from types import MethodType, SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.remote_representation.code_location import GrpcServerCodeLocation
from dagster._core.scheduler.instigation import SensorInstigatorData, TickStatus
from dagster._daemon import sensor as daemon_sensor

from statsig_dagster_integration import retries

FETCH = "dagster._api.snapshot_sensor.sync_get_external_sensor_execution_data_grpc"


def unavailable() -> DagsterUserCodeUnreachableError:
    return DagsterUserCodeUnreachableError(
        "Could not reach user code server. gRPC Error code: UNAVAILABLE"
    )


def location() -> SimpleNamespace:
    """A stand-in whose evaluation method is the pinned class method, looked up while patched."""
    fake = SimpleNamespace(client=Mock())
    fake.get_sensor_execution_data = MethodType(
        GrpcServerCodeLocation.get_sensor_execution_data, fake
    )
    return fake


def evaluation_arguments(cursor: str | None = "keep-me") -> tuple[MagicMock, MagicMock, MagicMock]:
    sensor = MagicMock()
    sensor.name = "exposure_trigger"
    state = MagicMock()
    state.instigator_data = SensorInstigatorData(cursor=cursor)
    instance = MagicMock()
    instance.get_instigator_state.return_value = state
    workspace = MagicMock()
    workspace.instance = instance
    return workspace, MagicMock(), sensor


def evaluate(workspace: MagicMock, context: MagicMock, sensor: MagicMock) -> list[None]:
    return list(
        daemon_sensor._evaluate_sensor(
            workspace, context, sensor, SimpleNamespace(instigator_data=None), None
        )
    )


def test_unavailable_classification() -> None:
    assert retries.sensor_unavailable(unavailable())
    assert not retries.sensor_unavailable(
        DagsterUserCodeUnreachableError(
            "Could not reach user code server. gRPC Error code: INTERNAL"
        )
    )
    assert not retries.sensor_unavailable(
        DagsterUserCodeUnreachableError("User code server request timed out")
    )
    assert not retries.sensor_unavailable(RuntimeError("UNAVAILABLE"))


def test_retries_then_returns_execution_data() -> None:
    sleep = Mock()
    data = object()
    with (
        patch(FETCH, side_effect=[unavailable(), unavailable(), data]) as fetch,
        retries.sensor_unavailable_skip(sleep),
    ):
        assert location().get_sensor_execution_data(*[Mock()] * 8) is data
    assert fetch.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [2.0, 4.0]


def test_exhaustion_raises_marked_error_after_three_attempts() -> None:
    sleep = Mock()
    with (
        patch(FETCH, side_effect=unavailable()) as fetch,
        retries.sensor_unavailable_skip(sleep),
        pytest.raises(retries.SensorUserCodeUnavailableError) as raised,
    ):
        location().get_sensor_execution_data(*[Mock()] * 8)
    assert isinstance(raised.value, DagsterUserCodeUnreachableError)
    assert "UNAVAILABLE" in str(raised.value)
    assert fetch.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [2.0, 4.0]


@pytest.mark.parametrize("message", ["gRPC Error code: INTERNAL", "request timed out"])
def test_other_unreachable_errors_are_not_retried(message: str) -> None:
    sleep = Mock()
    error = DagsterUserCodeUnreachableError(message)
    with (
        patch(FETCH, side_effect=error) as fetch,
        retries.sensor_unavailable_skip(sleep),
        pytest.raises(DagsterUserCodeUnreachableError) as raised,
    ):
        location().get_sensor_execution_data(*[Mock()] * 8)
    assert raised.value is error
    assert not isinstance(raised.value, retries.SensorUserCodeUnavailableError)
    assert fetch.call_count == 1
    sleep.assert_not_called()


def test_evaluate_skips_tick_and_keeps_cursor() -> None:
    workspace, context, sensor = evaluation_arguments()
    with (
        patch(FETCH, side_effect=unavailable()) as fetch,
        patch.object(daemon_sensor, "_get_code_location_for_sensor") as get_location,
        retries.sensor_unavailable_skip(Mock()),
    ):
        get_location.side_effect = lambda *_args: location()
        assert evaluate(workspace, context, sensor) == [None]
    assert fetch.call_count == 3
    context.update_state.assert_called_once_with(
        TickStatus.SKIPPED, skip_reason=retries.SENSOR_UNAVAILABLE_SKIP_REASON, cursor="keep-me"
    )
    context.logger.warning.assert_called_once()
    workspace.instance.get_instigator_state.assert_called_once_with(
        sensor.get_remote_origin_id(), sensor.selector_id
    )


def test_evaluate_skips_with_no_prior_cursor() -> None:
    workspace, context, sensor = evaluation_arguments(cursor=None)
    with (
        patch(FETCH, side_effect=unavailable()),
        patch.object(daemon_sensor, "_get_code_location_for_sensor") as get_location,
        retries.sensor_unavailable_skip(Mock()),
    ):
        get_location.side_effect = lambda *_args: location()
        evaluate(workspace, context, sensor)
    context.update_state.assert_called_once_with(
        TickStatus.SKIPPED, skip_reason=retries.SENSOR_UNAVAILABLE_SKIP_REASON, cursor=None
    )


def test_evaluate_propagates_other_errors_without_skipping() -> None:
    for error in (
        DagsterUserCodeUnreachableError("gRPC Error code: INTERNAL"),
        ValueError("bad sensor"),
    ):
        workspace, context, sensor = evaluation_arguments()
        with (
            patch(FETCH, side_effect=error) as fetch,
            patch.object(daemon_sensor, "_get_code_location_for_sensor") as get_location,
            retries.sensor_unavailable_skip(Mock()),
        ):
            get_location.side_effect = lambda *_args: location()
            with pytest.raises(type(error)) as raised:
                evaluate(workspace, context, sensor)
        assert raised.value is error
        assert fetch.call_count == 1
        context.update_state.assert_not_called()


def test_unavailable_before_the_evaluation_call_is_not_skipped() -> None:
    workspace, context, sensor = evaluation_arguments()
    error = unavailable()
    with (
        patch.object(daemon_sensor, "_get_code_location_for_sensor", side_effect=error),
        retries.sensor_unavailable_skip(Mock()),
        pytest.raises(DagsterUserCodeUnreachableError) as raised,
    ):
        evaluate(workspace, context, sensor)
    assert raised.value is error
    context.update_state.assert_not_called()


def test_launch_failure_after_successful_evaluation_propagates() -> None:
    workspace, context, sensor = evaluation_arguments()
    data = SimpleNamespace(
        log_key=None,
        asset_events=[],
        dynamic_partitions_requests=None,
        run_requests=[Mock()],
        automation_condition_evaluations=None,
        cursor="next",
    )
    launch_error = unavailable()
    with (
        patch(FETCH, return_value=data) as fetch,
        patch.object(daemon_sensor, "_get_code_location_for_sensor") as get_location,
        patch.object(
            daemon_sensor,
            "_handle_run_requests_and_automation_condition_evaluations",
            side_effect=launch_error,
        ),
        retries.sensor_unavailable_skip(Mock()),
    ):
        get_location.side_effect = lambda *_args: location()
        with pytest.raises(DagsterUserCodeUnreachableError) as raised:
            evaluate(workspace, context, sensor)
    assert raised.value is launch_error
    assert not isinstance(raised.value, retries.SensorUserCodeUnavailableError)
    assert fetch.call_count == 1
    context.update_state.assert_not_called()


def test_successful_evaluation_is_unchanged_and_methods_are_restored() -> None:
    original = (GrpcServerCodeLocation.get_sensor_execution_data, daemon_sensor._evaluate_sensor)
    workspace, context, sensor = evaluation_arguments()
    data = SimpleNamespace(
        log_key=None,
        asset_events=[],
        dynamic_partitions_requests=None,
        run_requests=[],
        automation_condition_evaluations=None,
        dagster_run_reactions=[],
        skip_message=None,
        cursor="next",
    )
    with (
        patch(FETCH, return_value=data),
        patch.object(daemon_sensor, "_get_code_location_for_sensor") as get_location,
        retries.sensor_unavailable_skip(Mock()),
    ):
        get_location.side_effect = lambda *_args: location()
        assert (
            GrpcServerCodeLocation.get_sensor_execution_data,
            daemon_sensor._evaluate_sensor,
        ) != original
        assert evaluate(workspace, context, sensor) == [None, None]
    context.update_state.assert_called_once_with(TickStatus.SKIPPED, cursor="next")
    assert (
        GrpcServerCodeLocation.get_sensor_execution_data,
        daemon_sensor._evaluate_sensor,
    ) == original
