import time
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from functools import wraps
from typing import TypeVar

import grpc
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.instance import DagsterInstance
from dagster._core.remote_origin import CodeLocationOrigin
from dagster._core.remote_representation.code_location import GrpcServerCodeLocation
from dagster._core.remote_representation.external import RemoteSensor
from dagster._core.scheduler.instigation import SensorInstigatorData, TickStatus
from dagster._core.workspace.context import (
    CodeLocationEntry,
    IWorkspaceProcessContext,
    WorkspaceProcessContext,
)
from dagster._daemon import sensor as daemon_sensor
from dagster._grpc.client import DEFAULT_GRPC_TIMEOUT, DagsterGrpcClient
from dagster._utils.error import SerializableErrorInfo
from google.protobuf.message import Message

from statsig_dagster_integration.compat import require_supported_versions

T = TypeVar("T")
RPC_DELAYS = (2.0, 8.0)
WORKSPACE_DELAYS = (2.0,)
SENSOR_UNAVAILABLE_ATTEMPTS = 3
SENSOR_UNAVAILABLE_DELAYS = (2.0, 4.0)
SENSOR_UNAVAILABLE_SKIP_REASON = (
    "Unable to reach the user code server. Sensor will resume once the server is available."
)
_ACTIVE = False
_SENSOR_ACTIVE = False


def _replace_pinned_method(owner: type, name: str, method: object) -> None:
    """Replace and restore methods only inside the version-guarded adapter below."""
    setattr(owner, name, method)


def is_unavailable(error: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, grpc.RpcError):
            return current.code() == grpc.StatusCode.UNAVAILABLE
        current = current.__cause__
    return False


def serialized_unavailable(error: SerializableErrorInfo | None) -> bool:
    if error is None:
        return False
    return error.cls_name == "DagsterUserCodeUnreachableError" and (
        "gRPC Error code: UNAVAILABLE" in error.message
    )


def retry_unavailable(operation: Callable[[], T], sleep: Callable[[float], None]) -> T:
    for attempt in range(len(RPC_DELAYS) + 1):
        try:
            return operation()
        except Exception as error:
            if not is_unavailable(error) or attempt == len(RPC_DELAYS):
                raise
            sleep(RPC_DELAYS[attempt])
    raise AssertionError("unreachable")


@contextmanager
def workspace_retries(sleep: Callable[[float], None] = time.sleep) -> Iterator[None]:
    """Adapt two pinned methods; never retry run submission or sensor/schedule evaluation."""
    require_supported_versions()
    global _ACTIVE  # noqa: PLW0603 -- paired restoration tracks the process-local adapter context.
    if _ACTIVE:
        yield
        return
    original_query = DagsterGrpcClient._query
    original_load = WorkspaceProcessContext._load_location

    @wraps(original_query)
    def query(
        self: DagsterGrpcClient,
        method: str,
        request_type: type[Message],
        timeout: int = DEFAULT_GRPC_TIMEOUT,
        custom_timeout_message: str | None = None,
        **kwargs: object,
    ) -> Message:
        def operation() -> Message:
            return original_query(
                self,
                method,
                request_type,
                timeout=timeout,
                custom_timeout_message=custom_timeout_message,
                **kwargs,
            )

        return retry_unavailable(operation, sleep) if method == "ListRepositories" else operation()

    @wraps(original_load)
    def load(
        self: WorkspaceProcessContext, origin: CodeLocationOrigin, reload: bool
    ) -> CodeLocationEntry:
        entry = original_load(self, origin, reload)
        for delay in WORKSPACE_DELAYS:
            if not serialized_unavailable(entry.load_error):
                break
            sleep(delay)
            entry = original_load(self, origin, reload)
        return entry

    _replace_pinned_method(DagsterGrpcClient, "_query", query)
    _replace_pinned_method(WorkspaceProcessContext, "_load_location", load)
    _ACTIVE = True
    try:
        yield
    finally:
        _replace_pinned_method(WorkspaceProcessContext, "_load_location", original_load)
        _replace_pinned_method(DagsterGrpcClient, "_query", original_query)
        _ACTIVE = False


class SensorUserCodeUnavailableError(DagsterUserCodeUnreachableError):
    """Sensor evaluation still saw UNAVAILABLE after its bounded retries."""


def sensor_unavailable(error: BaseException) -> bool:
    return isinstance(error, DagsterUserCodeUnreachableError) and "UNAVAILABLE" in str(error)


def _skip_unavailable_tick(
    instance: DagsterInstance, context: daemon_sensor.SensorLaunchContext, sensor: RemoteSensor
) -> None:
    context.logger.warning(
        "Unable to reach the user code server for sensor %s. "
        "Sensor will resume once the server is available.",
        sensor.name,
    )
    state = instance.get_instigator_state(sensor.get_remote_origin_id(), sensor.selector_id)
    data = state.instigator_data if state else None
    cursor = data.cursor if isinstance(data, SensorInstigatorData) else None
    # A fresh tick has no cursor and upstream writes the tick cursor on SKIPPED, so pass it.
    context.update_state(
        TickStatus.SKIPPED, skip_reason=SENSOR_UNAVAILABLE_SKIP_REASON, cursor=cursor
    )


@contextmanager
def sensor_unavailable_skip(sleep: Callable[[float], None] = time.sleep) -> Iterator[None]:
    """Retry only the sensor evaluation RPC on UNAVAILABLE; skip the tick, keeping its cursor.

    Two pinned hooks: the gRPC code location's evaluation call retries three times (2s, 4s) and
    marks exhaustion with a subclass; the daemon's evaluation generator turns only that marked
    error, raised before its first yield, into a SKIPPED tick. Everything later, such as run
    launch, and every other error propagate unchanged.
    """
    require_supported_versions()
    global _SENSOR_ACTIVE  # noqa: PLW0603 -- paired restoration tracks the process-local adapter context.
    if _SENSOR_ACTIVE:
        yield
        return
    original_fetch = GrpcServerCodeLocation.get_sensor_execution_data
    original_evaluate = daemon_sensor._evaluate_sensor

    @wraps(original_fetch)
    def fetch(self: GrpcServerCodeLocation, *args: object, **kwargs: object) -> object:
        for attempt in range(SENSOR_UNAVAILABLE_ATTEMPTS):
            try:
                return original_fetch(self, *args, **kwargs)
            except DagsterUserCodeUnreachableError as error:
                if not sensor_unavailable(error):
                    raise
                if attempt == SENSOR_UNAVAILABLE_ATTEMPTS - 1:
                    raise SensorUserCodeUnavailableError(str(error)) from error
                sleep(SENSOR_UNAVAILABLE_DELAYS[attempt])
        raise AssertionError("unreachable")

    @wraps(original_evaluate)
    def evaluate(
        workspace_process_context: IWorkspaceProcessContext,
        context: daemon_sensor.SensorLaunchContext,
        remote_sensor: RemoteSensor,
        *args: object,
        **kwargs: object,
    ) -> Generator[None, None, None]:
        generator = original_evaluate(
            workspace_process_context, context, remote_sensor, *args, **kwargs
        )
        try:
            try:
                first = next(generator)
            except StopIteration:
                return
            except SensorUserCodeUnavailableError:
                _skip_unavailable_tick(workspace_process_context.instance, context, remote_sensor)
                yield
                return
            yield first
            yield from generator
        finally:
            generator.close()

    _replace_pinned_method(GrpcServerCodeLocation, "get_sensor_execution_data", fetch)
    _replace_pinned_method(daemon_sensor, "_evaluate_sensor", evaluate)
    _SENSOR_ACTIVE = True
    try:
        yield
    finally:
        _replace_pinned_method(daemon_sensor, "_evaluate_sensor", original_evaluate)
        _replace_pinned_method(GrpcServerCodeLocation, "get_sensor_execution_data", original_fetch)
        _SENSOR_ACTIVE = False
