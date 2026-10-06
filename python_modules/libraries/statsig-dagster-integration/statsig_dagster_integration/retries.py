import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import wraps
from typing import TypeVar

import grpc
from dagster._core.remote_origin import CodeLocationOrigin
from dagster._core.workspace.context import CodeLocationEntry, WorkspaceProcessContext
from dagster._grpc.client import DEFAULT_GRPC_TIMEOUT, DagsterGrpcClient
from dagster._utils.error import SerializableErrorInfo
from google.protobuf.message import Message

from statsig_dagster_integration.compat import require_supported_versions

T = TypeVar("T")
RPC_DELAYS = (2.0, 8.0)
WORKSPACE_DELAYS = (2.0,)
_ACTIVE = False


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
    """Adapt two pinned methods; never retry run submission or sensor evaluation."""
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
