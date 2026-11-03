"""Real loopback recovery through the daemon and webserver production constructors."""

import json
import sys
import threading
import time
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import grpc
import pytest
from click.testing import CliRunner
from dagster import DagsterInstance
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.remote_origin import GrpcServerCodeLocationOrigin
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import GrpcServerTarget
from dagster._daemon.controller import daemon_controller_from_instance
from dagster._grpc.__generated__ import dagster_api_pb2
from dagster._grpc.client import DEFAULT_GRPC_TIMEOUT, DagsterGrpcClient
from dagster._grpc.server import GrpcServerProcess
from dagster_webserver import cli as webserver_cli
from dagster_webserver.app import create_app_from_workspace_process_context

from statsig_dagster_integration import retries
from tests.test_runtime import RpcFailure


@pytest.mark.parametrize("component", ["daemon", "webserver"])
@pytest.mark.parametrize("virtual_backoff", [False, True], ids=["real-backoff", "virtual-backoff"])
def test_real_server_outage_and_watcher_recovery(
    component: str, virtual_backoff: bool, tmp_path: Path
) -> None:
    """Virtual backoff isolates periodic recovery; real backoff proves ordinary outage recovery.

    Only the adapter's supported sleep dependency is injected. Both cases use the real watcher,
    real RPCs, and the same TCP endpoint and server ID after restart. No reload occurs after restart.
    """
    definitions = tmp_path / "definitions.py"
    definitions.write_text(
        "from dagster import repository\n@repository\ndef recovery_repo():\n    return []\n"
    )
    main_thread = threading.get_ident()
    failed_loads = []
    rpc_calls: list[dict[str, object]] = []
    sleeps: list[float] = []
    refresh_callers: list[str] = []
    events: list[str] = []
    watcher_ready = threading.Event()
    measuring_outage = False
    original_load = WorkspaceProcessContext._load_location  # noqa: SLF001 -- Instrument Dagster's pinned loader.
    original_rpc = DagsterGrpcClient._get_response  # noqa: SLF001 -- Instrument the RPC boundary.
    original_refresh = WorkspaceProcessContext.refresh_code_location
    original_event = WorkspaceProcessContext._send_state_event_to_subscribers  # noqa: SLF001 -- Observe watcher events.

    def sleep(delay: float) -> None:
        if measuring_outage and threading.get_ident() == main_thread:
            sleeps.append(delay)
        if not virtual_backoff:
            time.sleep(delay)

    def load(self, origin, reload):
        entry = original_load(self, origin, reload)
        if measuring_outage and threading.get_ident() == main_thread:
            failed_loads.append(entry)
        return entry

    def rpc(self, method, request, timeout=DEFAULT_GRPC_TIMEOUT):
        started = time.monotonic()
        try:
            result = original_rpc(self, method, request, timeout=timeout)
            if method == "GetServerId" and threading.current_thread().name == "grpc-server-watch":
                watcher_ready.set()
            return result
        finally:
            if measuring_outage and threading.get_ident() == main_thread:
                rpc_calls.append(
                    {"method": method, "timeout": timeout, "elapsed": time.monotonic() - started}
                )

    def refresh(self, name):
        refresh_callers.append(sys._getframe(1).f_code.co_name)  # noqa: SLF001 -- Capture the production caller.
        return original_refresh(self, name)

    def event(self, state_event):
        events.append(state_event.event_type.name)
        return original_event(self, state_event)

    def exercise(workspace: WorkspaceProcessContext, server: GrpcServerProcess) -> None:
        nonlocal measuring_outage
        assert WorkspaceProcessContext._load_location.__wrapped__ is load  # noqa: SLF001 -- Verify the retry wrapper retains the observing SDK loader.
        assert retries._ACTIVE  # noqa: SLF001 -- Confirm adapter context is active during the regression.
        origin = workspace.workspace_load_target.create_origins()[0]
        name = origin.location_name
        assert workspace.get_current_workspace().code_location_entries[name].load_error is None
        assert watcher_ready.wait(5), "Real watcher did not initialize"
        initial_port = server.port
        assert server.create_client().get_server_id() == "recovery-fixed-id"
        server.server_process.terminate()
        server.server_process.wait(timeout=5)
        measuring_outage = True
        started = time.monotonic()
        # Production refresh calls the wrapped _load_location and stores its serialized error.
        with pytest.warns(UserWarning):
            workspace.refresh_code_location(name)
        exhaustion_seconds = time.monotonic() - started
        measuring_outage = False
        assert len(failed_loads) == 2
        assert all(
            entry.load_error.cls_name == DagsterUserCodeUnreachableError.__name__
            and entry.code_location is None
            for entry in failed_loads
        )
        entry = workspace.get_current_workspace().code_location_entries[name]
        assert entry.load_error is not None
        assert entry.load_error.cls_name == DagsterUserCodeUnreachableError.__name__
        repository_calls = [call for call in rpc_calls if call["method"] == "ListRepositories"]
        assert len(repository_calls) == 6
        assert all(call["timeout"] == DEFAULT_GRPC_TIMEOUT for call in repository_calls)
        assert sleeps == [2.0, 8.0, 2.0, 2.0, 8.0]
        if not virtual_backoff:
            assert exhaustion_seconds >= sum(sleeps)
            assert exhaustion_seconds < 60
        # Independent watcher recovery requires reconnect before its ten failed-poll budget.
        restarted = time.monotonic()
        with patch.object(
            workspace,
            "reload_workspace",
            side_effect=AssertionError("Manual workspace reload must not rescue recovery"),
        ):
            server.start_server_process()
            assert server.port == initial_port
            assert server.create_client().get_server_id() == "recovery-fixed-id"
            deadline = restarted + 30
            while (
                workspace.get_current_workspace().code_location_entries[name].load_error is not None
            ):
                assert time.monotonic() < deadline, (
                    "Automatic recovery did not clear the error in 30s"
                )
                time.sleep(0.1)
            while not virtual_backoff and "LOCATION_UPDATED" not in events:
                assert time.monotonic() < deadline, "Watcher did not publish location update in 30s"
                time.sleep(0.1)
            recovery_seconds = time.monotonic() - restarted
        if virtual_backoff:
            assert "attempt_error_recovery" in refresh_callers
            assert "LOCATION_UPDATED" not in events
            assert "LOCATION_ERROR" not in events
        else:
            assert "LOCATION_UPDATED" in events
        assert (
            workspace.get_current_workspace().code_location_entries[name].code_location is not None
        )
        print(  # noqa: T201 -- raw regression timing/timeout receipt.
            json.dumps(
                {
                    "component": component,
                    "virtual_backoff": virtual_backoff,
                    "rpc_calls": rpc_calls,
                    "workspace_attempts": len(failed_loads),
                    "backoff_seconds": sum(sleeps),
                    "exhaustion_seconds": exhaustion_seconds,
                    "recovery_seconds": recovery_seconds,
                    "refresh_callers": refresh_callers,
                    "events": events,
                    "list_repositories_timeout_bound_seconds": 6 * DEFAULT_GRPC_TIMEOUT + 22,
                    "configured3600_timeout_bound_seconds": 21622,
                }
            )
        )

    with (
        DagsterInstance.local_temp(overrides={"telemetry": {"enabled": False}}) as instance,
        GrpcServerProcess(
            instance_ref=instance.get_ref(),
            loadable_target_origin=LoadableTargetOrigin(python_file=str(definitions)),
            force_port=True,
            fixed_server_id="recovery-fixed-id",
            wait_on_exit=True,
        ) as server,
        patch.object(WorkspaceProcessContext, "_load_location", load),
        patch.object(DagsterGrpcClient, "_get_response", rpc),
        patch.object(WorkspaceProcessContext, "refresh_code_location", refresh),
        patch.object(WorkspaceProcessContext, "_send_state_event_to_subscribers", event),
        retries.workspace_retries(sleep),
    ):
        if component == "daemon":
            target = GrpcServerTarget(
                host="localhost", port=server.port, socket=None, location_name="recovery"
            )
            with daemon_controller_from_instance(instance, target) as controller:
                workspace = controller._workspace_process_context  # noqa: SLF001 -- Inspect daemon-owned workspace.
                assert workspace._grpc_server_registry is controller._grpc_server_registry  # noqa: SLF001 -- Verify shared registry ownership.
                exercise(workspace, server)
        else:

            def host_ui(workspace, *_args) -> None:
                with patch("dagster_webserver.app.log_workspace_stats"):
                    assert create_app_from_workspace_process_context(workspace) is not None
                assert workspace.version == "1.13.25"
                exercise(workspace, server)

            with (
                patch.object(
                    webserver_cli,
                    "get_possibly_temporary_instance_for_cli",
                    return_value=nullcontext(instance),
                ),
                patch.object(webserver_cli, "setup_interrupt_handlers"),
                patch.object(
                    webserver_cli, "host_dagster_ui_with_workspace_process_context", host_ui
                ),
            ):
                result = CliRunner().invoke(
                    webserver_cli.dagster_webserver, ["--grpc-port", str(server.port)]
                )
                assert result.exit_code == 0, result.output
                print(result.output)  # noqa: T201 -- preserve Click-captured recovery receipt.


@pytest.mark.parametrize(
    ("timeout", "expected_bound"),
    [(3600, 21622), (60, 382)],
    ids=["configured-3600", "default-60"],
)
def test_serialized_workspace_failure_preserves_list_repositories_timeout_budget(
    timeout: int, expected_bound: int
) -> None:
    origin = GrpcServerCodeLocationOrigin(host="synthetic.invalid", port=1)
    request_timeouts: list[int] = []
    sleeps: list[float] = []

    def unavailable(_method, request, timeout=DEFAULT_GRPC_TIMEOUT):
        assert isinstance(request, dagster_api_pb2.ListRepositoriesRequest)
        request_timeouts.append(timeout)
        raise RpcFailure(grpc.StatusCode.UNAVAILABLE)

    def sleep(delay: float) -> None:
        sleeps.append(delay)

    def list_repositories(client: DagsterGrpcClient) -> object:
        return client._query(  # noqa: SLF001 -- Exercise the explicit timeout caller contract.
            "ListRepositories", dagster_api_pb2.ListRepositoriesRequest, timeout=timeout
        )

    with (
        DagsterInstance.local_temp() as instance,
        WorkspaceProcessContext(instance, None) as workspace,
        patch.object(DagsterGrpcClient, "_get_response", side_effect=unavailable),
        patch.object(DagsterGrpcClient, "list_repositories", list_repositories),
        retries.workspace_retries(sleep),
        pytest.warns(UserWarning),
    ):
        entry = workspace._load_location(origin, reload=False)  # noqa: SLF001 -- Exercise serialized location loading.

    assert len(request_timeouts) == 6
    assert request_timeouts == [timeout] * 6
    assert sleeps == [2.0, 8.0, 2.0, 2.0, 8.0]
    assert entry.code_location is None
    assert entry.load_error is not None
    assert entry.load_error.cls_name == DagsterUserCodeUnreachableError.__name__
    assert 6 * timeout + sum(sleeps) == expected_bound
