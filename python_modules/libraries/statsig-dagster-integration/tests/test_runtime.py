from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import grpc
import pytest
from dagster import DagsterInstance
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.remote_origin import GrpcServerCodeLocationOrigin
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._grpc.__generated__ import dagster_api_pb2
from dagster._grpc.client import DEFAULT_GRPC_TIMEOUT, DagsterGrpcClient
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool, QueuePool

from statsig_dagster_integration import compat, retries, storage


class RpcFailure(grpc.RpcError):
    def __init__(self, status: grpc.StatusCode) -> None:
        self.status = status

    def code(self) -> grpc.StatusCode:
        return self.status


@pytest.mark.parametrize("status", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INVALID_ARGUMENT])
def test_actual_list_repositories_retry_and_timeout(status: grpc.StatusCode) -> None:
    client = DagsterGrpcClient(port=1)
    response = SimpleNamespace(serialized_list_repositories_response_or_error="synthetic")
    sleep = Mock()
    with (
        patch.object(client, "_get_response", side_effect=[RpcFailure(status), response]) as query,
        retries.workspace_retries(sleep),
    ):
        if status == grpc.StatusCode.UNAVAILABLE:
            assert client.list_repositories() == "synthetic"
            assert query.call_count == 2
            sleep.assert_called_once_with(2.0)
        else:
            with pytest.raises(DagsterUserCodeUnreachableError):
                client.list_repositories()
            assert query.call_count == 1
            sleep.assert_not_called()
    assert query.call_args.kwargs["timeout"] == DEFAULT_GRPC_TIMEOUT


def test_rpc_exhaustion_and_unrelated_rpc_not_retried() -> None:
    client = DagsterGrpcClient(port=1)
    sleep = Mock()
    with (
        patch.object(
            client, "_get_response", side_effect=RpcFailure(grpc.StatusCode.UNAVAILABLE)
        ) as query,
        retries.workspace_retries(sleep),
    ):
        with pytest.raises(DagsterUserCodeUnreachableError):
            client.list_repositories()
        assert query.call_count == 3
        assert [call.args[0] for call in sleep.call_args_list] == [2.0, 8.0]
        query.reset_mock()
        with pytest.raises(DagsterUserCodeUnreachableError):
            client.ping("synthetic")
        assert query.call_count == 1


def test_actual_workspace_serialization_retry_and_permanent_error() -> None:
    origin = GrpcServerCodeLocationOrigin(host="synthetic.invalid", port=1)
    sleep = Mock()

    def unavailable(_instance: DagsterInstance) -> None:
        raise DagsterUserCodeUnreachableError("gRPC Error code: UNAVAILABLE") from RpcFailure(
            grpc.StatusCode.UNAVAILABLE
        )

    with (
        DagsterInstance.local_temp() as instance,
        WorkspaceProcessContext(instance, None) as workspace,
    ):
        for error, attempts, cls_name in (
            (unavailable, 2, DagsterUserCodeUnreachableError.__name__),
            (ValueError("bad definitions"), 1, "ValueError"),
        ):
            with patch.object(
                GrpcServerCodeLocationOrigin, "create_location", side_effect=error
            ) as create:
                with retries.workspace_retries(sleep), pytest.warns(UserWarning):
                    entry = workspace._load_location(origin, reload=False)
                assert entry.load_error is not None
                assert entry.code_location is None
                assert entry.load_error.cls_name == cls_name
                assert create.call_count == attempts


def test_adapters_restore_methods_and_reject_unknown_version() -> None:
    query = DagsterGrpcClient._query
    with pytest.raises(KeyboardInterrupt), retries.workspace_retries(Mock()):
        raise KeyboardInterrupt
    assert DagsterGrpcClient._query is query
    with (
        patch.object(compat, "version", return_value="1.4.16"),
        pytest.raises(RuntimeError),
        retries.workspace_retries(Mock()),
    ):
        pass
    assert DagsterGrpcClient._query is query


STORAGES = (
    (storage.event_log, storage.event_log.PostgresEventLogStorage),
    (storage.run_storage, storage.run_storage.PostgresRunStorage),
    (storage.schedule_storage, storage.schedule_storage.PostgresScheduleStorage),
)


def test_explicit_rpc_timeout_is_preserved() -> None:
    client = DagsterGrpcClient(port=1)
    response = SimpleNamespace(serialized_list_repositories_response_or_error="synthetic")
    with (
        patch.object(client, "_get_response", return_value=response) as request,
        retries.workspace_retries(Mock()),
    ):
        client._query("ListRepositories", dagster_api_pb2.ListRepositoriesRequest, timeout=3600)
    assert request.call_args.kwargs["timeout"] == 3600


def test_nested_adapter_install_is_idempotent() -> None:
    with retries.workspace_retries(Mock()), storage.daemon_pools():
        query, initialize = (
            DagsterGrpcClient._query,
            storage.run_storage.PostgresRunStorage.__init__,
        )
        with retries.workspace_retries(Mock()), storage.daemon_pools():
            assert DagsterGrpcClient._query is query
            assert storage.run_storage.PostgresRunStorage.__init__ is initialize
        assert DagsterGrpcClient._query is query
        with (
            pytest.raises(RuntimeError),
            storage.daemon_pools(storage.PoolSettings(max_overflow=0)),
        ):
            pass


@pytest.mark.parametrize("module,cls", STORAGES)
def test_constructor_optimization_disposal_and_config_preservation(
    module: ModuleType, cls: type[storage.PostgresStorage]
) -> None:
    engines = [Mock(), Mock()]
    for engine in engines:
        engine.url = make_url("postgresql://synthetic@/fixture?options=-capplication_name=fixture")
    with (
        patch.object(module, "create_pg_engine", side_effect=engines) as create,
        storage.daemon_pools(),
        patch.object(module.event, "listen"),
    ):
        store = cls(
            "postgresql://synthetic@/fixture?options=-capplication_name=fixture",
            should_autocreate_tables=False,
        )
        options = create.call_args.kwargs
        assert options["poolclass"] is QueuePool
        assert (
            options["pool_size"],
            options["pool_recycle"],
            options["max_overflow"],
            options["pool_timeout"],
        ) == (1, 3600, 10, 30.0)
        assert (
            options["connect_args"]["options"]
            == "-capplication_name=fixture -c statement_timeout=600000"
        )
        store.optimize_for_webserver(1234, 90, 2)
        engines[0].dispose.assert_called_once()
        assert create.call_args.kwargs["max_overflow"] == 2
        assert create.call_args.kwargs["pool_recycle"] == 90
        store.dispose()
        engines[1].dispose.assert_called_once()


@pytest.mark.parametrize("module,cls", STORAGES)
def test_constructor_failure_disposes_engine(module: object, cls: type) -> None:
    engine = Mock()
    with (
        patch.object(module, "create_pg_engine", return_value=engine),
        storage.daemon_pools(),
        patch.object(
            module,
            "retry_pg_connection_fn",
            side_effect=ValueError("synthetic failure"),
        ),
        pytest.raises(ValueError),
    ):
        cls("postgresql://synthetic@/fixture")
    engine.dispose.assert_called_once()


@pytest.mark.parametrize("module,cls", STORAGES)
def test_optimization_failure_restores_old_engine(
    module: ModuleType, cls: type[storage.PostgresStorage]
) -> None:
    old, new = (Mock(), Mock())
    old.url = make_url("postgresql://synthetic@/fixture")
    with (
        patch.object(module, "create_pg_engine", side_effect=[old, new]),
        storage.daemon_pools(),
        patch.object(module.event, "listen", side_effect=ValueError("listener failure")),
    ):
        store = cls("postgresql://synthetic@/fixture", should_autocreate_tables=False)
        with pytest.raises(ValueError):
            store.optimize_for_webserver(1234, 90, 2)
        assert store._engine is old
        new.dispose.assert_called_once()
        old.dispose.assert_not_called()
        store.dispose()
    old.dispose.assert_called_once()


@pytest.mark.parametrize("module,cls", STORAGES)
def test_clean_storage_timeout_and_temporary_engine_disposal(
    module: ModuleType, cls: type[storage.PostgresStorage]
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
    temporary, final = (Mock(), Mock())
    with (
        patch.object(module, "create_engine", return_value=temporary) as create,
        patch.object(module, "create_pg_engine", return_value=final),
        storage.daemon_pools(),
        patch.object(metadata, "drop_all"),
    ):
        store = cls.create_clean_storage(
            "postgresql://synthetic@/fixture", should_autocreate_tables=False
        )
        assert create.call_args.kwargs["poolclass"] is NullPool
        assert "statement_timeout=600000" in create.call_args.kwargs["connect_args"]["options"]
        temporary.dispose.assert_called_once()
        store.dispose()
    final.dispose.assert_called_once()
