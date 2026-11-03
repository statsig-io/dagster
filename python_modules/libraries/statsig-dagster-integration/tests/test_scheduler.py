import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import yaml
from dagster import DagsterInstance
from dagster._core.definitions.run_request import InstigatorType
from dagster._core.remote_origin import (
    RegisteredCodeLocationOrigin,
    RemoteInstigatorOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    ScheduleInstigatorData,
    TickData,
    TickStatus,
)
from dagster._scheduler.scheduler import _ScheduleLaunchContext

from statsig_dagster_integration.storage import daemon_pools

if TYPE_CHECKING:
    from dagster._core.remote_representation.external import RemoteSchedule


def test_real_concurrent_schedule_tick_writes_and_reopen(postgres_url: str, tmp_path: Path) -> None:
    config = {
        name: {"module": module, "class": cls, "config": {"postgres_url": postgres_url}}
        for name, module, cls in (
            ("run_storage", "dagster_postgres.run_storage", "PostgresRunStorage"),
            (
                "event_log_storage",
                "dagster_postgres.event_log",
                "PostgresEventLogStorage",
            ),
            (
                "schedule_storage",
                "dagster_postgres.schedule_storage",
                "PostgresScheduleStorage",
            ),
        )
    }
    (tmp_path / "dagster.yaml").write_text(yaml.safe_dump(config))
    location = RegisteredCodeLocationOrigin("synthetic")
    repo = RemoteRepositoryOrigin(location, "fixture")
    logger = logging.getLogger("synthetic-scheduler")
    ready = threading.Barrier(4, timeout=10)
    saved: list[tuple[RemoteInstigatorOrigin, str, int]] = []
    with daemon_pools():
        with DagsterInstance.from_config(str(tmp_path)) as instance:
            for index in range(3):
                origin = RemoteInstigatorOrigin(repo, f"daemon_schedule_{index}")
                state = instance.add_instigator_state(
                    InstigatorState(
                        origin,
                        InstigatorType.SCHEDULE,
                        InstigatorStatus.RUNNING,
                        ScheduleInstigatorData("* * * * *", 100.0),
                    )
                )
                tick = instance.create_tick(
                    TickData(
                        instigator_origin_id=origin.get_id(),
                        instigator_name=origin.instigator_name,
                        instigator_type=InstigatorType.SCHEDULE,
                        status=TickStatus.STARTED,
                        timestamp=100.0,
                        selector_id=state.selector_id,
                    )
                )
                saved.append((origin, state.selector_id, tick.tick_id))

            def write(index: int) -> None:
                origin, selector, tick_id = saved[index]
                remote = SimpleNamespace(
                    name=origin.instigator_name,
                    selector_id=selector,
                    handle=SimpleNamespace(repository_name="fixture"),
                )
                tick = next(
                    tick
                    for tick in instance.get_ticks(origin.get_id(), selector)
                    if tick.tick_id == tick_id
                )
                ready.wait()
                with _ScheduleLaunchContext(
                    cast("RemoteSchedule", remote), tick, instance, logger, {}
                ) as context:
                    context.add_run_info(
                        run_id=f"synthetic-run-{index}",
                        run_key=f"synthetic-key-{index}",
                    )
                    context.update_state(TickStatus.SUCCESS)

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(write, index) for index in range(3)]
                ready.wait()
                for future in futures:
                    future.result(timeout=10)

        with DagsterInstance.from_config(str(tmp_path)) as reopened:
            for index, (origin, selector, tick_id) in enumerate(saved):
                tick = next(
                    tick
                    for tick in reopened.get_ticks(origin.get_id(), selector)
                    if tick.tick_id == tick_id
                )
                assert tick.status == TickStatus.SUCCESS
                assert tick.run_ids == [f"synthetic-run-{index}"]
                assert tick.run_keys == [f"synthetic-key-{index}"]
                persisted_state = reopened.get_instigator_state(origin.get_id(), selector)
                assert persisted_state is not None
                data = persisted_state.instigator_data
                assert isinstance(data, ScheduleInstigatorData)
                assert data.start_timestamp == 100.0
            origin, selector, _ = saved[0]
            interrupted = reopened.create_tick(
                TickData(
                    instigator_origin_id=origin.get_id(),
                    instigator_name=origin.instigator_name,
                    instigator_type=InstigatorType.SCHEDULE,
                    status=TickStatus.STARTED,
                    timestamp=200.0,
                    selector_id=selector,
                )
            )
        with DagsterInstance.from_config(str(tmp_path)) as restarted:
            persisted = next(
                tick
                for tick in restarted.get_ticks(origin.get_id(), selector)
                if tick.tick_id == interrupted.tick_id
            )
            remote = SimpleNamespace(name=origin.instigator_name)
            with _ScheduleLaunchContext(
                cast("RemoteSchedule", remote), persisted, restarted, logger, {}
            ) as context:
                context.update_state(TickStatus.SUCCESS)
            assert len(restarted.get_ticks(origin.get_id(), selector)) == 2
            assert restarted.get_ticks(origin.get_id(), selector)[0].tick_id == interrupted.tick_id
