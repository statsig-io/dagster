import logging
import threading
import tracemalloc
from collections.abc import Iterator
from contextlib import contextmanager

import googlecloudprofiler

LOGGER = logging.getLogger(__name__)


@contextmanager
def daemon_telemetry(
    *,
    profiler_enabled: bool = False,
    tracemalloc_enabled: bool = False,
    deployment: str = "UNSET",
    interval_seconds: float = 60.0,
) -> Iterator[None]:
    if interval_seconds <= 0:
        raise ValueError("Tracemalloc interval must be positive")
    if profiler_enabled:
        try:
            googlecloudprofiler.start(
                service="dagster-daemon", service_version=deployment, verbose=3
            )
        except (ValueError, NotImplementedError):
            LOGGER.exception("Daemon profiler could not start")

    stop = threading.Event()
    thread: threading.Thread | None = None
    owns_tracing = tracemalloc_enabled and not tracemalloc.is_tracing()

    def dump() -> None:
        while not stop.wait(interval_seconds):
            try:
                LOGGER.info(
                    "Daemon memory allocation: %s",
                    tracemalloc.take_snapshot().statistics("lineno")[:50],
                )
            except Exception:
                LOGGER.exception("Daemon memory snapshot failed")

    try:
        if tracemalloc_enabled:
            if owns_tracing:
                tracemalloc.start()
            thread = threading.Thread(target=dump, name="dagster-tracemalloc", daemon=True)
            thread.start()
        yield
    finally:
        stop.set()
        if thread is not None and thread.ident is not None:
            thread.join(timeout=5.0)
            if thread.is_alive():
                LOGGER.error("Daemon memory snapshot thread did not stop within 5 seconds")
        if owns_tracing:
            tracemalloc.stop()
