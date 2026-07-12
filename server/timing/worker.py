"""Command-line entry point for the independent live timing ingest worker."""

from __future__ import annotations

import asyncio
import logging
import signal

from .ingest import TimingIngestSupervisor
from .logging_json import configure_json_logging
from .normalizer_writer import TimingNormalizerRegistry
from .operations import OperationalMonitor
from .worker_heartbeat import WorkerHeartbeat


async def run() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop_event.set)
    supervisor = TimingIngestSupervisor(frame_processor=TimingNormalizerRegistry())
    heartbeat = WorkerHeartbeat()
    monitor = OperationalMonitor()
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(supervisor.run_forever(stop_event=stop_event), name="timing-supervisor")
        tasks.create_task(
            heartbeat.run(stop_event, active_session_count=lambda: len(supervisor.active_sessions())),
            name="timing-heartbeat",
        )
        tasks.create_task(monitor.run(stop_event), name="timing-operations")


def main() -> int:
    configure_json_logging(logging.INFO)
    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
