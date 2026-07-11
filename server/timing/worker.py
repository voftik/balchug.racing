"""Command-line entry point for the independent live timing ingest worker."""

from __future__ import annotations

import asyncio
import logging
import signal

from .ingest import TimingIngestSupervisor
from .normalizer_writer import TimingNormalizerRegistry
from .worker_heartbeat import WorkerHeartbeat


async def run() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop_event.set)
    supervisor = TimingIngestSupervisor(frame_processor=TimingNormalizerRegistry())
    heartbeat = WorkerHeartbeat()
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(supervisor.run_forever(stop_event=stop_event), name="timing-supervisor")
        tasks.create_task(
            heartbeat.run(stop_event, active_session_count=lambda: len(supervisor.active_sessions())),
            name="timing-heartbeat",
        )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
