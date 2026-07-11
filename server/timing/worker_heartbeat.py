"""Durable and systemd-visible liveness for the timing ingest worker."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .config import now_us
from .db import connect
from .systemd_notify import notify, watchdog_interval_s


WORKER_KIND = "timing-ingest"


@dataclass(frozen=True)
class WorkerHeartbeatSettings:
    database_interval_s: float = 1.0


def write_worker_heartbeat(
    connection: sqlite3.Connection,
    *,
    instance_id: str,
    state: str,
    active_session_count: int,
    observed_at_us: int,
    pid: int | None = None,
    stop_reason: str | None = None,
) -> None:
    """Upsert the current worker identity without recording source payloads."""

    if state not in {"STARTING", "READY", "STOPPING", "STOPPED", "FAILED"}:
        raise ValueError(f"unsupported worker heartbeat state: {state}")
    if active_session_count < 0:
        raise ValueError("active_session_count must be non-negative")
    process_id = os.getpid() if pid is None else pid
    connection.execute(
        """
        INSERT INTO timing_worker_heartbeats(
          worker_kind,instance_id,pid,state,active_session_count,started_at_us,
          ready_at_us,heartbeat_at_us,stopped_at_us,stop_reason,updated_at_us
        ) VALUES (?,?,?,?,?,?,CASE WHEN ? = 'READY' THEN ? END,?,
                  CASE WHEN ? IN ('STOPPED','FAILED') THEN ? END,?,?)
        ON CONFLICT(worker_kind) DO UPDATE SET
          instance_id = excluded.instance_id,
          pid = excluded.pid,
          state = excluded.state,
          active_session_count = excluded.active_session_count,
          started_at_us = CASE
            WHEN timing_worker_heartbeats.instance_id = excluded.instance_id
              THEN timing_worker_heartbeats.started_at_us
            ELSE excluded.started_at_us
          END,
          ready_at_us = CASE
            WHEN timing_worker_heartbeats.instance_id = excluded.instance_id
              THEN COALESCE(timing_worker_heartbeats.ready_at_us, excluded.ready_at_us)
            ELSE excluded.ready_at_us
          END,
          heartbeat_at_us = excluded.heartbeat_at_us,
          stopped_at_us = excluded.stopped_at_us,
          stop_reason = excluded.stop_reason,
          updated_at_us = excluded.updated_at_us
        """,
        (
            WORKER_KIND,
            instance_id,
            process_id,
            state,
            active_session_count,
            observed_at_us,
            state,
            observed_at_us,
            observed_at_us,
            state,
            observed_at_us,
            stop_reason[:200] if stop_reason else None,
            observed_at_us,
        ),
    )
    connection.commit()


class WorkerHeartbeat:
    """Keep SQLite and the systemd watchdog current until shutdown."""

    def __init__(
        self,
        database: str | Path | None = None,
        *,
        settings: WorkerHeartbeatSettings = WorkerHeartbeatSettings(),
    ):
        self.database = database
        self.settings = settings
        self.instance_id = str(uuid.uuid4())

    async def run(
        self,
        stop_event: asyncio.Event,
        *,
        active_session_count: Callable[[], int],
    ) -> None:
        connection = connect(self.database)
        interval = min(
            max(0.25, self.settings.database_interval_s),
            watchdog_interval_s() or max(0.25, self.settings.database_interval_s),
        )
        try:
            timestamp = now_us()
            write_worker_heartbeat(
                connection,
                instance_id=self.instance_id,
                state="STARTING",
                active_session_count=active_session_count(),
                observed_at_us=timestamp,
            )
            notify("READY=1\nSTATUS=Timing ingest supervisor ready")
            write_worker_heartbeat(
                connection,
                instance_id=self.instance_id,
                state="READY",
                active_session_count=active_session_count(),
                observed_at_us=now_us(),
            )
            while not stop_event.is_set():
                notify("WATCHDOG=1\nSTATUS=Timing ingest supervisor healthy")
                write_worker_heartbeat(
                    connection,
                    instance_id=self.instance_id,
                    state="READY",
                    active_session_count=active_session_count(),
                    observed_at_us=now_us(),
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as error:
            try:
                write_worker_heartbeat(
                    connection,
                    instance_id=self.instance_id,
                    state="FAILED",
                    active_session_count=active_session_count(),
                    observed_at_us=now_us(),
                    stop_reason=type(error).__name__,
                )
            finally:
                notify("STATUS=Timing ingest heartbeat failed")
            raise
        finally:
            if stop_event.is_set():
                write_worker_heartbeat(
                    connection,
                    instance_id=self.instance_id,
                    state="STOPPED",
                    active_session_count=0,
                    observed_at_us=now_us(),
                    stop_reason="operator_stop",
                )
                notify("STOPPING=1\nSTATUS=Timing ingest supervisor stopped")
            connection.close()
