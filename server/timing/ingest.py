"""Recoverable live timing ingest supervisor.

The HTTP lifecycle API stores only engineer intent.  This worker observes active
sessions, opens the provider WebSocket, persists raw frames first and delegates
normalization only after that durable commit.  It never starts a session itself.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import sqlite3
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import now_us
from .db import connect
from .ingest_store import IngestConnection, ProcessedFrameCheckpoint, RawIngestStore, StoredFrame
from .lifecycle import AnalysisSession, list_active_sessions
from .protocol import DEFAULT_GROUPS, LiveTimingClient, SignalRMessage


LOGGER = logging.getLogger(__name__)
FrameProcessor = Callable[[sqlite3.Connection, StoredFrame, tuple[SignalRMessage, ...]], object]
ClientFactory = Callable[[str], Any]


@dataclass(frozen=True)
class IngestSettings:
    """Small, bounded reconnect policy suitable for a race-length worker."""

    reconnect_backoff_s: tuple[float, ...] = (1, 2, 5, 10, 30)
    active_poll_s: float = 1.0


def _live_client(source_url: str) -> LiveTimingClient:
    return LiveTimingClient(source_url, DEFAULT_GROUPS)


async def _maybe_await(value: object) -> None:
    if inspect.isawaitable(value):
        await value


async def _checkpoint_for_processed_frame(
    processor: FrameProcessor | None,
    connection: sqlite3.Connection,
    frame: StoredFrame,
) -> ProcessedFrameCheckpoint | None:
    """Ask an optional stateful processor for an atomic processed-frame anchor."""

    if processor is None:
        return None
    candidate = getattr(processor, "checkpoint_for_processed_frame", None)
    if candidate is None or not callable(candidate):
        return None
    value = candidate(connection, frame)
    if inspect.isawaitable(value):
        value = await value
    if value is not None and not isinstance(value, ProcessedFrameCheckpoint):
        raise TypeError("checkpoint_for_processed_frame must return ProcessedFrameCheckpoint or None")
    return value


async def _wait_until_inactive(store: RawIngestStore, *, poll_s: float) -> None:
    """Observe lifecycle without ever cancelling a live WebSocket read for silence."""
    while store.is_session_active():
        await asyncio.sleep(max(0.05, poll_s))


class TimingIngestSupervisor:
    """Own active session worker tasks and retain every disconnect explicitly."""

    def __init__(
        self,
        database: str | Path | None = None,
        *,
        client_factory: ClientFactory = _live_client,
        frame_processor: FrameProcessor | None = None,
        settings: IngestSettings = IngestSettings(),
    ):
        self.database = database
        self.client_factory = client_factory
        self.frame_processor = frame_processor
        self.settings = settings
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def active_sessions(self) -> tuple[AnalysisSession, ...]:
        connection = connect(self.database)
        try:
            return list_active_sessions(connection)
        finally:
            connection.close()

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        """Keep exactly one task per durable active session until cancelled."""
        event = stop_event or asyncio.Event()
        try:
            while not event.is_set():
                desired = {session.id: session for session in self.active_sessions()}
                for session_id, task in tuple(self._tasks.items()):
                    if task.done():
                        try:
                            task.result()
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            LOGGER.exception("timing ingest task crashed", extra={"session_id": session_id})
                        del self._tasks[session_id]
                    elif session_id not in desired:
                        # A stopped/aborted lifecycle is authoritative. Leave
                        # its task one short poll interval to close the socket
                        # and write its own clean ``session_inactive`` reason.
                        # That avoids recording an operator stop as a crash.
                        continue
                for session in desired.values():
                    if session.id not in self._tasks:
                        self._tasks[session.id] = asyncio.create_task(
                            self.run_session(session), name=f"timing-ingest:{session.id}"
                        )
                try:
                    await asyncio.wait_for(event.wait(), timeout=self.settings.active_poll_s)
                except asyncio.TimeoutError:
                    pass
        finally:
            tasks = tuple(self._tasks.values())
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._tasks.clear()

    async def run_session(self, session: AnalysisSession) -> None:
        """Run one session through reconnects until lifecycle no longer says active."""
        connection = connect(self.database)
        store = RawIngestStore(connection, analysis_session_id=session.id)
        run_started = False
        pending_gap_id: int | None = None
        failures = 0
        try:
            store.start_run()
            run_started = True
            pending_gap_id = store.recovered_gap_id
            # A process may have died after decoded messages committed but
            # before derived facts and ``processed_at_us`` committed. Replay
            # those rows before accepting newer source frames, preserving the
            # original receive order and making restart recovery deterministic.
            if self.frame_processor is not None:
                for pending_frame in store.pending_decoded_frames():
                    pending_messages = store.decode_frame(pending_frame)
                    if pending_messages:
                        await _maybe_await(self.frame_processor(connection, pending_frame, pending_messages))
                    store.mark_processed(
                        pending_frame,
                        checkpoint=await _checkpoint_for_processed_frame(
                            self.frame_processor,
                            connection,
                            pending_frame,
                        )
                        if pending_messages
                        else None,
                    )
            while store.is_session_active():
                upstream: IngestConnection | None = None
                disconnect_started_at_us: int | None = None
                reason = "socket_closed"
                try:
                    client = self.client_factory(session.source_url)
                    stream = client.raw_frames().__aiter__()
                    sequence = 0
                    try:
                        while store.is_session_active():
                            read_task = asyncio.create_task(anext(stream))
                            stopped_task = asyncio.create_task(
                                _wait_until_inactive(store, poll_s=self.settings.active_poll_s)
                            )
                            try:
                                done, _ = await asyncio.wait(
                                    (read_task, stopped_task), return_when=asyncio.FIRST_COMPLETED
                                )
                                if stopped_task in done:
                                    read_task.cancel()
                                    await asyncio.gather(read_task, return_exceptions=True)
                                    break
                                stopped_task.cancel()
                                await asyncio.gather(stopped_task, return_exceptions=True)
                                try:
                                    bootstrap, raw_text = read_task.result()
                                except StopAsyncIteration:
                                    break
                            finally:
                                if not read_task.done():
                                    read_task.cancel()
                                if not stopped_task.done():
                                    stopped_task.cancel()
                                await asyncio.gather(read_task, stopped_task, return_exceptions=True)
                            if upstream is None:
                                connected_at_us = now_us()
                                upstream = store.open_connection(bootstrap, connected_at_us=connected_at_us)
                                if pending_gap_id is not None:
                                    store.close_gap(pending_gap_id, ended_at_us=connected_at_us)
                                    pending_gap_id = None
                                failures = 0
                            sequence += 1
                            frame = store.persist_raw_frame(
                                upstream,
                                sequence=sequence,
                                raw_text=raw_text,
                                received_at_us=now_us(),
                                monotonic_ns=time.monotonic_ns(),
                            )
                            messages = store.decode_frame(frame)
                            if messages:
                                if self.frame_processor is not None:
                                    await _maybe_await(self.frame_processor(connection, frame, messages))
                                    store.mark_processed(
                                        frame,
                                        checkpoint=await _checkpoint_for_processed_frame(
                                            self.frame_processor,
                                            connection,
                                            frame,
                                        ),
                                    )
                            else:
                                # A malformed frame is terminally recorded as
                                # failed by the store. An empty valid envelope
                                # has no derived work but is still replayed.
                                state = connection.execute(
                                    "SELECT decode_state FROM feed_frames WHERE id = ?", (frame.id,)
                                ).fetchone()["decode_state"]
                                if state == "decoded":
                                    store.mark_processed(frame)
                    finally:
                        close = getattr(stream, "aclose", None)
                        if close is not None:
                            await close()
                except asyncio.CancelledError:
                    reason = "supervisor_cancelled"
                    raise
                except Exception as error:
                    reason = f"error:{type(error).__name__}"
                    LOGGER.warning("timing source connection failed: %s", error, extra={"session_id": session.id})
                finally:
                    disconnect_started_at_us = now_us()
                    if upstream is not None:
                        store.close_connection(upstream, reason=reason, disconnected_at_us=disconnect_started_at_us)

                if not store.is_session_active():
                    break
                pending_gap_id = store.record_gap(
                    ingest_connection=upstream,
                    reason=reason,
                    started_at_us=disconnect_started_at_us,
                )
                delay = self.settings.reconnect_backoff_s[min(failures, len(self.settings.reconnect_backoff_s) - 1)]
                failures += 1
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            if run_started:
                if pending_gap_id is None and store.is_session_active():
                    pending_gap_id = store.record_gap(
                        ingest_connection=None,
                        reason="worker_restart",
                        started_at_us=now_us(),
                    )
                store.finish_run(reason="supervisor_cancelled")
            raise
        except Exception as error:
            if run_started:
                store.finish_run(reason=f"error:{type(error).__name__}")
            raise
        else:
            if run_started:
                store.finish_run(reason="session_inactive")
        finally:
            connection.close()
