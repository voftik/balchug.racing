"""Bounded in-process fanout for the durable live-timing event outbox.

There is one Uvicorn worker for the timing API.  A single broker therefore
turns SQLite's replayable event journal into many SSE subscriptions without
giving every browser its own database polling loop.  The broker is read-only;
the normalizer and metric runner remain the sole fact writers.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import now_us
from .db import connect


LIVE_SCHEMA_VERSION = "timing-live.v1"
LIVE_FRESHNESS_US = 3_000_000
STALE_FRESHNESS_US = 10_000_000
DEFAULT_BATCH_SIZE = 128
DEFAULT_QUEUE_SIZE = 128


class StreamCursorError(ValueError):
    """A client supplied a malformed or unsafe SSE replay cursor."""


@dataclass(frozen=True)
class StreamEvent:
    """One immutable row from ``stream_events`` ready for SSE serialization."""

    id: int
    analysis_session_id: str
    source_heat_id: int | None
    source_frame_id: int | None
    source_message_id: int | None
    source_key: str | None
    event_type: str
    observed_at_us: int | None
    payload: Mapping[str, Any]

    @property
    def generation(self) -> int | None:
        value = self.payload.get("generation")
        return value if type(value) is int else None


@dataclass(frozen=True)
class ResetRequired:
    """A bounded subscriber queue discarded deltas and needs a new snapshot."""

    reason: str


@dataclass(frozen=True)
class CursorWindow:
    """Retention-aware validity information for one session's event cursor."""

    deleted_through_id: int
    latest_id: int
    cursor_exists: bool

    def requires_reset(self, cursor: int) -> bool:
        if cursor < 0 or cursor > self.latest_id:
            return True
        # A retained cursor floor marks a destructive retention/rebuild
        # boundary. Be conservative at the exact boundary too: a full
        # snapshot is inexpensive and cannot combine new deltas with an old
        # client-side generation.
        if self.deleted_through_id and cursor <= self.deleted_through_id:
            return True
        if cursor in {0, self.latest_id}:
            return False
        return not self.cursor_exists


def parse_last_event_id(value: str | None) -> int | None:
    """Parse a decimal EventSource cursor without accepting ambiguous numbers."""

    if value is None or value == "":
        return None
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal() or len(value) > 19:
        raise StreamCursorError("Last-Event-ID must be a non-negative decimal stream cursor")
    cursor = int(value)
    if cursor > 9_223_372_036_854_775_807:
        raise StreamCursorError("Last-Event-ID is outside the SQLite integer range")
    return cursor


def format_sse_event(
    event_type: str,
    payload: Mapping[str, Any],
    *,
    event_id: int | None = None,
    retry_ms: int | None = None,
) -> bytes:
    """Encode a JSON event using the SSE line protocol, never raw user text."""

    if not isinstance(event_type, str) or not event_type or "\n" in event_type or "\r" in event_type:
        raise ValueError("SSE event_type must be one line")
    if event_id is not None and (type(event_id) is not int or event_id < 0):
        raise ValueError("SSE event_id must be a non-negative integer or None")
    if retry_ms is not None and (type(retry_ms) is not int or retry_ms < 0):
        raise ValueError("SSE retry_ms must be a non-negative integer or None")
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    lines: list[str] = []
    if retry_ms is not None:
        lines.append(f"retry: {retry_ms}")
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event_type}")
    for line in encoded.splitlines() or [""]:
        lines.append(f"data: {line}")
    return ("\n".join(lines) + "\n\n").encode("utf-8")


def format_sse_comment(value: str) -> bytes:
    """Encode a short heartbeat comment without granting it an event id."""

    clean = " ".join(str(value).splitlines())
    return f": {clean}\n\n".encode("utf-8")


def _require_cursor(cursor: int) -> int:
    if type(cursor) is not int or cursor < 0:
        raise StreamCursorError("stream cursor must be a non-negative integer")
    return cursor


def _require_limit(limit: int) -> int:
    if type(limit) is not int or not 1 <= limit <= DEFAULT_BATCH_SIZE:
        raise StreamCursorError(f"stream batch limit must be from 1 through {DEFAULT_BATCH_SIZE}")
    return limit


def _decode_payload(value: Any, *, event_id: int) -> Mapping[str, Any]:
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {
            "schema_version": LIVE_SCHEMA_VERSION,
            "data": {"stream_payload_error": "invalid_json", "event_id": event_id},
        }
    if not isinstance(decoded, Mapping):
        return {
            "schema_version": LIVE_SCHEMA_VERSION,
            "data": {"stream_payload_error": "not_an_object", "event_id": event_id},
        }
    return dict(decoded)


def _row_to_event(row: sqlite3.Row) -> StreamEvent:
    return StreamEvent(
        id=int(row["id"]),
        analysis_session_id=row["analysis_session_id"],
        source_heat_id=int(row["source_heat_id"]) if row["source_heat_id"] is not None else None,
        source_frame_id=int(row["source_frame_id"]) if row["source_frame_id"] is not None else None,
        source_message_id=int(row["source_message_id"]) if row["source_message_id"] is not None else None,
        source_key=row["source_key"],
        event_type=row["event_type"],
        observed_at_us=int(row["observed_at_us"]) if row["observed_at_us"] is not None else None,
        payload=_decode_payload(row["payload_json"], event_id=int(row["id"])),
    )


def _select_events(
    connection: sqlite3.Connection,
    *,
    after_id: int,
    limit: int,
    analysis_session_id: str | None = None,
) -> tuple[StreamEvent, ...]:
    where = ["e.id > ?"]
    parameters: list[Any] = [after_id]
    if analysis_session_id is not None:
        where.append("e.analysis_session_id = ?")
        parameters.append(analysis_session_id)
    parameters.append(limit)
    rows = connection.execute(
        f"""
        SELECT e.id,e.analysis_session_id,e.source_heat_id,
               COALESCE(e.source_frame_id,m.frame_id) AS source_frame_id,
               e.source_message_id,e.source_key,e.event_type,e.observed_at_us,e.payload_json
        FROM stream_events e
        LEFT JOIN feed_messages m ON m.id = e.source_message_id
        WHERE {' AND '.join(where)}
        ORDER BY e.id
        LIMIT ?
        """,
        tuple(parameters),
    ).fetchall()
    return tuple(_row_to_event(row) for row in rows)


def read_stream_events(
    analysis_session_id: str,
    *,
    after_id: int,
    database: str | Path | None = None,
    limit: int = DEFAULT_BATCH_SIZE,
) -> tuple[StreamEvent, ...]:
    """Read one ordered replay batch for a single session using a short RO connection."""

    if not isinstance(analysis_session_id, str) or not analysis_session_id:
        raise StreamCursorError("analysis_session_id must be non-empty")
    after_id = _require_cursor(after_id)
    limit = _require_limit(limit)
    connection = connect(database, readonly=True)
    try:
        return _select_events(connection, after_id=after_id, limit=limit, analysis_session_id=analysis_session_id)
    finally:
        connection.close()


def read_cursor_window(
    analysis_session_id: str,
    *,
    cursor: int,
    database: str | Path | None = None,
) -> CursorWindow:
    """Tell an SSE endpoint whether a Last-Event-ID can be replayed exactly."""

    if not isinstance(analysis_session_id, str) or not analysis_session_id:
        raise StreamCursorError("analysis_session_id must be non-empty")
    cursor = _require_cursor(cursor)
    connection = connect(database, readonly=True)
    try:
        floor = connection.execute(
            "SELECT deleted_through_id FROM stream_event_cursor_floors WHERE analysis_session_id = ?",
            (analysis_session_id,),
        ).fetchone()
        latest = connection.execute(
            "SELECT MAX(id) AS cursor FROM stream_events WHERE analysis_session_id = ?",
            (analysis_session_id,),
        ).fetchone()
        exists = (
            cursor == 0
            or connection.execute(
                "SELECT 1 FROM stream_events WHERE analysis_session_id = ? AND id = ?",
                (analysis_session_id, cursor),
            ).fetchone()
            is not None
        )
        return CursorWindow(
            deleted_through_id=int(floor["deleted_through_id"]) if floor is not None else 0,
            latest_id=int(latest["cursor"]) if latest is not None and latest["cursor"] is not None else 0,
            cursor_exists=exists,
        )
    finally:
        connection.close()


def _last_global_event_id(database: str | Path | None) -> int:
    connection = connect(database, readonly=True)
    try:
        row = connection.execute("SELECT MAX(id) AS cursor FROM stream_events").fetchone()
        return int(row["cursor"]) if row is not None and row["cursor"] is not None else 0
    finally:
        connection.close()


def _read_global_events(database: str | Path | None, after_id: int, limit: int) -> tuple[StreamEvent, ...]:
    connection = connect(database, readonly=True)
    try:
        return _select_events(connection, after_id=after_id, limit=limit)
    finally:
        connection.close()


def _freshness_payload(
    connection: sqlite3.Connection,
    session_id: str,
    *,
    at_us: int,
) -> dict[str, Any] | None:
    session = connection.execute(
        "SELECT lifecycle FROM analysis_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if session is None:
        return None
    heat = connection.execute(
        """
        SELECT id,generation FROM source_heats
        WHERE analysis_session_id = ?
        ORDER BY generation DESC,id DESC LIMIT 1
        """,
        (session_id,),
    ).fetchone()
    status = "OFFLINE"
    reason = "no_source_heat"
    observed_at_us: int | None = None
    source_key: str | None = None
    age_ms: int | None = None
    heat_id: int | None = None
    generation: int | None = None
    if heat is not None:
        heat_id, generation = int(heat["id"]), int(heat["generation"])
        gap = connection.execute(
            """
            SELECT reason FROM ingest_gaps
            WHERE source_heat_id = ? AND ended_at_us IS NULL
            ORDER BY started_at_us DESC,id DESC LIMIT 1
            """,
            (heat_id,),
        ).fetchone()
        flag = connection.execute(
            "SELECT flag FROM track_flag_current WHERE source_heat_id = ?", (heat_id,)
        ).fetchone()
        tick = connection.execute(
            """
            SELECT observed_at_us,source_key FROM state_ticks
            WHERE source_heat_id = ? ORDER BY observed_second DESC LIMIT 1
            """,
            (heat_id,),
        ).fetchone()
        if tick is not None:
            observed_at_us, source_key = int(tick["observed_at_us"]), tick["source_key"]
            age_ms = max(0, (at_us - observed_at_us) // 1_000)
        if session["lifecycle"] in {"stopped", "aborted"}:
            reason = f"session_{session['lifecycle']}"
        elif flag is not None and flag["flag"] == "FINISH":
            reason = "track_finished"
        elif gap is not None:
            reason = "source_gap"
        elif tick is None:
            reason = "no_state_tick"
        elif at_us - observed_at_us <= LIVE_FRESHNESS_US:
            status, reason = "LIVE", "fresh"
        elif at_us - observed_at_us <= STALE_FRESHNESS_US:
            status, reason = "STALE", "stale"
        else:
            reason = "source_timeout"
    elif session["lifecycle"] in {"stopped", "aborted"}:
        reason = f"session_{session['lifecycle']}"
    return {
        "schema_version": LIVE_SCHEMA_VERSION,
        "session_id": session_id,
        "source_heat_id": heat_id,
        "generation": generation,
        "observed_at_us": observed_at_us,
        "source_key": source_key,
        "freshness": {
            "status": status,
            "age_ms": age_ms,
            "observed_at_us": observed_at_us,
            "source_key": source_key,
            "reason": reason,
            "computed_at_us": at_us,
        },
    }


def read_stream_quality(
    session_ids: Iterable[str],
    *,
    database: str | Path | None = None,
    at_us: int | None = None,
) -> dict[str, dict[str, Any]]:
    """Read current freshness once for all subscribed sessions without metrics."""

    identities = tuple(sorted({session_id for session_id in session_ids if isinstance(session_id, str) and session_id}))
    if not identities:
        return {}
    timestamp = now_us() if at_us is None else at_us
    connection = connect(database, readonly=True)
    try:
        return {
            session_id: payload
            for session_id in identities
            if (payload := _freshness_payload(connection, session_id, at_us=timestamp)) is not None
        }
    finally:
        connection.close()


class TimingStreamBroker:
    """Fan out fresh outbox rows and 1 Hz health transitions to SSE clients."""

    def __init__(
        self,
        database: str | Path | None = None,
        *,
        poll_interval_s: float = 0.25,
        quality_interval_s: float = 1.0,
        batch_size: int = DEFAULT_BATCH_SIZE,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        if poll_interval_s <= 0 or quality_interval_s <= 0:
            raise ValueError("stream polling intervals must be positive")
        self.database = database
        self.poll_interval_s = poll_interval_s
        self.quality_interval_s = quality_interval_s
        self.batch_size = _require_limit(batch_size)
        if type(queue_size) is not int or queue_size < 1:
            raise ValueError("queue_size must be a positive integer")
        self.queue_size = queue_size
        self._subscribers: dict[str, set[asyncio.Queue[StreamEvent | ResetRequired]]] = defaultdict(set)
        self._quality_fingerprints: dict[str, tuple[Any, ...]] = {}
        self._last_event_id = 0
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("timing stream broker is closed")
        if self._task is not None:
            return
        self._last_event_id = await asyncio.to_thread(_last_global_event_id, self.database)
        self._task = asyncio.create_task(self._run(), name="balchug-timing-sse-broker")

    async def stop(self) -> None:
        self._closed = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._subscribers.clear()
        self._quality_fingerprints.clear()

    async def subscribe(self, analysis_session_id: str) -> asyncio.Queue[StreamEvent | ResetRequired]:
        if not isinstance(analysis_session_id, str) or not analysis_session_id:
            raise StreamCursorError("analysis_session_id must be non-empty")
        await self.start()
        if not self._subscribers:
            # While no browser is connected there is nothing to fan out. Start
            # at the current global cursor when the first one arrives; its
            # snapshot barrier covers everything before this point.
            self._last_event_id = await asyncio.to_thread(_last_global_event_id, self.database)
        queue: asyncio.Queue[StreamEvent | ResetRequired] = asyncio.Queue(maxsize=self.queue_size)
        self._subscribers[analysis_session_id].add(queue)
        return queue

    def unsubscribe(self, analysis_session_id: str, queue: asyncio.Queue[StreamEvent | ResetRequired]) -> None:
        queues = self._subscribers.get(analysis_session_id)
        if queues is None:
            return
        queues.discard(queue)
        if not queues:
            self._subscribers.pop(analysis_session_id, None)
            self._quality_fingerprints.pop(analysis_session_id, None)

    @property
    def subscriber_count(self) -> int:
        return sum(len(queues) for queues in self._subscribers.values())

    async def _run(self) -> None:
        next_quality_at = 0.0
        loop = asyncio.get_running_loop()
        try:
            while not self._closed:
                if not self._subscribers:
                    await asyncio.sleep(self.poll_interval_s)
                    continue
                try:
                    events = await asyncio.to_thread(
                        _read_global_events, self.database, self._last_event_id, self.batch_size
                    )
                    for event in events:
                        self._last_event_id = max(self._last_event_id, event.id)
                        self._fanout(event.analysis_session_id, event)
                    if len(events) == self.batch_size:
                        continue
                    if self._subscribers and loop.time() >= next_quality_at:
                        qualities = await asyncio.to_thread(
                            read_stream_quality, tuple(self._subscribers), database=self.database
                        )
                        for session_id, payload in qualities.items():
                            freshness = payload["freshness"]
                            fingerprint = (
                                payload["source_heat_id"],
                                payload["generation"],
                                freshness["status"],
                                freshness["reason"],
                            )
                            if self._quality_fingerprints.get(session_id) != fingerprint:
                                self._quality_fingerprints[session_id] = fingerprint
                                self._fanout(session_id, StreamEvent(
                                    id=0,
                                    analysis_session_id=session_id,
                                    source_heat_id=payload["source_heat_id"],
                                    source_frame_id=None,
                                    source_message_id=None,
                                    source_key=payload["source_key"],
                                    event_type="quality",
                                    observed_at_us=payload["observed_at_us"],
                                    payload=payload,
                                ))
                        next_quality_at = loop.time() + self.quality_interval_s
                except asyncio.CancelledError:
                    raise
                except (OSError, sqlite3.Error):
                    # A transient WAL/replace race must not take down all SSE
                    # clients. The next bounded poll retries from the cursor.
                    pass
                await asyncio.sleep(self.poll_interval_s)
        except asyncio.CancelledError:
            raise

    def _fanout(self, session_id: str, item: StreamEvent | ResetRequired) -> None:
        for queue in tuple(self._subscribers.get(session_id, ())):
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                # A reset marker is more useful than a silently truncated
                # queue. Drain stale deltas first; the reader will resnapshot.
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                try:
                    queue.put_nowait(ResetRequired("subscriber_backpressure"))
                except asyncio.QueueFull:
                    pass
