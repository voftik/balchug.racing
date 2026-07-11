"""Durable raw-first persistence primitives for the timing ingest worker.

The live worker must be able to fail between a WebSocket receive and any
derived calculation without losing the source evidence.  This module owns only
that durable boundary: it commits the exact raw text first, then stores decoded
SignalR messages in a separate transaction.  A normalizer marks decoded frames
processed only after its own idempotent database writes have committed.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from .config import now_us
from .db import RUNTIME_CHECKPOINT_FORMAT, RUNTIME_CHECKPOINT_FORMAT_VERSION, save_checkpoint
from .protocol import Bootstrap, SignalRMessage, decode_envelope


RAW_REDUCER_VERSION = "timeservice-signalr-normalizer-v1"


class IngestStoreError(RuntimeError):
    """The durable ingest lifecycle cannot safely continue."""


@dataclass(frozen=True)
class IngestConnection:
    """One provider socket associated with one recoverable ingest run."""

    id: str
    ingest_run_id: str
    ordinal: int


@dataclass(frozen=True)
class StoredFrame:
    """The immutable raw evidence row for one provider WebSocket frame."""

    id: int
    connection_id: str
    sequence: int
    received_at_us: int
    source_key: str


@dataclass(frozen=True)
class ProcessedFrameCheckpoint:
    """One reducer state committed atomically with a processed RAW frame."""

    source_heat_id: int
    source_frame_id: int
    source_key: str
    observed_at_us: int
    reducer_version: str
    state: Any


@contextmanager
def _write_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    if connection.in_transaction:
        raise IngestStoreError("Ingest write requires a connection without an open transaction")
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_error(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"[:1_000]


class RawIngestStore:
    """Write raw frames and decoded handles with explicit restart provenance."""

    def __init__(self, connection: sqlite3.Connection, *, analysis_session_id: str):
        self.connection = connection
        self.analysis_session_id = analysis_session_id
        self.ingest_run_id: str | None = None
        self.recovered_gap_id: int | None = None

    def start_run(self, *, reducer_version: str = RAW_REDUCER_VERSION, started_at_us: int | None = None) -> str:
        """Create one worker-run record after confirming the session is active."""
        timestamp_us = now_us() if started_at_us is None else started_at_us
        run_id = str(uuid.uuid4())
        self.recovered_gap_id = None
        with _write_transaction(self.connection):
            row = self.connection.execute(
                "SELECT lifecycle FROM analysis_sessions WHERE id = ?", (self.analysis_session_id,)
            ).fetchone()
            if row is None:
                raise IngestStoreError(f"Analysis session does not exist: {self.analysis_session_id}")
            if row["lifecycle"] != "active":
                raise IngestStoreError(
                    f"Cannot start timing ingest for a {row['lifecycle']} analysis session"
                )
            orphan = self.connection.execute(
                """
                SELECT run.id AS run_id,run.started_at_us,upstream.id AS connection_id,
                       upstream.connected_at_us,
                       (SELECT MAX(frame.received_at_us)
                        FROM feed_frames AS frame
                        WHERE frame.ingest_connection_id = upstream.id) AS last_frame_at_us
                FROM ingest_runs AS run
                LEFT JOIN ingest_connections AS upstream
                  ON upstream.ingest_run_id = run.id AND upstream.disconnected_at_us IS NULL
                WHERE run.analysis_session_id = ? AND run.stopped_at_us IS NULL
                ORDER BY run.started_at_us DESC,upstream.ordinal DESC
                LIMIT 1
                """,
                (self.analysis_session_id,),
            ).fetchone()
            existing_gap = self.connection.execute(
                """
                SELECT id FROM ingest_gaps
                WHERE analysis_session_id = ? AND ended_at_us IS NULL
                ORDER BY started_at_us DESC,id DESC LIMIT 1
                """,
                (self.analysis_session_id,),
            ).fetchone()
            if orphan is not None:
                self.connection.execute(
                    """
                    UPDATE ingest_connections
                    SET disconnected_at_us = COALESCE(disconnected_at_us, ?),
                        disconnect_reason = COALESCE(disconnect_reason, 'worker_restart')
                    WHERE ingest_run_id IN (
                      SELECT id FROM ingest_runs
                      WHERE analysis_session_id = ? AND stopped_at_us IS NULL
                    ) AND disconnected_at_us IS NULL
                    """,
                    (timestamp_us, self.analysis_session_id),
                )
                self.connection.execute(
                    """
                    UPDATE ingest_runs
                    SET stopped_at_us = COALESCE(stopped_at_us, ?),
                        stop_reason = COALESCE(stop_reason, 'worker_restart_recovered')
                    WHERE analysis_session_id = ? AND stopped_at_us IS NULL
                    """,
                    (timestamp_us, self.analysis_session_id),
                )
                if existing_gap is None:
                    gap_started_at_us = min(
                        timestamp_us,
                        int(
                            orphan["last_frame_at_us"]
                            or orphan["connected_at_us"]
                            or orphan["started_at_us"]
                        ),
                    )
                    cursor = self.connection.execute(
                        """
                        INSERT INTO ingest_gaps(
                          analysis_session_id,ingest_connection_id,started_at_us,reason,created_at_us
                        ) VALUES (?,?,?,?,?)
                        """,
                        (
                            self.analysis_session_id,
                            orphan["connection_id"],
                            gap_started_at_us,
                            "worker_restart",
                            timestamp_us,
                        ),
                    )
                    self.recovered_gap_id = int(cursor.lastrowid)
                else:
                    self.recovered_gap_id = int(existing_gap["id"])
            elif existing_gap is not None:
                self.recovered_gap_id = int(existing_gap["id"])
            self.connection.execute(
                """
                INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us)
                VALUES (?,?,?,?)
                """,
                (run_id, self.analysis_session_id, reducer_version, timestamp_us),
            )
        self.ingest_run_id = run_id
        return run_id

    def finish_run(self, *, reason: str, stopped_at_us: int | None = None) -> None:
        """Close the run exactly once; raw rows remain immutable and replayable."""
        if self.ingest_run_id is None:
            return
        timestamp_us = now_us() if stopped_at_us is None else stopped_at_us
        with _write_transaction(self.connection):
            self.connection.execute(
                """
                UPDATE ingest_runs
                SET stopped_at_us = COALESCE(stopped_at_us, ?), stop_reason = COALESCE(stop_reason, ?)
                WHERE id = ?
                """,
                (timestamp_us, reason[:1_000], self.ingest_run_id),
            )

    def is_session_active(self) -> bool:
        row = self.connection.execute(
            "SELECT lifecycle FROM analysis_sessions WHERE id = ?", (self.analysis_session_id,)
        ).fetchone()
        return row is not None and row["lifecycle"] == "active"

    def open_connection(self, bootstrap: Bootstrap, *, connected_at_us: int | None = None) -> IngestConnection:
        """Associate a fresh upstream socket with the current ingest run."""
        if self.ingest_run_id is None:
            raise IngestStoreError("start_run must be called before opening an upstream connection")
        timestamp_us = now_us() if connected_at_us is None else connected_at_us
        connection_id = str(uuid.uuid4())
        with _write_transaction(self.connection):
            ordinal = int(
                self.connection.execute(
                    "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM ingest_connections WHERE ingest_run_id = ?",
                    (self.ingest_run_id,),
                ).fetchone()[0]
            )
            self.connection.execute(
                """
                INSERT INTO ingest_connections(
                  id,ingest_run_id,ordinal,timekeeper_id,display_marker,connected_at_us
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    connection_id,
                    self.ingest_run_id,
                    ordinal,
                    bootstrap.timekeeper_id,
                    bootstrap.display_marker,
                    timestamp_us,
                ),
            )
        return IngestConnection(connection_id, self.ingest_run_id, ordinal)

    def close_connection(
        self,
        ingest_connection: IngestConnection,
        *,
        reason: str,
        disconnected_at_us: int | None = None,
    ) -> None:
        timestamp_us = now_us() if disconnected_at_us is None else disconnected_at_us
        with _write_transaction(self.connection):
            self.connection.execute(
                """
                UPDATE ingest_connections
                SET disconnected_at_us = COALESCE(disconnected_at_us, ?),
                    disconnect_reason = COALESCE(disconnect_reason, ?)
                WHERE id = ? AND ingest_run_id = ?
                """,
                (timestamp_us, reason[:1_000], ingest_connection.id, ingest_connection.ingest_run_id),
            )

    def record_gap(
        self,
        *,
        ingest_connection: IngestConnection | None,
        reason: str,
        started_at_us: int,
        ended_at_us: int | None = None,
    ) -> int:
        """Persist a real source break rather than pretending the feed was continuous."""
        timestamp_us = now_us()
        with _write_transaction(self.connection):
            cursor = self.connection.execute(
                """
                INSERT INTO ingest_gaps(
                  analysis_session_id,ingest_connection_id,started_at_us,ended_at_us,reason,created_at_us
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    self.analysis_session_id,
                    ingest_connection.id if ingest_connection else None,
                    started_at_us,
                    ended_at_us,
                    reason[:1_000],
                    timestamp_us,
                ),
            )
        return int(cursor.lastrowid)

    def close_gap(self, gap_id: int, *, ended_at_us: int) -> None:
        """Close a previously recorded reconnect gap when the next socket is live."""
        with _write_transaction(self.connection):
            self.connection.execute(
                """
                UPDATE ingest_gaps
                SET ended_at_us = ?
                WHERE id = ? AND analysis_session_id = ? AND ended_at_us IS NULL
                """,
                (ended_at_us, gap_id, self.analysis_session_id),
            )

    def persist_raw_frame(
        self,
        ingest_connection: IngestConnection,
        *,
        sequence: int,
        raw_text: str,
        received_at_us: int,
        monotonic_ns: int,
    ) -> StoredFrame:
        """Commit exact source bytes before parsing any JSON or compressed payload."""
        if not isinstance(sequence, int) or sequence < 1:
            raise IngestStoreError("Frame sequence must be a positive integer")
        if not isinstance(raw_text, str):
            raise IngestStoreError("Raw provider frame must be text")
        raw_payload = raw_text.encode("utf-8")
        raw_hash = hashlib.sha256(raw_payload).hexdigest()
        timestamp_us = now_us()
        with _write_transaction(self.connection):
            cursor = self.connection.execute(
                """
                INSERT INTO feed_frames(
                  analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,monotonic_ns,
                  raw_payload,raw_sha256,created_at_us
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    self.analysis_session_id,
                    ingest_connection.id,
                    sequence,
                    received_at_us,
                    monotonic_ns,
                    raw_payload,
                    raw_hash,
                    timestamp_us,
                ),
            )
            self.connection.execute(
                """
                UPDATE timing_sources
                SET last_seen_at_us = ?
                WHERE id = (SELECT source_id FROM analysis_sessions WHERE id = ?)
                """,
                (received_at_us, self.analysis_session_id),
            )
        return StoredFrame(
            id=int(cursor.lastrowid),
            connection_id=ingest_connection.id,
            sequence=sequence,
            received_at_us=received_at_us,
            source_key=f"{ingest_connection.id}:{sequence}",
        )

    def decode_frame(self, frame: StoredFrame) -> tuple[SignalRMessage, ...]:
        """Decode a previously committed raw frame and persist each handle atomically."""
        row = self.connection.execute(
            """
            SELECT raw_payload, decode_state
            FROM feed_frames
            WHERE id = ? AND analysis_session_id = ? AND ingest_connection_id = ? AND frame_sequence = ?
            """,
            (frame.id, self.analysis_session_id, frame.connection_id, frame.sequence),
        ).fetchone()
        if row is None:
            raise IngestStoreError(f"Raw frame not found: {frame.source_key}")
        if row["decode_state"] == "failed":
            return ()
        if row["decode_state"] == "decoded":
            # The immutable decoded messages are the replay contract. A raw
            # SignalR frame can depend on a compression dictionary that no
            # longer exists in a later process, so re-decoding it here could
            # silently return fewer messages than the ones committed during
            # live ingest (notably the initial r_i layout snapshot).
            existing = self.connection.execute(
                "SELECT ordinal,handle,args_json,compressed FROM feed_messages WHERE frame_id = ? ORDER BY ordinal",
                (frame.id,),
            ).fetchall()
            return tuple(
                SignalRMessage(
                    item["handle"], tuple(json.loads(item["args_json"])), bool(item["compressed"])
                )
                for item in existing
            )
        raw_payload = bytes(row["raw_payload"])
        try:
            raw_text = raw_payload.decode("utf-8")
            envelope, messages = decode_envelope(raw_text)
        except Exception as error:
            # A provider compression/shape regression must not crash the live
            # worker. The exact raw frame is already durable for a later parser
            # fix, and this row records why it was not decoded today.
            with _write_transaction(self.connection):
                self.connection.execute(
                    """
                    UPDATE feed_frames
                    SET decode_state = 'failed', decode_error = ?, processed_at_us = ?
                    WHERE id = ? AND decode_state = 'pending'
                    """,
                    (_safe_error(error), now_us(), frame.id),
                )
            return ()

        with _write_transaction(self.connection):
            current = self.connection.execute(
                "SELECT decode_state FROM feed_frames WHERE id = ?", (frame.id,)
            ).fetchone()
            if current is None:
                raise IngestStoreError(f"Raw frame disappeared during decode: {frame.source_key}")
            if current["decode_state"] == "decoded":
                existing = self.connection.execute(
                    "SELECT ordinal,handle,args_json,compressed FROM feed_messages WHERE frame_id = ? ORDER BY ordinal",
                    (frame.id,),
                ).fetchall()
                return tuple(
                    SignalRMessage(
                        item["handle"], tuple(json.loads(item["args_json"])), bool(item["compressed"])
                    )
                    for item in existing
                )
            if current["decode_state"] != "pending":
                raise IngestStoreError(f"Unsupported frame decode state: {current['decode_state']}")
            self.connection.execute(
                """
                UPDATE feed_frames
                SET upstream_cursor = ?, groups_token = ?, decode_state = 'decoded', decode_error = NULL
                WHERE id = ?
                """,
                (envelope.get("C"), envelope.get("G"), frame.id),
            )
            for ordinal, message in enumerate(messages):
                self.connection.execute(
                    """
                    INSERT INTO feed_messages(frame_id,ordinal,handle,args_json,compressed,created_at_us)
                    VALUES (?,?,?,?,?,?)
                    """,
                    (
                        frame.id,
                        ordinal,
                        message.handle,
                        _canonical_json(list(message.args)),
                        int(message.compressed),
                        now_us(),
                    ),
                )
        return tuple(messages)

    def mark_processed(
        self,
        frame: StoredFrame,
        *,
        processed_at_us: int | None = None,
        checkpoint: ProcessedFrameCheckpoint | None = None,
    ) -> None:
        """Atomically mark a frame processed and optionally anchor reducer state.

        A runtime checkpoint must never point at a frame left pending by a
        crash. The normalizer/metric writes have already committed before this
        method starts; this transaction commits the marker and checkpoint as a
        single durable boundary.
        """

        timestamp_us = now_us() if processed_at_us is None else processed_at_us
        with _write_transaction(self.connection):
            row = self.connection.execute(
                """
                SELECT decode_state,processed_at_us
                FROM feed_frames
                WHERE id = ? AND analysis_session_id = ? AND ingest_connection_id = ? AND frame_sequence = ?
                """,
                (frame.id, self.analysis_session_id, frame.connection_id, frame.sequence),
            ).fetchone()
            if row is None:
                raise IngestStoreError(f"Raw frame not found while marking processed: {frame.source_key}")
            if row["decode_state"] != "decoded":
                raise IngestStoreError(f"Only decoded frames can be marked processed: {frame.source_key}")
            if row["processed_at_us"] is not None:
                return
            if checkpoint is not None:
                self._validate_checkpoint(frame, checkpoint)
                save_checkpoint(
                    self.connection,
                    source_heat_id=checkpoint.source_heat_id,
                    source_frame_id=checkpoint.source_frame_id,
                    source_key=checkpoint.source_key,
                    observed_at_us=checkpoint.observed_at_us,
                    state=checkpoint.state,
                    checkpoint_format=RUNTIME_CHECKPOINT_FORMAT,
                    checkpoint_format_version=RUNTIME_CHECKPOINT_FORMAT_VERSION,
                    reducer_version=checkpoint.reducer_version,
                )
            cursor = self.connection.execute(
                """
                UPDATE feed_frames
                SET processed_at_us = ?
                WHERE id = ? AND decode_state = 'decoded' AND processed_at_us IS NULL
                """,
                (timestamp_us, frame.id),
            )
            if cursor.rowcount != 1:  # pragma: no cover - writer lock makes this defensive
                raise IngestStoreError(f"Raw frame could not be marked processed: {frame.source_key}")

    @staticmethod
    def _validate_checkpoint(frame: StoredFrame, checkpoint: ProcessedFrameCheckpoint) -> None:
        if not isinstance(checkpoint, ProcessedFrameCheckpoint):
            raise IngestStoreError("checkpoint must be a ProcessedFrameCheckpoint")
        if type(checkpoint.source_heat_id) is not int or checkpoint.source_heat_id <= 0:
            raise IngestStoreError("checkpoint source_heat_id must be a positive integer")
        if checkpoint.source_frame_id != frame.id:
            raise IngestStoreError("checkpoint source_frame_id must match the processed frame")
        if checkpoint.source_key != frame.source_key:
            raise IngestStoreError("checkpoint source_key must match the processed frame")
        if checkpoint.observed_at_us != frame.received_at_us:
            raise IngestStoreError("checkpoint observed_at_us must match the processed frame")
        if not isinstance(checkpoint.reducer_version, str) or not checkpoint.reducer_version:
            raise IngestStoreError("checkpoint reducer_version must be a non-empty string")

    def pending_decoded_frames(self) -> tuple[StoredFrame, ...]:
        """Return replay work left after a process crash, in original receive order."""
        rows = self.connection.execute(
            """
            SELECT id,ingest_connection_id,frame_sequence,received_at_us
            FROM feed_frames
            WHERE analysis_session_id = ? AND decode_state = 'decoded' AND processed_at_us IS NULL
            ORDER BY id
            """,
            (self.analysis_session_id,),
        ).fetchall()
        return tuple(
            StoredFrame(
                id=int(row["id"]),
                connection_id=row["ingest_connection_id"],
                sequence=int(row["frame_sequence"]),
                received_at_us=int(row["received_at_us"]),
                source_key=f"{row['ingest_connection_id']}:{row['frame_sequence']}",
            )
            for row in rows
        )
