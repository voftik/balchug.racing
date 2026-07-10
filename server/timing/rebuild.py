"""Rebuild derived timing facts from immutable raw frames for a stopped session.

This is an operational recovery tool for sessions captured before a newer
normalizer or metric materializer was deployed. It never edits a raw frame or
decoded provider message. Instead it removes only reconstructible state,
replays decoded frames in their original receive order, and leaves the session
stopped throughout the operation.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import now_us, timing_db_path
from .db import connect, migrate
from .ingest_store import RawIngestStore, StoredFrame
from .normalizer_writer import TimingNormalizer


class RebuildError(RuntimeError):
    """A session is unsafe or impossible to rebuild from its raw evidence."""


@dataclass(frozen=True)
class RebuildPlan:
    """Read-only preflight result for a deterministic derived-state rebuild."""

    session_id: str
    lifecycle: str
    decoded_frames: int
    source_heats: int
    previous_stream_cursor: int


@dataclass(frozen=True)
class RebuildResult:
    """Durable facts produced after one complete raw-frame replay."""

    session_id: str
    frames_replayed: int
    source_heats: int
    metric_current: int
    metric_samples: int
    stream_events: int


def _session_plan(connection: sqlite3.Connection, session_id: str) -> RebuildPlan:
    if not isinstance(session_id, str) or not session_id:
        raise RebuildError("session_id must be a non-empty string")
    session = connection.execute(
        "SELECT lifecycle FROM analysis_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if session is None:
        raise RebuildError(f"Analysis session not found: {session_id}")
    if session["lifecycle"] not in {"stopped", "aborted"}:
        raise RebuildError("Only a stopped or aborted analysis session may be rebuilt")
    decoded_frames = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM feed_frames
            WHERE analysis_session_id = ? AND decode_state = 'decoded'
            """,
            (session_id,),
        ).fetchone()[0]
    )
    if decoded_frames == 0:
        raise RebuildError("Session has no decoded raw frames to replay")
    source_heats = int(
        connection.execute(
            "SELECT COUNT(*) FROM source_heats WHERE analysis_session_id = ?", (session_id,)
        ).fetchone()[0]
    )
    previous_stream_cursor = int(
        connection.execute(
            "SELECT COALESCE(MAX(id), 0) FROM stream_events WHERE analysis_session_id = ?", (session_id,)
        ).fetchone()[0]
    )
    return RebuildPlan(session_id, session["lifecycle"], decoded_frames, source_heats, previous_stream_cursor)


def plan_rebuild(database: str | Path, session_id: str) -> RebuildPlan:
    """Validate that a raw-frame rebuild is allowed without changing the database."""

    connection = connect(database, readonly=True)
    try:
        return _session_plan(connection, session_id)
    finally:
        connection.close()


def _reset_derived_state(connection: sqlite3.Connection, plan: RebuildPlan) -> tuple[tuple[StoredFrame, int], ...]:
    """Delete only regenerable state and expose decoded frames as pending work."""

    frame_rows = connection.execute(
        """
        SELECT id,ingest_connection_id,frame_sequence,received_at_us,processed_at_us
        FROM feed_frames
        WHERE analysis_session_id = ? AND decode_state = 'decoded'
        ORDER BY id
        """,
        (plan.session_id,),
    ).fetchall()
    frames = tuple(
        (
            StoredFrame(
                id=int(row["id"]),
                connection_id=row["ingest_connection_id"],
                sequence=int(row["frame_sequence"]),
                received_at_us=int(row["received_at_us"]),
                source_key=f"{row['ingest_connection_id']}:{row['frame_sequence']}",
            ),
            int(row["processed_at_us"]) if row["processed_at_us"] is not None else int(row["received_at_us"]),
        )
        for row in frame_rows
    )
    if len(frames) != plan.decoded_frames:
        raise RebuildError("Decoded frame preflight changed before rebuild began")

    connection.execute("BEGIN IMMEDIATE")
    try:
        # Keep a retention floor for old EventSource cursors. The corresponding
        # rows disappear below, so a connected panel will receive a reset
        # instead of applying a new generation to an old snapshot.
        if plan.previous_stream_cursor:
            connection.execute(
                """
                INSERT INTO stream_event_cursor_floors(analysis_session_id,deleted_through_id,updated_at_us)
                VALUES (?,?,?)
                ON CONFLICT(analysis_session_id) DO UPDATE SET
                  deleted_through_id = MAX(deleted_through_id, excluded.deleted_through_id),
                  updated_at_us = excluded.updated_at_us
                """,
                (plan.session_id, plan.previous_stream_cursor, now_us()),
            )
        connection.execute("DELETE FROM stream_events WHERE analysis_session_id = ?", (plan.session_id,))
        connection.execute("DELETE FROM strategy_advisories WHERE analysis_session_id = ?", (plan.session_id,))
        connection.execute(
            """
            DELETE FROM connection_clock_samples
            WHERE ingest_connection_id IN (
              SELECT c.id
              FROM ingest_connections c
              JOIN ingest_runs r ON r.id = c.ingest_run_id
              WHERE r.analysis_session_id = ?
            )
            """,
            (plan.session_id,),
        )
        connection.execute(
            """
            DELETE FROM connection_clock_calibrations
            WHERE ingest_connection_id IN (
              SELECT c.id
              FROM ingest_connections c
              JOIN ingest_runs r ON r.id = c.ingest_run_id
              WHERE r.analysis_session_id = ?
            )
            """,
            (plan.session_id,),
        )
        # All source-heat children are reconstructible normalized facts. Raw
        # frames/messages refer directly to the analysis session and survive.
        connection.execute("DELETE FROM source_heats WHERE analysis_session_id = ?", (plan.session_id,))
        connection.execute(
            """
            UPDATE analysis_sessions
            SET our_participant_id = NULL, our_class = NULL, identity_state = 'pending', updated_at_us = ?
            WHERE id = ? AND lifecycle IN ('stopped', 'aborted')
            """,
            (now_us(), plan.session_id),
        )
        connection.execute(
            """
            UPDATE feed_frames
            SET processed_at_us = NULL
            WHERE analysis_session_id = ? AND decode_state = 'decoded'
            """,
            (plan.session_id,),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return frames


def _reattach_gaps(connection: sqlite3.Connection, session_id: str) -> None:
    """Restore a heat association for raw reconnect gaps after heat recreation."""

    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            UPDATE ingest_gaps
            SET source_heat_id = (
              SELECT h.id
              FROM source_heats h
              WHERE h.analysis_session_id = ingest_gaps.analysis_session_id
                AND h.created_at_us <= ingest_gaps.started_at_us
              ORDER BY h.generation DESC,h.id DESC
              LIMIT 1
            )
            WHERE analysis_session_id = ? AND source_heat_id IS NULL
            """,
            (session_id,),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def rebuild_session(database: str | Path, session_id: str) -> RebuildResult:
    """Reconstruct a stopped session's normalized state and tactical metrics."""

    migrate(database)
    connection = connect(database)
    try:
        plan = _session_plan(connection, session_id)
        frames = _reset_derived_state(connection, plan)
        store = RawIngestStore(connection, analysis_session_id=session_id)
        normalizer = TimingNormalizer(session_id)
        for frame, original_processed_at_us in frames:
            messages = store.decode_frame(frame)
            if messages:
                normalizer(connection, frame, messages)
            store.mark_processed(frame, processed_at_us=original_processed_at_us)
        _reattach_gaps(connection, session_id)
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_error = connection.execute("PRAGMA foreign_key_check").fetchone()
        if integrity != "ok" or foreign_key_error is not None:
            raise RebuildError("Rebuild produced an invalid timing database")
        return RebuildResult(
            session_id=session_id,
            frames_replayed=len(frames),
            source_heats=int(
                connection.execute("SELECT COUNT(*) FROM source_heats WHERE analysis_session_id = ?", (session_id,)).fetchone()[0]
            ),
            metric_current=int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM metric_current
                    WHERE source_heat_id IN (SELECT id FROM source_heats WHERE analysis_session_id = ?)
                    """,
                    (session_id,),
                ).fetchone()[0]
            ),
            metric_samples=int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM metric_samples
                    WHERE source_heat_id IN (SELECT id FROM source_heats WHERE analysis_session_id = ?)
                    """,
                    (session_id,),
                ).fetchone()[0]
            ),
            stream_events=int(
                connection.execute("SELECT COUNT(*) FROM stream_events WHERE analysis_session_id = ?", (session_id,)).fetchone()[0]
            ),
        )
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rebuild derived timing facts from raw frames for a stopped session")
    parser.add_argument("--db", default=None, help="override TIMING_DB")
    parser.add_argument("--session", required=True, help="stopped or aborted analysis session id")
    parser.add_argument("--dry-run", action="store_true", help="validate rebuild preconditions without mutating data")
    args = parser.parse_args(argv)
    database = timing_db_path(args.db)
    result = plan_rebuild(database, args.session) if args.dry_run else rebuild_session(database, args.session)
    print(json.dumps(asdict(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
