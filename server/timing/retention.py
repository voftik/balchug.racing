"""Retention planning for mutable raw timing artifacts."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from bisect import bisect_right
from collections.abc import Mapping
from dataclasses import dataclass

from .config import now_us, timing_db_path
from .db import (
    CheckpointError,
    RUNTIME_CHECKPOINT_FORMAT,
    RUNTIME_CHECKPOINT_FORMAT_VERSION,
    connect,
)
from .normalizer_writer import (
    RUNTIME_CHECKPOINT_REDUCER_VERSION,
    NormalizerError,
    validate_runtime_checkpoint,
)
from .result_grid import ResultGridStateError


DAY_US = 86_400_000_000


@dataclass(frozen=True)
class RetentionPlan:
    raw_before_us: int
    stream_before_us: int
    feed_frame_ids: tuple[int, ...]
    stream_event_ids: tuple[int, ...]

    @property
    def total(self) -> int:
        return len(self.feed_frame_ids) + len(self.stream_event_ids)


class RetentionError(RuntimeError):
    """RAW retention would leave no restorable reducer boundary."""


@dataclass(frozen=True)
class _RuntimeCheckpoint:
    """One fully validated runtime checkpoint usable after RAW pruning."""

    id: int
    analysis_session_id: str
    source_heat_id: int
    source_frame_id: int
    source_frame_received_at_us: int


def _compatible_runtime_checkpoints(
    connection: sqlite3.Connection,
) -> dict[str, tuple[_RuntimeCheckpoint, ...]]:
    """Return the newest restorable current-version checkpoint per session.

    A checkpoint is useful for RAW retention only when its immutable frame
    anchor still exists and is already processed.  We also verify the stored
    hash before accepting its payload; an invalid/corrupt newer checkpoint
    therefore never becomes a retention boundary. One newest valid anchor is
    sufficient for a session: it sits after every RAW frame that this pass can
    safely prune, avoiding a full decompression of endurance-race checkpoint
    history during routine retention.
    """

    rows = connection.execute(
        """
        SELECT checkpoint.id,checkpoint.source_heat_id,checkpoint.source_frame_id,
               checkpoint.source_key,checkpoint.observed_at_us,checkpoint.state_hash,
               checkpoint.codec,checkpoint.payload,
               heat.analysis_session_id,
               anchor.ingest_connection_id AS anchor_connection_id,
               anchor.frame_sequence AS anchor_frame_sequence,
               anchor.analysis_session_id AS anchor_analysis_session_id,
               anchor.received_at_us AS anchor_received_at_us
        FROM state_checkpoints AS checkpoint
        JOIN source_heats AS heat ON heat.id = checkpoint.source_heat_id
        JOIN feed_frames AS anchor ON anchor.id = checkpoint.source_frame_id
        LEFT JOIN timing_raw_retention_floors AS floor
          ON floor.analysis_session_id = heat.analysis_session_id
        WHERE checkpoint.checkpoint_format = ?
          AND checkpoint.checkpoint_format_version = ?
          AND checkpoint.reducer_version = ?
          AND checkpoint.source_frame_id IS NOT NULL
          AND anchor.analysis_session_id = heat.analysis_session_id
          AND anchor.processed_at_us IS NOT NULL
          AND (
            floor.deleted_through_frame_id IS NULL
            OR checkpoint.source_frame_id > floor.deleted_through_frame_id
          )
          AND checkpoint.source_key = anchor.ingest_connection_id || ':' || anchor.frame_sequence
          AND checkpoint.observed_at_us = anchor.received_at_us
        ORDER BY heat.analysis_session_id,checkpoint.source_frame_id DESC,checkpoint.id DESC
        """,
        (
            RUNTIME_CHECKPOINT_FORMAT,
            RUNTIME_CHECKPOINT_FORMAT_VERSION,
            RUNTIME_CHECKPOINT_REDUCER_VERSION,
        ),
    )
    grouped: dict[str, tuple[_RuntimeCheckpoint, ...]] = {}
    for row in rows:
        session_id = row["analysis_session_id"]
        if session_id in grouped:
            continue
        try:
            # Use the exact reducer restore contract, not a duplicated
            # approximation of it. This method is deliberately write-free.
            validate_runtime_checkpoint(connection, row)
        except (CheckpointError, NormalizerError, ResultGridStateError, ValueError, TypeError, KeyError, IndexError):
            continue
        grouped[session_id] = (
            _RuntimeCheckpoint(
                id=int(row["id"]),
                analysis_session_id=session_id,
                source_heat_id=int(row["source_heat_id"]),
                source_frame_id=int(row["source_frame_id"]),
                source_frame_received_at_us=int(row["anchor_received_at_us"]),
            ),
        )
    return grouped


def _checkpoint_after(
    checkpoints_by_session: Mapping[str, tuple[_RuntimeCheckpoint, ...]],
    analysis_session_id: str,
    frame_id: int,
) -> _RuntimeCheckpoint | None:
    """Return the first retained reducer checkpoint strictly after a frame."""

    checkpoints = checkpoints_by_session.get(analysis_session_id, ())
    index = bisect_right(tuple(checkpoint.source_frame_id for checkpoint in checkpoints), frame_id)
    return checkpoints[index] if index < len(checkpoints) else None


def plan_retention(
    connection: sqlite3.Connection,
    *,
    now_at_us: int,
    raw_days: int = 7,
    stream_days: int = 2,
) -> RetentionPlan:
    """Select only records belonging to stopped/aborted sessions."""
    if raw_days < 0 or stream_days < 0:
        raise ValueError("Retention periods cannot be negative")
    raw_before = now_at_us - raw_days * DAY_US
    stream_before = now_at_us - stream_days * DAY_US
    checkpoints_by_session = _compatible_runtime_checkpoints(connection)
    feed_frame_ids = tuple(
        int(row["id"])
        for row in connection.execute(
            """
            SELECT e.id,e.analysis_session_id
            FROM feed_frames AS e
            JOIN analysis_sessions AS session ON session.id = e.analysis_session_id
            WHERE session.lifecycle IN ('stopped', 'aborted')
              AND e.processed_at_us IS NOT NULL
              AND e.received_at_us < ?
              AND NOT EXISTS (
                SELECT 1
                FROM source_heats AS heat
                WHERE heat.analysis_session_id = e.analysis_session_id
                  AND NOT EXISTS (
                    SELECT 1 FROM playback_snapshots AS snapshot WHERE snapshot.source_heat_id = heat.id
                  )
              )
            ORDER BY e.id
            """,
            (raw_before,),
        ).fetchall()
        if _checkpoint_after(checkpoints_by_session, row["analysis_session_id"], int(row["id"])) is not None
    )
    stream_event_ids = tuple(
        row[0]
        for row in connection.execute(
            """
            SELECT e.id FROM stream_events e
            JOIN analysis_sessions s ON s.id = e.analysis_session_id
            WHERE s.lifecycle IN ('stopped', 'aborted') AND e.created_at_us < ?
            """,
            (stream_before,),
        )
    )
    return RetentionPlan(raw_before, stream_before, feed_frame_ids, stream_event_ids)


def apply_retention(connection: sqlite3.Connection, plan: RetentionPlan) -> int:
    """Apply a reviewed plan, rechecking that it is still safe to delete."""
    deleted = 0
    deleted_stream_cursors: dict[str, int] = {}
    deleted_raw: dict[str, tuple[int, int]] = {}
    with connection:
        # Recheck checkpoint rows in the write transaction after the planned
        # ids may have aged.  The final floor check below decodes/hash-validates
        # a surviving checkpoint after every selected RAW row.
        for frame_id in sorted(set(plan.feed_frame_ids)):
            frame = connection.execute(
                """
                SELECT id,analysis_session_id,received_at_us
                FROM feed_frames
                WHERE id = ? AND processed_at_us IS NOT NULL AND received_at_us < ?
                  AND analysis_session_id IN (
                    SELECT id FROM analysis_sessions WHERE lifecycle IN ('stopped', 'aborted')
                  )
                """,
                (frame_id, plan.raw_before_us),
            ).fetchone()
            if frame is None:
                continue
            cursor = connection.execute(
                """
                DELETE FROM feed_frames
                WHERE id = ?
                  AND processed_at_us IS NOT NULL
                  AND received_at_us < ?
                  AND analysis_session_id IN (
                    SELECT id FROM analysis_sessions WHERE lifecycle IN ('stopped', 'aborted')
                  )
                  AND EXISTS (
                    SELECT 1
                    FROM state_checkpoints AS checkpoint
                    JOIN source_heats AS heat ON heat.id = checkpoint.source_heat_id
                    JOIN feed_frames AS anchor ON anchor.id = checkpoint.source_frame_id
                    WHERE heat.analysis_session_id = feed_frames.analysis_session_id
                      AND anchor.analysis_session_id = heat.analysis_session_id
                      AND anchor.processed_at_us IS NOT NULL
                      AND checkpoint.checkpoint_format = ?
                      AND checkpoint.checkpoint_format_version = ?
                      AND checkpoint.reducer_version = ?
                      AND checkpoint.source_frame_id > feed_frames.id
                      AND checkpoint.source_key = anchor.ingest_connection_id || ':' || anchor.frame_sequence
                      AND checkpoint.observed_at_us = anchor.received_at_us
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM source_heats h
                    WHERE h.analysis_session_id = feed_frames.analysis_session_id
                      AND NOT EXISTS (
                        SELECT 1 FROM playback_snapshots p WHERE p.source_heat_id = h.id
                      )
                  )
                """,
                (
                    frame_id,
                    plan.raw_before_us,
                    RUNTIME_CHECKPOINT_FORMAT,
                    RUNTIME_CHECKPOINT_FORMAT_VERSION,
                    RUNTIME_CHECKPOINT_REDUCER_VERSION,
                ),
            )
            if cursor.rowcount > 0:
                deleted += cursor.rowcount
                session_id = frame["analysis_session_id"]
                previous = deleted_raw.get(session_id)
                deleted_raw[session_id] = (
                    max(previous[0], int(frame["id"])) if previous is not None else int(frame["id"]),
                    max(previous[1], int(frame["received_at_us"])) if previous is not None else int(frame["received_at_us"]),
                )

        # The generic SQL guard above protects against concurrent/state drift;
        # this stricter post-delete check is the actual retention boundary. It
        # verifies a hash-valid reducer envelope that remains after the latest
        # deleted frame, then records it in the same transaction as the delete.
        checkpoints_by_session = _compatible_runtime_checkpoints(connection)
        timestamp_us = now_us()
        for session_id, (deleted_through_frame_id, deleted_through_received_at_us) in deleted_raw.items():
            checkpoint = _checkpoint_after(checkpoints_by_session, session_id, deleted_through_frame_id)
            if checkpoint is None:
                raise RetentionError(
                    "RAW retention lost its compatible runtime checkpoint "
                    f"for analysis session {session_id}; transaction rolled back"
                )
            existing_floor = connection.execute(
                """
                SELECT deleted_through_frame_id,deleted_through_received_at_us
                FROM timing_raw_retention_floors
                WHERE analysis_session_id = ?
                """,
                (session_id,),
            ).fetchone()
            if existing_floor is not None and int(existing_floor["deleted_through_frame_id"]) >= deleted_through_frame_id:
                continue
            if existing_floor is not None:
                # Frame ids are the replay boundary; receive time is retained
                # as an operational high-water mark even if a reconnect made
                # source receive clocks non-monotonic.
                deleted_through_received_at_us = max(
                    deleted_through_received_at_us,
                    int(existing_floor["deleted_through_received_at_us"]),
                )
            connection.execute(
                """
                INSERT INTO timing_raw_retention_floors(
                  analysis_session_id,deleted_through_frame_id,deleted_through_received_at_us,
                  checkpoint_id,created_at_us,updated_at_us
                ) VALUES (?,?,?,?,?,?)
                ON CONFLICT(analysis_session_id) DO UPDATE SET
                  deleted_through_frame_id = excluded.deleted_through_frame_id,
                  deleted_through_received_at_us = excluded.deleted_through_received_at_us,
                  checkpoint_id = excluded.checkpoint_id,
                  updated_at_us = excluded.updated_at_us
                """,
                (
                    session_id,
                    deleted_through_frame_id,
                    deleted_through_received_at_us,
                    checkpoint.id,
                    timestamp_us,
                    timestamp_us,
                ),
            )
        for event_id in plan.stream_event_ids:
            event = connection.execute(
                """
                SELECT analysis_session_id FROM stream_events
                WHERE id = ?
                  AND created_at_us < ?
                  AND analysis_session_id IN (
                    SELECT id FROM analysis_sessions WHERE lifecycle IN ('stopped', 'aborted')
                  )
                """,
                (event_id, plan.stream_before_us),
            ).fetchone()
            cursor = connection.execute(
                """
                DELETE FROM stream_events
                WHERE id = ?
                  AND created_at_us < ?
                  AND analysis_session_id IN (
                    SELECT id FROM analysis_sessions WHERE lifecycle IN ('stopped', 'aborted')
                  )
                """,
                (event_id, plan.stream_before_us),
            )
            deleted += max(cursor.rowcount, 0)
            if event is not None and cursor.rowcount > 0:
                session_id = event["analysis_session_id"]
                deleted_stream_cursors[session_id] = max(deleted_stream_cursors.get(session_id, 0), event_id)
        for session_id, cursor_id in deleted_stream_cursors.items():
            connection.execute(
                """
                INSERT INTO stream_event_cursor_floors(analysis_session_id,deleted_through_id,updated_at_us)
                VALUES (?,?,?)
                ON CONFLICT(analysis_session_id) DO UPDATE SET
                  deleted_through_id = MAX(deleted_through_id, excluded.deleted_through_id),
                  updated_at_us = excluded.updated_at_us
                """,
                (session_id, cursor_id, now_us()),
            )
    return deleted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan or apply timing.db retention")
    parser.add_argument("--db", default=None, help="override TIMING_DB")
    parser.add_argument("--raw-days", type=int, default=7)
    parser.add_argument("--stream-days", type=int, default=2)
    parser.add_argument("--apply", action="store_true", help="delete the reviewed plan")
    args = parser.parse_args(argv)
    if args.raw_days < 0 or args.stream_days < 0:
        parser.error("--raw-days and --stream-days must be zero or greater")
    database = timing_db_path(args.db)
    connection = connect(database)
    try:
        plan = plan_retention(connection, now_at_us=now_us(), raw_days=args.raw_days, stream_days=args.stream_days)
        deleted = apply_retention(connection, plan) if args.apply else 0
    finally:
        connection.close()
    print(
        json.dumps(
            {
                "database": str(database),
                "dry_run": not args.apply,
                "feed_frames": len(plan.feed_frame_ids),
                "stream_events": len(plan.stream_event_ids),
                "deleted": deleted,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
