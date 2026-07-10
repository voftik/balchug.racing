"""Retention planning for mutable raw timing artifacts."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass

from .config import now_us, timing_db_path
from .db import connect


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
    feed_frame_ids = tuple(
        row[0]
        for row in connection.execute(
            """
            SELECT e.id FROM feed_frames e
            JOIN analysis_sessions s ON s.id = e.analysis_session_id
            WHERE s.lifecycle IN ('stopped', 'aborted')
              AND e.processed_at_us IS NOT NULL
              AND e.received_at_us < ?
              AND EXISTS (
                SELECT 1
                FROM state_checkpoints c
                JOIN source_heats h ON h.id = c.source_heat_id
                WHERE h.analysis_session_id = e.analysis_session_id
                  -- Keep the newest checkpoint's anchor frame. A later
                  -- checkpoint is required before an older anchor may go.
                  AND c.source_frame_id > e.id
              )
            """,
            (raw_before,),
        )
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
    with connection:
        for frame_id in plan.feed_frame_ids:
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
                    FROM state_checkpoints c
                    JOIN source_heats h ON h.id = c.source_heat_id
                    WHERE h.analysis_session_id = feed_frames.analysis_session_id
                      AND c.source_frame_id > feed_frames.id
                  )
                """,
                (frame_id, plan.raw_before_us),
            )
            deleted += max(cursor.rowcount, 0)
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
