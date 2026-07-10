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
                  AND c.source_frame_id >= e.id
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
    return RetentionPlan(feed_frame_ids, stream_event_ids)


def apply_retention(connection: sqlite3.Connection, plan: RetentionPlan) -> int:
    """Apply a previously reviewed plan in one transaction."""
    with connection:
        if plan.feed_frame_ids:
            connection.executemany("DELETE FROM feed_frames WHERE id = ?", ((item,) for item in plan.feed_frame_ids))
        if plan.stream_event_ids:
            connection.executemany("DELETE FROM stream_events WHERE id = ?", ((item,) for item in plan.stream_event_ids))
    return plan.total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan or apply timing.db retention")
    parser.add_argument("--db", default=None, help="override TIMING_DB")
    parser.add_argument("--raw-days", type=int, default=7)
    parser.add_argument("--stream-days", type=int, default=2)
    parser.add_argument("--apply", action="store_true", help="delete the reviewed plan")
    args = parser.parse_args(argv)
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
