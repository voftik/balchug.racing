"""Rebuild Race Control projection from retained raw `m_*` messages.

This recovery path exists for data captured before the normal live worker had
the `m` subscription.  It does not alter raw frames or any unrelated derived
facts.  Reprojection is deliberately restricted to stopped sessions so it
cannot race a live normalizer or turn an old snapshot into a newer board.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from .db import connect, migrate
from .race_control_store import RaceControlSource, project_screen_message


class RaceControlBackfillError(RuntimeError):
    """A raw Race Control recovery would be ambiguous or unsafe."""


@dataclass(frozen=True)
class RaceControlBackfillResult:
    """Auditable outcome of one complete raw replay."""

    session_id: str
    source_heats: int
    frames_seen: int
    messages_seen: int
    observations_written: int
    current_messages: int
    active_messages: int


def rebuild_race_control_messages(database: str | Path | None, session_id: str) -> RaceControlBackfillResult:
    """Replace only Race Control derived tables using immutable session RAW.

    `m_i` is a mutable snapshot, so an incremental repair can be unsafe if it
    is applied after newer deltas.  Rebuilding chronological source evidence is
    deterministic and makes recovery after a schema deployment explicit.
    """

    if not isinstance(session_id, str) or not session_id.strip():
        raise RaceControlBackfillError("session_id must be a non-empty string")
    migrate(database)
    connection = connect(database)
    try:
        connection.execute("BEGIN IMMEDIATE")
        try:
            lifecycle = connection.execute(
                "SELECT lifecycle FROM analysis_sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if lifecycle is None:
                raise RaceControlBackfillError(f"Analysis session not found: {session_id}")
            if lifecycle["lifecycle"] not in {"stopped", "aborted"}:
                raise RaceControlBackfillError("Race Control backfill requires a stopped or aborted session")

            heats = connection.execute(
                """
                SELECT id,generation,created_at_us
                FROM source_heats
                WHERE analysis_session_id = ?
                ORDER BY generation,id
                """,
                (session_id,),
            ).fetchall()
            if not heats:
                raise RaceControlBackfillError("Session has no source heat to receive Race Control messages")
            heat_by_created_at = tuple((int(row["id"]), int(row["created_at_us"])) for row in heats)

            rows = connection.execute(
                """
                SELECT frame.id AS source_frame_id,frame.ingest_connection_id,frame.frame_sequence,
                       frame.received_at_us,message.id AS source_message_id,message.ordinal,
                       message.handle,message.args_json
                FROM feed_frames AS frame
                JOIN feed_messages AS message ON message.frame_id = frame.id
                WHERE frame.analysis_session_id = ? AND substr(message.handle, 1, 2) = 'm_'
                ORDER BY frame.received_at_us,frame.id,message.ordinal
                """,
                (session_id,),
            ).fetchall()

            heat_ids = tuple(int(row["id"]) for row in heats)
            placeholders = ",".join("?" for _ in heat_ids)
            connection.execute(
                f"DELETE FROM race_control_messages_current WHERE source_heat_id IN ({placeholders})",
                heat_ids,
            )
            connection.execute(
                f"DELETE FROM race_control_message_observations WHERE source_heat_id IN ({placeholders})",
                heat_ids,
            )

            observations_written = 0
            frame_ids: set[int] = set()
            for row in rows:
                observed_at_us = int(row["received_at_us"])
                heat_id = _heat_for_observation(heat_by_created_at, observed_at_us)
                if heat_id is None:
                    raise RaceControlBackfillError(
                        "Race Control source frame predates every known source heat: "
                        f"frame={row['source_frame_id']} observed_at_us={observed_at_us}"
                    )
                try:
                    args = json.loads(row["args_json"])
                except (TypeError, json.JSONDecodeError) as error:
                    raise RaceControlBackfillError(
                        f"Race Control source message {row['source_message_id']} has invalid args JSON"
                    ) from error
                if not isinstance(args, list):
                    raise RaceControlBackfillError(
                        f"Race Control source message {row['source_message_id']} args are not an array"
                    )
                source_key = f"{row['ingest_connection_id']}:{row['frame_sequence']}:{row['ordinal']}"
                result = project_screen_message(
                    connection,
                    source=RaceControlSource(
                        source_heat_id=heat_id,
                        source_frame_id=int(row["source_frame_id"]),
                        source_message_id=int(row["source_message_id"]),
                        source_message_ordinal=int(row["ordinal"]),
                        source_key=source_key,
                        observed_at_us=observed_at_us,
                    ),
                    handle=str(row["handle"]),
                    args=tuple(args),
                )
                observations_written += result.observations_written
                frame_ids.add(int(row["source_frame_id"]))

            current_messages = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM race_control_messages_current WHERE source_heat_id IN ({placeholders})",
                    heat_ids,
                ).fetchone()[0]
            )
            active_messages = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM race_control_messages_current
                    WHERE source_heat_id IN ({placeholders}) AND is_active = 1
                    """,
                    heat_ids,
                ).fetchone()[0]
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return RaceControlBackfillResult(
            session_id=session_id,
            source_heats=len(heats),
            frames_seen=len(frame_ids),
            messages_seen=len(rows),
            observations_written=observations_written,
            current_messages=current_messages,
            active_messages=active_messages,
        )
    finally:
        connection.close()


def _heat_for_observation(heats: tuple[tuple[int, int], ...], observed_at_us: int) -> int | None:
    """Assign an observation to the latest already-created source heat."""

    result: int | None = None
    for heat_id, created_at_us in heats:
        if created_at_us > observed_at_us:
            break
        result = heat_id
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rebuild Race Control facts from a stopped session's raw m_* frames")
    parser.add_argument("--db", default=None, help="override TIMING_DB")
    parser.add_argument("--session", required=True, help="stopped or aborted analysis session id")
    args = parser.parse_args(argv)
    try:
        result = rebuild_race_control_messages(args.db, args.session)
    except RaceControlBackfillError as error:
        parser.error(str(error))
    print(json.dumps(asdict(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
