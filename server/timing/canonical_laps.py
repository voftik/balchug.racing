"""Canonical lap chronology reconstructed from immutable Tracker and grid facts."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Any, Mapping

from .config import now_us
from .normalization import OPEN_ENDED_TS_TIME, parse_ts_time


START_SIGNAL_WINDOW_US = 5_000_000
LAST_MATCH_WINDOW_US = 5_000_000


@dataclass(frozen=True)
class CanonicalLapMatch:
    id: str
    participant_id: str
    lap_ordinal: int
    lap_number: int | None
    tracker_duration_us: int


def _id(kind: str, *parts: object) -> str:
    payload = ":".join(str(part) for part in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"balchug-racing:canonical:{kind}:{payload}"))


def _duration_us(value: Any) -> int | None:
    parsed = parse_ts_time(value)
    if parsed is None or not 1_000_000 <= parsed < OPEN_ENDED_TS_TIME:
        return None
    return parsed


def _tracker_observation(
    connection: sqlite3.Connection, source_heat_id: int, observation_id: int
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT observation.*,message.handle,message.frame_id,message.ordinal AS message_ordinal
        FROM tracker_passing_observations AS observation
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        WHERE observation.id = ? AND observation.source_heat_id = ?
        """,
        (observation_id, source_heat_id),
    ).fetchone()


def _track_length_mm(connection: sqlite3.Connection, source_heat_id: int) -> int | None:
    row = connection.execute(
        """
        SELECT MAX(stop_distance_mm)
        FROM tracker_passing_observations
        WHERE source_heat_id = ? AND stop_distance_mm > 0
        """,
        (source_heat_id,),
    ).fetchone()
    return int(row[0]) if row is not None and row[0] is not None else None


def _green_flag(connection: sqlite3.Connection, source_heat_id: int) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT green_flag_provider_ts_raw,green_flag_provider_ts_us,green_flag_at_us,
               source_message_id,source_key,observed_at_us
        FROM heat_statistics_current
        WHERE source_heat_id = ? AND green_flag_provider_ts_us IS NOT NULL
        """,
        (source_heat_id,),
    ).fetchone()


def _start_signal(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    participant_id: str,
    green_provider_us: int,
) -> sqlite3.Row | None:
    track_length = _track_length_mm(connection, source_heat_id)
    if track_length is None:
        return None
    return connection.execute(
        """
        SELECT observation.*
        FROM tracker_passing_observations AS observation
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        WHERE observation.source_heat_id = ? AND observation.participant_id = ?
          AND message.handle = 't_p'
          AND observation.provider_passed_at_provider_us > 0
          AND observation.start_distance_mm = 0
          AND observation.stop_distance_mm = ?
          AND observation.is_in_pit = 0
          AND ABS(observation.provider_passed_at_provider_us - ?) <= ?
        ORDER BY ABS(observation.provider_passed_at_provider_us - ?),observation.id
        LIMIT 1
        """,
        (
            source_heat_id,
            participant_id,
            track_length,
            green_provider_us,
            START_SIGNAL_WINDOW_US,
            green_provider_us,
        ),
    ).fetchone()


def _insert_heat_start(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    participant_id: str,
    green: sqlite3.Row,
    start_signal: sqlite3.Row,
) -> sqlite3.Row:
    provider_us = int(green["green_flag_provider_ts_us"])
    boundary_id = _id("boundary", source_heat_id, participant_id, "HEAT_START", provider_us)
    timestamp = now_us()
    connection.execute(
        """
        INSERT OR IGNORE INTO canonical_lap_boundaries(
          id,source_heat_id,participant_id,boundary_ordinal,boundary_kind,source_kind,
          passing_observation_id,corroborating_passing_observation_id,
          provider_passed_at_raw,provider_passed_at_provider_us,passed_at_us,observed_at_us,
          start_distance_mm,stop_distance_mm,sector_id,is_in_pit,
          source_message_id,source_key,created_at_us,updated_at_us
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            boundary_id,
            source_heat_id,
            participant_id,
            0,
            "HEAT_START",
            "HEAT_GREEN",
            None,
            int(start_signal["id"]),
            str(green["green_flag_provider_ts_raw"]),
            provider_us,
            green["green_flag_at_us"],
            int(green["observed_at_us"]),
            None,
            None,
            None,
            None,
            green["source_message_id"],
            str(green["source_key"]),
            timestamp,
            timestamp,
        ),
    )
    row = connection.execute(
        "SELECT * FROM canonical_lap_boundaries WHERE id = ?", (boundary_id,)
    ).fetchone()
    if row is None:
        raise RuntimeError("canonical heat-start boundary was not persisted")
    return row


def _insert_coverage_start(
    connection: sqlite3.Connection, observation: sqlite3.Row
) -> sqlite3.Row:
    source_heat_id = int(observation["source_heat_id"])
    participant_id = str(observation["participant_id"])
    provider_us = int(observation["provider_passed_at_provider_us"])
    boundary_id = _id("boundary", source_heat_id, participant_id, "COVERAGE_START", observation["id"])
    timestamp = now_us()
    connection.execute(
        """
        INSERT OR IGNORE INTO canonical_lap_boundaries(
          id,source_heat_id,participant_id,boundary_ordinal,boundary_kind,source_kind,
          passing_observation_id,corroborating_passing_observation_id,
          provider_passed_at_raw,provider_passed_at_provider_us,passed_at_us,observed_at_us,
          start_distance_mm,stop_distance_mm,sector_id,is_in_pit,
          source_message_id,source_key,created_at_us,updated_at_us
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            boundary_id,
            source_heat_id,
            participant_id,
            0,
            "COVERAGE_START",
            "TRACKER_PASSING",
            int(observation["id"]),
            None,
            str(observation["provider_passed_at_raw"]),
            provider_us,
            observation["passed_at_us"],
            int(observation["observed_at_us"]),
            observation["start_distance_mm"],
            observation["stop_distance_mm"],
            observation["sector_id"],
            observation["is_in_pit"],
            observation["source_message_id"],
            str(observation["source_key"]),
            timestamp,
            timestamp,
        ),
    )
    row = connection.execute(
        "SELECT * FROM canonical_lap_boundaries WHERE id = ?", (boundary_id,)
    ).fetchone()
    if row is None:
        raise RuntimeError("canonical coverage boundary was not persisted")
    return row


def _initial_boundary(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    participant_id: str,
    fallback_observation: sqlite3.Row,
) -> sqlite3.Row:
    current = connection.execute(
        """
        SELECT * FROM canonical_lap_boundaries
        WHERE source_heat_id = ? AND participant_id = ?
        ORDER BY boundary_ordinal DESC LIMIT 1
        """,
        (source_heat_id, participant_id),
    ).fetchone()
    if current is not None:
        return current
    green = _green_flag(connection, source_heat_id)
    if green is not None:
        signal = _start_signal(
            connection,
            source_heat_id=source_heat_id,
            participant_id=participant_id,
            green_provider_us=int(green["green_flag_provider_ts_us"]),
        )
        if signal is not None:
            return _insert_heat_start(
                connection,
                source_heat_id=source_heat_id,
                participant_id=participant_id,
                green=green,
                start_signal=signal,
            )
    return _insert_coverage_start(connection, fallback_observation)


def _passing_role(row: sqlite3.Row, *, finish_observation_id: int, track_length: int | None) -> str:
    if int(row["id"]) == finish_observation_id:
        return "FINISH"
    if row["sector_id"] == 1:
        return "SECTOR_1_END"
    if row["sector_id"] == 2:
        return "SECTOR_2_END"
    if (
        track_length is not None
        and row["start_distance_mm"] == 0
        and row["stop_distance_mm"] == track_length
    ):
        return "START_CORROBORATION"
    if row["is_in_pit"] == 1 or (row["sector_id"] is not None and int(row["sector_id"]) < 0):
        return "PIT_PATH"
    return "OTHER"


def _lap_passings(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    participant_id: str,
    started_at_provider_us: int,
    finished_at_provider_us: int,
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT observation.*
        FROM tracker_passing_observations AS observation
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        WHERE observation.source_heat_id = ? AND observation.participant_id = ?
          AND message.handle = 't_p'
          AND observation.provider_passed_at_provider_us > ?
          AND observation.provider_passed_at_provider_us <= ?
        ORDER BY observation.provider_passed_at_provider_us,observation.id
        """,
        (source_heat_id, participant_id, started_at_provider_us, finished_at_provider_us),
    ).fetchall()


def _write_sector_rows(
    connection: sqlite3.Connection,
    *,
    lap_id: str,
    start_boundary: sqlite3.Row,
    finish_boundary: sqlite3.Row,
    passings: list[sqlite3.Row],
) -> None:
    sector_1 = next(
        (row for row in passings if row["sector_id"] == 1),
        None,
    )
    sector_2 = next(
        (row for row in passings if row["sector_id"] == 2),
        None,
    )
    finish = next(
        (row for row in passings if int(row["id"]) == int(finish_boundary["passing_observation_id"])),
        None,
    )
    points = (
        (
            1,
            None,
            sector_1,
            int(start_boundary["provider_passed_at_provider_us"]),
            int(sector_1["provider_passed_at_provider_us"]) if sector_1 is not None else None,
        ),
        (
            2,
            sector_1,
            sector_2,
            int(sector_1["provider_passed_at_provider_us"]) if sector_1 is not None else None,
            int(sector_2["provider_passed_at_provider_us"]) if sector_2 is not None else None,
        ),
        (
            3,
            sector_2,
            finish,
            int(sector_2["provider_passed_at_provider_us"]) if sector_2 is not None else None,
            int(finish_boundary["provider_passed_at_provider_us"]),
        ),
    )
    timestamp = now_us()
    for sector_number, start, end, started_us, finished_us in points:
        duration_us = (
            finished_us - started_us
            if started_us is not None and finished_us is not None and finished_us > started_us
            else None
        )
        connection.execute(
            """
            INSERT INTO canonical_lap_sectors(
              canonical_lap_id,sector_number,tracker_start_passing_observation_id,
              tracker_finish_passing_observation_id,tracker_started_at_provider_us,
              tracker_finished_at_provider_us,tracker_duration_us,tracker_duration_ms,
              source_cell_observation_id,source_duration_raw,source_duration_us,source_duration_ms,
              duration_reconciliation,created_at_us,updated_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                lap_id,
                sector_number,
                int(start["id"]) if start is not None else start_boundary["passing_observation_id"],
                int(end["id"]) if end is not None else None,
                started_us,
                finished_us,
                duration_us,
                duration_us // 1_000 if duration_us is not None else None,
                None,
                None,
                None,
                None,
                "PENDING",
                timestamp,
                timestamp,
            ),
        )


def record_tracker_passing(
    connection: sqlite3.Connection, *, source_heat_id: int, observation_id: int
) -> CanonicalLapMatch | None:
    """Append one exact lap boundary for a new physical t_p observation."""

    observation = _tracker_observation(connection, source_heat_id, observation_id)
    if (
        observation is None
        or observation["handle"] != "t_p"
        or observation["participant_id"] is None
        or observation["provider_passed_at_provider_us"] is None
        or int(observation["provider_passed_at_provider_us"]) <= 0
        or observation["start_distance_mm"] != 0
    ):
        return None
    participant_id = str(observation["participant_id"])
    track_length = _track_length_mm(connection, source_heat_id)
    provider_us = int(observation["provider_passed_at_provider_us"])
    if (
        track_length is not None
        and observation["stop_distance_mm"] == track_length
        and observation["is_in_pit"] == 0
    ):
        _initial_boundary(
            connection,
            source_heat_id=source_heat_id,
            participant_id=participant_id,
            fallback_observation=observation,
        )
        return None

    previous = _initial_boundary(
        connection,
        source_heat_id=source_heat_id,
        participant_id=participant_id,
        fallback_observation=observation,
    )
    if int(previous["provider_passed_at_provider_us"]) >= provider_us:
        return None
    existing = connection.execute(
        "SELECT id FROM canonical_lap_boundaries WHERE passing_observation_id = ?",
        (observation_id,),
    ).fetchone()
    if existing is not None:
        return None
    boundary_kind = "PIT_FINISH" if observation["is_in_pit"] == 1 else "MAIN_FINISH"
    boundary_ordinal = int(previous["boundary_ordinal"]) + 1
    boundary_id = _id("boundary", source_heat_id, participant_id, boundary_kind, observation_id)
    timestamp = now_us()
    connection.execute(
        """
        INSERT INTO canonical_lap_boundaries(
          id,source_heat_id,participant_id,boundary_ordinal,boundary_kind,source_kind,
          passing_observation_id,corroborating_passing_observation_id,
          provider_passed_at_raw,provider_passed_at_provider_us,passed_at_us,observed_at_us,
          start_distance_mm,stop_distance_mm,sector_id,is_in_pit,
          source_message_id,source_key,created_at_us,updated_at_us
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            boundary_id,
            source_heat_id,
            participant_id,
            boundary_ordinal,
            boundary_kind,
            "TRACKER_PASSING",
            observation_id,
            None,
            str(observation["provider_passed_at_raw"]),
            provider_us,
            observation["passed_at_us"],
            int(observation["observed_at_us"]),
            observation["start_distance_mm"],
            observation["stop_distance_mm"],
            observation["sector_id"],
            observation["is_in_pit"],
            observation["source_message_id"],
            str(observation["source_key"]),
            timestamp,
            timestamp,
        ),
    )
    finish = connection.execute(
        "SELECT * FROM canonical_lap_boundaries WHERE id = ?", (boundary_id,)
    ).fetchone()
    if finish is None:
        raise RuntimeError("canonical finish boundary was not persisted")

    lap_ordinal = boundary_ordinal
    coverage_complete = previous["boundary_kind"] == "HEAT_START" or connection.execute(
        """
        SELECT boundary_kind FROM canonical_lap_boundaries
        WHERE source_heat_id = ? AND participant_id = ? AND boundary_ordinal = 0
        """,
        (source_heat_id, participant_id),
    ).fetchone()[0] == "HEAT_START"
    tracker_duration_us = provider_us - int(previous["provider_passed_at_provider_us"])
    lap_id = _id("lap", source_heat_id, participant_id, observation_id)
    passings = _lap_passings(
        connection,
        source_heat_id=source_heat_id,
        participant_id=participant_id,
        started_at_provider_us=int(previous["provider_passed_at_provider_us"]),
        finished_at_provider_us=provider_us,
    )
    crosses_pit = boundary_kind == "PIT_FINISH" or any(row["is_in_pit"] == 1 for row in passings)
    connection.execute(
        """
        INSERT INTO canonical_laps(
          id,source_heat_id,participant_id,lap_ordinal,lap_number,coverage_complete,
          start_boundary_id,finish_boundary_id,started_at_provider_us,finished_at_provider_us,
          started_at_us,finished_at_us,start_observed_at_us,finish_observed_at_us,
          tracker_duration_us,tracker_duration_ms,source_last_cell_observation_id,
          source_duration_raw,source_duration_us,source_duration_ms,duration_reconciliation,
          is_pit_lap,source_message_id,source_key,created_at_us,updated_at_us
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            lap_id,
            source_heat_id,
            participant_id,
            lap_ordinal,
            lap_ordinal if coverage_complete else None,
            int(coverage_complete),
            previous["id"],
            boundary_id,
            int(previous["provider_passed_at_provider_us"]),
            provider_us,
            previous["passed_at_us"],
            observation["passed_at_us"],
            int(previous["observed_at_us"]),
            int(observation["observed_at_us"]),
            tracker_duration_us,
            tracker_duration_us // 1_000,
            None,
            None,
            None,
            None,
            "PENDING",
            int(crosses_pit),
            observation["source_message_id"],
            str(observation["source_key"]),
            timestamp,
            timestamp,
        ),
    )
    for ordinal, passing in enumerate(passings, start=1):
        connection.execute(
            """
            INSERT OR IGNORE INTO canonical_lap_tracker_passings(
              canonical_lap_id,passing_observation_id,passing_ordinal,role
            ) VALUES (?,?,?,?)
            """,
            (
                lap_id,
                int(passing["id"]),
                ordinal,
                _passing_role(
                    passing,
                    finish_observation_id=observation_id,
                    track_length=track_length,
                ),
            ),
        )
    _write_sector_rows(
        connection,
        lap_id=lap_id,
        start_boundary=previous,
        finish_boundary=finish,
        passings=passings,
    )
    return CanonicalLapMatch(
        id=lap_id,
        participant_id=participant_id,
        lap_ordinal=lap_ordinal,
        lap_number=lap_ordinal if coverage_complete else None,
        tracker_duration_us=tracker_duration_us,
    )


def match_last_cell(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    participant_id: str,
    duration_raw: Any,
    observed_at_us: int,
) -> CanonicalLapMatch | None:
    """Return one unambiguous pending lap whose Tracker duration is exact."""

    source_duration_us = _duration_us(duration_raw)
    if source_duration_us is None:
        return None
    rows = connection.execute(
        """
        SELECT id,participant_id,lap_ordinal,lap_number,tracker_duration_us
        FROM canonical_laps
        WHERE source_heat_id = ? AND participant_id = ?
          AND source_last_cell_observation_id IS NULL
          AND tracker_duration_us = ?
          AND ABS(finish_observed_at_us - ?) <= ?
        ORDER BY ABS(finish_observed_at_us - ?),lap_ordinal
        LIMIT 2
        """,
        (
            source_heat_id,
            participant_id,
            source_duration_us,
            observed_at_us,
            LAST_MATCH_WINDOW_US,
            observed_at_us,
        ),
    ).fetchall()
    if len(rows) != 1:
        return None
    row = rows[0]
    return CanonicalLapMatch(
        id=str(row["id"]),
        participant_id=str(row["participant_id"]),
        lap_ordinal=int(row["lap_ordinal"]),
        lap_number=int(row["lap_number"]) if row["lap_number"] is not None else None,
        tracker_duration_us=int(row["tracker_duration_us"]),
    )


def linked_lap_for_last_cell(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    source_cell_observation_id: int,
) -> CanonicalLapMatch | None:
    """Return the canonical lap already linked to an idempotently replayed cell."""

    rows = connection.execute(
        """
        SELECT id,participant_id,lap_ordinal,lap_number,tracker_duration_us
        FROM canonical_laps
        WHERE source_heat_id = ? AND source_last_cell_observation_id = ?
        LIMIT 2
        """,
        (source_heat_id, source_cell_observation_id),
    ).fetchall()
    if len(rows) != 1:
        return None
    row = rows[0]
    return CanonicalLapMatch(
        id=str(row["id"]),
        participant_id=str(row["participant_id"]),
        lap_ordinal=int(row["lap_ordinal"]),
        lap_number=int(row["lap_number"]) if row["lap_number"] is not None else None,
        tracker_duration_us=int(row["tracker_duration_us"]),
    )


def attach_last_cell(
    connection: sqlite3.Connection,
    *,
    match: CanonicalLapMatch,
    source_cell_observation_id: int,
    duration_raw: Any,
    source_sectors: Mapping[str, tuple[str | None, int]],
) -> None:
    """Attach authoritative LAST/SECT cells and reconcile each raw duration."""

    duration_us = _duration_us(duration_raw)
    if duration_us is None:
        raise ValueError("canonical LAST attachment requires a valid source duration")
    timestamp = now_us()
    updated = connection.execute(
        """
        UPDATE canonical_laps
        SET source_last_cell_observation_id = ?,source_duration_raw = ?,source_duration_us = ?,
            source_duration_ms = ?,duration_reconciliation = ?,updated_at_us = ?
        WHERE id = ? AND source_last_cell_observation_id IS NULL
        """,
        (
            source_cell_observation_id,
            str(duration_raw),
            duration_us,
            duration_us // 1_000,
            "EXACT" if duration_us == match.tracker_duration_us else "MISMATCH",
            timestamp,
            match.id,
        ),
    ).rowcount
    if updated != 1:
        existing = connection.execute(
            "SELECT source_last_cell_observation_id FROM canonical_laps WHERE id = ?", (match.id,)
        ).fetchone()
        if existing is None or existing[0] != source_cell_observation_id:
            raise RuntimeError("canonical lap LAST source conflicts with an existing attachment")
    for sector_number in range(1, 4):
        key = f"sector_{sector_number}"
        source = source_sectors.get(key)
        source_raw = source[0] if source is not None else None
        source_id = source[1] if source is not None else None
        source_us = _duration_us(source_raw)
        tracker = connection.execute(
            """
            SELECT tracker_duration_us FROM canonical_lap_sectors
            WHERE canonical_lap_id = ? AND sector_number = ?
            """,
            (match.id, sector_number),
        ).fetchone()
        tracker_us = int(tracker[0]) if tracker is not None and tracker[0] is not None else None
        if source_us is None:
            reconciliation = "MISSING_SOURCE"
        elif tracker_us is None:
            reconciliation = "MISSING_TRACKER"
        elif source_us == tracker_us:
            reconciliation = "EXACT"
        else:
            reconciliation = "MISMATCH"
        connection.execute(
            """
            UPDATE canonical_lap_sectors
            SET source_cell_observation_id = ?,source_duration_raw = ?,source_duration_us = ?,
                source_duration_ms = ?,duration_reconciliation = ?,updated_at_us = ?
            WHERE canonical_lap_id = ? AND sector_number = ?
            """,
            (
                source_id,
                source_raw,
                source_us,
                source_us // 1_000 if source_us is not None else None,
                reconciliation,
                timestamp,
                match.id,
                sector_number,
            ),
        )


def _source_sectors_for_last(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    participant_id: str,
    last: sqlite3.Row,
) -> dict[str, tuple[str | None, int]]:
    previous = connection.execute(
        """
        SELECT observation.id,observation.source_change_ordinal,
               message.frame_id,message.ordinal AS message_ordinal
        FROM participant_result_cell_observations AS observation
        JOIN result_column_definitions AS definition
          ON definition.layout_version_id = observation.layout_version_id
         AND definition.column_index = observation.column_index
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        WHERE observation.source_heat_id = ? AND observation.participant_id = ?
          AND definition.canonical_key = 'last_lap'
          AND (message.frame_id < ? OR (message.frame_id = ? AND message.ordinal < ?))
        ORDER BY message.frame_id DESC,message.ordinal DESC,observation.source_change_ordinal DESC
        LIMIT 1
        """,
        (
            source_heat_id,
            participant_id,
            int(last["source_frame_id"]),
            int(last["source_frame_id"]),
            int(last["source_message_ordinal"]),
        ),
    ).fetchone()
    parameters: tuple[Any, ...]
    if previous is None:
        where = "observation.source_message_id = ?"
        parameters = (int(last["source_message_id"]),)
    else:
        where = """
          (message.frame_id > ? OR (message.frame_id = ? AND message.ordinal > ?))
          AND (message.frame_id < ? OR (message.frame_id = ? AND message.ordinal <= ?))
        """
        parameters = (
            int(previous["frame_id"]),
            int(previous["frame_id"]),
            int(previous["message_ordinal"]),
            int(last["source_frame_id"]),
            int(last["source_frame_id"]),
            int(last["source_message_ordinal"]),
        )
    rows = connection.execute(
        f"""
        SELECT definition.canonical_key,observation.id,observation.value_text,
               message.frame_id,message.ordinal AS message_ordinal,observation.source_change_ordinal
        FROM participant_result_cell_observations AS observation
        JOIN result_column_definitions AS definition
          ON definition.layout_version_id = observation.layout_version_id
         AND definition.column_index = observation.column_index
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        WHERE observation.source_heat_id = ? AND observation.participant_id = ?
          AND definition.canonical_key GLOB 'sector_[0-9]*' AND {where}
        ORDER BY definition.canonical_key,message.frame_id,message.ordinal,observation.source_change_ordinal
        """,
        (source_heat_id, participant_id, *parameters),
    ).fetchall()
    result: dict[str, tuple[str | None, int]] = {}
    for row in rows:
        key = row["canonical_key"]
        if isinstance(key, str):
            raw = row["value_text"]
            result[key] = (raw if _duration_us(raw) is not None else None, int(row["id"]))
    return result


def rebuild_canonical_heat(connection: sqlite3.Connection, source_heat_id: int) -> dict[str, int]:
    """Rebuild only canonical projections for one heat from retained RAW facts."""

    connection.execute("DELETE FROM canonical_lap_boundaries WHERE source_heat_id = ?", (source_heat_id,))
    observations = connection.execute(
        """
        SELECT observation.id
        FROM tracker_passing_observations AS observation
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        WHERE observation.source_heat_id = ? AND observation.participant_id IS NOT NULL
          AND message.handle = 't_p' AND observation.provider_passed_at_provider_us > 0
        ORDER BY observation.provider_passed_at_provider_us,observation.id
        """,
        (source_heat_id,),
    ).fetchall()
    for observation in observations:
        record_tracker_passing(
            connection,
            source_heat_id=source_heat_id,
            observation_id=int(observation["id"]),
        )

    last_cells = connection.execute(
        """
        SELECT ledger.*,observation.value_text
        FROM result_last_cell_ledger AS ledger
        JOIN participant_result_cell_observations AS observation
          ON observation.id = ledger.source_cell_observation_id
        WHERE ledger.source_heat_id = ? AND ledger.participant_id IS NOT NULL
          AND ledger.source_handle = 'r_c' AND ledger.duration_ms IS NOT NULL
          AND ledger.classification_reason IN (
            'SOURCE_LAP_BOUNDARY','DIRECT_FIRST_OBSERVATION','DIRECT_VALUE_CHANGED',
            'AMBIGUOUS_EQUAL_DURATION','CANONICAL_TRACKER_DURATION_MATCH'
          )
        ORDER BY ledger.source_frame_id,ledger.source_message_ordinal,
                 ledger.source_change_ordinal,ledger.source_cell_observation_id
        """,
        (source_heat_id,),
    ).fetchall()
    matched = 0
    for last in last_cells:
        match = match_last_cell(
            connection,
            source_heat_id=source_heat_id,
            participant_id=str(last["participant_id"]),
            duration_raw=last["value_text"],
            observed_at_us=int(last["observed_at_us"]),
        )
        if match is None:
            continue
        sectors = _source_sectors_for_last(
            connection,
            source_heat_id=source_heat_id,
            participant_id=match.participant_id,
            last=last,
        )
        attach_last_cell(
            connection,
            match=match,
            source_cell_observation_id=int(last["source_cell_observation_id"]),
            duration_raw=last["value_text"],
            source_sectors=sectors,
        )
        connection.execute(
            """
            UPDATE result_last_cell_ledger
            SET classification = 'CONFIRMED_LAP',
                classification_reason = 'CANONICAL_TRACKER_DURATION_MATCH',
                linked_canonical_lap_id = ?,sectors_json = ?,
                sectors_source_cell_observation_ids_json = ?
            WHERE source_cell_observation_id = ?
            """,
            (
                match.id,
                json.dumps(
                    {key: value[0] for key, value in sectors.items()},
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if sectors
                else None,
                json.dumps(
                    {key: value[1] for key, value in sectors.items()},
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if sectors
                else None,
                int(last["source_cell_observation_id"]),
            ),
        )
        matched += 1
    summary = connection.execute(
        """
        SELECT COUNT(*) AS laps,
               SUM(CASE WHEN duration_reconciliation = 'EXACT' THEN 1 ELSE 0 END) AS exact_laps,
               SUM(CASE WHEN coverage_complete = 1 THEN 1 ELSE 0 END) AS full_coverage_laps
        FROM canonical_laps WHERE source_heat_id = ?
        """,
        (source_heat_id,),
    ).fetchone()
    return {
        "observations": len(observations),
        "laps": int(summary["laps"] or 0),
        "exact_laps": int(summary["exact_laps"] or 0),
        "full_coverage_laps": int(summary["full_coverage_laps"] or 0),
        "matched_last_cells": matched,
    }
