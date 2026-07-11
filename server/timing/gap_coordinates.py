"""One-Hz mixed GAP coordinates reconstructed from the absolute result grid."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Sequence

from .config import now_us


_LAP_GROUP = re.compile(r"^\s*-+\s*(\d+)\s+laps?\s*-+\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class GapDisplayValue:
    raw: str | None
    kind: str
    time_ms: int | None = None
    completed_laps: int | None = None


@dataclass(frozen=True)
class GapCoordinateInput:
    participant_id: str
    position_overall: int | None
    position_class: int | None
    raw_gap_value: str | None
    source_cell_observation_id: int | None
    source_cell_message_id: int | None
    source_cell_key: str | None
    source_cell_observed_at_us: int | None


def parse_gap_display(value: Any) -> GapDisplayValue:
    """Parse every observed endurance GAP display form without guessing units."""

    if value is None:
        return GapDisplayValue(raw=None, kind="EMPTY")
    raw = str(value).strip()
    if not raw:
        return GapDisplayValue(raw=None, kind="EMPTY")
    lap_group = _LAP_GROUP.fullmatch(raw)
    if lap_group is not None:
        return GapDisplayValue(raw=raw, kind="LAP_GROUP", completed_laps=int(lap_group.group(1)))
    parts = raw.split(":")
    if len(parts) not in {1, 2, 3}:
        return GapDisplayValue(raw=raw, kind="UNKNOWN")
    try:
        if len(parts) == 1:
            total_seconds = Decimal(parts[0])
        elif len(parts) == 2:
            minutes = int(parts[0])
            seconds = Decimal(parts[1])
            if minutes < 0 or seconds < 0 or seconds >= 60:
                raise ValueError
            total_seconds = Decimal(minutes * 60) + seconds
        else:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = Decimal(parts[2])
            if hours < 0 or minutes < 0 or minutes >= 60 or seconds < 0 or seconds >= 60:
                raise ValueError
            total_seconds = Decimal(hours * 3600 + minutes * 60) + seconds
        if total_seconds < 0:
            raise ValueError
        milliseconds = int((total_seconds * 1000).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError):
        return GapDisplayValue(raw=raw, kind="UNKNOWN")
    return GapDisplayValue(raw=raw, kind="TIME", time_ms=milliseconds)


def _coordinate_records(rows: Sequence[GapCoordinateInput]) -> tuple[list[dict[str, Any]], dict[str, int | str | None]]:
    positioned = sorted(
        (row for row in rows if row.position_overall is not None and row.position_overall > 0),
        key=lambda row: (int(row.position_overall or 0), row.participant_id),
    )
    unpositioned = [row for row in rows if row not in positioned]
    ordered = positioned + unpositioned
    positions = [int(row.position_overall or 0) for row in positioned]
    positions_are_contiguous = positions == list(range(1, len(positions) + 1))
    leader_completed_laps: int | None = None
    group_completed_laps: int | None = None
    group_leader_id: str | None = None
    group_leader_position: int | None = None
    group_count = 0
    resolved = 0
    result: list[dict[str, Any]] = []
    for row in ordered:
        parsed = parse_gap_display(row.raw_gap_value)
        group_time_ms: int | None = None
        if row.position_overall is None:
            status = "POSITION_UNRESOLVED"
        elif parsed.kind == "LAP_GROUP":
            group_completed_laps = parsed.completed_laps
            group_leader_id = row.participant_id
            group_leader_position = row.position_overall
            group_time_ms = 0
            group_count += 1
            if leader_completed_laps is None:
                leader_completed_laps = group_completed_laps
            status = "EXACT"
        elif parsed.kind == "TIME" and group_completed_laps is not None and group_leader_id is not None:
            group_time_ms = parsed.time_ms
            status = "EXACT"
        elif parsed.kind in {"EMPTY", "UNKNOWN"}:
            status = "VALUE_UNSUPPORTED"
        else:
            status = "GROUP_UNRESOLVED"
        lap_delta = (
            leader_completed_laps - group_completed_laps
            if status == "EXACT"
            and leader_completed_laps is not None
            and group_completed_laps is not None
            and leader_completed_laps >= group_completed_laps
            else None
        )
        if status == "EXACT" and lap_delta is None:
            status = "GROUP_UNRESOLVED"
        if status == "EXACT":
            resolved += 1
        result.append(
            {
                "participant_id": row.participant_id,
                "source_position_overall": row.position_overall,
                "source_position_class": row.position_class,
                "raw_gap_value": parsed.raw,
                "display_value_kind": parsed.kind,
                "lap_group_completed_laps": group_completed_laps if status == "EXACT" else None,
                "time_from_lap_group_leader_ms": group_time_ms if status == "EXACT" else None,
                "lap_group_leader_participant_id": group_leader_id if status == "EXACT" else None,
                "lap_group_leader_position_overall": group_leader_position if status == "EXACT" else None,
                "gap_to_overall_leader_laps": lap_delta,
                "gap_to_overall_leader_residual_ms": group_time_ms if status == "EXACT" else None,
                "coordinate_status": status,
                "source_cell_observation_id": row.source_cell_observation_id,
                "source_cell_message_id": row.source_cell_message_id,
                "source_cell_key": row.source_cell_key,
                "source_cell_observed_at_us": row.source_cell_observed_at_us,
            }
        )
    completeness = (
        "COMPLETE"
        if rows and resolved == len(rows) and positions_are_contiguous
        else "PARTIAL"
        if resolved
        else "UNRESOLVED"
    )
    return result, {
        "leader_completed_laps": leader_completed_laps,
        "participant_count": len(rows),
        "positioned_participant_count": len(positioned),
        "resolved_coordinate_count": resolved,
        "lap_group_count": group_count,
        "completeness": completeness,
    }


def write_gap_coordinate_snapshot(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    source_frame_id: int,
    source_message_id: int | None,
    source_key: str,
    observed_at_us: int,
    rows: Sequence[GapCoordinateInput],
) -> bool:
    """Persist at most one full-table coordinate projection per receive second."""

    observed_second = observed_at_us // 1_000_000
    exists = connection.execute(
        """
        SELECT 1 FROM gap_coordinate_snapshots
        WHERE source_heat_id = ? AND observed_second = ?
        """,
        (source_heat_id, observed_second),
    ).fetchone()
    if exists is not None:
        return False
    coordinates, summary = _coordinate_records(rows)
    timestamp = now_us()
    connection.execute(
        """
        INSERT INTO gap_coordinate_snapshots(
          source_heat_id,observed_second,observed_at_us,source_frame_id,source_message_id,
          source_key,leader_completed_laps,participant_count,positioned_participant_count,
          resolved_coordinate_count,lap_group_count,completeness,created_at_us
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source_heat_id,
            observed_second,
            observed_at_us,
            source_frame_id,
            source_message_id,
            source_key,
            summary["leader_completed_laps"],
            summary["participant_count"],
            summary["positioned_participant_count"],
            summary["resolved_coordinate_count"],
            summary["lap_group_count"],
            summary["completeness"],
            timestamp,
        ),
    )
    snapshot_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    connection.executemany(
        """
        INSERT INTO participant_gap_coordinates(
          snapshot_id,participant_id,source_position_overall,source_position_class,
          raw_gap_value,display_value_kind,lap_group_completed_laps,
          time_from_lap_group_leader_ms,lap_group_leader_participant_id,
          lap_group_leader_position_overall,gap_to_overall_leader_laps,
          gap_to_overall_leader_residual_ms,coordinate_status,source_cell_observation_id,
          source_cell_message_id,source_cell_key,source_cell_observed_at_us,created_at_us
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                snapshot_id,
                coordinate["participant_id"],
                coordinate["source_position_overall"],
                coordinate["source_position_class"],
                coordinate["raw_gap_value"],
                coordinate["display_value_kind"],
                coordinate["lap_group_completed_laps"],
                coordinate["time_from_lap_group_leader_ms"],
                coordinate["lap_group_leader_participant_id"],
                coordinate["lap_group_leader_position_overall"],
                coordinate["gap_to_overall_leader_laps"],
                coordinate["gap_to_overall_leader_residual_ms"],
                coordinate["coordinate_status"],
                coordinate["source_cell_observation_id"],
                coordinate["source_cell_message_id"],
                coordinate["source_cell_key"],
                coordinate["source_cell_observed_at_us"],
                timestamp,
            )
            for coordinate in coordinates
        ],
    )
    return True


def write_current_gap_snapshot(connection: sqlite3.Connection, source_heat_id: int) -> bool:
    """Seed the newest materialized table coordinate during an online backfill."""

    frame = connection.execute(
        """
        SELECT frame.id AS frame_id,frame.received_at_us,frame.frame_sequence,
               frame.ingest_connection_id,message.id AS message_id,message.ordinal
        FROM source_heats AS heat
        JOIN feed_frames AS frame ON frame.analysis_session_id = heat.analysis_session_id
        JOIN feed_messages AS message ON message.frame_id = frame.id
        WHERE heat.id = ? AND frame.processed_at_us IS NOT NULL
        ORDER BY frame.id DESC,message.ordinal DESC LIMIT 1
        """,
        (source_heat_id,),
    ).fetchone()
    if frame is None:
        return False
    rows = connection.execute(
        """
        SELECT participant.id AS participant_id,state.position_overall,state.position_class,
               state.gap_raw,fact.source_cell_observation_id,fact.source_message_id,
               fact.source_key AS source_cell_key,fact.observed_at_us AS source_cell_observed_at_us
        FROM participants AS participant
        JOIN participant_state_current AS state
          ON state.source_heat_id = participant.source_heat_id AND state.participant_id = participant.id
        LEFT JOIN participant_interval_source_facts AS fact ON fact.id = state.gap_interval_fact_id
        WHERE participant.source_heat_id = ?
        ORDER BY state.position_overall,participant.id
        """,
        (source_heat_id,),
    ).fetchall()
    if not rows:
        return False
    inputs = [
        GapCoordinateInput(
            participant_id=str(row["participant_id"]),
            position_overall=int(row["position_overall"]) if row["position_overall"] is not None else None,
            position_class=int(row["position_class"]) if row["position_class"] is not None else None,
            raw_gap_value=row["gap_raw"],
            source_cell_observation_id=(
                int(row["source_cell_observation_id"])
                if row["source_cell_observation_id"] is not None
                else None
            ),
            source_cell_message_id=(
                int(row["source_message_id"]) if row["source_message_id"] is not None else None
            ),
            source_cell_key=row["source_cell_key"],
            source_cell_observed_at_us=(
                int(row["source_cell_observed_at_us"])
                if row["source_cell_observed_at_us"] is not None
                else None
            ),
        )
        for row in rows
    ]
    source_key = f"{frame['ingest_connection_id']}:{frame['frame_sequence']}:{frame['ordinal']}"
    return write_gap_coordinate_snapshot(
        connection,
        source_heat_id=source_heat_id,
        source_frame_id=int(frame["frame_id"]),
        source_message_id=int(frame["message_id"]),
        source_key=source_key,
        observed_at_us=int(frame["received_at_us"]),
        rows=inputs,
    )
