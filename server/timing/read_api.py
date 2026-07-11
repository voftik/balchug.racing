"""Bounded, read-only views for the live timing dashboard.

This module is deliberately independent of FastAPI.  It opens SQLite in
``mode=ro`` for every public read, pins all queries for that read to one
snapshot, and returns ordinary JSON-ready dictionaries.  The HTTP layer can
therefore expose the same stable contract through REST and SSE without gaining
permission to mutate timing facts.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .config import now_us
from .db import connect
from .metric_store import PLAYBACK_PAYLOAD_CODEC, PLAYBACK_PROJECTION_VERSION
from .normalization import OPEN_ENDED_TS_TIME, parse_ts_time, time_service_to_unix_us
from .playback import PLAYBACK_SCHEMA_VERSION


US_PER_SECOND = 1_000_000
US_PER_DAY = 86_400 * US_PER_SECOND
LIVE_FRESHNESS_US = 3 * US_PER_SECOND
STALE_FRESHNESS_US = 10 * US_PER_SECOND
MAX_HISTORY_RANGE_US = US_PER_DAY
MAX_CHART_POINTS = 720
DEFAULT_FACT_LIMIT = 200
MAX_FACT_LIMIT = 500
LIVE_SCHEMA_VERSION = "timing-live.v1"
ARCHIVE_SCHEMA_VERSION = PLAYBACK_SCHEMA_VERSION
MAX_ARCHIVE_SESSIONS = 100
# Markers are limited independently from chart keyframes.  A 24-hour stint can
# contain more than 500 own laps, so retain the entire plausible engineer
# timeline while keeping the public manifest bounded.
MAX_ARCHIVE_MARKERS = 2_000
ARCHIVE_FINALIZATION_WINDOW_US = 60 * US_PER_SECOND
# Legacy player series for our car and one selected competitor are bounded.
# The additive archive ``lap_series.competitors`` contract deliberately keeps
# every raw competitor lap: a per-lap engineer view must not silently discard
# a slow, pit-affected, or time-less source observation.
MAX_ARCHIVE_COMPARISON_LAPS_PER_PARTICIPANT = 2_000
ARCHIVE_COMPARISON_LAP_BUCKET_US = 60 * US_PER_SECOND
# Time Service emits an explicit server clock about every 30 seconds.  Archive
# playback remains seekable on receive time, while this bounded map lets a UI
# label its x-axis with the clock actually shown on the timing dashboard.
ARCHIVE_SOURCE_CLOCK_MAX_INTERPOLATION_US = 90 * US_PER_SECOND
MAX_ARCHIVE_SOURCE_CLOCK_ANCHORS = 4_000
PIT_LANE_DURATION_SOURCE_KIND = "RESULT_L_PIT"

SCOPE_KINDS = frozenset(("participant", "class", "session"))
FreshnessStatus = Literal["LIVE", "STALE", "OFFLINE"]


class TimingReadError(RuntimeError):
    """Base class for a rejected read-model request or corrupt read value."""


class SessionNotFoundError(TimingReadError):
    """The requested durable analysis session does not exist."""


class ReadValidationError(TimingReadError):
    """A read filter would be ambiguous, unbounded, or unsupported."""


class ScopeNotFoundError(TimingReadError):
    """A requested metric scope is not a source-derived scope in this heat."""


class ArchiveProjectionMissingError(TimingReadError):
    """A stopped session has no durable historical playback projection yet."""


@dataclass(frozen=True)
class MetricScopeRequest:
    """One allowlisted, source-derived metric scope selected by the reader."""

    kind: str
    key: str


@dataclass(frozen=True)
class Freshness:
    """Read-time channel health; it is never persisted back into timing.db."""

    status: FreshnessStatus
    age_ms: int | None
    observed_at_us: int | None
    source_key: str | None
    open_gap: Mapping[str, Any] | None
    reason: str
    computed_at_us: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "age_ms": self.age_ms,
            "observed_at_us": self.observed_at_us,
            "source_key": self.source_key,
            "open_gap": dict(self.open_gap) if self.open_gap is not None else None,
            "reason": self.reason,
            "computed_at_us": self.computed_at_us,
        }


@dataclass(frozen=True)
class TimingSnapshot:
    """One coherent, JSON-ready dashboard snapshot for an analysis session."""

    session: Mapping[str, Any]
    heat: Mapping[str, Any] | None
    freshness: Freshness
    measured: Mapping[str, Any]
    computed: Mapping[str, Any]
    stream_cursor: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": LIVE_SCHEMA_VERSION,
            "session": dict(self.session),
            "heat": dict(self.heat) if self.heat is not None else None,
            "freshness": self.freshness.as_dict(),
            "measured": dict(self.measured),
            "computed": dict(self.computed),
            "cursor": {"stream_event_id": self.stream_cursor},
            "barrier": {"stream_event_id": self.stream_cursor},
            "system_assumption": _system_assumptions(),
            "provenance_contract": _provenance_contract(),
        }


@dataclass(frozen=True)
class TimingReadModel:
    """Small façade used by the HTTP process and deterministic tests.

    ``clock`` is injected only for read-time freshness tests.  It has no effect
    on persisted event timestamps or calculated tactical metrics.
    """

    database: str | Path | None = None
    clock: Callable[[], int] = now_us

    def snapshot(self, session_id: str, *, now_at_us: int | None = None) -> TimingSnapshot:
        return read_snapshot(
            session_id,
            database=self.database,
            now_at_us=self.clock() if now_at_us is None else now_at_us,
        )

    def current_metrics(
        self,
        session_id: str,
        *,
        scope: MetricScopeRequest | None = None,
        now_at_us: int | None = None,
    ) -> dict[str, Any]:
        return read_current_metrics(
            session_id,
            database=self.database,
            scope=scope,
            now_at_us=self.clock() if now_at_us is None else now_at_us,
        )

    def metric_history(
        self,
        session_id: str,
        *,
        scope: MetricScopeRequest,
        from_at_us: int | None = None,
        to_at_us: int | None = None,
        max_points: int = MAX_CHART_POINTS,
        now_at_us: int | None = None,
    ) -> dict[str, Any]:
        return read_metric_history(
            session_id,
            database=self.database,
            scope=scope,
            from_at_us=from_at_us,
            to_at_us=to_at_us,
            max_points=max_points,
            now_at_us=self.clock() if now_at_us is None else now_at_us,
        )

    def laps(
        self,
        session_id: str,
        *,
        participant_id: str | None = None,
        from_at_us: int | None = None,
        to_at_us: int | None = None,
        limit: int = DEFAULT_FACT_LIMIT,
    ) -> dict[str, Any]:
        return read_laps(
            session_id,
            database=self.database,
            participant_id=participant_id,
            from_at_us=from_at_us,
            to_at_us=to_at_us,
            limit=limit,
        )

    def pit_stops(
        self,
        session_id: str,
        *,
        participant_id: str | None = None,
        from_at_us: int | None = None,
        to_at_us: int | None = None,
        limit: int = DEFAULT_FACT_LIMIT,
    ) -> dict[str, Any]:
        return read_pit_stops(
            session_id,
            database=self.database,
            participant_id=participant_id,
            from_at_us=from_at_us,
            to_at_us=to_at_us,
            limit=limit,
        )

    def archived_sessions(self, *, limit: int = 50) -> dict[str, Any]:
        return read_archived_sessions(database=self.database, limit=limit)

    def archive_manifest(
        self,
        session_id: str,
        *,
        generation: int | None = None,
        max_points: int = MAX_CHART_POINTS,
    ) -> dict[str, Any]:
        return read_archive_manifest(
            session_id,
            database=self.database,
            generation=generation,
            max_points=max_points,
        )

    def archive_comparison(
        self,
        session_id: str,
        *,
        generation: int | None = None,
        mode: Literal["all", "participant"] = "all",
        participant_id: str | None = None,
        max_points: int = MAX_CHART_POINTS,
    ) -> dict[str, Any]:
        return read_archive_comparison(
            session_id,
            database=self.database,
            generation=generation,
            mode=mode,
            participant_id=participant_id,
            max_points=max_points,
        )

    def archive_snapshot(
        self,
        session_id: str,
        *,
        at_us: int | None = None,
        generation: int | None = None,
    ) -> dict[str, Any]:
        return read_archive_snapshot(
            session_id,
            database=self.database,
            at_us=at_us,
            generation=generation,
        )


def _provenance_contract() -> dict[str, Any]:
    """Return a literal so callers cannot mutate shared module state."""

    return {
        "version": 1,
        "measured": {
            "meaning": "Normalized source observations retained from the timing provider.",
            "paths": ["measured", "freshness"],
        },
        "computed": {
            "meaning": "Deterministic tactical calculations materialized by the metric engine.",
            "paths": ["computed.metrics"],
        },
        "system_assumption": {
            "meaning": "Product rules fixed by the system, not values entered by an engineer.",
            "paths": ["system_assumption"],
        },
    }


def _system_assumptions() -> dict[str, Any]:
    return {
        "tyre_change_on_confirmed_pit_out": True,
        "tyre_age_laps": "completed laps in the automatically reconstructed stint",
        "manual_tactical_inputs": False,
    }


@contextmanager
def _readonly_snapshot(database: str | Path | None) -> Iterator[sqlite3.Connection]:
    """Open and pin one SQLite read transaction without ever writing state."""

    connection = connect(database, readonly=True)
    try:
        connection.execute("BEGIN")
        yield connection
    finally:
        if connection.in_transaction:
            connection.rollback()
        connection.close()


def _require_session_id(session_id: str) -> str:
    if not isinstance(session_id, str) or not session_id.strip() or len(session_id) > 255:
        raise ReadValidationError("session_id must be a non-empty string up to 255 characters")
    return session_id


def _require_timestamp(name: str, value: int | None) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise ReadValidationError(f"{name} must be a non-negative integer timestamp in microseconds")
    return value


def _require_now(now_at_us: int | None) -> int:
    if now_at_us is None:
        return now_us()
    if type(now_at_us) is not int or now_at_us < 0:
        raise ReadValidationError("now_at_us must be a non-negative integer timestamp in microseconds")
    return now_at_us


def _require_limit(limit: int) -> int:
    if type(limit) is not int or not 1 <= limit <= MAX_FACT_LIMIT:
        raise ReadValidationError(f"limit must be an integer from 1 through {MAX_FACT_LIMIT}")
    return limit


def _validate_range(
    *,
    from_at_us: int | None,
    to_at_us: int | None,
    max_range_us: int = MAX_HISTORY_RANGE_US,
) -> tuple[int | None, int | None]:
    start = _require_timestamp("from_at_us", from_at_us)
    end = _require_timestamp("to_at_us", to_at_us)
    if start is not None and end is not None:
        if end < start:
            raise ReadValidationError("to_at_us must not be earlier than from_at_us")
        if end - start > max_range_us:
            raise ReadValidationError("requested time range must not exceed 24 hours")
    return start, end


def _class_key(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.casefold().split())
    return normalized or None


def _has_measured_pit_lane_duration(row: sqlite3.Row) -> bool:
    """Return whether the row has an explicit, usable Time Service L-PIT fact."""

    return (
        row["pit_lane_duration_source_kind"] == PIT_LANE_DURATION_SOURCE_KIND
        and row["pit_lane_ms"] is not None
    )


def _published_pit_lane_ms(row: sqlite3.Row) -> int | None:
    """Expose a pit duration only when its L-PIT source is explicit.

    Older captures persisted an inferred boundary delta in ``pit_lane_ms``.
    Those rows remain useful pit in/out chronology, but must not reach an
    engineer-facing payload as a Time Service measured duration.
    """

    return row["pit_lane_ms"] if _has_measured_pit_lane_duration(row) else None


def _json_object(value: Any, *, context: str) -> dict[str, Any]:
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as error:
        raise TimingReadError(f"Stored {context} JSON is invalid") from error
    if not isinstance(decoded, dict):
        raise TimingReadError(f"Stored {context} JSON must be an object")
    return decoded


def _json_value(value: Any, *, context: str) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError) as error:
        raise TimingReadError(f"Stored {context} JSON is invalid") from error


def _session_row(connection: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT s.id,ts.slug AS source_slug,ts.source_url,ts.display_name AS source_name,
               s.timezone_name,s.mode,s.lifecycle,s.race_duration_s,s.required_pits,
               s.our_participant_id,s.our_class,s.identity_state,s.started_at_us,
               s.stopped_at_us,s.stop_intent,s.created_at_us,s.updated_at_us
        FROM analysis_sessions s
        JOIN timing_sources ts ON ts.id = s.source_id
        WHERE s.id = ?
        """,
        (session_id,),
    ).fetchone()
    if row is None:
        raise SessionNotFoundError(f"Analysis session not found: {session_id}")
    return row


def _session_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "source_slug": row["source_slug"],
        "source_url": row["source_url"],
        "source_name": row["source_name"],
        "timezone_name": row["timezone_name"],
        "mode": row["mode"],
        "lifecycle": row["lifecycle"],
        "race_duration_s": row["race_duration_s"],
        "required_pits": row["required_pits"],
        "our_participant_id": row["our_participant_id"],
        "our_class": row["our_class"],
        "identity_state": row["identity_state"],
        "started_at_us": row["started_at_us"],
        "stopped_at_us": row["stopped_at_us"],
        "stop_intent": row["stop_intent"],
        "created_at_us": row["created_at_us"],
        "updated_at_us": row["updated_at_us"],
    }


def _latest_heat_row(connection: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id,analysis_session_id,generation,external_name,provider_started_at_us,
               provider_finished_at_us,created_at_us
        FROM source_heats
        WHERE analysis_session_id = ?
        ORDER BY generation DESC,id DESC
        LIMIT 1
        """,
        (session_id,),
    ).fetchone()


def _stream_cursor(connection: sqlite3.Connection, session_id: str) -> int:
    """Return the highest durable SSE cursor visible in this read snapshot."""

    row = connection.execute(
        "SELECT MAX(id) AS cursor FROM stream_events WHERE analysis_session_id = ?",
        (session_id,),
    ).fetchone()
    return int(row["cursor"]) if row is not None and row["cursor"] is not None else 0


def _heat_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "source_heat_id": int(row["id"]),
        "generation": int(row["generation"]),
        "external_name": row["external_name"],
        "provider_started_at_us": row["provider_started_at_us"],
        "provider_finished_at_us": row["provider_finished_at_us"],
        "created_at_us": row["created_at_us"],
    }


def _open_gap(connection: sqlite3.Connection, heat_id: int) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT started_at_us,reason,ingest_connection_id
        FROM ingest_gaps
        WHERE ended_at_us IS NULL
          AND (
            source_heat_id = ?
            OR (
              source_heat_id IS NULL
              AND analysis_session_id = (
                SELECT analysis_session_id FROM source_heats WHERE id = ?
              )
            )
          )
        ORDER BY started_at_us DESC,id DESC
        LIMIT 1
        """,
        (heat_id, heat_id),
    ).fetchone()
    return (
        {
            "started_at_us": int(row["started_at_us"]),
            "reason": row["reason"],
            "connection_id": row["ingest_connection_id"],
        }
        if row is not None
        else None
    )


def _current_flag(connection: sqlite3.Connection, heat_id: int) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT flag,provider_code,provider_label,started_at_us,observed_started_at_us,
               calibrated_started_at_us,start_provider_ts_raw,start_provider_ts_us,
               source_flag_kind_raw,reconciliation_key,source_message_id,source_key,
               updated_at_us,reconciled_at_us
        FROM track_flag_current
        WHERE source_heat_id = ?
        """,
        (heat_id,),
    ).fetchone()
    if row is None:
        return None
    authoritative_started_at_us = (
        row["calibrated_started_at_us"]
        if row["calibrated_started_at_us"] is not None
        else row["observed_started_at_us"]
        if row["observed_started_at_us"] is not None
        else row["started_at_us"]
    )
    return {
        "flag": row["flag"],
        "provider_code": row["provider_code"],
        "provider_label": row["provider_label"],
        "source_flag_kind_raw": row["source_flag_kind_raw"],
        "started_at_us": int(row["started_at_us"]),
        "authoritative_started_at_us": int(authoritative_started_at_us),
        "observed_started_at_us": row["observed_started_at_us"],
        "calibrated_started_at_us": row["calibrated_started_at_us"],
        "start_provider_ts_raw": row["start_provider_ts_raw"],
        "start_provider_ts_us": row["start_provider_ts_us"],
        "reconciliation_key": row["reconciliation_key"],
        "reconciled_at_us": row["reconciled_at_us"],
        "source": {
            "message_id": row["source_message_id"],
            "key": row["source_key"],
            "observed_at_us": int(row["updated_at_us"]),
        },
    }


def _statistics(connection: sqlite3.Connection, heat_id: int) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT heat_name_raw,green_flag_provider_ts_raw,green_flag_provider_ts_us,green_flag_at_us,
               finish_flag_provider_ts_raw,finish_flag_provider_ts_us,finish_flag_at_us,
               participants_started,participants_classified,participants_not_classified,
               participants_on_track,participants_in_pit_zone,participants_in_tank_zone,
               total_laps,total_pitstops,leader_laps_green,leader_laps_safety_car,
               leader_laps_code_60,leader_laps_full_course_yellow,safety_car_count,
               code_60_count,full_course_yellow_count,safety_car_total_time_raw,
               code_60_total_time_raw,full_course_yellow_total_time_raw,source_message_id,
               source_key,observed_at_us
        FROM heat_statistics_current
        WHERE source_heat_id = ?
        """,
        (heat_id,),
    ).fetchone()
    if row is None:
        return None
    values = {
        key: row[key]
        for key in (
            "heat_name_raw",
            "green_flag_provider_ts_raw",
            "green_flag_provider_ts_us",
            "green_flag_at_us",
            "finish_flag_provider_ts_raw",
            "finish_flag_provider_ts_us",
            "finish_flag_at_us",
            "participants_started",
            "participants_classified",
            "participants_not_classified",
            "participants_on_track",
            "participants_in_pit_zone",
            "participants_in_tank_zone",
            "total_laps",
            "total_pitstops",
            "leader_laps_green",
            "leader_laps_safety_car",
            "leader_laps_code_60",
            "leader_laps_full_course_yellow",
            "safety_car_count",
            "code_60_count",
            "full_course_yellow_count",
            "safety_car_total_time_raw",
            "code_60_total_time_raw",
            "full_course_yellow_total_time_raw",
        )
    }
    values["source"] = {
        "message_id": row["source_message_id"],
        "key": row["source_key"],
        "observed_at_us": int(row["observed_at_us"]),
    }
    return values


def _participants(connection: sqlite3.Connection, heat_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT p.id,p.external_key,p.transponder_id,p.start_number,p.team_name,p.car_name,
               p.class_name,p.class_name_key,p.is_ours,p.active,p.first_seen_at_us,p.last_seen_at_us,
               c.position_overall,c.position_class,c.marker,c.laps,c.state,c.state_raw,c.state_kind,
               c.current_driver_name,c.current_driver_stint_raw,c.last_lap_ms,c.last_lap_number,
               c.best_lap_ms,c.best_lap_number,c.last_sectors_json,c.best_sectors_json,
               c.last_speeds_json,c.gap_ms,c.gap_raw,c.gap_kind,c.diff_ms,c.diff_raw,c.diff_kind,
               c.sector_json,c.speed_kph,c.pit_time_raw,c.provider_pit_count,
               c.state_timer_target_raw,c.state_timer_target_provider_us,c.state_timer_target_at_us,
               c.provider_pit_count_raw,c.source_message_id,c.source_key,c.updated_at_us,
               i.driver_name_raw AS identity_driver_name,i.source_message_id AS identity_source_message_id,
               i.source_key AS identity_source_key,i.observed_at_us AS identity_observed_at_us
        FROM participants p
        LEFT JOIN participant_state_current c
          ON c.source_heat_id = p.source_heat_id AND c.participant_id = p.id
        LEFT JOIN participant_identity_segments i
          ON i.source_heat_id = p.source_heat_id AND i.participant_id = p.id AND i.ended_at_us IS NULL
        WHERE p.source_heat_id = ?
        ORDER BY
          CASE WHEN c.position_class IS NULL THEN 1 ELSE 0 END,
          c.position_class,p.start_number,p.id
        """,
        (heat_id,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        driver_name = row["current_driver_name"] or row["identity_driver_name"]
        state = None
        if row["source_key"] is not None:
            state = {
                "position_overall": row["position_overall"],
                "position_class": row["position_class"],
                "marker": row["marker"],
                "laps": row["laps"],
                "state": row["state"],
                "state_raw": row["state_raw"],
                "state_kind": row["state_kind"],
                "current_driver_name": row["current_driver_name"],
                "current_driver_stint_raw": row["current_driver_stint_raw"],
                "last_lap_ms": row["last_lap_ms"],
                "last_lap_number": row["last_lap_number"],
                "best_lap_ms": row["best_lap_ms"],
                "best_lap_number": row["best_lap_number"],
                "last_sectors": _json_value(row["last_sectors_json"], context="participant last sectors"),
                "best_sectors": _json_value(row["best_sectors_json"], context="participant best sectors"),
                "last_speeds": _json_value(row["last_speeds_json"], context="participant last speeds"),
                "gap_ms": row["gap_ms"],
                "gap_raw": row["gap_raw"],
                "gap_kind": row["gap_kind"],
                "diff_ms": row["diff_ms"],
                "diff_raw": row["diff_raw"],
                "diff_kind": row["diff_kind"],
                "sector": _json_value(row["sector_json"], context="participant sector"),
                "speed_kph": row["speed_kph"],
                "pit_time_raw": row["pit_time_raw"],
                "provider_pit_count": row["provider_pit_count"],
                "provider_pit_count_raw": row["provider_pit_count_raw"],
                "state_timer_target_raw": row["state_timer_target_raw"],
                "state_timer_target_provider_us": row["state_timer_target_provider_us"],
                "state_timer_target_at_us": row["state_timer_target_at_us"],
                "source": {
                    "message_id": row["source_message_id"],
                    "key": row["source_key"],
                    "observed_at_us": row["updated_at_us"],
                },
            }
        identity_source = (
            {
                "message_id": row["identity_source_message_id"],
                "key": row["identity_source_key"],
                "observed_at_us": row["identity_observed_at_us"],
            }
            if row["identity_source_key"] is not None
            else None
        )
        result.append(
            {
                "participant_id": row["id"],
                "external_key": row["external_key"],
                "transponder_id": row["transponder_id"],
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "driver_name": driver_name,
                "car_name": row["car_name"],
                "class_name": row["class_name"],
                "class_key": row["class_name_key"] or _class_key(row["class_name"]),
                "is_ours": bool(row["is_ours"]),
                "active": bool(row["active"]),
                "first_seen_at_us": row["first_seen_at_us"],
                "last_seen_at_us": row["last_seen_at_us"],
                "identity_source": identity_source,
                "state": state,
            }
        )
    return result


def _latest_tick(connection: sqlite3.Connection, heat_id: int) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT observed_at_us,source_key,state_hash
        FROM state_ticks
        WHERE source_heat_id = ?
        ORDER BY observed_second DESC
        LIMIT 1
        """,
        (heat_id,),
    ).fetchone()


def _freshness(
    *,
    session: sqlite3.Row,
    heat: sqlite3.Row | None,
    flag: Mapping[str, Any] | None,
    tick: sqlite3.Row | None,
    gap: Mapping[str, Any] | None,
    now_at_us: int,
) -> Freshness:
    observed_at_us = int(tick["observed_at_us"]) if tick is not None else None
    source_key = tick["source_key"] if tick is not None else None
    age_ms = max(0, (now_at_us - observed_at_us) // 1_000) if observed_at_us is not None else None
    lifecycle = session["lifecycle"]
    if lifecycle in {"stopped", "aborted"}:
        return Freshness(
            status="OFFLINE",
            age_ms=age_ms,
            observed_at_us=observed_at_us,
            source_key=source_key,
            open_gap=gap,
            reason=f"session_{lifecycle}",
            computed_at_us=now_at_us,
        )
    if heat is None:
        return Freshness(
            status="OFFLINE",
            age_ms=None,
            observed_at_us=None,
            source_key=None,
            open_gap=None,
            reason="no_source_heat",
            computed_at_us=now_at_us,
        )
    if flag is not None and flag["flag"] == "FINISH":
        return Freshness(
            status="OFFLINE",
            age_ms=age_ms,
            observed_at_us=observed_at_us,
            source_key=source_key,
            open_gap=gap,
            reason="track_finished",
            computed_at_us=now_at_us,
        )
    if gap is not None:
        return Freshness(
            status="OFFLINE",
            age_ms=age_ms,
            observed_at_us=observed_at_us,
            source_key=source_key,
            open_gap=gap,
            reason="source_gap",
            computed_at_us=now_at_us,
        )
    if observed_at_us is None:
        return Freshness(
            status="OFFLINE",
            age_ms=None,
            observed_at_us=None,
            source_key=None,
            open_gap=gap,
            reason="no_state_tick",
            computed_at_us=now_at_us,
        )
    age_us = max(0, now_at_us - observed_at_us)
    if age_us <= LIVE_FRESHNESS_US:
        status: FreshnessStatus = "LIVE"
        reason = "fresh"
    elif age_us <= STALE_FRESHNESS_US:
        status = "STALE"
        reason = "stale"
    else:
        status = "OFFLINE"
        reason = "source_timeout"
    return Freshness(
        status=status,
        age_ms=age_ms,
        observed_at_us=observed_at_us,
        source_key=source_key,
        open_gap=gap,
        reason=reason,
        computed_at_us=now_at_us,
    )


def _validate_scope_request(scope: MetricScopeRequest) -> MetricScopeRequest:
    if not isinstance(scope, MetricScopeRequest):
        raise ReadValidationError("scope must be a MetricScopeRequest")
    if scope.kind not in SCOPE_KINDS:
        allowed = ", ".join(sorted(SCOPE_KINDS))
        raise ReadValidationError(f"scope.kind must be one of: {allowed}")
    if not isinstance(scope.key, str) or not scope.key.strip() or len(scope.key) > 255:
        raise ReadValidationError("scope.key must be a non-empty string up to 255 characters")
    return scope


def _validate_scope_exists(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    heat_id: int,
    scope: MetricScopeRequest,
) -> None:
    scope = _validate_scope_request(scope)
    if scope.kind == "session":
        if scope.key == session_id:
            return
    elif scope.kind == "participant":
        exists = connection.execute(
            "SELECT 1 FROM participants WHERE source_heat_id = ? AND id = ?",
            (heat_id, scope.key),
        ).fetchone()
        if exists is not None:
            return
    else:
        rows = connection.execute(
            "SELECT class_name_key,class_name FROM participants WHERE source_heat_id = ?",
            (heat_id,),
        ).fetchall()
        if any((row["class_name_key"] or _class_key(row["class_name"])) == scope.key for row in rows):
            return
    raise ScopeNotFoundError(f"Metric scope does not exist in this source heat: {scope.kind}/{scope.key}")


def _metric_current(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    heat_id: int,
    scope: MetricScopeRequest | None = None,
) -> list[dict[str, Any]]:
    parameters: list[Any] = [heat_id]
    where = ["source_heat_id = ?"]
    if scope is not None:
        _validate_scope_exists(connection, session_id=session_id, heat_id=heat_id, scope=scope)
        where.extend(("scope_kind = ?", "scope_key = ?"))
        parameters.extend((scope.kind, scope.key))
    rows = connection.execute(
        f"""
        SELECT scope_kind,scope_key,observed_at_us,metric_version,values_json,
               source_message_id,source_key
        FROM metric_current
        WHERE {' AND '.join(where)}
        ORDER BY CASE scope_kind WHEN 'session' THEN 0 WHEN 'class' THEN 1 ELSE 2 END,scope_key
        """,
        tuple(parameters),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        stored_scope = MetricScopeRequest(kind=row["scope_kind"], key=row["scope_key"])
        # A stale or malformed materialization must not leak a non-source scope.
        _validate_scope_exists(connection, session_id=session_id, heat_id=heat_id, scope=stored_scope)
        result.append(
            {
                "scope": {"kind": stored_scope.kind, "key": stored_scope.key},
                "observed_at_us": int(row["observed_at_us"]),
                "metric_version": int(row["metric_version"]),
                "values": _json_object(row["values_json"], context="current metric"),
                "source": {"message_id": row["source_message_id"], "key": row["source_key"]},
                "provenance": "computed",
            }
        )
    return result


def _require_heat(heat: sqlite3.Row | None) -> sqlite3.Row:
    if heat is None:
        raise ScopeNotFoundError("Analysis session has no source heat yet")
    return heat


def read_snapshot(
    session_id: str,
    *,
    database: str | Path | None = None,
    now_at_us: int | None = None,
) -> TimingSnapshot:
    """Return one source-consistent dashboard snapshot without mutating SQLite."""

    session_id = _require_session_id(session_id)
    evaluation_at_us = _require_now(now_at_us)
    with _readonly_snapshot(database) as connection:
        session = _session_row(connection, session_id)
        heat = _latest_heat_row(connection, session_id)
        if heat is None:
            freshness = _freshness(
                session=session,
                heat=None,
                flag=None,
                tick=None,
                gap=None,
                now_at_us=evaluation_at_us,
            )
            return TimingSnapshot(
                session=_session_payload(session),
                heat=None,
                freshness=freshness,
                measured={"track_flag": None, "statistics": None, "participants": []},
                computed={"metrics": []},
                stream_cursor=_stream_cursor(connection, session_id),
            )
        heat_id = int(heat["id"])
        flag = _current_flag(connection, heat_id)
        statistics = _statistics(connection, heat_id)
        gap = _open_gap(connection, heat_id)
        tick = _latest_tick(connection, heat_id)
        return TimingSnapshot(
            session=_session_payload(session),
            heat=_heat_payload(heat),
            freshness=_freshness(
                session=session,
                heat=heat,
                flag=flag,
                tick=tick,
                gap=gap,
                now_at_us=evaluation_at_us,
            ),
            measured={
                "track_flag": flag,
                "statistics": statistics,
                "participants": _participants(connection, heat_id),
            },
            computed={"metrics": _metric_current(connection, session_id=session_id, heat_id=heat_id)},
            stream_cursor=_stream_cursor(connection, session_id),
        )


def read_current_metrics(
    session_id: str,
    *,
    database: str | Path | None = None,
    scope: MetricScopeRequest | None = None,
    now_at_us: int | None = None,
) -> dict[str, Any]:
    """Return current metric materializations for one source-derived scope."""

    session_id = _require_session_id(session_id)
    evaluation_at_us = _require_now(now_at_us)
    if scope is not None:
        scope = _validate_scope_request(scope)
    with _readonly_snapshot(database) as connection:
        session = _session_row(connection, session_id)
        heat = _latest_heat_row(connection, session_id)
        if heat is None:
            if scope is not None and (scope.kind != "session" or scope.key != session_id):
                raise ScopeNotFoundError("Analysis session has no source heat for this metric scope yet")
            return {
                "schema_version": LIVE_SCHEMA_VERSION,
                "session_id": session_id,
                "heat": None,
                "freshness": _freshness(
                    session=session,
                    heat=None,
                    flag=None,
                    tick=None,
                    gap=None,
                    now_at_us=evaluation_at_us,
                ).as_dict(),
                "metrics": [],
                "cursor": {"stream_event_id": _stream_cursor(connection, session_id)},
                "barrier": {"stream_event_id": _stream_cursor(connection, session_id)},
                "provenance_contract": _provenance_contract(),
            }
        heat_id = int(heat["id"])
        return {
            "schema_version": LIVE_SCHEMA_VERSION,
            "session_id": session_id,
            "heat": _heat_payload(heat),
            "freshness": _freshness(
                session=session,
                heat=heat,
                flag=_current_flag(connection, heat_id),
                tick=_latest_tick(connection, heat_id),
                gap=_open_gap(connection, heat_id),
                now_at_us=evaluation_at_us,
            ).as_dict(),
            "metrics": _metric_current(connection, session_id=session_id, heat_id=heat_id, scope=scope),
            "cursor": {"stream_event_id": _stream_cursor(connection, session_id)},
            "barrier": {"stream_event_id": _stream_cursor(connection, session_id)},
            "provenance_contract": _provenance_contract(),
        }


def _history_bounds(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    scope: MetricScopeRequest,
    from_at_us: int | None,
    to_at_us: int | None,
    now_at_us: int,
) -> tuple[int, int]:
    start, end = _validate_range(from_at_us=from_at_us, to_at_us=to_at_us)
    if end is None:
        latest = connection.execute(
            """
            SELECT observed_at_us FROM metric_samples
            WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ?
            ORDER BY observed_at_us DESC,observed_second DESC
            LIMIT 1
            """,
            (heat_id, scope.kind, scope.key),
        ).fetchone()
        end = int(latest["observed_at_us"]) if latest is not None else now_at_us
    if start is None:
        start = max(0, end - MAX_HISTORY_RANGE_US)
    if end < start or end - start > MAX_HISTORY_RANGE_US:
        raise ReadValidationError("requested time range must not exceed 24 hours")
    return start, end


def _require_max_points(max_points: int) -> int:
    if type(max_points) is not int or not 2 <= max_points <= MAX_CHART_POINTS:
        raise ReadValidationError(f"max_points must be an integer from 2 through {MAX_CHART_POINTS}")
    return max_points


def _sampled_metric_rows(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    scope: MetricScopeRequest,
    from_at_us: int,
    to_at_us: int,
    max_points: int,
) -> tuple[list[sqlite3.Row], int]:
    where_parameters = (heat_id, scope.kind, scope.key, from_at_us, to_at_us)
    count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM metric_samples
            WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ?
              AND observed_at_us >= ? AND observed_at_us <= ?
            """,
            where_parameters,
        ).fetchone()[0]
    )
    if count <= max_points:
        rows = connection.execute(
            """
            SELECT observed_at_us,metric_version,values_json,source_message_id,source_key
            FROM metric_samples
            WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ?
              AND observed_at_us >= ? AND observed_at_us <= ?
            ORDER BY observed_at_us,observed_second
            """,
            where_parameters,
        ).fetchall()
        return list(rows), count

    # Keep the first row in each of max_points - 1 chronological buckets plus
    # the true final row.  This stays bounded at max_points while retaining
    # both endpoint values for a tactical chart.
    rows = connection.execute(
        """
        WITH ordered AS (
          SELECT observed_at_us,observed_second,metric_version,values_json,
                 source_message_id,source_key,
                 ROW_NUMBER() OVER (ORDER BY observed_at_us,observed_second) AS ordinal,
                 COUNT(*) OVER () AS total,
                 NTILE(?) OVER (ORDER BY observed_at_us,observed_second) AS bucket
          FROM metric_samples
          WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ?
            AND observed_at_us >= ? AND observed_at_us <= ?
        ), bucketed AS (
          SELECT *,ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY ordinal) AS bucket_ordinal
          FROM ordered
        )
        SELECT observed_at_us,metric_version,values_json,source_message_id,source_key
        FROM bucketed
        WHERE bucket_ordinal = 1 OR ordinal = total
        ORDER BY ordinal
        """,
        (max_points - 1, *where_parameters),
    ).fetchall()
    return list(rows), count


def read_metric_history(
    session_id: str,
    *,
    scope: MetricScopeRequest,
    database: str | Path | None = None,
    from_at_us: int | None = None,
    to_at_us: int | None = None,
    max_points: int = MAX_CHART_POINTS,
    now_at_us: int | None = None,
) -> dict[str, Any]:
    """Return at most 720 source-derived chart points over no more than 24h."""

    session_id = _require_session_id(session_id)
    scope = _validate_scope_request(scope)
    max_points = _require_max_points(max_points)
    evaluation_at_us = _require_now(now_at_us)
    with _readonly_snapshot(database) as connection:
        _session_row(connection, session_id)
        heat = _latest_heat_row(connection, session_id)
        if heat is None:
            if scope.kind != "session" or scope.key != session_id:
                raise ScopeNotFoundError("Analysis session has no source heat for this metric scope yet")
            start, end = _validate_range(from_at_us=from_at_us, to_at_us=to_at_us)
            end = evaluation_at_us if end is None else end
            start = max(0, end - MAX_HISTORY_RANGE_US) if start is None else start
            if end - start > MAX_HISTORY_RANGE_US:
                raise ReadValidationError("requested time range must not exceed 24 hours")
            return {
                "session_id": session_id,
                "heat": None,
                "scope": {"kind": scope.kind, "key": scope.key},
                "from_at_us": start,
                "to_at_us": end,
                "source_point_count": 0,
                "downsampled": False,
                "points": [],
                "provenance_contract": _provenance_contract(),
            }
        heat_id = int(heat["id"])
        _validate_scope_exists(connection, session_id=session_id, heat_id=heat_id, scope=scope)
        start, end = _history_bounds(
            connection,
            heat_id=heat_id,
            scope=scope,
            from_at_us=from_at_us,
            to_at_us=to_at_us,
            now_at_us=evaluation_at_us,
        )
        rows, source_point_count = _sampled_metric_rows(
            connection,
            heat_id=heat_id,
            scope=scope,
            from_at_us=start,
            to_at_us=end,
            max_points=max_points,
        )
        return {
            "session_id": session_id,
            "heat": _heat_payload(heat),
            "scope": {"kind": scope.kind, "key": scope.key},
            "from_at_us": start,
            "to_at_us": end,
            "source_point_count": source_point_count,
            "downsampled": source_point_count > len(rows),
            "points": [
                {
                    "observed_at_us": int(row["observed_at_us"]),
                    "metric_version": int(row["metric_version"]),
                    "values": _json_object(row["values_json"], context="metric history"),
                    "source": {"message_id": row["source_message_id"], "key": row["source_key"]},
                    "provenance": "computed",
                }
                for row in rows
            ],
            "provenance_contract": _provenance_contract(),
        }


def _require_archived_session(connection: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    session = _session_row(connection, session_id)
    if session["lifecycle"] not in {"stopped", "aborted"}:
        raise ReadValidationError("Archive playback is available only for stopped or aborted sessions")
    return session


def _archive_window(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    capture_started_at_us: int,
    transport_tail_end_at_us: int,
    source_started_at_us: int | None,
) -> dict[str, int | str | None]:
    """Select the replayable interval without treating heartbeat tail as racing.

    Providers can continue sending transport keepalives long after a terminal
    flag. We retain one promptly observed terminal snapshot, then end the
    playback axis there. A capture that starts after FINISH collapses to one
    final snapshot instead of masquerading as a short race replay.
    """

    terminal = connection.execute(
        """
        SELECT MIN(COALESCE(calibrated_started_at_us,observed_started_at_us,started_at_us)) AS at_us
        FROM track_flag_periods
        WHERE source_heat_id = ? AND flag = 'FINISH'
          AND COALESCE(calibrated_started_at_us,observed_started_at_us,started_at_us) <= ?
        """,
        (heat_id, transport_tail_end_at_us),
    ).fetchone()["at_us"]
    finish_at_us = int(terminal) if terminal is not None else None
    view_ended_at_us = transport_tail_end_at_us
    if finish_at_us is not None:
        final = connection.execute(
            """
            SELECT observed_at_us
            FROM playback_snapshots
            WHERE source_heat_id = ? AND observed_at_us >= ?
            ORDER BY observed_at_us,observed_second
            LIMIT 1
            """,
            (heat_id, finish_at_us),
        ).fetchone()
        if final is not None and int(final["observed_at_us"]) - finish_at_us <= ARCHIVE_FINALIZATION_WINDOW_US:
            view_ended_at_us = int(final["observed_at_us"])
        elif finish_at_us < capture_started_at_us:
            view_ended_at_us = capture_started_at_us
        else:
            previous = connection.execute(
                """
                SELECT observed_at_us
                FROM playback_snapshots
                WHERE source_heat_id = ? AND observed_at_us <= ?
                ORDER BY observed_at_us DESC,observed_second DESC
                LIMIT 1
                """,
                (heat_id, finish_at_us),
            ).fetchone()
            if previous is not None:
                view_ended_at_us = int(previous["observed_at_us"])

    point_count = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM playback_snapshots
            WHERE source_heat_id = ? AND observed_at_us >= ? AND observed_at_us <= ?
            """,
            (heat_id, capture_started_at_us, view_ended_at_us),
        ).fetchone()[0]
    )
    omitted_tail_point_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM playback_snapshots WHERE source_heat_id = ? AND observed_at_us > ?",
            (heat_id, view_ended_at_us),
        ).fetchone()[0]
    )
    missing_prefix_us = (
        max(0, capture_started_at_us - int(source_started_at_us))
        if source_started_at_us is not None
        else None
    )
    coverage_kind = "replay"
    if finish_at_us is not None and capture_started_at_us > finish_at_us:
        coverage_kind = "terminal_snapshot"
    elif missing_prefix_us:
        coverage_kind = "partial_capture"
    return {
        "first_at_us": capture_started_at_us,
        "last_at_us": view_ended_at_us,
        "point_count": point_count,
        "capture_started_at_us": capture_started_at_us,
        "transport_tail_end_at_us": transport_tail_end_at_us,
        "finish_at_us": finish_at_us,
        "missing_prefix_us": missing_prefix_us,
        "omitted_tail_point_count": omitted_tail_point_count,
        "coverage_kind": coverage_kind,
    }


def _archive_heat_rows(connection: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT h.id,h.analysis_session_id,h.generation,h.external_name,h.provider_started_at_us,
               h.provider_finished_at_us,h.created_at_us,
               MIN(p.observed_at_us) AS raw_first_at_us,MAX(p.observed_at_us) AS raw_last_at_us,
               COUNT(*) AS raw_point_count,MIN(p.projection_version) AS projection_version,
               MAX(p.metric_version) AS metric_version
        FROM source_heats h
        JOIN playback_snapshots p ON p.source_heat_id = h.id
        WHERE h.analysis_session_id = ?
        GROUP BY h.id
        ORDER BY h.generation,h.id
        """,
        (session_id,),
    ).fetchall()
    heats: list[dict[str, Any]] = []
    for row in rows:
        heat = dict(row)
        heat.update(
            _archive_window(
                connection,
                heat_id=int(row["id"]),
                capture_started_at_us=int(row["raw_first_at_us"]),
                transport_tail_end_at_us=int(row["raw_last_at_us"]),
                source_started_at_us=row["provider_started_at_us"],
            )
        )
        heat["raw_point_count"] = int(row["raw_point_count"])
        heats.append(heat)
    return heats


def _select_archive_heat(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    generation: int | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if generation is not None and (type(generation) is not int or generation < 1):
        raise ReadValidationError("generation must be a positive integer")
    heats = _archive_heat_rows(connection, session_id)
    if not heats:
        raise ArchiveProjectionMissingError(
            "Archive playback projection is missing; rebuild this stopped session from retained raw frames"
        )
    if generation is None:
        if len(heats) != 1:
            raise ReadValidationError("generation is required when an archived session contains multiple heats")
        return heats[0], heats
    selected = next((heat for heat in heats if int(heat["generation"]) == generation), None)
    if selected is None:
        raise ScopeNotFoundError(f"Archived heat generation not found: {generation}")
    return selected, heats


def _decode_playback_payload(row: sqlite3.Row) -> dict[str, Any]:
    if row["payload_codec"] != PLAYBACK_PAYLOAD_CODEC:
        raise TimingReadError("Stored archive playback payload has an unsupported codec")
    if int(row["projection_version"]) != PLAYBACK_PROJECTION_VERSION:
        raise TimingReadError("Stored archive playback payload has an unsupported projection version")
    try:
        encoded = gzip.decompress(bytes(row["payload"]))
        digest = hashlib.sha256(encoded).hexdigest()
        payload = json.loads(encoded)
    except (OSError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TimingReadError("Stored archive playback payload is invalid") from error
    if digest != row["payload_sha256"]:
        raise TimingReadError("Stored archive playback payload hash does not match")
    if not isinstance(payload, dict) or payload.get("schema_version") != ARCHIVE_SCHEMA_VERSION:
        raise TimingReadError("Stored archive playback payload has an unsupported schema")
    if payload.get("observed_at_us") != int(row["observed_at_us"]):
        raise TimingReadError("Stored archive playback payload does not match its timeline boundary")
    return payload


def _manifest_playback_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Keep chart manifests small; detailed class state is seek-only data."""

    manifest_payload = dict(payload)
    manifest_payload.pop("class_participants", None)
    return manifest_payload


def _archive_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _archive_participant_state(entry: Mapping[str, Any]) -> Mapping[str, Any]:
    measured = entry.get("measured")
    measured = measured if isinstance(measured, Mapping) else {}
    state = measured.get("state")
    return state if isinstance(state, Mapping) else {}


def _archive_participant_values(entry: Mapping[str, Any]) -> Mapping[str, Any]:
    computed = entry.get("computed")
    return computed if isinstance(computed, Mapping) else {}


def _archive_participant_id(entry: Mapping[str, Any]) -> str | None:
    values = _archive_participant_values(entry)
    measured = entry.get("measured")
    measured = measured if isinstance(measured, Mapping) else {}
    return _comparison_text(values.get("participant_id"), measured.get("participant_id"))


def _archive_explicit_laps(entry: Mapping[str, Any]) -> int | None:
    """Read only the provider's current LAPS field, never tracker fallback."""

    laps = _archive_int(_archive_participant_state(entry).get("laps"))
    return laps if laps is not None and laps >= 0 else None


def _archive_source_time(entry: Mapping[str, Any], field: Literal["gap", "diff"]) -> int | None:
    state = _archive_participant_state(entry)
    value = _archive_int(state.get(f"{field}_ms"))
    if value is not None and state.get(f"{field}_kind") == "TIME":
        return value
    return _archive_int(_archive_participant_values(entry).get(f"source_{field}_ms"))


def _archive_position(entry: Mapping[str, Any]) -> int | None:
    values = _archive_participant_values(entry)
    state = _archive_participant_state(entry)
    value = _archive_int(values.get("position_overall"))
    if value is None:
        value = _archive_int(state.get("position_overall"))
    return value if value is not None and value >= 1 else None


def _archive_relative_gap_ms(
    ours: Mapping[str, Any], target: Mapping[str, Any],
) -> int | None:
    """Re-evaluate one source interval from immutable archive facts.

    Older playback projections can contain tracker-derived local lap counts
    when a partial capture had no grid LAPS column.  Those counts are not
    allowed to veto a provider TIME GAP.  Explicit source LAPS still protect
    against presenting a time interval for genuinely lapped cars.
    """

    if _archive_participant_id(ours) == _archive_participant_id(target):
        return 0
    ours_laps = _archive_explicit_laps(ours)
    target_laps = _archive_explicit_laps(target)
    if ours_laps is not None and target_laps is not None and ours_laps != target_laps:
        return None
    ours_gap = _archive_source_time(ours, "gap")
    target_gap = _archive_source_time(target, "gap")
    if ours_gap is not None and target_gap is not None:
        return abs(ours_gap - target_gap)
    ours_position = _archive_position(ours)
    target_position = _archive_position(target)
    if target_position == 1 and ours_gap is not None:
        return ours_gap
    if ours_position == 1 and target_gap is not None:
        return target_gap
    if ours_position is not None and target_position == ours_position - 1:
        return _archive_source_time(ours, "diff")
    if ours_position is not None and target_position == ours_position + 1:
        return _archive_source_time(target, "diff")
    return None


def _archive_interval_derivation(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return read-time interval values without mutating durable projections."""

    computed = payload.get("computed")
    computed = computed if isinstance(computed, Mapping) else {}
    session = computed.get("session")
    session = session if isinstance(session, Mapping) else {}
    ours_id = _comparison_text(session.get("ours_participant_id"))
    raw_participants = payload.get("class_participants")
    if ours_id is None or not isinstance(raw_participants, list):
        return {"lap_count_scope": "unknown"}
    participants: dict[str, Mapping[str, Any]] = {}
    for candidate in raw_participants:
        if not isinstance(candidate, Mapping):
            continue
        participant_id = _archive_participant_id(candidate)
        if participant_id is not None:
            participants[participant_id] = candidate
    ours = participants.get(ours_id)
    if ours is None:
        return {"lap_count_scope": "unknown"}
    result: dict[str, Any] = {
        "lap_count_scope": "source_grid" if _archive_explicit_laps(ours) is not None else "capture_tracker",
    }
    for relation, gap_key, lap_key in (
        ("class_leader", "gap_to_class_leader_ms", "lap_delta_to_class_leader"),
        ("class_ahead", "gap_to_ahead_ms", "lap_delta_to_ahead"),
        ("class_behind", "gap_to_behind_ms", "lap_delta_to_behind"),
    ):
        target_id = _comparison_text(session.get(f"{relation}_id"))
        target = participants.get(target_id) if target_id is not None else None
        result[gap_key] = _archive_relative_gap_ms(ours, target) if target is not None else None
        ours_laps = _archive_explicit_laps(ours)
        target_laps = _archive_explicit_laps(target) if target is not None else None
        result[lap_key] = (
            ours_laps - target_laps
            if ours_laps is not None and target_laps is not None
            else None
        )
    return result


def _with_archive_interval_derivation(payload: Mapping[str, Any]) -> dict[str, Any]:
    derived = dict(payload)
    derived["archive_intervals"] = _archive_interval_derivation(payload)
    return derived


def _archive_point_rows(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    first_at_us: int,
    last_at_us: int,
    max_points: int,
) -> tuple[list[sqlite3.Row], int]:
    metadata = connection.execute(
        """
        SELECT observed_second,observed_at_us,is_event_boundary
        FROM playback_snapshots
        WHERE source_heat_id = ? AND observed_at_us >= ? AND observed_at_us <= ?
        ORDER BY observed_at_us,observed_second
        """,
        (heat_id, first_at_us, last_at_us),
    ).fetchall()
    count = len(metadata)
    if count <= max_points:
        selected_seconds = [int(row["observed_second"]) for row in metadata]
    else:
        mandatory = [int(metadata[0]["observed_second"]), int(metadata[-1]["observed_second"])]
        mandatory.extend(int(row["observed_second"]) for row in metadata if row["is_event_boundary"])
        mandatory = sorted(set(mandatory))
        if len(mandatory) >= max_points:
            selected_seconds = _evenly_spaced(mandatory, max_points)
        else:
            selected = set(mandatory)
            candidates = [int(row["observed_second"]) for row in metadata if int(row["observed_second"]) not in selected]
            selected.update(_evenly_spaced(candidates, max_points - len(selected)))
            selected_seconds = sorted(selected)
    placeholders = ",".join("?" for _ in selected_seconds)
    rows = connection.execute(
        f"""
        SELECT observed_second,observed_at_us,source_frame_id,source_message_id,source_key,
               projection_version,metric_version,is_event_boundary,payload_codec,payload,payload_sha256
        FROM playback_snapshots
        WHERE source_heat_id = ? AND observed_at_us >= ? AND observed_at_us <= ?
          AND observed_second IN ({placeholders})
        ORDER BY observed_at_us,observed_second
        """,
        (heat_id, first_at_us, last_at_us, *selected_seconds),
    ).fetchall()
    return list(rows), count


def _valid_source_clock_timestamp(value: Any) -> int | None:
    """Return a usable raw Time Service clock value without treating zero as time."""

    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value <= 0 or value == OPEN_ENDED_TS_TIME:
        return None
    return value


def _archive_source_clock_anchors(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    first_at_us: int,
    last_at_us: int,
) -> list[dict[str, Any]]:
    """Return explicit Time Service clock anchors around one replay range.

    Archive seek stays on the durable receive-time coordinate.  These anchors
    are intentionally additive: a client can interpolate *labels* between
    adjacent samples from the same SignalR connection without claiming that
    each historical frame carried its own provider timestamp.
    """

    lower_bound = max(0, first_at_us - ARCHIVE_SOURCE_CLOCK_MAX_INTERPOLATION_US)
    upper_bound = last_at_us + ARCHIVE_SOURCE_CLOCK_MAX_INTERPOLATION_US
    rows = connection.execute(
        """
        SELECT sample.ingest_connection_id,sample.provider_timestamp_us,sample.received_at_us,
               (
                 SELECT calibration.id
                 FROM connection_clock_calibrations AS calibration
                 WHERE calibration.source_heat_id = ?
                   AND calibration.ingest_connection_id = sample.ingest_connection_id
                   AND calibration.valid_from_observed_at_us <= sample.received_at_us
                 ORDER BY calibration.valid_from_observed_at_us DESC,calibration.id DESC
                 LIMIT 1
               ) AS calibration_id,
               (
                 SELECT calibration.offset_us
                 FROM connection_clock_calibrations AS calibration
                 WHERE calibration.source_heat_id = ?
                   AND calibration.ingest_connection_id = sample.ingest_connection_id
                   AND calibration.valid_from_observed_at_us <= sample.received_at_us
                 ORDER BY calibration.valid_from_observed_at_us DESC,calibration.id DESC
                 LIMIT 1
               ) AS offset_us
        FROM connection_clock_samples AS sample
        WHERE sample.source_heat_id = ?
          AND sample.received_at_us >= ? AND sample.received_at_us <= ?
        ORDER BY sample.received_at_us,sample.id
        LIMIT ?
        """,
        (heat_id, heat_id, heat_id, lower_bound, upper_bound, MAX_ARCHIVE_SOURCE_CLOCK_ANCHORS),
    ).fetchall()
    anchors: list[dict[str, Any]] = []
    for row in rows:
        provider_timestamp_us = _valid_source_clock_timestamp(row["provider_timestamp_us"])
        offset_us = row["offset_us"]
        provider_epoch_us = time_service_to_unix_us(provider_timestamp_us)
        if provider_timestamp_us is None or provider_epoch_us is None or offset_us is None:
            continue
        anchors.append(
            {
                "capture_at_us": int(row["received_at_us"]),
                "provider_ts_time_us": provider_timestamp_us,
                "calibrated_utc_at_us": provider_epoch_us + int(offset_us),
                "basis": "provider_explicit",
                "connection_id": row["ingest_connection_id"],
                "calibration_id": int(row["calibration_id"]) if row["calibration_id"] is not None else None,
            }
        )
    return anchors


def _archive_time_axes(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    session: Mapping[str, Any],
    first_at_us: int,
    last_at_us: int,
) -> dict[str, Any]:
    """Describe immutable playback and provider-clock axes without conflating them."""

    timezone_name = session["timezone_name"]
    anchors = _archive_source_clock_anchors(
        connection,
        heat_id=heat_id,
        first_at_us=first_at_us,
        last_at_us=last_at_us,
    )
    origin_row = connection.execute(
        """
        SELECT green_flag_provider_ts_us,green_flag_at_us
        FROM heat_statistics_current
        WHERE source_heat_id = ?
        """,
        (heat_id,),
    ).fetchone()
    origin_provider_us = _valid_source_clock_timestamp(
        origin_row["green_flag_provider_ts_us"] if origin_row is not None else None
    )
    origin_at_us = origin_row["green_flag_at_us"] if origin_row is not None else None
    session_origin = (
        {
            "provider_ts_time_us": origin_provider_us,
            "calibrated_utc_at_us": int(origin_at_us),
            "basis": "provider_explicit_calibrated",
        }
        if origin_provider_us is not None and origin_at_us is not None
        else None
    )
    if session_origin is None and anchors:
        first_anchor = anchors[0]
        session_origin = {
            "provider_ts_time_us": first_anchor["provider_ts_time_us"],
            "calibrated_utc_at_us": first_anchor["calibrated_utc_at_us"],
            "basis": "provider_explicit",
        }
    return {
        "playback": {
            "id": "capture_received",
            "seekable": True,
            "origin_received_at_us": first_at_us,
            "timezone_name": timezone_name,
        },
        "source": {
            "id": "timeservice",
            "label": "Время табло Time Service",
            "timezone_name": timezone_name,
            "provider_epoch": "2000-01-01T00:00:00Z",
            "interpolation_max_gap_us": ARCHIVE_SOURCE_CLOCK_MAX_INTERPOLATION_US,
            "fallback": "capture_received",
            "session_origin": session_origin,
            "anchors": anchors,
        },
    }


def _comparison_number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return value


def _comparison_text(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value
    return None


def _archive_class_participants(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Extract only the compact class state needed for an archive benchmark."""

    raw_participants = payload.get("class_participants")
    if not isinstance(raw_participants, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for raw_item in raw_participants:
        if not isinstance(raw_item, Mapping):
            continue
        measured = raw_item.get("measured")
        measured = measured if isinstance(measured, Mapping) else {}
        computed = raw_item.get("computed")
        computed = computed if isinstance(computed, Mapping) else {}
        measured_state = measured.get("state")
        measured_state = measured_state if isinstance(measured_state, Mapping) else {}
        participant_id = _comparison_text(computed.get("participant_id"), measured.get("participant_id"))
        if participant_id is None:
            continue
        result[participant_id] = {
            "participant_id": participant_id,
            "start_number": _comparison_text(computed.get("start_number"), measured.get("start_number")),
            "team_name": _comparison_text(computed.get("team_name"), measured.get("team_name")),
            "car_name": _comparison_text(computed.get("car_name"), measured.get("car_name")),
            "class_name": _comparison_text(computed.get("class_name"), measured.get("class_name")),
            "driver_name": _comparison_text(
                computed.get("current_driver_name"), measured_state.get("driver_name")
            ),
            "is_ours": bool(computed.get("is_ours") or measured.get("is_ours")),
            "current_state": _comparison_text(
                computed.get("current_state"), measured_state.get("state_kind"), measured_state.get("state")
            ),
            "pace_5_ms": _comparison_number(computed.get("pace_5_ms")),
        }
    return result


def _archive_declared_ours_id(payload: Mapping[str, Any]) -> str | None:
    computed = payload.get("computed")
    computed = computed if isinstance(computed, Mapping) else {}
    session = computed.get("session")
    session = session if isinstance(session, Mapping) else {}
    measured = payload.get("measured")
    measured = measured if isinstance(measured, Mapping) else {}
    ours = measured.get("ours")
    ours = ours if isinstance(ours, Mapping) else {}
    return _comparison_text(session.get("ours_participant_id"), ours.get("participant_id"))


def _comparison_percentile(sorted_values: Sequence[int | float], percentile: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    index = (len(sorted_values) - 1) * percentile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return float(sorted_values[lower])
    fraction = index - lower
    return float(sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction)


def _comparison_pace(entry: Mapping[str, Any] | None) -> int | float | None:
    if not isinstance(entry, Mapping) or entry.get("current_state") != "ON_TRACK":
        return None
    return _comparison_number(entry.get("pace_5_ms"))


def _archive_participant_sort_key(participant: Mapping[str, Any]) -> tuple[int, int, str, str]:
    number = str(participant.get("start_number") or "")
    try:
        numeric_number = int(number)
    except ValueError:
        numeric_number = 10**9
    return (0 if participant.get("is_ours") else 1, numeric_number, number, str(participant.get("participant_id") or ""))


def _merge_archive_participant(
    roster_by_id: dict[str, dict[str, Any]],
    participant: Mapping[str, Any],
) -> None:
    """Merge durable roster metadata without overwriting a populated value."""

    participant_id = _comparison_text(participant.get("participant_id"))
    if participant_id is None:
        return
    existing = roster_by_id.setdefault(
        participant_id,
        {
            "participant_id": participant_id,
            "start_number": None,
            "team_name": None,
            "car_name": None,
            "class_name": None,
            "driver_name": None,
            "is_ours": False,
        },
    )
    for key in ("start_number", "team_name", "car_name", "class_name", "driver_name"):
        if participant.get(key) is not None:
            existing[key] = participant[key]
    existing["is_ours"] = bool(existing["is_ours"] or participant.get("is_ours"))


def _archive_class_roster_from_facts(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    class_key: str | None,
) -> list[dict[str, Any]]:
    """Return every recorded member of the archived class, including retirees."""

    if class_key is None:
        return []
    rows = connection.execute(
        """
        SELECT p.id,p.start_number,p.team_name,p.car_name,p.class_name,p.class_name_key,p.is_ours,
               c.current_driver_name AS driver_name
        FROM participants AS p
        LEFT JOIN participant_state_current AS c
          ON c.source_heat_id = p.source_heat_id AND c.participant_id = p.id
        WHERE p.source_heat_id = ?
        """,
        (heat_id,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        participant_class_key = row["class_name_key"] or _class_key(row["class_name"])
        if participant_class_key != class_key:
            continue
        result.append(
            {
                "participant_id": row["id"],
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "car_name": row["car_name"],
                "class_name": row["class_name"],
                "driver_name": row["driver_name"],
                "is_ours": bool(row["is_ours"]),
            }
        )
    return result


def _archive_comparison_pit_stops(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_ids: Sequence[str],
    ours_id: str,
    first_at_us: int,
    last_at_us: int,
) -> list[dict[str, Any]]:
    if not participant_ids:
        return []
    placeholders = ",".join("?" for _ in participant_ids)
    rows = connection.execute(
        f"""
        SELECT f.participant_id,p.start_number,p.team_name,f.stop_number,f.entered_at_us,f.exited_at_us,
               f.entered_lap,f.exited_lap,f.pit_lane_ms,f.pit_lane_duration_source_kind,f.completed
        FROM pit_stops f
        JOIN participants p ON p.id = f.participant_id
        WHERE f.source_heat_id = ? AND f.participant_id IN ({placeholders})
          AND f.entered_at_us <= ? AND (f.exited_at_us IS NULL OR f.exited_at_us >= ?)
        ORDER BY f.entered_at_us,f.participant_id,f.stop_number
        LIMIT ?
        """,
        (heat_id, *participant_ids, last_at_us, first_at_us, MAX_ARCHIVE_MARKERS),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        entered_at_us = int(row["entered_at_us"])
        exited_at_us = int(row["exited_at_us"]) if row["exited_at_us"] is not None else None
        result.append(
            {
                "participant_id": row["participant_id"],
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "is_ours": row["participant_id"] == ours_id,
                "stop_number": int(row["stop_number"]),
                "entered_at_us": entered_at_us,
                "exited_at_us": exited_at_us,
                "timeline_started_at_us": max(first_at_us, entered_at_us),
                "timeline_ended_at_us": min(last_at_us, exited_at_us) if exited_at_us is not None else None,
                "carried_into_range": entered_at_us < first_at_us,
                "entered_lap": row["entered_lap"],
                "exited_lap": row["exited_lap"],
                "pit_lane_ms": _published_pit_lane_ms(row),
                "completed": bool(row["completed"]),
            }
        )
    return result


def _archive_comparison_lap_rows(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_ids: Sequence[str],
    first_at_us: int,
    last_at_us: int,
    include_unplaced: bool = False,
    clip_to_archive_range: bool = True,
) -> dict[str, list[dict[str, Any]]]:
    """Return persisted raw lap facts in deterministic lap-number order.

    The legacy comparison series has always required a completed timestamp so
    it can be plotted on the archive timeline.  The raw competitor contract
    additionally preserves every persisted row in the selected heat, including
    rows without a completion time: source LAPS counters can advance by more
    than one and the normalizer correctly retains those unknown facts instead
    of inventing a completion time or duration.
    """

    if not participant_ids:
        return {}
    placeholders = ",".join("?" for _ in participant_ids)
    completed_at_filter = "" if include_unplaced else " AND f.completed_at_us IS NOT NULL"
    lap_order = (
        "f.participant_id,f.lap_number,f.completed_at_us,f.id"
        if include_unplaced
        else "f.participant_id,f.completed_at_us,f.lap_number"
    )
    rows = connection.execute(
        f"""
        SELECT f.participant_id,p.start_number,p.team_name,f.lap_number,f.completed_at_us,f.duration_ms,
               f.sectors_json,f.sectors_source_cell_observation_ids_json,
               f.duration_source_cell_observation_id,f.duration_source_kind,
               f.flag,f.is_in_lap,f.is_out_lap,f.crosses_pit,f.is_clean
        FROM laps f
        JOIN participants p ON p.id = f.participant_id
        WHERE f.source_heat_id = ? AND f.participant_id IN ({placeholders})
          {completed_at_filter}
        ORDER BY {lap_order}
        """,
        (heat_id, *participant_ids),
    ).fetchall()
    pit_rows = connection.execute(
        f"""
        SELECT participant_id,entered_at_us,exited_at_us,completed
        FROM pit_stops
        WHERE source_heat_id = ? AND participant_id IN ({placeholders})
        ORDER BY participant_id,entered_at_us,stop_number
        """,
        (heat_id, *participant_ids),
    ).fetchall()
    pits_by_participant: dict[str, list[sqlite3.Row]] = {}
    for pit in pit_rows:
        pits_by_participant.setdefault(pit["participant_id"], []).append(pit)
    grouped: dict[str, list[dict[str, Any]]] = {}
    previous_completed_at_us: dict[str, int] = {}
    for row in rows:
        participant_id = row["participant_id"]
        raw_completed_at_us = row["completed_at_us"]
        completed_at_us = int(raw_completed_at_us) if raw_completed_at_us is not None else None
        crosses_pit_interval = False
        previous_at_us = previous_completed_at_us.get(participant_id)
        if completed_at_us is not None and previous_at_us is not None:
            for pit in pits_by_participant.get(participant_id, ()):
                if not bool(pit["completed"]):
                    continue
                entered_at_us = int(pit["entered_at_us"])
                exited_at_us = int(pit["exited_at_us"]) if pit["exited_at_us"] is not None else None
                if entered_at_us < completed_at_us and (exited_at_us is None or exited_at_us > previous_at_us):
                    crosses_pit_interval = True
                    break
        if completed_at_us is not None:
            previous_completed_at_us[participant_id] = completed_at_us
        if completed_at_us is None and not include_unplaced:
            continue
        if (
            clip_to_archive_range
            and completed_at_us is not None
            and (completed_at_us < first_at_us or completed_at_us > last_at_us)
        ):
            continue
        source_is_clean = bool(row["is_clean"])
        grouped.setdefault(row["participant_id"], []).append(
            {
                "participant_id": participant_id,
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "lap_number": int(row["lap_number"]),
                "completed_at_us": completed_at_us,
                "duration_ms": row["duration_ms"],
                "sectors": _archive_source_proven_sectors(
                    sectors_json=row["sectors_json"],
                    source_cell_observation_ids_json=row["sectors_source_cell_observation_ids_json"],
                    is_linked_to_last=(
                        row["duration_source_cell_observation_id"] is not None
                        and row["duration_source_kind"] == "RESULT_GRID_LAST"
                    ),
                ),
                "flag": row["flag"],
                "is_in_lap": bool(row["is_in_lap"]),
                "is_out_lap": bool(row["is_out_lap"]),
                "crosses_pit": bool(row["crosses_pit"]) or crosses_pit_interval,
                "source_is_clean": source_is_clean,
                "is_clean": source_is_clean and not crosses_pit_interval,
            }
        )
    return grouped


def _result_grid_duration_ms(value: Any) -> int | None:
    """Decode one result-grid duration without accepting provider sentinels."""

    source_us = parse_ts_time(value)
    if source_us is None or not 1_000_000 <= source_us < OPEN_ENDED_TS_TIME:
        return None
    return source_us // 1_000


_ARCHIVE_SECTOR_KEYS = ("sector_1", "sector_2", "sector_3")


def _archive_source_proven_sectors(
    *,
    sectors_json: Any,
    source_cell_observation_ids_json: Any,
    is_linked_to_last: bool,
) -> dict[str, dict[str, int] | None] | None:
    """Publish only sector values tied to an exact persisted ``LAST`` cell.

    ``laps.sectors_json`` is useful for historical inspection only once the
    corresponding lap has an immutable result-grid ``LAST`` source link.  The
    per-sector source-cell mapping is required as well: without it, a value
    could be an old derived/legacy projection rather than an auditable timing
    fact.  Missing or malformed individual values remain explicit ``null``;
    this read path must never fill them from a neighboring lap or a current
    participant-state field.
    """

    if not is_linked_to_last or sectors_json is None or source_cell_observation_ids_json is None:
        return None
    sectors = _json_value(sectors_json, context="linked lap sectors")
    source_ids = _json_value(
        source_cell_observation_ids_json,
        context="linked lap sector source-cell observations",
    )
    if not isinstance(sectors, Mapping) or not isinstance(source_ids, Mapping):
        return None

    result: dict[str, dict[str, int] | None] = {}
    for sector_key in _ARCHIVE_SECTOR_KEYS:
        duration_ms = _result_grid_duration_ms(sectors.get(sector_key))
        source_cell_observation_id = _archive_int(source_ids.get(sector_key))
        if duration_ms is None or source_cell_observation_id is None or source_cell_observation_id <= 0:
            result[sector_key] = None
            continue
        result[sector_key] = {
            "duration_ms": duration_ms,
            "source_cell_observation_id": source_cell_observation_id,
        }
    return result if any(value is not None for value in result.values()) else None


def _archive_result_last_rows(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_ids: Sequence[str],
    first_at_us: int,
    last_at_us: int,
    clip_to_archive_range: bool = True,
) -> dict[str, list[dict[str, Any]]]:
    """Expose every immutable `LAST` cell without inventing a finish crossing.

    A Time Service result grid can omit ``LAPS`` and deliver tracker passings
    in different SignalR frames.  The result-grid cell is still the source of
    the lap time, but its observation time is not a claimed finish timestamp.
    A left join to ``laps`` provides tracker/explicit-grid chronology only when
    the normalizer proved that exact cell belongs to a completed lap.
    """

    if not participant_ids:
        return {}
    placeholders = ",".join("?" for _ in participant_ids)
    rows = connection.execute(
        f"""
        SELECT observation.id AS source_cell_observation_id,
               observation.participant_id,observation.value_text AS duration_raw,
               observation.observed_at_us AS board_observed_at_us,
               observation.source_message_id,observation.source_key,
               observation.source_change_ordinal,
               message.handle,message.ordinal AS message_ordinal,
               frame.id AS frame_id,
               participant.start_number,participant.team_name,
               lap.id AS linked_lap_id,lap.lap_number,lap.completed_at_us,
               lap.flag,lap.is_in_lap,lap.is_out_lap,lap.crosses_pit,lap.is_clean,
               lap.sectors_json,lap.sectors_source_cell_observation_ids_json,
               lap.duration_source_kind
        FROM participant_result_cell_observations AS observation
        JOIN result_column_definitions AS definition
          ON definition.layout_version_id = observation.layout_version_id
         AND definition.column_index = observation.column_index
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        JOIN feed_frames AS frame ON frame.id = message.frame_id
        JOIN participants AS participant ON participant.id = observation.participant_id
        LEFT JOIN laps AS lap ON lap.id = (
          SELECT candidate.id
          FROM laps AS candidate
          WHERE candidate.source_heat_id = observation.source_heat_id
            AND candidate.duration_source_cell_observation_id = observation.id
          ORDER BY candidate.completed_at_us,candidate.lap_number,candidate.id
          LIMIT 1
        )
        WHERE observation.source_heat_id = ?
          AND observation.participant_id IN ({placeholders})
          AND definition.canonical_key = 'last_lap'
          AND NOT EXISTS (
            SELECT 1
            FROM result_column_definitions AS duplicate_last
            WHERE duplicate_last.layout_version_id = observation.layout_version_id
              AND duplicate_last.canonical_key = 'last_lap'
              AND duplicate_last.column_index <> observation.column_index
          )
        ORDER BY frame.id,message.ordinal,observation.source_change_ordinal,observation.id
        """,
        (heat_id, *participant_ids),
    ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        board_observed_at_us = int(row["board_observed_at_us"])
        if clip_to_archive_range and not first_at_us <= board_observed_at_us <= last_at_us:
            continue
        linked_lap = row["linked_lap_id"] is not None
        if linked_lap:
            timeline_kind = "confirmed_lap"
        elif row["handle"] == "r_i":
            # A full grid snapshot can repeat a stale LAST display after a
            # reconnect. Preserve it for audit, but never connect it as a new
            # lap in the chart.
            timeline_kind = "snapshot_baseline"
        else:
            timeline_kind = "table_observation"
        participant_id = row["participant_id"]
        grouped.setdefault(participant_id, []).append(
            {
                "participant_id": participant_id,
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "lap_number": int(row["lap_number"]) if linked_lap else None,
                "board_observed_at_us": board_observed_at_us,
                "completed_at_us": int(row["completed_at_us"]) if linked_lap and row["completed_at_us"] is not None else None,
                "duration_ms": _result_grid_duration_ms(row["duration_raw"]),
                "duration_raw": row["duration_raw"],
                "sectors": _archive_source_proven_sectors(
                    sectors_json=row["sectors_json"],
                    source_cell_observation_ids_json=row["sectors_source_cell_observation_ids_json"],
                    is_linked_to_last=linked_lap and row["duration_source_kind"] == "RESULT_GRID_LAST",
                ),
                "timeline_kind": timeline_kind,
                "flag": row["flag"] if linked_lap else None,
                "source_is_clean": bool(row["is_clean"]) if linked_lap else None,
                "is_clean": bool(row["is_clean"]) if linked_lap else None,
                "is_in_lap": bool(row["is_in_lap"]) if linked_lap else None,
                "is_out_lap": bool(row["is_out_lap"]) if linked_lap else None,
                "crosses_pit": bool(row["crosses_pit"]) if linked_lap else None,
                "source": {
                    "cell_observation_id": int(row["source_cell_observation_id"]),
                    "message_id": int(row["source_message_id"]),
                    "frame_id": int(row["frame_id"]),
                    "message_ordinal": int(row["message_ordinal"]),
                    "change_ordinal": int(row["source_change_ordinal"]),
                    "handle": row["handle"],
                    "key": row["source_key"],
                },
            }
        )
    return grouped


def _archive_raw_last_or_lap_rows(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_ids: Sequence[str],
    first_at_us: int,
    last_at_us: int,
    clip_to_archive_range: bool,
) -> dict[str, list[dict[str, Any]]]:
    """Use source LAST timing when present, otherwise retain legacy laps.

    An ``r_i`` reconnect snapshot is an audit baseline, not evidence that the
    source stream contains timing events for this participant. It therefore
    cannot hide already-confirmed legacy laps from a pre-provenance archive.
    Once a valid ``r_c`` LAST or a source-linked confirmed lap exists, that
    source stream remains authoritative and legacy duration projections stay
    out to avoid duplicating the same physical lap.
    """

    source_rows = _archive_result_last_rows(
        connection,
        heat_id=heat_id,
        participant_ids=participant_ids,
        first_at_us=first_at_us,
        last_at_us=last_at_us,
        clip_to_archive_range=clip_to_archive_range,
    )
    fallback = _archive_comparison_lap_rows(
        connection,
        heat_id=heat_id,
        participant_ids=participant_ids,
        first_at_us=first_at_us,
        last_at_us=last_at_us,
        include_unplaced=True,
        clip_to_archive_range=clip_to_archive_range,
    )
    result: dict[str, list[dict[str, Any]]] = {}
    for participant_id in participant_ids:
        source = source_rows.get(participant_id, ())
        has_timed_source = any(
            row.get("timeline_kind") == "confirmed_lap"
            or (
                row.get("timeline_kind") == "table_observation"
                and row.get("duration_ms") is not None
            )
            for row in source
        )
        if has_timed_source:
            result[participant_id] = list(source)
        elif source:
            # Retain the visible reconnect baseline for audit, then preserve
            # old confirmed lap facts that have no source LAST timing stream.
            result[participant_id] = [*source, *fallback.get(participant_id, ())]
        else:
            result[participant_id] = list(fallback.get(participant_id, ()))
    return result


def _archive_raw_lap_competitors(
    participants: Sequence[Mapping[str, Any]],
    laps_by_participant: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    participant_ids: Sequence[str],
) -> list[dict[str, Any]]:
    """Attach every raw lap fact to an explicitly ordered competitor roster."""

    by_id = {
        participant_id: participant
        for participant in participants
        if (participant_id := _comparison_text(participant.get("participant_id"))) is not None
    }
    result: list[dict[str, Any]] = []
    for participant_id in participant_ids:
        participant = by_id[participant_id]
        result.append(
            {
                "participant_id": participant_id,
                "start_number": participant.get("start_number"),
                "team_name": participant.get("team_name"),
                "car_name": participant.get("car_name"),
                "class_name": participant.get("class_name"),
                "driver_name": participant.get("driver_name"),
                "laps": [dict(lap) for lap in laps_by_participant.get(participant_id, ())],
            }
        )
    return result


def _bounded_archive_lap_rows(laps: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Bound player lap points while retaining every non-clean boundary as a break."""

    if not laps:
        return []
    indexes = (
        list(range(len(laps)))
        if len(laps) <= MAX_ARCHIVE_COMPARISON_LAPS_PER_PARTICIPANT
        else _evenly_spaced(list(range(len(laps))), MAX_ARCHIVE_COMPARISON_LAPS_PER_PARTICIPANT)
    )
    result: list[dict[str, Any]] = []
    previous_index: int | None = None
    for index in indexes:
        item = dict(laps[index])
        previous_was_not_clean = previous_index is not None and not bool(laps[previous_index].get("is_clean"))
        omitted_non_clean = (
            previous_index is not None
            and any(not bool(laps[between].get("is_clean")) for between in range(previous_index + 1, index))
        )
        item["break_before"] = bool(
            item.get("is_clean") and (previous_index is None or previous_was_not_clean or omitted_non_clean)
        )
        result.append(item)
        previous_index = index
    return result


def _archive_lap_benchmark(
    laps_by_participant: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    excluded_participant_id: str,
    first_at_us: int,
) -> list[dict[str, Any]]:
    # Each machine contributes at most its latest confirmed clean lap to a
    # one-minute window.  A short-lap car must not outweigh its competitors.
    buckets: dict[int, dict[str, int | float]] = {}
    for participant_id, laps in laps_by_participant.items():
        if participant_id == excluded_participant_id:
            continue
        for lap in laps:
            duration_ms = _comparison_number(lap.get("duration_ms"))
            if not lap.get("is_clean") or duration_ms is None:
                continue
            bucket = (int(lap["completed_at_us"]) - first_at_us) // ARCHIVE_COMPARISON_LAP_BUCKET_US
            buckets.setdefault(bucket, {})[participant_id] = duration_ms
    result: list[dict[str, Any]] = []
    for bucket, durations_by_participant in sorted(buckets.items()):
        ordered = sorted(durations_by_participant.values())
        result.append(
            {
                "window_started_at_us": first_at_us + bucket * ARCHIVE_COMPARISON_LAP_BUCKET_US,
                "window_ended_at_us": first_at_us + (bucket + 1) * ARCHIVE_COMPARISON_LAP_BUCKET_US,
                "median_duration_ms": _comparison_percentile(ordered, 0.5),
                "p25_duration_ms": _comparison_percentile(ordered, 0.25),
                "p75_duration_ms": _comparison_percentile(ordered, 0.75),
                "participant_count": len(durations_by_participant),
            }
        )
    return result


def read_archive_comparison(
    session_id: str,
    *,
    database: str | Path | None = None,
    generation: int | None = None,
    mode: Literal["all", "participant"] = "all",
    participant_id: str | None = None,
    max_points: int = MAX_CHART_POINTS,
) -> dict[str, Any]:
    """Return one bounded own-versus-class benchmark series for an archived heat."""

    session_id = _require_session_id(session_id)
    if mode not in {"all", "participant"}:
        raise ReadValidationError("comparison mode must be 'all' or 'participant'")
    if mode == "all" and participant_id is not None:
        raise ReadValidationError("participant_id is only valid for participant comparison mode")
    if mode == "participant" and (not isinstance(participant_id, str) or not participant_id.strip()):
        raise ReadValidationError("participant comparison mode requires participant_id")
    max_points = _require_max_points(max_points)
    with _readonly_snapshot(database) as connection:
        session = _require_archived_session(connection, session_id)
        heat, _ = _select_archive_heat(connection, session_id=session_id, generation=generation)
        first_at_us = int(heat["first_at_us"])
        last_at_us = int(heat["last_at_us"])
        point_rows, source_point_count = _archive_point_rows(
            connection,
            heat_id=int(heat["id"]),
            first_at_us=first_at_us,
            last_at_us=last_at_us,
            max_points=max_points,
        )
        decoded_points: list[tuple[sqlite3.Row, dict[str, dict[str, Any]]]] = []
        roster_by_id: dict[str, dict[str, Any]] = {}
        observed_ours_ids: list[str] = []
        for row in point_rows:
            payload = _decode_playback_payload(row)
            declared_ours_id = _archive_declared_ours_id(payload)
            if declared_ours_id is not None:
                observed_ours_ids.append(declared_ours_id)
            entries = _archive_class_participants(payload)
            for entry in entries.values():
                _merge_archive_participant(roster_by_id, entry)
            decoded_points.append((row, entries))

        ours_id = _comparison_text(session["our_participant_id"], *(observed_ours_ids or ()))
        if ours_id is None:
            ours_id = next((entry_id for entry_id, entry in roster_by_id.items() if entry.get("is_ours")), None)
        ours_was_observed = ours_id is not None and ours_id in roster_by_id
        ours_metadata = connection.execute(
            """
            SELECT class_name,class_name_key
            FROM participants
            WHERE source_heat_id = ? AND id = ?
            """,
            (int(heat["id"]), ours_id),
        ).fetchone() if ours_id is not None else None
        class_name = _comparison_text(
            ours_metadata["class_name"] if ours_metadata is not None else None,
            roster_by_id.get(ours_id, {}).get("class_name") if ours_id is not None else None,
            session["our_class"],
        )
        class_key = (
            (ours_metadata["class_name_key"] or _class_key(ours_metadata["class_name"]))
            if ours_metadata is not None
            else _class_key(class_name)
        )
        for participant in _archive_class_roster_from_facts(
            connection,
            heat_id=int(heat["id"]),
            class_key=class_key,
        ):
            _merge_archive_participant(roster_by_id, participant)

        unavailable = not ours_was_observed
        if unavailable:
            return {
                "schema_version": ARCHIVE_SCHEMA_VERSION,
                "session": _session_payload(session),
                "heat": _archive_heat_summary(heat),
                "range": {
                    "first_at_us": first_at_us,
                    "last_at_us": last_at_us,
                    "source_point_count": source_point_count,
                    "downsampled": source_point_count > len(point_rows),
                },
                "comparison": {
                    "available": False,
                    "reason": "our_class_participants_unavailable",
                    "mode": mode,
                    "ours_participant_id": ours_id,
                    "participant_id": participant_id if mode == "participant" else None,
                },
                "participants": [],
                "points": [],
                "pit_stops": [],
                "lap_series": {
                    "ours": [],
                    "ours_raw": [],
                    "benchmark": [],
                    "benchmark_kind": None,
                    "competitors": [],
                },
                "semantics": {
                    "series": "step",
                    "lap_series_competitors": (
                        "unaggregated raw lap facts for the requested competitors; no raw lap is filtered, "
                        "averaged, or decimated"
                    ),
                    "lap_series_sectors": (
                        "sector durations appear only for an exact result-grid LAST cell linked to a persisted lap "
                        "and to its individual source cell; missing sectors are null and never interpolated"
                    ),
                    "missing_values": "null values are not interpolated",
                },
            }

        roster_by_id[ours_id]["is_ours"] = True
        if mode == "participant":
            assert participant_id is not None
            if participant_id == ours_id:
                raise ReadValidationError("participant comparison must select a competitor, not our participant")
            if participant_id not in roster_by_id:
                raise ScopeNotFoundError(f"Participant does not exist in our archived class: {participant_id}")

        class_name = _comparison_text(roster_by_id[ours_id].get("class_name"), class_name, session["our_class"])
        points: list[dict[str, Any]] = []
        for row, entries in decoded_points:
            ours_pace = _comparison_pace(entries.get(ours_id))
            point: dict[str, Any] = {
                "observed_at_us": int(row["observed_at_us"]),
                "ours_pace_5_ms": ours_pace,
            }
            if mode == "participant":
                benchmark_pace = _comparison_pace(entries.get(participant_id))
                point.update(
                    {
                        "benchmark_pace_5_ms": benchmark_pace,
                        "benchmark_participant_count": 1 if benchmark_pace is not None else 0,
                    }
                )
            else:
                competitor_paces = sorted(
                    pace
                    for entry_id, entry in entries.items()
                    if entry_id != ours_id and (pace := _comparison_pace(entry)) is not None
                )
                point.update(
                    {
                        "benchmark_pace_5_ms": _comparison_percentile(competitor_paces, 0.5),
                        "benchmark_p25_pace_5_ms": _comparison_percentile(competitor_paces, 0.25),
                        "benchmark_p75_pace_5_ms": _comparison_percentile(competitor_paces, 0.75),
                        "benchmark_participant_count": len(competitor_paces),
                    }
                )
            points.append(point)

        participants = sorted(roster_by_id.values(), key=_archive_participant_sort_key)
        participant_ids = [str(participant["participant_id"]) for participant in participants]
        pit_stops = _archive_comparison_pit_stops(
            connection,
            heat_id=int(heat["id"]),
            participant_ids=participant_ids,
            ours_id=ours_id,
            first_at_us=first_at_us,
            last_at_us=last_at_us,
        )
        raw_lap_rows = _archive_comparison_lap_rows(
            connection,
            heat_id=int(heat["id"]),
            participant_ids=participant_ids if mode == "all" else [ours_id, str(participant_id)],
            first_at_us=first_at_us,
            last_at_us=last_at_us,
        )
        raw_competitor_ids = (
            [str(participant_id)]
            if mode == "participant"
            else [
                str(participant["participant_id"])
                for participant in participants
                if participant["participant_id"] != ours_id
            ]
        )
        raw_competitor_laps = _archive_raw_last_or_lap_rows(
            connection,
            heat_id=int(heat["id"]),
            participant_ids=raw_competitor_ids,
            first_at_us=first_at_us,
            last_at_us=last_at_us,
            clip_to_archive_range=False,
        )
        raw_ours_laps = _archive_raw_last_or_lap_rows(
            connection,
            heat_id=int(heat["id"]),
            participant_ids=[ours_id],
            first_at_us=first_at_us,
            last_at_us=last_at_us,
            clip_to_archive_range=False,
        )
        lap_series = {
            "ours": _bounded_archive_lap_rows(raw_lap_rows.get(ours_id, [])),
            "ours_raw": [dict(lap) for lap in raw_ours_laps.get(ours_id, ())],
            "benchmark": (
                _bounded_archive_lap_rows(raw_lap_rows.get(str(participant_id), []))
                if mode == "participant"
                else _archive_lap_benchmark(
                    raw_lap_rows,
                    excluded_participant_id=ours_id,
                    first_at_us=first_at_us,
                )
            ),
            "benchmark_kind": "participant" if mode == "participant" else "minute_median",
            "competitors": _archive_raw_lap_competitors(
                participants,
                raw_competitor_laps,
                participant_ids=raw_competitor_ids,
            ),
        }
        return {
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "session": _session_payload(session),
            "heat": _archive_heat_summary(heat),
            "range": {
                "first_at_us": first_at_us,
                "last_at_us": last_at_us,
                "source_point_count": source_point_count,
                "downsampled": source_point_count > len(point_rows),
            },
            "comparison": {
                "available": True,
                "mode": mode,
                "ours_participant_id": ours_id,
                "participant_id": participant_id if mode == "participant" else None,
                "class_name": class_name,
                "benchmark": (
                    "participant_pace_5_ms"
                    if mode == "participant"
                    else "median_on_track_competitor_pace_5_ms"
                ),
            },
            "participants": participants,
            "points": points,
            "pit_stops": pit_stops,
            "lap_series": lap_series,
            "semantics": {
                "series": "step",
                "ours_pace_5_ms": "confirmed rolling pace for five clean laps while our car is on track",
                "benchmark_pace_5_ms": (
                    "confirmed rolling pace for the selected on-track competitor"
                    if mode == "participant"
                    else "median confirmed rolling pace for on-track competitors in our archived class, excluding our car"
                ),
                "lap_series": (
                    "clean laps for our car and the selected participant; an interval that intersects a persisted pit stop is excluded even if an older source row was marked clean; display points are bounded and preserve non-clean breaks"
                    if mode == "participant"
                    else "one-minute median of the latest clean lap per competitor, excluding our car; pit-crossing intervals are excluded"
                ),
                "lap_series_competitors": (
                    "unaggregated result-grid LAST observations for every competitor in our archived class, ordered "
                    "by source stream position without averaging or decimation; tracker lap numbers are attached only "
                    "when the exact LAST cell is proven to match a lap, and snapshot baselines are not new laps"
                    if mode == "all"
                    else "unaggregated result-grid LAST observations for the selected competitor, ordered by source "
                    "stream position without averaging or decimation; a lap number exists only when the exact LAST "
                    "cell has a proven lap link"
                ),
                "lap_series_ours_raw": (
                    "unaggregated result-grid LAST observations for BALCHUG Racing, ordered by source stream position "
                    "without averaging or decimation; board_observed_at_us is the table observation time, not an "
                    "invented finish crossing timestamp"
                ),
                "lap_series_sectors": (
                    "sector durations appear only for an exact result-grid LAST cell linked to a persisted lap "
                    "and to its individual source cell; missing sectors are null and never interpolated"
                ),
                "pit_stops": "confirmed pit in/out facts clipped only for timeline display; pit_lane_ms is populated only from an explicit Time Service L-PIT source",
                "missing_values": "null values are not interpolated or converted to zero",
            },
        }


def _evenly_spaced(values: Sequence[int], count: int) -> list[int]:
    if count <= 0 or not values:
        return []
    if count >= len(values):
        return list(values)
    if count == 1:
        return [values[0]]
    return sorted({values[(index * (len(values) - 1)) // (count - 1)] for index in range(count)})


def _archive_markers(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    first_at_us: int,
    last_at_us: int,
) -> dict[str, list[dict[str, Any]]]:
    def authoritative(*values: Any) -> Any:
        return next((value for value in values if value is not None), None)

    flag_rows = connection.execute(
        """
        SELECT flag,provider_code,provider_label,started_at_us,ended_at_us,
               observed_started_at_us,observed_ended_at_us,calibrated_started_at_us,calibrated_ended_at_us
        FROM track_flag_periods
        WHERE source_heat_id = ? AND started_at_us <= ?
          AND (ended_at_us IS NULL OR ended_at_us >= ?)
        ORDER BY started_at_us,id
        LIMIT ?
        """,
        (heat_id, last_at_us, first_at_us, MAX_ARCHIVE_MARKERS),
    ).fetchall()
    pit_rows = connection.execute(
        """
        SELECT f.entered_at_us,f.exited_at_us,f.entered_lap,f.exited_lap,f.pit_lane_ms,
               f.pit_lane_duration_source_kind,f.completed,
               f.stop_number,p.id AS participant_id,p.start_number,p.team_name
        FROM pit_stops f
        JOIN participants p ON p.id = f.participant_id
        WHERE f.source_heat_id = ? AND p.is_ours = 1
          AND f.entered_at_us >= ? AND f.entered_at_us <= ?
        ORDER BY f.entered_at_us,f.stop_number
        LIMIT ?
        """,
        (heat_id, first_at_us, last_at_us, MAX_ARCHIVE_MARKERS),
    ).fetchall()
    lap_rows = connection.execute(
        """
        SELECT f.completed_at_us,f.lap_number,f.duration_ms,f.flag,f.is_clean,p.id AS participant_id
        FROM laps f
        JOIN participants p ON p.id = f.participant_id
        WHERE f.source_heat_id = ? AND p.is_ours = 1 AND f.completed_at_us IS NOT NULL
          AND f.completed_at_us >= ? AND f.completed_at_us <= ?
        ORDER BY f.completed_at_us,f.lap_number
        LIMIT ?
        """,
        (heat_id, first_at_us, last_at_us, MAX_ARCHIVE_MARKERS),
    ).fetchall()
    flags = []
    for row in flag_rows:
        started_at_us = authoritative(
            row["calibrated_started_at_us"], row["observed_started_at_us"], row["started_at_us"]
        )
        ended_at_us = authoritative(
            row["calibrated_ended_at_us"], row["observed_ended_at_us"], row["ended_at_us"]
        )
        carried_into_range = started_at_us is not None and int(started_at_us) < first_at_us
        flags.append(
            {
                "flag": row["flag"],
                "provider_code": row["provider_code"],
                "provider_label": row["provider_label"],
                "started_at_us": max(first_at_us, int(started_at_us)) if started_at_us is not None else first_at_us,
                "ended_at_us": min(last_at_us, int(ended_at_us)) if ended_at_us is not None else None,
                "carried_into_range": carried_into_range,
            }
        )
    return {
        "flags": flags,
        "pits": [
            {
                "participant_id": row["participant_id"],
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "stop_number": row["stop_number"],
                "entered_at_us": row["entered_at_us"],
                "exited_at_us": row["exited_at_us"],
                "entered_lap": row["entered_lap"],
                "exited_lap": row["exited_lap"],
                "pit_lane_ms": _published_pit_lane_ms(row),
                "completed": bool(row["completed"]),
            }
            for row in pit_rows
        ],
        "laps": [
            {
                "participant_id": row["participant_id"],
                "completed_at_us": row["completed_at_us"],
                "lap_number": row["lap_number"],
                "duration_ms": row["duration_ms"],
                "flag": row["flag"],
                "is_clean": bool(row["is_clean"]),
            }
            for row in lap_rows
        ],
    }


def _archive_heat_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_heat_payload(row),
        "first_at_us": int(row["first_at_us"]),
        "last_at_us": int(row["last_at_us"]),
        "point_count": int(row["point_count"]),
        "projection_version": int(row["projection_version"]),
        "metric_version": int(row["metric_version"]),
        "coverage": {
            "kind": row["coverage_kind"],
            "source_started_at_us": row["provider_started_at_us"],
            "capture_started_at_us": int(row["capture_started_at_us"]),
            "finish_at_us": row["finish_at_us"],
            "transport_tail_end_at_us": int(row["transport_tail_end_at_us"]),
            "missing_prefix_us": row["missing_prefix_us"],
            "omitted_tail_point_count": int(row["omitted_tail_point_count"]),
            "raw_point_count": int(row["raw_point_count"]),
        },
    }


def read_archived_sessions(
    *,
    database: str | Path | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """List bounded archive-ready sessions without exposing any writer surface."""

    if type(limit) is not int or not 1 <= limit <= MAX_ARCHIVE_SESSIONS:
        raise ReadValidationError(f"limit must be an integer from 1 through {MAX_ARCHIVE_SESSIONS}")
    with _readonly_snapshot(database) as connection:
        rows = connection.execute(
            """
            SELECT s.id,ts.slug AS source_slug,ts.source_url,ts.display_name AS source_name,
                   s.timezone_name,s.mode,s.lifecycle,s.race_duration_s,s.required_pits,
                   s.our_participant_id,s.our_class,s.identity_state,s.started_at_us,
                   s.stopped_at_us,s.stop_intent,s.created_at_us,s.updated_at_us,
                   MIN(p.observed_at_us) AS first_at_us,MAX(p.observed_at_us) AS last_at_us,
                   COUNT(DISTINCT h.id) AS heat_count,COUNT(p.observed_second) AS point_count
            FROM analysis_sessions s
            JOIN timing_sources ts ON ts.id = s.source_id
            JOIN source_heats h ON h.analysis_session_id = s.id
            JOIN playback_snapshots p ON p.source_heat_id = h.id
            WHERE s.lifecycle IN ('stopped','aborted')
              AND NOT EXISTS (
                SELECT 1
                FROM archive_session_replacements replacement
                WHERE replacement.superseded_session_id = s.id
              )
            GROUP BY s.id
            ORDER BY last_at_us DESC,s.created_at_us DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            heats = [_archive_heat_summary(heat) for heat in _archive_heat_rows(connection, row["id"])]
            first_at_us = min(int(heat["first_at_us"]) for heat in heats)
            last_at_us = max(int(heat["last_at_us"]) for heat in heats)
            point_count = sum(int(heat["point_count"]) for heat in heats)
            items.append(
                {
                    "session": _session_payload(row),
                    "first_at_us": first_at_us,
                    "last_at_us": last_at_us,
                    "heat_count": int(row["heat_count"]),
                    "point_count": point_count,
                    "heats": heats,
                }
            )
        return {
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "items": items,
            "semantics": {"state": "last_observed", "series": "step"},
        }


def read_archive_manifest(
    session_id: str,
    *,
    database: str | Path | None = None,
    generation: int | None = None,
    max_points: int = MAX_CHART_POINTS,
) -> dict[str, Any]:
    """Read one archived heat's bounded keyframes and source event markers."""

    session_id = _require_session_id(session_id)
    max_points = _require_max_points(max_points)
    with _readonly_snapshot(database) as connection:
        session = _require_archived_session(connection, session_id)
        heat, available_heats = _select_archive_heat(connection, session_id=session_id, generation=generation)
        point_rows, source_point_count = _archive_point_rows(
            connection,
            heat_id=int(heat["id"]),
            first_at_us=int(heat["first_at_us"]),
            last_at_us=int(heat["last_at_us"]),
            max_points=max_points,
        )
        first_at_us = int(heat["first_at_us"])
        last_at_us = int(heat["last_at_us"])
        time_axes = _archive_time_axes(
            connection,
            heat_id=int(heat["id"]),
            session=session,
            first_at_us=first_at_us,
            last_at_us=last_at_us,
        )
        return {
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "session": _session_payload(session),
            "heat": _archive_heat_summary(heat),
            "available_heats": [_archive_heat_summary(candidate) for candidate in available_heats],
            "range": {
                "first_at_us": first_at_us,
                "last_at_us": last_at_us,
                "source_point_count": source_point_count,
                "downsampled": source_point_count > len(point_rows),
            },
            "keyframes": [
                {
                    "observed_at_us": int(row["observed_at_us"]),
                    "is_event_boundary": bool(row["is_event_boundary"]),
                    "source": {
                        "frame_id": row["source_frame_id"],
                        "message_id": row["source_message_id"],
                        "key": row["source_key"],
                    },
                    "snapshot": _manifest_playback_payload(
                        _with_archive_interval_derivation(_decode_playback_payload(row))
                    ),
                }
                for row in point_rows
            ],
            "markers": _archive_markers(
                connection,
                heat_id=int(heat["id"]),
                first_at_us=first_at_us,
                last_at_us=last_at_us,
            ),
            "time_axes": time_axes,
            "semantics": {
                "state": "last_observed",
                "series": "step",
                "time_axes": "playback uses durable receive time; source uses explicit Time Service clock anchors and may only be interpolated within one connection",
            },
            "system_assumption": _system_assumptions(),
        }


def read_archive_snapshot(
    session_id: str,
    *,
    database: str | Path | None = None,
    at_us: int | None = None,
    generation: int | None = None,
) -> dict[str, Any]:
    """Return the last confirmed archive state at or before one playhead time."""

    session_id = _require_session_id(session_id)
    at_us = _require_timestamp("at_us", at_us)
    with _readonly_snapshot(database) as connection:
        session = _require_archived_session(connection, session_id)
        heat, _ = _select_archive_heat(connection, session_id=session_id, generation=generation)
        first_at_us = int(heat["first_at_us"])
        last_at_us = int(heat["last_at_us"])
        requested_at_us = last_at_us if at_us is None else at_us
        if not first_at_us <= requested_at_us <= last_at_us:
            raise ReadValidationError("at_us must be inside the archived heat range")
        row = connection.execute(
            """
            SELECT observed_second,observed_at_us,source_frame_id,source_message_id,source_key,
                   projection_version,metric_version,is_event_boundary,payload_codec,payload,payload_sha256
            FROM playback_snapshots
            WHERE source_heat_id = ? AND observed_at_us <= ?
            ORDER BY observed_at_us DESC,observed_second DESC
            LIMIT 1
            """,
            (int(heat["id"]), requested_at_us),
        ).fetchone()
        if row is None:
            raise TimingReadError("Archive projection has no snapshot at the requested time")
        next_row = connection.execute(
            """
            SELECT observed_at_us
            FROM playback_snapshots
            WHERE source_heat_id = ? AND observed_at_us > ? AND observed_at_us <= ?
            ORDER BY observed_at_us,observed_second
            LIMIT 1
            """,
            (int(heat["id"]), requested_at_us, last_at_us),
        ).fetchone()
        return {
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "session": _session_payload(session),
            "heat": _archive_heat_summary(heat),
            "playback": {
                "requested_at_us": requested_at_us,
                "effective_at_us": int(row["observed_at_us"]),
                "next_at_us": int(next_row["observed_at_us"]) if next_row is not None else None,
                "is_event_boundary": bool(row["is_event_boundary"]),
            },
            "snapshot": _with_archive_interval_derivation(_decode_playback_payload(row)),
            "semantics": {"state": "last_observed", "series": "step"},
            "system_assumption": _system_assumptions(),
        }


def _validate_participant_filter(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_id: str | None,
) -> str | None:
    if participant_id is None:
        return None
    if not isinstance(participant_id, str) or not participant_id.strip() or len(participant_id) > 255:
        raise ReadValidationError("participant_id must be a non-empty string up to 255 characters")
    exists = connection.execute(
        "SELECT 1 FROM participants WHERE source_heat_id = ? AND id = ?",
        (heat_id, participant_id),
    ).fetchone()
    if exists is None:
        raise ScopeNotFoundError(f"Participant does not exist in this source heat: {participant_id}")
    return participant_id


def _fact_filters(
    *,
    participant_id: str | None,
    from_at_us: int | None,
    to_at_us: int | None,
    time_column: str,
) -> tuple[str, list[Any]]:
    start, end = _validate_range(from_at_us=from_at_us, to_at_us=to_at_us)
    clauses: list[str] = []
    parameters: list[Any] = []
    if participant_id is not None:
        clauses.append("f.participant_id = ?")
        parameters.append(participant_id)
    if start is not None:
        clauses.append(f"{time_column} >= ?")
        parameters.append(start)
    if end is not None:
        clauses.append(f"{time_column} <= ?")
        parameters.append(end)
    return (" AND " + " AND ".join(clauses)) if clauses else "", parameters


def read_laps(
    session_id: str,
    *,
    database: str | Path | None = None,
    participant_id: str | None = None,
    from_at_us: int | None = None,
    to_at_us: int | None = None,
    limit: int = DEFAULT_FACT_LIMIT,
) -> dict[str, Any]:
    """Read the newest bounded matching lap facts, returned chronologically."""

    session_id = _require_session_id(session_id)
    limit = _require_limit(limit)
    with _readonly_snapshot(database) as connection:
        _session_row(connection, session_id)
        heat = _latest_heat_row(connection, session_id)
        if heat is None:
            if participant_id is not None:
                raise ScopeNotFoundError("Analysis session has no source heat for participant facts yet")
            _validate_range(from_at_us=from_at_us, to_at_us=to_at_us)
            return {
                "session_id": session_id,
                "heat": None,
                "participant_id": None,
                "limit": limit,
                "items": [],
                "provenance_contract": _provenance_contract(),
            }
        heat_id = int(heat["id"])
        participant_id = _validate_participant_filter(connection, heat_id=heat_id, participant_id=participant_id)
        suffix, parameters = _fact_filters(
            participant_id=participant_id,
            from_at_us=from_at_us,
            to_at_us=to_at_us,
            time_column="COALESCE(f.completed_at_us,f.created_at_us)",
        )
        rows = connection.execute(
            f"""
            SELECT f.id,f.participant_id,p.start_number,p.team_name,p.class_name,
                   f.lap_number,f.completed_at_us,f.duration_ms,f.sectors_json,f.flag,
                   f.is_in_lap,f.is_out_lap,f.crosses_pit,f.is_clean,f.source_message_id,
                   f.source_key,f.created_at_us
            FROM laps f
            JOIN participants p ON p.id = f.participant_id
            WHERE f.source_heat_id = ?{suffix}
            ORDER BY COALESCE(f.completed_at_us,f.created_at_us) DESC,f.lap_number DESC
            LIMIT ?
            """,
            (heat_id, *parameters, limit),
        ).fetchall()
        items = [
            {
                "lap_id": row["id"],
                "participant_id": row["participant_id"],
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "class_name": row["class_name"],
                "lap_number": int(row["lap_number"]),
                "completed_at_us": row["completed_at_us"],
                "duration_ms": row["duration_ms"],
                "sectors": _json_value(row["sectors_json"], context="lap sectors"),
                "flag": row["flag"],
                "is_in_lap": bool(row["is_in_lap"]),
                "is_out_lap": bool(row["is_out_lap"]),
                "crosses_pit": bool(row["crosses_pit"]),
                "is_clean": bool(row["is_clean"]),
                "source": {
                    "message_id": row["source_message_id"],
                    "key": row["source_key"],
                    "created_at_us": row["created_at_us"],
                },
                "provenance": "measured",
            }
            for row in reversed(rows)
        ]
        return {
            "session_id": session_id,
            "heat": _heat_payload(heat),
            "participant_id": participant_id,
            "limit": limit,
            "items": items,
            "provenance_contract": _provenance_contract(),
        }


def read_pit_stops(
    session_id: str,
    *,
    database: str | Path | None = None,
    participant_id: str | None = None,
    from_at_us: int | None = None,
    to_at_us: int | None = None,
    limit: int = DEFAULT_FACT_LIMIT,
) -> dict[str, Any]:
    """Read the newest bounded matching confirmed/in-progress pit stop facts."""

    session_id = _require_session_id(session_id)
    limit = _require_limit(limit)
    with _readonly_snapshot(database) as connection:
        _session_row(connection, session_id)
        heat = _latest_heat_row(connection, session_id)
        if heat is None:
            if participant_id is not None:
                raise ScopeNotFoundError("Analysis session has no source heat for participant facts yet")
            _validate_range(from_at_us=from_at_us, to_at_us=to_at_us)
            return {
                "session_id": session_id,
                "heat": None,
                "participant_id": None,
                "limit": limit,
                "items": [],
                "provenance_contract": _provenance_contract(),
            }
        heat_id = int(heat["id"])
        participant_id = _validate_participant_filter(connection, heat_id=heat_id, participant_id=participant_id)
        suffix, parameters = _fact_filters(
            participant_id=participant_id,
            from_at_us=from_at_us,
            to_at_us=to_at_us,
            time_column="f.entered_at_us",
        )
        rows = connection.execute(
            f"""
            SELECT f.id,f.participant_id,p.start_number,p.team_name,p.class_name,
                   f.stop_number,f.entered_at_us,f.exited_at_us,f.entered_lap,f.exited_lap,
                   f.pit_lane_ms,f.pit_lane_duration_source_kind,f.completed,
                   f.entered_source_message_id,f.entered_source_key,
                   f.exited_source_message_id,f.exited_source_key,f.updated_at_us
            FROM pit_stops f
            JOIN participants p ON p.id = f.participant_id
            WHERE f.source_heat_id = ?{suffix}
            ORDER BY f.entered_at_us DESC,f.stop_number DESC
            LIMIT ?
            """,
            (heat_id, *parameters, limit),
        ).fetchall()
        items = [
            {
                "pit_stop_id": row["id"],
                "participant_id": row["participant_id"],
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "class_name": row["class_name"],
                "stop_number": int(row["stop_number"]),
                "entered_at_us": int(row["entered_at_us"]),
                "exited_at_us": row["exited_at_us"],
                "entered_lap": row["entered_lap"],
                "exited_lap": row["exited_lap"],
                "pit_lane_ms": _published_pit_lane_ms(row),
                "completed": bool(row["completed"]),
                "entered_source": {
                    "message_id": row["entered_source_message_id"],
                    "key": row["entered_source_key"],
                },
                "exited_source": {
                    "message_id": row["exited_source_message_id"],
                    "key": row["exited_source_key"],
                }
                if row["exited_source_key"] is not None
                else None,
                "updated_at_us": row["updated_at_us"],
                "provenance": "measured" if _has_measured_pit_lane_duration(row) else "observed_boundaries",
            }
            for row in reversed(rows)
        ]
        return {
            "session_id": session_id,
            "heat": _heat_payload(heat),
            "participant_id": participant_id,
            "limit": limit,
            "items": items,
            "provenance_contract": _provenance_contract(),
        }
