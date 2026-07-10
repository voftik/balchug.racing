"""Bounded, read-only views for the live timing dashboard.

This module is deliberately independent of FastAPI.  It opens SQLite in
``mode=ro`` for every public read, pins all queries for that read to one
snapshot, and returns ordinary JSON-ready dictionaries.  The HTTP layer can
therefore expose the same stable contract through REST and SSE without gaining
permission to mutate timing facts.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .config import now_us
from .db import connect


US_PER_SECOND = 1_000_000
US_PER_DAY = 86_400 * US_PER_SECOND
LIVE_FRESHNESS_US = 3 * US_PER_SECOND
STALE_FRESHNESS_US = 10 * US_PER_SECOND
MAX_HISTORY_RANGE_US = US_PER_DAY
MAX_CHART_POINTS = 720
DEFAULT_FACT_LIMIT = 200
MAX_FACT_LIMIT = 500
LIVE_SCHEMA_VERSION = "timing-live.v1"

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
        WHERE source_heat_id = ? AND ended_at_us IS NULL
        ORDER BY started_at_us DESC,id DESC
        LIMIT 1
        """,
        (heat_id,),
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
                   f.pit_lane_ms,f.completed,f.entered_source_message_id,f.entered_source_key,
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
                "pit_lane_ms": row["pit_lane_ms"],
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
