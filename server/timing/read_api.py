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
from .db import CheckpointError, RUNTIME_CHECKPOINT_FORMAT, RUNTIME_CHECKPOINT_FORMAT_VERSION, connect
from .metric_store import PLAYBACK_PAYLOAD_CODEC, PLAYBACK_PROJECTION_VERSION
from .normalization import OPEN_ENDED_TS_TIME, parse_ts_time, time_service_to_unix_us
from .normalizer_writer import RUNTIME_CHECKPOINT_REDUCER_VERSION, NormalizerError, validate_runtime_checkpoint
from .playback import PLAYBACK_SCHEMA_VERSION
from .result_grid import ResultGridStateError


US_PER_SECOND = 1_000_000
US_PER_DAY = 86_400 * US_PER_SECOND
LIVE_FRESHNESS_US = 3 * US_PER_SECOND
STALE_FRESHNESS_US = 10 * US_PER_SECOND
MAX_HISTORY_RANGE_US = US_PER_DAY
MAX_CHART_POINTS = 720
MAX_DASHBOARD_PARTICIPANTS = 4
MAX_DASHBOARD_LAPS_PER_PARTICIPANT = 2_000
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
# Health is a public diagnostic surface. Validate only a small newest window
# of compressed reducer checkpoints per request; restart/retention retain the
# unbounded fail-closed scan needed for recovery decisions.
MAX_HEALTH_CHECKPOINT_VALIDATIONS = 8
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

    def dashboard_history(
        self,
        session_id: str,
        *,
        participant_ids: Sequence[str],
        from_at_us: int | None = None,
        to_at_us: int | None = None,
        max_points: int = MAX_CHART_POINTS,
        now_at_us: int | None = None,
    ) -> dict[str, Any]:
        return read_dashboard_history(
            session_id,
            database=self.database,
            participant_ids=participant_ids,
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

    def race_control_messages(
        self,
        session_id: str,
        *,
        active_only: bool = False,
        limit: int = DEFAULT_FACT_LIMIT,
        observation_limit: int = DEFAULT_FACT_LIMIT,
    ) -> dict[str, Any]:
        """Read Race Control's current board and its immutable message ledger.

        The provider does not attach an occurrence timestamp to these events.
        ``observed_at_us`` is therefore the recorder receive time, while
        ``provider_occurred_at_us`` remains null unless a future provider
        payload supplies it explicitly.
        """

        return read_race_control_messages(
            session_id,
            database=self.database,
            active_only=active_only,
            limit=limit,
            observation_limit=observation_limit,
        )

    def ingest_health(self, session_id: str) -> dict[str, Any]:
        """Return the durable recorder/reducer health for one session.

        This is deliberately an operational read surface: it publishes frame
        counts and immutable anchors, never a provider payload or reducer
        checkpoint payload.
        """

        return read_ingest_health(session_id, database=self.database)

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


def _health_frame_payload(row: sqlite3.Row | None) -> dict[str, Any] | None:
    """Publish frame provenance without exposing its provider payload."""

    if row is None:
        return None
    return {
        "frame_id": int(row["id"]),
        "ingest_connection_id": row["ingest_connection_id"],
        "frame_sequence": int(row["frame_sequence"]),
        "received_at_us": int(row["received_at_us"]),
        "decode_state": row["decode_state"],
        "processed_at_us": row["processed_at_us"],
    }


def _health_frame_counts(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    after_frame_id: int | None = None,
) -> dict[str, int]:
    """Count retained recorder frames for a whole session or one replay tail."""

    suffix = " AND id > ?" if after_frame_id is not None else ""
    parameters: tuple[Any, ...] = (
        (session_id, after_frame_id) if after_frame_id is not None else (session_id,)
    )
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS retained_frame_count,
               COALESCE(SUM(CASE WHEN decode_state = 'decoded' THEN 1 ELSE 0 END), 0) AS decoded_frame_count,
               COALESCE(SUM(CASE WHEN processed_at_us IS NOT NULL THEN 1 ELSE 0 END), 0) AS processed_frame_count,
               COALESCE(
                 SUM(CASE WHEN processed_at_us IS NULL AND decode_state != 'failed' THEN 1 ELSE 0 END),
                 0
               ) AS pending_frame_count,
               COALESCE(SUM(CASE WHEN decode_state = 'failed' THEN 1 ELSE 0 END), 0) AS failed_frame_count
        FROM feed_frames
        WHERE analysis_session_id = ?{suffix}
        """,
        parameters,
    ).fetchone()
    assert row is not None
    return {
        "retained_frame_count": int(row["retained_frame_count"]),
        "decoded_frame_count": int(row["decoded_frame_count"]),
        "processed_frame_count": int(row["processed_frame_count"]),
        "pending_frame_count": int(row["pending_frame_count"]),
        "failed_frame_count": int(row["failed_frame_count"]),
    }


def _health_frame_row(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    after_frame_id: int | None = None,
    processed_only: bool = False,
    ascending: bool = False,
) -> sqlite3.Row | None:
    """Select one immutable recorder frame using the reducer's id ordering."""

    clauses = ["analysis_session_id = ?"]
    parameters: list[Any] = [session_id]
    if after_frame_id is not None:
        clauses.append("id > ?")
        parameters.append(after_frame_id)
    if processed_only:
        clauses.append("processed_at_us IS NOT NULL")
    direction = "ASC" if ascending else "DESC"
    return connection.execute(
        f"""
        SELECT id,ingest_connection_id,frame_sequence,received_at_us,decode_state,processed_at_us
        FROM feed_frames
        WHERE {' AND '.join(clauses)}
        ORDER BY id {direction}
        LIMIT 1
        """,
        tuple(parameters),
    ).fetchone()


def _runtime_checkpoint_health(
    connection: sqlite3.Connection,
    *,
    session_id: str,
) -> dict[str, Any]:
    """Return runtime checkpoint provenance and the newest semantic restore point.

    A structurally eligible checkpoint still may be corrupt or from an older
    reducer. The latest field therefore comes only from a write-free semantic
    reducer restore validation, matching the worker/retention contract.
    """

    total_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM state_checkpoints AS checkpoint
            JOIN source_heats AS heat ON heat.id = checkpoint.source_heat_id
            WHERE heat.analysis_session_id = ?
              AND checkpoint.checkpoint_format = ?
              AND checkpoint.checkpoint_format_version = ?
            """,
            (session_id, RUNTIME_CHECKPOINT_FORMAT, RUNTIME_CHECKPOINT_FORMAT_VERSION),
        ).fetchone()[0]
    )
    eligible_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM state_checkpoints AS checkpoint
            JOIN source_heats AS heat ON heat.id = checkpoint.source_heat_id
            JOIN feed_frames AS frame ON frame.id = checkpoint.source_frame_id
            LEFT JOIN timing_raw_retention_floors AS floor
              ON floor.analysis_session_id = heat.analysis_session_id
            WHERE heat.analysis_session_id = ?
              AND checkpoint.checkpoint_format = ?
              AND checkpoint.checkpoint_format_version = ?
              AND frame.analysis_session_id = heat.analysis_session_id
              AND frame.processed_at_us IS NOT NULL
              AND (
                floor.deleted_through_frame_id IS NULL
                OR checkpoint.source_frame_id > floor.deleted_through_frame_id
              )
            """,
            (session_id, RUNTIME_CHECKPOINT_FORMAT, RUNTIME_CHECKPOINT_FORMAT_VERSION),
        ).fetchone()[0]
    )
    rows = connection.execute(
        """
        SELECT checkpoint.id,checkpoint.source_heat_id,heat.generation AS source_heat_generation,
               checkpoint.source_frame_id,checkpoint.source_key,checkpoint.observed_at_us,
               checkpoint.state_hash,checkpoint.checkpoint_format,checkpoint.checkpoint_format_version,
               checkpoint.reducer_version,checkpoint.codec,checkpoint.payload,checkpoint.created_at_us,
               heat.analysis_session_id,
               frame.ingest_connection_id AS anchor_connection_id,
               frame.frame_sequence AS anchor_frame_sequence,
               frame.analysis_session_id AS anchor_analysis_session_id,
               frame.received_at_us AS anchor_received_at_us,
               frame.processed_at_us AS anchor_processed_at_us
        FROM state_checkpoints AS checkpoint
        JOIN source_heats AS heat ON heat.id = checkpoint.source_heat_id
        JOIN feed_frames AS frame ON frame.id = checkpoint.source_frame_id
        LEFT JOIN timing_raw_retention_floors AS floor
          ON floor.analysis_session_id = heat.analysis_session_id
        WHERE heat.analysis_session_id = ?
          AND checkpoint.checkpoint_format = ?
          AND checkpoint.checkpoint_format_version = ?
          AND frame.analysis_session_id = heat.analysis_session_id
          AND frame.processed_at_us IS NOT NULL
          AND (
            floor.deleted_through_frame_id IS NULL
            OR checkpoint.source_frame_id > floor.deleted_through_frame_id
          )
        ORDER BY checkpoint.source_frame_id DESC,checkpoint.id DESC
        LIMIT ?
        """,
        (
            session_id,
            RUNTIME_CHECKPOINT_FORMAT,
            RUNTIME_CHECKPOINT_FORMAT_VERSION,
            MAX_HEALTH_CHECKPOINT_VALIDATIONS,
        ),
    )
    row: sqlite3.Row | None = None
    rejected_newer_count = 0
    for row in rows:
        if row["reducer_version"] != RUNTIME_CHECKPOINT_REDUCER_VERSION:
            rejected_newer_count += 1
            continue
        try:
            validate_runtime_checkpoint(connection, row)
        except (CheckpointError, NormalizerError, ResultGridStateError, ValueError, TypeError, KeyError, IndexError):
            rejected_newer_count += 1
            continue
        break
    else:
        row = None
    validation_truncated = row is None and eligible_count > rejected_newer_count
    latest = (
        {
            "checkpoint_id": int(row["id"]),
            "source_heat_id": int(row["source_heat_id"]),
            "source_heat_generation": int(row["source_heat_generation"]),
            "source_frame_id": int(row["source_frame_id"]),
            "source_key": row["source_key"],
            "observed_at_us": int(row["observed_at_us"]),
            "anchor_received_at_us": int(row["anchor_received_at_us"]),
            "anchor_processed_at_us": int(row["anchor_processed_at_us"]),
            "state_hash": row["state_hash"],
            "checkpoint_format": row["checkpoint_format"],
            "checkpoint_format_version": int(row["checkpoint_format_version"]),
            "reducer_version": row["reducer_version"],
            "created_at_us": int(row["created_at_us"]),
        }
        if row is not None
        else None
    )
    return {
        "runtime_checkpoint_count": total_count,
        "eligible_runtime_checkpoint_count": eligible_count,
        "latest_validation": {
            "status": (
                "RESTORABLE"
                if row is not None
                else "SCAN_LIMIT_REACHED"
                if validation_truncated
                else "NO_RESTORABLE"
            ),
            "rejected_newer_or_incompatible_checkpoint_count": rejected_newer_count,
            "scan_limit": MAX_HEALTH_CHECKPOINT_VALIDATIONS,
            "truncated": validation_truncated,
        },
        "latest": latest,
    }


def _retention_floor_payload(connection: sqlite3.Connection, *, session_id: str) -> dict[str, Any] | None:
    """Make an irreversible RAW retention boundary explicit to operators."""

    row = connection.execute(
        """
        SELECT deleted_through_frame_id,deleted_through_received_at_us,checkpoint_id,created_at_us,updated_at_us
        FROM timing_raw_retention_floors
        WHERE analysis_session_id = ?
        """,
        (session_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "deleted_through_frame_id": int(row["deleted_through_frame_id"]),
        "deleted_through_received_at_us": int(row["deleted_through_received_at_us"]),
        "checkpoint_id": row["checkpoint_id"],
        "created_at_us": int(row["created_at_us"]),
        "updated_at_us": int(row["updated_at_us"]),
    }


def _session_open_gap(connection: sqlite3.Connection, *, session_id: str) -> dict[str, Any] | None:
    """Return the newest still-open ingest outage across every heat in a session."""

    row = connection.execute(
        """
        SELECT id,source_heat_id,ingest_connection_id,started_at_us,reason
        FROM ingest_gaps
        WHERE analysis_session_id = ? AND ended_at_us IS NULL
        ORDER BY started_at_us DESC,id DESC
        LIMIT 1
        """,
        (session_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "gap_id": int(row["id"]),
        "source_heat_id": row["source_heat_id"],
        "connection_id": row["ingest_connection_id"],
        "started_at_us": int(row["started_at_us"]),
        "reason": row["reason"],
    }


def _latest_restore_event(connection: sqlite3.Connection, *, session_id: str) -> dict[str, Any] | None:
    """Expose the last durable reducer bootstrap outcome, not worker memory."""

    row = connection.execute(
        """
        SELECT id,source_heat_id,checkpoint_id,anchor_frame_id,outcome,reason,replayed_tail_frames,created_at_us
        FROM normalizer_restore_events
        WHERE analysis_session_id = ?
        ORDER BY created_at_us DESC,id DESC
        LIMIT 1
        """,
        (session_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "restore_event_id": int(row["id"]),
        "source_heat_id": row["source_heat_id"],
        "checkpoint_id": row["checkpoint_id"],
        "anchor_frame_id": row["anchor_frame_id"],
        "outcome": row["outcome"],
        "reason": row["reason"],
        "replayed_tail_frames": int(row["replayed_tail_frames"]),
        "created_at_us": int(row["created_at_us"]),
    }


def _ingest_health_semantics() -> dict[str, str]:
    """Keep operational counter meanings literal for API and runbook users."""

    return {
        "raw.retained_frame_count": "Retained immutable feed_frames for this analysis session.",
        "processing.pending_frame_count": (
            "Frames without a processed marker and without a terminal decode failure."
        ),
        "processing.failed_frame_count": (
            "Frames whose decoder entered terminal failed state; they are not silently retried."
        ),
        "runtime_checkpoints.runtime_checkpoint_count": (
            "All persisted timing-normalizer checkpoints for the session, including anchors no longer eligible for restore."
        ),
        "runtime_checkpoints.eligible_runtime_checkpoint_count": (
            "Structurally eligible checkpoints whose anchor frame remains retained, processed, and belongs to this session."
        ),
        "runtime_checkpoints.latest_validation": (
            "Bounded validation of newest checkpoint candidates; invalid/incompatible candidates are skipped until one passes or the scan limit is reached."
        ),
        "runtime_checkpoints.latest": (
            "Newest semantically restorable timing-normalizer checkpoint anchored to a retained processed frame."
        ),
        "tail": "Frames after the newest restorable checkpoint in durable feed_frame id order; received_span_us is their recorder-time span.",
        "last_restore": "Latest durable normalizer bootstrap audit event, not transient worker memory.",
    }


def read_ingest_health(
    session_id: str,
    *,
    database: str | Path | None = None,
) -> dict[str, Any]:
    """Return a bounded, read-only ingestion and reducer health snapshot.

    Frame identity is intentionally based on the monotonically assigned
    ``feed_frames.id`` rather than receive timestamps: several physical
    SignalR frames may carry the same recorder timestamp.
    """

    session_id = _require_session_id(session_id)
    with _readonly_snapshot(database) as connection:
        session = _session_row(connection, session_id)
        heat = _latest_heat_row(connection, session_id)
        all_frames = _health_frame_counts(connection, session_id=session_id)
        latest_runtime = _runtime_checkpoint_health(connection, session_id=session_id)
        checkpoint = latest_runtime["latest"]
        anchor_frame_id = checkpoint["source_frame_id"] if checkpoint is not None else None
        tail_counts = _health_frame_counts(
            connection,
            session_id=session_id,
            after_frame_id=anchor_frame_id,
        )
        tail_first = _health_frame_row(
            connection,
            session_id=session_id,
            after_frame_id=anchor_frame_id,
            ascending=True,
        )
        tail_latest = _health_frame_row(
            connection,
            session_id=session_id,
            after_frame_id=anchor_frame_id,
        )
        tail_received_span_us = (
            int(tail_latest["received_at_us"]) - int(tail_first["received_at_us"])
            if tail_first is not None and tail_latest is not None
            else None
        )
        return {
            "schema_version": LIVE_SCHEMA_VERSION,
            "session_id": session_id,
            "session": _session_payload(session),
            "heat": _heat_payload(heat) if heat is not None else None,
            "raw": {
                "retained_frame_count": all_frames["retained_frame_count"],
                "decoded_frame_count": all_frames["decoded_frame_count"],
                "latest_frame": _health_frame_payload(
                    _health_frame_row(connection, session_id=session_id)
                ),
                "retention_floor": _retention_floor_payload(connection, session_id=session_id),
            },
            "processing": {
                "processed_frame_count": all_frames["processed_frame_count"],
                "pending_frame_count": all_frames["pending_frame_count"],
                "failed_frame_count": all_frames["failed_frame_count"],
                "latest_processed_frame": _health_frame_payload(
                    _health_frame_row(connection, session_id=session_id, processed_only=True)
                ),
            },
            "runtime_checkpoints": latest_runtime,
            "tail": {
                "anchor_frame_id": anchor_frame_id,
                "scope": (
                    "after_latest_runtime_checkpoint"
                    if anchor_frame_id is not None
                    else "all_retained_raw_no_checkpoint"
                ),
                **tail_counts,
                "received_span_us": tail_received_span_us,
                "first_frame": _health_frame_payload(tail_first),
                "latest_frame": _health_frame_payload(tail_latest),
            },
            "open_gap": _session_open_gap(connection, session_id=session_id),
            "last_restore": _latest_restore_event(connection, session_id=session_id),
            "semantics": _ingest_health_semantics(),
            "provenance_contract": _provenance_contract(),
        }


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


def _interval_source_fact_payload(row: sqlite3.Row, prefix: str) -> dict[str, Any] | None:
    """Expose one current GAP/DIFF pointer with its own source context.

    GAP and DIFF cells are sparse in the provider grid.  The generic current
    state row can therefore be newer than the displayed interval.  The live
    read model must never use that row's cached scalar as interval evidence:
    only the immutable source fact joined by its pointer is public here.
    """

    if row[f"{prefix}_id"] is None:
        return None
    return {
        "id": row[f"{prefix}_id"],
        "field_kind": row[f"{prefix}_interval_kind"],
        "raw_value": row[f"{prefix}_raw_value"],
        "value_ms": row[f"{prefix}_interval_ms"],
        "value_kind": row[f"{prefix}_value_kind"],
        "cell_observation_id": row[f"{prefix}_source_cell_observation_id"],
        "source_message_id": row[f"{prefix}_source_message_id"],
        "source_key": row[f"{prefix}_source_key"],
        "source_change_ordinal": row[f"{prefix}_source_change_ordinal"],
        "observed_at_us": row[f"{prefix}_observed_at_us"],
        "source_handle": row[f"{prefix}_source_handle"],
        "observation_kind": row[f"{prefix}_observation_kind"],
        "subject_position_overall": row[f"{prefix}_source_position_overall"],
        "subject_state_kind": row[f"{prefix}_source_state_kind"],
        "subject_laps": row[f"{prefix}_source_laps"],
        "target_participant_id": row[f"{prefix}_target_participant_id"],
        "target_position_overall": row[f"{prefix}_target_position_overall"],
        "target_state_kind": row[f"{prefix}_target_state_kind"],
        "target_laps": row[f"{prefix}_target_laps"],
        "relation_kind": row[f"{prefix}_relation_kind"],
    }


def _interval_source_value(fact: Mapping[str, Any] | None) -> tuple[int | None, str | None, str | None]:
    """Return compatibility scalar fields only when the exact fact says TIME."""

    if fact is None:
        return None, None, None
    value_ms = fact.get("value_ms")
    value_ms = value_ms if _archive_int(value_ms) is not None and fact.get("value_kind") == "TIME" else None
    raw_value = fact.get("raw_value")
    raw_value = raw_value if isinstance(raw_value, str) else None
    value_kind = fact.get("value_kind")
    value_kind = value_kind if isinstance(value_kind, str) else None
    return value_ms, raw_value, value_kind


def _participants(connection: sqlite3.Connection, heat_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT p.id,p.external_key,p.transponder_id,p.start_number,p.team_name,p.car_name,
               p.class_name,p.class_name_key,p.is_ours,p.active,p.first_seen_at_us,p.last_seen_at_us,
               c.position_overall,c.position_class,c.marker,c.laps,c.state,c.state_raw,c.state_kind,
               c.current_driver_name,c.current_driver_stint_raw,c.last_lap_ms,c.last_lap_number,
               c.best_lap_ms,c.best_lap_number,c.last_sectors_json,c.best_sectors_json,
               c.last_speeds_json,
               c.sector_json,c.speed_kph,c.pit_time_raw,c.provider_pit_count,
               c.state_timer_target_raw,c.state_timer_target_provider_us,c.state_timer_target_at_us,
               c.provider_pit_count_raw,c.source_message_id,c.source_key,c.updated_at_us,
               c.state_source_cell_observation_id,c.state_source_message_id,
               c.state_source_key,c.state_observed_at_us,
               gap_fact.id AS gap_fact_id,gap_fact.interval_kind AS gap_fact_interval_kind,
               gap_fact.raw_value AS gap_fact_raw_value,gap_fact.interval_ms AS gap_fact_interval_ms,
               gap_fact.value_kind AS gap_fact_value_kind,
               gap_fact.source_cell_observation_id AS gap_fact_source_cell_observation_id,
               gap_fact.source_message_id AS gap_fact_source_message_id,gap_fact.source_key AS gap_fact_source_key,
               gap_fact.source_change_ordinal AS gap_fact_source_change_ordinal,
               gap_fact.observed_at_us AS gap_fact_observed_at_us,gap_fact.source_handle AS gap_fact_source_handle,
               gap_fact.observation_kind AS gap_fact_observation_kind,
               gap_fact.source_position_overall AS gap_fact_source_position_overall,
               gap_fact.source_state_kind AS gap_fact_source_state_kind,gap_fact.source_laps AS gap_fact_source_laps,
               gap_fact.target_participant_id AS gap_fact_target_participant_id,
               gap_fact.target_position_overall AS gap_fact_target_position_overall,
               gap_fact.target_state_kind AS gap_fact_target_state_kind,gap_fact.target_laps AS gap_fact_target_laps,
               gap_fact.relation_kind AS gap_fact_relation_kind,
               diff_fact.id AS diff_fact_id,diff_fact.interval_kind AS diff_fact_interval_kind,
               diff_fact.raw_value AS diff_fact_raw_value,diff_fact.interval_ms AS diff_fact_interval_ms,
               diff_fact.value_kind AS diff_fact_value_kind,
               diff_fact.source_cell_observation_id AS diff_fact_source_cell_observation_id,
               diff_fact.source_message_id AS diff_fact_source_message_id,diff_fact.source_key AS diff_fact_source_key,
               diff_fact.source_change_ordinal AS diff_fact_source_change_ordinal,
               diff_fact.observed_at_us AS diff_fact_observed_at_us,diff_fact.source_handle AS diff_fact_source_handle,
               diff_fact.observation_kind AS diff_fact_observation_kind,
               diff_fact.source_position_overall AS diff_fact_source_position_overall,
               diff_fact.source_state_kind AS diff_fact_source_state_kind,diff_fact.source_laps AS diff_fact_source_laps,
               diff_fact.target_participant_id AS diff_fact_target_participant_id,
               diff_fact.target_position_overall AS diff_fact_target_position_overall,
               diff_fact.target_state_kind AS diff_fact_target_state_kind,diff_fact.target_laps AS diff_fact_target_laps,
               diff_fact.relation_kind AS diff_fact_relation_kind,
               i.driver_name_raw AS identity_driver_name,i.source_message_id AS identity_source_message_id,
               i.source_key AS identity_source_key,i.observed_at_us AS identity_observed_at_us,
               lap_count.completed_laps AS canonical_completed_laps,
               lap_count.observed_laps AS canonical_observed_laps,
               lap_count.coverage_complete AS canonical_coverage_complete,
               lap_count.exact_laps AS canonical_exact_laps,
               lap_count.latest_finished_at_provider_us AS canonical_latest_finished_at_provider_us,
               gap_coordinate.raw_gap_value AS coordinate_raw_gap_value,
               gap_coordinate.display_value_kind AS coordinate_display_value_kind,
               gap_coordinate.lap_group_completed_laps AS coordinate_lap_group_completed_laps,
               gap_coordinate.time_from_lap_group_leader_ms AS coordinate_group_time_ms,
               gap_coordinate.lap_group_leader_participant_id AS coordinate_group_leader_id,
               gap_coordinate.lap_group_leader_position_overall AS coordinate_group_leader_position,
               gap_coordinate.gap_to_overall_leader_laps AS coordinate_gap_laps,
               gap_coordinate.gap_to_overall_leader_residual_ms AS coordinate_gap_residual_ms,
               gap_coordinate.coordinate_status,gap_coordinate.source_cell_observation_id AS coordinate_source_cell_id,
               gap_coordinate.source_cell_message_id AS coordinate_source_cell_message_id,
               gap_coordinate.source_cell_key AS coordinate_source_cell_key,
               gap_coordinate.source_cell_observed_at_us AS coordinate_source_cell_observed_at_us,
               gap_snapshot.id AS coordinate_snapshot_id,gap_snapshot.observed_at_us AS coordinate_observed_at_us,
               gap_snapshot.completeness AS coordinate_snapshot_completeness
            FROM participants p
            LEFT JOIN participant_state_current c
              ON c.source_heat_id = p.source_heat_id AND c.participant_id = p.id
            LEFT JOIN participant_interval_source_facts AS gap_fact ON gap_fact.id = c.gap_interval_fact_id
            LEFT JOIN participant_interval_source_facts AS diff_fact ON diff_fact.id = c.diff_interval_fact_id
            LEFT JOIN participant_identity_segments i
          ON i.source_heat_id = p.source_heat_id AND i.participant_id = p.id AND i.ended_at_us IS NULL
            LEFT JOIN (
              SELECT source_heat_id,participant_id,MAX(lap_number) AS completed_laps,
                     COUNT(*) AS observed_laps,MIN(coverage_complete) AS coverage_complete,
                     SUM(CASE WHEN duration_reconciliation = 'EXACT' THEN 1 ELSE 0 END) AS exact_laps,
                     MAX(finished_at_provider_us) AS latest_finished_at_provider_us
              FROM canonical_laps
              GROUP BY source_heat_id,participant_id
            ) AS lap_count
              ON lap_count.source_heat_id = p.source_heat_id AND lap_count.participant_id = p.id
            LEFT JOIN gap_coordinate_snapshots AS gap_snapshot
              ON gap_snapshot.id = (
                SELECT latest_gap.id FROM gap_coordinate_snapshots AS latest_gap
                WHERE latest_gap.source_heat_id = p.source_heat_id
                ORDER BY latest_gap.observed_second DESC,latest_gap.id DESC LIMIT 1
              )
            LEFT JOIN participant_gap_coordinates AS gap_coordinate
              ON gap_coordinate.snapshot_id = gap_snapshot.id AND gap_coordinate.participant_id = p.id
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
        gap_fact = _interval_source_fact_payload(row, "gap_fact")
        diff_fact = _interval_source_fact_payload(row, "diff_fact")
        gap_ms, gap_raw, gap_kind = _interval_source_value(gap_fact)
        diff_ms, diff_raw, diff_kind = _interval_source_value(diff_fact)
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
                "gap_ms": gap_ms,
                "gap_raw": gap_raw,
                "gap_kind": gap_kind,
                "gap_source_fact": gap_fact,
                "diff_ms": diff_ms,
                "diff_raw": diff_raw,
                "diff_kind": diff_kind,
                "diff_source_fact": diff_fact,
                "sector": _json_value(row["sector_json"], context="participant sector"),
                "speed_kph": row["speed_kph"],
                "pit_time_raw": row["pit_time_raw"],
                "provider_pit_count": row["provider_pit_count"],
                "provider_pit_count_raw": row["provider_pit_count_raw"],
                "state_timer_target_raw": row["state_timer_target_raw"],
                "state_timer_target_provider_us": row["state_timer_target_provider_us"],
                "state_timer_target_at_us": row["state_timer_target_at_us"],
                "state_source": (
                    {
                        "cell_observation_id": row["state_source_cell_observation_id"],
                        "message_id": row["state_source_message_id"],
                        "key": row["state_source_key"],
                        "observed_at_us": row["state_observed_at_us"],
                    }
                    if row["state_source_key"] is not None
                    else None
                ),
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
                "lap_count": (
                    {
                        "completed_laps": int(row["canonical_completed_laps"])
                        if row["canonical_completed_laps"] is not None
                        else None,
                        "observed_complete_laps": int(row["canonical_observed_laps"]),
                        "coverage_complete": bool(row["canonical_coverage_complete"]),
                        "exact_last_laps": int(row["canonical_exact_laps"] or 0),
                        "latest_finished_at_provider_us": row["canonical_latest_finished_at_provider_us"],
                    }
                    if row["canonical_observed_laps"] is not None
                    else None
                ),
                "gap_coordinate": (
                    {
                        "snapshot_id": int(row["coordinate_snapshot_id"]),
                        "observed_at_us": int(row["coordinate_observed_at_us"]),
                        "snapshot_completeness": row["coordinate_snapshot_completeness"],
                        "status": row["coordinate_status"],
                        "raw_gap_value": row["coordinate_raw_gap_value"],
                        "display_value_kind": row["coordinate_display_value_kind"],
                        "lap_group_completed_laps": row["coordinate_lap_group_completed_laps"],
                        "time_from_lap_group_leader_ms": row["coordinate_group_time_ms"],
                        "lap_group_leader_participant_id": row["coordinate_group_leader_id"],
                        "lap_group_leader_position_overall": row["coordinate_group_leader_position"],
                        "gap_to_overall_leader_laps": row["coordinate_gap_laps"],
                        "gap_to_overall_leader_residual_ms": row["coordinate_gap_residual_ms"],
                        "source": {
                            "cell_observation_id": row["coordinate_source_cell_id"],
                            "message_id": row["coordinate_source_cell_message_id"],
                            "key": row["coordinate_source_cell_key"],
                            "observed_at_us": row["coordinate_source_cell_observed_at_us"],
                        },
                    }
                    if row["coordinate_snapshot_id"] is not None
                    else None
                ),
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


def _dashboard_participant_ids(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_ids: Sequence[str],
) -> list[str]:
    if isinstance(participant_ids, (str, bytes)):
        raise ReadValidationError("participant_ids must be a sequence")
    unique: list[str] = []
    for participant_id in participant_ids:
        validated = _validate_participant_filter(
            connection,
            heat_id=heat_id,
            participant_id=participant_id,
        )
        assert validated is not None
        if validated not in unique:
            unique.append(validated)
    if not unique:
        raise ReadValidationError("at least one participant_id is required")
    if len(unique) > MAX_DASHBOARD_PARTICIPANTS:
        raise ReadValidationError(
            f"no more than {MAX_DASHBOARD_PARTICIPANTS} participants may be displayed"
        )
    return unique


def _dashboard_participants(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_ids: Sequence[str],
) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in participant_ids)
    rows = connection.execute(
        f"""
        SELECT participant.id,participant.start_number,participant.team_name,
               participant.car_name,participant.class_name,participant.class_name_key,
               participant.is_ours,participant.active,state.current_driver_name
        FROM participants AS participant
        LEFT JOIN participant_state_current AS state
          ON state.source_heat_id = participant.source_heat_id
         AND state.participant_id = participant.id
        WHERE participant.source_heat_id = ?
          AND participant.id IN ({placeholders})
        """,
        (heat_id, *participant_ids),
    ).fetchall()
    by_id = {
        row["id"]: {
            "participant_id": row["id"],
            "start_number": row["start_number"],
            "team_name": row["team_name"],
            "car_name": row["car_name"],
            "class_name": row["class_name"],
            "class_key": row["class_name_key"] or _class_key(row["class_name"]),
            "driver_name": row["current_driver_name"],
            "is_ours": bool(row["is_ours"]),
            "active": bool(row["active"]),
        }
        for row in rows
    }
    return [by_id[participant_id] for participant_id in participant_ids]


def _dashboard_lap_series(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_ids: Sequence[str],
    first_at_us: int,
    last_at_us: int,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    placeholders = ",".join("?" for _ in participant_ids)
    rows = connection.execute(
        f"""
        WITH ranked AS (
          SELECT ledger.source_cell_observation_id,ledger.participant_id,
                 ledger.source_message_id,ledger.source_key,ledger.source_change_ordinal,
                 ledger.source_frame_id,ledger.source_message_ordinal,ledger.source_handle,
                 ledger.observed_at_us,ledger.duration_ms,ledger.classification,
                 ledger.linked_lap_id,ledger.sectors_json,
                 ledger.sectors_source_cell_observation_ids_json,
                 COALESCE(canonical_lap.lap_number,lap.lap_number) AS lap_number,
                 COALESCE(canonical_lap.finished_at_us,lap.completed_at_us) AS completed_at_us,
                 canonical_lap.id AS canonical_lap_id,
                 canonical_lap.started_at_provider_us,canonical_lap.finished_at_provider_us,
                 lap.flag,
                 COALESCE(lap.is_in_lap,finish_boundary.boundary_kind = 'PIT_FINISH') AS is_in_lap,
                 COALESCE(
                   lap.is_out_lap,
                   start_boundary.boundary_kind = 'PIT_FINISH' AND finish_boundary.boundary_kind = 'MAIN_FINISH'
                 ) AS is_out_lap,
                 COALESCE(lap.crosses_pit,canonical_lap.is_pit_lap) AS crosses_pit,
                 lap.is_clean,
                 ROW_NUMBER() OVER (
                   PARTITION BY ledger.participant_id
                   ORDER BY ledger.source_frame_id,ledger.source_message_ordinal,
                            ledger.source_change_ordinal,ledger.source_cell_observation_id
                 ) AS capture_lap_index
          FROM result_last_cell_ledger AS ledger
          LEFT JOIN laps AS lap ON lap.id = ledger.linked_lap_id
          LEFT JOIN canonical_laps AS canonical_lap
            ON canonical_lap.source_last_cell_observation_id = ledger.source_cell_observation_id
          LEFT JOIN canonical_lap_boundaries AS start_boundary
            ON start_boundary.id = canonical_lap.start_boundary_id
          LEFT JOIN canonical_lap_boundaries AS finish_boundary
            ON finish_boundary.id = canonical_lap.finish_boundary_id
          WHERE ledger.source_heat_id = ?
            AND ledger.participant_id IN ({placeholders})
            AND ledger.classification = 'CONFIRMED_LAP'
            AND ledger.source_handle = 'r_c'
            AND ledger.duration_ms IS NOT NULL AND ledger.duration_ms > 0
        )
        SELECT ranked.*,
               (
                 SELECT segment.driver_name_raw
                 FROM participant_identity_segments AS segment
                 WHERE segment.participant_id = ranked.participant_id
                   AND segment.started_at_us <= ranked.observed_at_us
                   AND (segment.ended_at_us IS NULL OR segment.ended_at_us > ranked.observed_at_us)
                 ORDER BY segment.started_at_us DESC,segment.id DESC
                 LIMIT 1
               ) AS driver_name
        FROM ranked
        WHERE observed_at_us >= ? AND observed_at_us <= ?
        ORDER BY source_frame_id,source_message_ordinal,source_change_ordinal,
                 source_cell_observation_id
        """,
        (heat_id, *participant_ids, first_at_us, last_at_us),
    ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = {participant_id: [] for participant_id in participant_ids}
    source_counts: dict[str, int] = {participant_id: 0 for participant_id in participant_ids}
    for row in rows:
        participant_id = str(row["participant_id"])
        source_counts[participant_id] += 1
        grouped[participant_id].append(
            {
                "capture_at_us": int(row["completed_at_us"])
                if row["canonical_lap_id"] is not None and row["completed_at_us"] is not None
                else int(row["observed_at_us"]),
                "completed_at_us": int(row["completed_at_us"]) if row["completed_at_us"] is not None else None,
                "capture_lap_index": int(row["capture_lap_index"]),
                "lap_number": int(row["lap_number"]) if row["lap_number"] is not None else None,
                "duration_ms": int(row["duration_ms"]),
                "sectors": _archive_source_proven_sectors(
                    sectors_json=row["sectors_json"],
                    source_cell_observation_ids_json=row["sectors_source_cell_observation_ids_json"],
                    is_linked_to_last=True,
                ),
                "driver_name": row["driver_name"],
                "flag": row["flag"],
                "is_in_lap": bool(row["is_in_lap"])
                if row["linked_lap_id"] is not None or row["canonical_lap_id"] is not None
                else None,
                "is_out_lap": bool(row["is_out_lap"])
                if row["linked_lap_id"] is not None or row["canonical_lap_id"] is not None
                else None,
                "crosses_pit": bool(row["crosses_pit"])
                if row["linked_lap_id"] is not None or row["canonical_lap_id"] is not None
                else None,
                "is_clean": bool(row["is_clean"]) if row["linked_lap_id"] is not None else None,
                "canonical_lap_id": row["canonical_lap_id"],
                "started_at_provider_us": row["started_at_provider_us"],
                "finished_at_provider_us": row["finished_at_provider_us"],
                "source": {
                    "cell_observation_id": int(row["source_cell_observation_id"]),
                    "message_id": int(row["source_message_id"]),
                    "frame_id": int(row["source_frame_id"]),
                    "message_ordinal": int(row["source_message_ordinal"]),
                    "change_ordinal": int(row["source_change_ordinal"]),
                    "handle": row["source_handle"],
                    "key": row["source_key"],
                    "classification": row["classification"],
                },
            }
        )
    for participant_id, events in grouped.items():
        if len(events) > MAX_DASHBOARD_LAPS_PER_PARTICIPANT:
            grouped[participant_id] = events[-MAX_DASHBOARD_LAPS_PER_PARTICIPANT:]
    return grouped, source_counts


def _dashboard_flags(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    first_at_us: int,
    last_at_us: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT flag,provider_code,provider_label,started_at_us,ended_at_us,
               observed_started_at_us,observed_ended_at_us,
               calibrated_started_at_us,calibrated_ended_at_us
        FROM track_flag_periods
        WHERE source_heat_id = ?
          AND started_at_us <= ? AND (ended_at_us IS NULL OR ended_at_us >= ?)
        ORDER BY started_at_us,id
        LIMIT ?
        """,
        (heat_id, last_at_us, first_at_us, MAX_ARCHIVE_MARKERS),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        started = next(
            (
                int(value)
                for value in (
                    row["calibrated_started_at_us"],
                    row["observed_started_at_us"],
                    row["started_at_us"],
                )
                if value is not None
            ),
            first_at_us,
        )
        ended = next(
            (
                int(value)
                for value in (
                    row["calibrated_ended_at_us"],
                    row["observed_ended_at_us"],
                    row["ended_at_us"],
                )
                if value is not None
            ),
            None,
        )
        result.append(
            {
                "flag": row["flag"],
                "provider_code": row["provider_code"],
                "provider_label": row["provider_label"],
                "started_at_us": max(first_at_us, started),
                "ended_at_us": min(last_at_us, ended) if ended is not None else None,
                "carried_into_range": started < first_at_us,
            }
        )
    return result


def _dashboard_ingest_gaps(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    heat_id: int,
    first_at_us: int,
    last_at_us: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT id,started_at_us,ended_at_us,reason
        FROM ingest_gaps
        WHERE analysis_session_id = ? AND (source_heat_id = ? OR source_heat_id IS NULL)
          AND started_at_us <= ? AND (ended_at_us IS NULL OR ended_at_us >= ?)
        ORDER BY started_at_us,id
        LIMIT ?
        """,
        (session_id, heat_id, last_at_us, first_at_us, MAX_ARCHIVE_MARKERS),
    ).fetchall()
    return [
        {
            "gap_id": int(row["id"]),
            "started_at_us": max(first_at_us, int(row["started_at_us"])),
            "ended_at_us": min(last_at_us, int(row["ended_at_us"])) if row["ended_at_us"] is not None else None,
            "reason": row["reason"],
            "carried_into_range": int(row["started_at_us"]) < first_at_us,
        }
        for row in rows
    ]


def _dashboard_interval_points(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    heat_id: int,
    first_at_us: int,
    last_at_us: int,
    max_points: int,
) -> tuple[list[dict[str, Any]], int]:
    scope = MetricScopeRequest("session", session_id)
    rows, source_count = _sampled_metric_rows(
        connection,
        heat_id=heat_id,
        scope=scope,
        from_at_us=first_at_us,
        to_at_us=last_at_us,
        max_points=max_points,
    )
    points_by_source: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        values = _json_object(row["values_json"], context="dashboard session metric")
        relations = values.get("relation_intervals")
        relations = relations if isinstance(relations, Mapping) else {}
        for relation_name, relation_key, sign in (
            ("ahead", "class_ahead", 1),
            ("behind", "class_behind", -1),
        ):
            relation = relations.get(relation_key)
            relation = relation if isinstance(relation, Mapping) else None
            if relation is not None:
                value_ms = _comparison_number(relation.get("value_ms"))
                target_id = relation.get("target_participant_id")
                source_at_us = _comparison_number(relation.get("source_observed_at_us"))
                if relation.get("status") != "VALID" or value_ms is None or source_at_us is None:
                    continue
                ours_laps = _comparison_number(relation.get("ours_laps"))
            else:
                value_ms = _comparison_number(values.get(f"gap_to_{relation_name}_ms"))
                target_id = values.get(f"class_{relation_name}_id")
                source_at_us = int(row["observed_at_us"])
                ours_laps = _comparison_number(values.get("observed_lap_count"))
            if not isinstance(target_id, str) or not target_id or value_ms is None:
                continue
            key = (relation_name, target_id, int(source_at_us), value_ms)
            points_by_source[key] = {
                "observed_at_us": int(source_at_us),
                "participant_id": target_id,
                "signed_ms": sign * value_ms,
                "relation": relation_name,
                "ours_laps": ours_laps,
                "flag": values.get("track_flag"),
            }
    points = sorted(
        points_by_source.values(),
        key=lambda point: (point["observed_at_us"], point["participant_id"], point["relation"]),
    )
    return points, source_count


def read_dashboard_history(
    session_id: str,
    *,
    participant_ids: Sequence[str],
    database: str | Path | None = None,
    from_at_us: int | None = None,
    to_at_us: int | None = None,
    max_points: int = MAX_CHART_POINTS,
    now_at_us: int | None = None,
) -> dict[str, Any]:
    """Return one bounded source-backed history payload for the live dashboard."""

    session_id = _require_session_id(session_id)
    max_points = _require_max_points(max_points)
    evaluation_at_us = _require_now(now_at_us)
    requested_start, requested_end = _validate_range(from_at_us=from_at_us, to_at_us=to_at_us)
    with _readonly_snapshot(database) as connection:
        session = _session_row(connection, session_id)
        heat = _require_heat(_latest_heat_row(connection, session_id))
        heat_id = int(heat["id"])
        selected_ids = _dashboard_participant_ids(
            connection,
            heat_id=heat_id,
            participant_ids=participant_ids,
        )
        last_at_us = evaluation_at_us if requested_end is None else requested_end
        first_at_us = (
            max(0, last_at_us - MAX_HISTORY_RANGE_US)
            if requested_start is None
            else requested_start
        )
        if requested_start is None:
            first_at_us = max(first_at_us, min(int(heat["created_at_us"]), last_at_us))
        if last_at_us < first_at_us or last_at_us - first_at_us > MAX_HISTORY_RANGE_US:
            raise ReadValidationError("requested time range must not exceed 24 hours")

        lap_series, lap_source_counts = _dashboard_lap_series(
            connection,
            heat_id=heat_id,
            participant_ids=selected_ids,
            first_at_us=first_at_us,
            last_at_us=last_at_us,
        )
        ours_id = session["our_participant_id"]
        interval_points, interval_source_count = _dashboard_interval_points(
            connection,
            session_id=session_id,
            heat_id=heat_id,
            first_at_us=first_at_us,
            last_at_us=last_at_us,
            max_points=max_points,
        )
        cursor = _stream_cursor(connection, session_id)
        return {
            "schema_version": LIVE_SCHEMA_VERSION,
            "session_id": session_id,
            "heat": _heat_payload(heat),
            "range": {
                "first_at_us": first_at_us,
                "last_at_us": last_at_us,
                "max_points": max_points,
            },
            "participants": _dashboard_participants(
                connection,
                heat_id=heat_id,
                participant_ids=selected_ids,
            ),
            "lap_series": {
                participant_id: {
                    "source_point_count": lap_source_counts[participant_id],
                    "truncated": lap_source_counts[participant_id] > len(lap_series[participant_id]),
                    "points": lap_series[participant_id],
                }
                for participant_id in selected_ids
            },
            "interval_series": {
                "source_point_count": interval_source_count,
                "downsampled": interval_source_count > len(interval_points),
                "points": interval_points,
            },
            "pit_stops": _archive_comparison_pit_stops(
                connection,
                heat_id=heat_id,
                participant_ids=selected_ids,
                ours_id=str(ours_id or ""),
                first_at_us=first_at_us,
                last_at_us=last_at_us,
            ),
            "flags": _dashboard_flags(
                connection,
                heat_id=heat_id,
                first_at_us=first_at_us,
                last_at_us=last_at_us,
            ),
            "ingest_gaps": _dashboard_ingest_gaps(
                connection,
                session_id=session_id,
                heat_id=heat_id,
                first_at_us=first_at_us,
                last_at_us=last_at_us,
            ),
            "time_axes": _archive_time_axes(
                connection,
                heat_id=heat_id,
                session=session,
                first_at_us=first_at_us,
                last_at_us=last_at_us,
            ),
            "cursor": {"stream_event_id": cursor},
            "barrier": {"stream_event_id": cursor},
            "semantics": {
                "lap_series": "every confirmed sparse result-grid LAST event; values are neither averaged nor interpolated",
                "capture_lap_index": "participant-local sequence of confirmed LAST events; lap_number remains null unless source-linked",
                "interval_series": "computed only where source interval facts resolve the current class neighbour; null stays null",
                "missing_values": "null values are never converted to zero or joined across an ingest gap",
            },
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


_ARCHIVE_INTERVAL_RELATIONS = (
    ("class_leader", "gap_to_class_leader_ms", "lap_delta_to_class_leader"),
    ("class_ahead", "gap_to_ahead_ms", "lap_delta_to_ahead"),
    ("class_behind", "gap_to_behind_ms", "lap_delta_to_behind"),
)

_ARCHIVE_INTERVAL_FACT_KEYS = (
    "id",
    "field_kind",
    "raw_value",
    "value_ms",
    "value_kind",
    "cell_observation_id",
    "source_message_id",
    "source_key",
    "source_change_ordinal",
    "observed_at_us",
    "source_handle",
    "observation_kind",
    "subject_position_overall",
    "subject_state_kind",
    "subject_laps",
    "target_participant_id",
    "target_position_overall",
    "target_state_kind",
    "target_laps",
    "relation_kind",
)


def _archive_interval_fact_payload(value: Any) -> dict[str, Any] | None:
    """Retain only named field-level provenance from a metric projection."""

    if not isinstance(value, Mapping):
        return None
    return {key: value.get(key) for key in _ARCHIVE_INTERVAL_FACT_KEYS}


def _archive_valid_time_fact(fact: Mapping[str, Any]) -> bool:
    """Require the provenance fields that make a GAP/DIFF scalar auditable."""

    return (
        fact.get("field_kind") in {"GAP", "DIFF"}
        and fact.get("value_kind") == "TIME"
        and (_archive_int(fact.get("value_ms")) is not None and _archive_int(fact.get("value_ms")) >= 0)
        and (_archive_int(fact.get("cell_observation_id")) is not None and _archive_int(fact.get("cell_observation_id")) >= 1)
        and (_archive_int(fact.get("source_message_id")) is not None and _archive_int(fact.get("source_message_id")) >= 1)
        and isinstance(fact.get("source_key"), str)
        and bool(fact["source_key"].strip())
        and (_archive_int(fact.get("source_change_ordinal")) is not None and _archive_int(fact.get("source_change_ordinal")) >= 0)
        and (_archive_int(fact.get("observed_at_us")) is not None and _archive_int(fact.get("observed_at_us")) >= 0)
        and isinstance(fact.get("source_handle"), str)
        and bool(fact["source_handle"].strip())
        and isinstance(fact.get("observation_kind"), str)
        and bool(fact["observation_kind"].strip())
    )


def _archive_unavailable_relation(target_participant_id: str | None) -> dict[str, Any]:
    """Make a legacy projection explicitly unavailable rather than inferred."""

    return {
        "target_participant_id": target_participant_id,
        "status": "UNAVAILABLE_PROVENANCE",
        "value_ms": None,
        "relation_kind": None,
        "source_facts": [],
        "source_observed_at_us": None,
        "source_age_ms": None,
        "ours_state_kind": None,
        "target_state_kind": None,
        "ours_laps": None,
        "target_laps": None,
    }


def _archive_relation_payload(value: Any, *, expected_target_id: str | None) -> dict[str, Any]:
    """Project an engine-evaluated relation without re-evaluating raw cells.

    Archive playback is an audit surface.  Older snapshots did not retain
    target-bound GAP/DIFF provenance, so a read-time subtraction of cached
    grid fields would make a stale interval look current.  Keep only the
    structured engine relation and fail closed for every older shape.
    """

    if not isinstance(value, Mapping):
        return _archive_unavailable_relation(expected_target_id)
    status = value.get("status")
    if not isinstance(status, str) or not status.strip():
        return _archive_unavailable_relation(expected_target_id)
    target_id = _comparison_text(value.get("target_participant_id"), expected_target_id)
    if expected_target_id is not None and target_id != expected_target_id:
        return _archive_unavailable_relation(expected_target_id)
    raw_facts = value.get("source_facts")
    if not isinstance(raw_facts, Sequence) or isinstance(raw_facts, (str, bytes, bytearray)):
        return _archive_unavailable_relation(expected_target_id)
    source_facts = [_archive_interval_fact_payload(fact) for fact in raw_facts]
    if any(fact is None for fact in source_facts):
        return _archive_unavailable_relation(expected_target_id)
    facts = [fact for fact in source_facts if fact is not None]
    raw_value_ms = _archive_int(value.get("value_ms"))
    is_self = status == "SELF"
    is_valid = status == "VALID"
    if is_self:
        if raw_value_ms != 0 or facts:
            return _archive_unavailable_relation(expected_target_id)
    elif is_valid:
        if raw_value_ms is None or raw_value_ms < 0 or not facts or not all(_archive_valid_time_fact(fact) for fact in facts):
            return _archive_unavailable_relation(expected_target_id)
    else:
        raw_value_ms = None
    return {
        "target_participant_id": target_id,
        "status": status,
        "value_ms": raw_value_ms if is_valid or is_self else None,
        "relation_kind": _comparison_text(value.get("relation_kind")),
        "source_facts": facts,
        "source_observed_at_us": _archive_int(value.get("source_observed_at_us")),
        "source_age_ms": _archive_int(value.get("source_age_ms")),
        "ours_state_kind": _comparison_text(value.get("ours_state_kind")),
        "target_state_kind": _comparison_text(value.get("target_state_kind")),
        "ours_laps": _archive_int(value.get("ours_laps")),
        "target_laps": _archive_int(value.get("target_laps")),
    }


def _archive_relation_value_ms(relation: Mapping[str, Any]) -> int | None:
    value = _archive_int(relation.get("value_ms"))
    return value if relation.get("status") in {"VALID", "SELF"} and value is not None and value >= 0 else None


def _archive_interval_derivation(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Expose engine-evaluated interval provenance without synthesizing it."""

    computed = payload.get("computed")
    computed = computed if isinstance(computed, Mapping) else {}
    session = computed.get("session")
    session = session if isinstance(session, Mapping) else {}
    relation_values = session.get("relation_intervals")
    relation_values = relation_values if isinstance(relation_values, Mapping) else {}
    relations = {
        relation: _archive_relation_payload(
            relation_values.get(relation),
            expected_target_id=_comparison_text(session.get(f"{relation}_id")),
        )
        for relation, _, _ in _ARCHIVE_INTERVAL_RELATIONS
    }

    ours_id = _comparison_text(session.get("ours_participant_id"))
    raw_participants = payload.get("class_participants")
    participants: dict[str, Mapping[str, Any]] = {}
    if isinstance(raw_participants, list):
        for candidate in raw_participants:
            if not isinstance(candidate, Mapping):
                continue
            participant_id = _archive_participant_id(candidate)
            if participant_id is not None:
                participants[participant_id] = candidate
    ours = participants.get(ours_id) if ours_id is not None else None
    result: dict[str, Any] = {
        "lap_count_scope": (
            "source_grid"
            if ours is not None and _archive_explicit_laps(ours) is not None
            else "capture_tracker"
            if ours is not None
            else "unknown"
        ),
        "relations": relations,
    }
    for relation, gap_key, lap_key in _ARCHIVE_INTERVAL_RELATIONS:
        result[gap_key] = _archive_relation_value_ms(relations[relation])
        target_id = _comparison_text(session.get(f"{relation}_id"))
        target = participants.get(target_id) if target_id is not None else None
        ours_laps = _archive_explicit_laps(ours) if ours is not None else None
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


def _archive_manifest_ours_participant_id(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    session: Mapping[str, Any],
) -> str | None:
    """Resolve the one archived BALCHUG participant without reading a chart.

    The archive manifest must be usable before the asynchronous comparison
    request completes. The session's resolved identity is preferred, but a
    historical replay can predate that field, so a single durable ``is_ours``
    participant is an explicit fallback. Ambiguous fallback identities fail
    closed rather than attaching another car's LAST events to BALCHUG.
    """

    candidate = session["our_participant_id"]
    if isinstance(candidate, str) and candidate:
        row = connection.execute(
            """
            SELECT id
            FROM participants
            WHERE source_heat_id = ? AND id = ?
            """,
            (heat_id, candidate),
        ).fetchone()
        if row is not None:
            return str(row["id"])

    rows = connection.execute(
        """
        SELECT id
        FROM participants
        WHERE source_heat_id = ? AND is_ours = 1
        ORDER BY id
        LIMIT 2
        """,
        (heat_id,),
    ).fetchall()
    return str(rows[0]["id"]) if len(rows) == 1 else None


def _archive_capture_lap_events(
    connection: sqlite3.Connection,
    *,
    heat_id: int,
    participant_id: str | None,
    first_at_us: int,
    last_at_us: int,
) -> list[dict[str, Any]]:
    """Return exact confirmed ``LAST`` events for the archived BALCHUG car.

    A raw result-grid value alone is not a lap event: ``r_i`` can replay a
    whole table after a reconnect and sparse ``r_c`` observations can be
    unrelated to a confirmed finish. The immutable LAST-cell ledger classifies
    that source evidence before this read model sees it. A confirmed sparse
    ``r_c`` LAST remains a real capture event even when the provider has not
    supplied a lap number; its optional lap link is therefore never invented.
    """

    if participant_id is None:
        return []
    rows = connection.execute(
        """
        SELECT ledger.source_cell_observation_id,ledger.source_message_id,
               ledger.source_key,ledger.source_change_ordinal,
               ledger.source_frame_id,ledger.source_message_ordinal,
               ledger.source_handle,ledger.observed_at_us,ledger.duration_ms,
               ledger.classification,ledger.linked_lap_id,
               lap.lap_number,lap.completed_at_us
        FROM result_last_cell_ledger AS ledger
        LEFT JOIN laps AS lap ON lap.id = ledger.linked_lap_id
        WHERE ledger.source_heat_id = ?
          AND ledger.participant_id = ?
          AND ledger.observed_at_us >= ? AND ledger.observed_at_us <= ?
          AND ledger.classification = 'CONFIRMED_LAP'
          AND ledger.source_handle = 'r_c'
          AND ledger.duration_ms IS NOT NULL AND ledger.duration_ms > 0
        ORDER BY ledger.source_frame_id,ledger.source_message_ordinal,
                 ledger.source_change_ordinal,ledger.source_cell_observation_id
        """,
        (heat_id, participant_id, first_at_us, last_at_us),
    ).fetchall()
    events: list[dict[str, Any]] = []
    for row in rows:
        events.append(
            {
                "capture_at_us": int(row["observed_at_us"]),
                "completed_at_us": int(row["completed_at_us"]) if row["completed_at_us"] is not None else None,
                "lap_number": int(row["lap_number"]) if row["lap_number"] is not None else None,
                "duration_ms": int(row["duration_ms"]),
                "timeline_kind": "confirmed_lap",
                "source": {
                    "cell_observation_id": int(row["source_cell_observation_id"]),
                    "message_id": int(row["source_message_id"]),
                    "frame_id": int(row["source_frame_id"]),
                    "message_ordinal": int(row["source_message_ordinal"]),
                    "change_ordinal": int(row["source_change_ordinal"]),
                    "handle": row["source_handle"],
                    "key": row["source_key"],
                    "classification": row["classification"],
                },
            }
        )
    return events


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
        ours_participant_id = _archive_manifest_ours_participant_id(
            connection,
            heat_id=int(heat["id"]),
            session=session,
        )
        capture_lap_events = _archive_capture_lap_events(
            connection,
            heat_id=int(heat["id"]),
            participant_id=ours_participant_id,
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
            "capture_lap_events": capture_lap_events,
            "time_axes": time_axes,
            "semantics": {
                "state": "last_observed",
                "series": "step",
                "time_axes": "playback uses durable receive time; source uses explicit Time Service clock anchors and may only be interpolated within one connection",
                "capture_lap_events": "exact LAST-cell-ledger events for BALCHUG Racing only; each is a CONFIRMED_LAP r_c delta, while REFRESH_REPEAT table refreshes and unconfirmed observations are excluded; a provider lap number is null when that exact source cell has no linked completed lap",
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


def _canonical_lap_counts(connection: sqlite3.Connection, heat_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT participant.id AS participant_id,participant.start_number,participant.team_name,
               participant.car_name,participant.class_name,participant.class_name_key,
               participant.is_ours,state.position_overall,state.position_class,
               COUNT(lap.id) AS observed_complete_laps,MAX(lap.lap_number) AS completed_laps,
               MIN(lap.coverage_complete) AS coverage_complete,
               SUM(CASE WHEN lap.duration_reconciliation = 'EXACT' THEN 1 ELSE 0 END) AS exact_last_laps,
               SUM(CASE WHEN lap.duration_reconciliation <> 'EXACT' THEN 1 ELSE 0 END) AS unmatched_laps
        FROM participants AS participant
        LEFT JOIN participant_state_current AS state
          ON state.source_heat_id = participant.source_heat_id AND state.participant_id = participant.id
        LEFT JOIN canonical_laps AS lap
          ON lap.source_heat_id = participant.source_heat_id AND lap.participant_id = participant.id
        WHERE participant.source_heat_id = ?
        GROUP BY participant.id
        ORDER BY CASE WHEN state.position_overall IS NULL THEN 1 ELSE 0 END,
                 state.position_overall,participant.start_number,participant.id
        """,
        (heat_id,),
    ).fetchall()
    return [
        {
            "participant_id": row["participant_id"],
            "start_number": row["start_number"],
            "team_name": row["team_name"],
            "car_name": row["car_name"],
            "class_name": row["class_name"],
            "class_key": row["class_name_key"] or _class_key(row["class_name"]),
            "is_ours": bool(row["is_ours"]),
            "position_overall": row["position_overall"],
            "position_class": row["position_class"],
            "completed_laps": int(row["completed_laps"]) if row["completed_laps"] is not None else None,
            "observed_complete_laps": int(row["observed_complete_laps"]),
            "coverage_complete": bool(row["coverage_complete"])
            if row["coverage_complete"] is not None
            else False,
            "exact_last_laps": int(row["exact_last_laps"] or 0),
            "unmatched_laps": int(row["unmatched_laps"] or 0),
        }
        for row in rows
    ]


def _read_canonical_laps(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    heat: sqlite3.Row,
    participant_id: str | None,
    from_at_us: int | None,
    to_at_us: int | None,
    limit: int,
) -> dict[str, Any]:
    heat_id = int(heat["id"])
    suffix, parameters = _fact_filters(
        participant_id=participant_id,
        from_at_us=from_at_us,
        to_at_us=to_at_us,
        time_column="COALESCE(f.finished_at_us,f.finish_observed_at_us)",
    )
    rows = connection.execute(
        f"""
        SELECT f.*,participant.start_number,participant.team_name,participant.car_name,
               participant.class_name,
               start_boundary.boundary_kind AS start_boundary_kind,
               start_boundary.source_kind AS start_source_kind,
               start_boundary.passing_observation_id AS start_passing_observation_id,
               start_boundary.corroborating_passing_observation_id AS start_corroborating_passing_id,
               start_boundary.provider_passed_at_raw AS start_provider_raw,
               start_boundary.source_message_id AS start_source_message_id,
               start_boundary.source_key AS start_source_key,
               finish_boundary.boundary_kind AS finish_boundary_kind,
               finish_boundary.source_kind AS finish_source_kind,
               finish_boundary.passing_observation_id AS finish_passing_observation_id,
               finish_boundary.provider_passed_at_raw AS finish_provider_raw,
               finish_boundary.source_message_id AS finish_source_message_id,
               finish_boundary.source_key AS finish_source_key,
               last_cell.raw_value_json AS source_last_raw_json,
               last_cell.value_text AS source_last_value_text,
               last_cell.source_message_id AS source_last_message_id,
               last_cell.source_key AS source_last_key,
               (
                 SELECT flag FROM track_flag_periods AS flag
                 WHERE flag.source_heat_id = f.source_heat_id
                   AND flag.started_at_us <= COALESCE(f.finished_at_us,f.finish_observed_at_us)
                   AND (flag.ended_at_us IS NULL OR flag.ended_at_us > COALESCE(f.finished_at_us,f.finish_observed_at_us))
                 ORDER BY flag.started_at_us DESC,flag.id DESC LIMIT 1
               ) AS flag
        FROM canonical_laps AS f
        JOIN participants AS participant ON participant.id = f.participant_id
        JOIN canonical_lap_boundaries AS start_boundary ON start_boundary.id = f.start_boundary_id
        JOIN canonical_lap_boundaries AS finish_boundary ON finish_boundary.id = f.finish_boundary_id
        LEFT JOIN participant_result_cell_observations AS last_cell
          ON last_cell.id = f.source_last_cell_observation_id
        WHERE f.source_heat_id = ?{suffix}
        ORDER BY COALESCE(f.finished_at_us,f.finish_observed_at_us) DESC,f.lap_ordinal DESC
        LIMIT ?
        """,
        (heat_id, *parameters, limit),
    ).fetchall()
    chronological = list(reversed(rows))
    lap_ids = [str(row["id"]) for row in chronological]
    sectors_by_lap: dict[str, list[dict[str, Any]]] = {lap_id: [] for lap_id in lap_ids}
    passings_by_lap: dict[str, list[dict[str, Any]]] = {lap_id: [] for lap_id in lap_ids}
    if lap_ids:
        placeholders = ",".join("?" for _ in lap_ids)
        sector_rows = connection.execute(
            f"""
            SELECT sector.*,source.raw_value_json AS source_raw_value_json,
                   tracker_start.raw_passing_json AS tracker_start_raw_json,
                   tracker_finish.raw_passing_json AS tracker_finish_raw_json
            FROM canonical_lap_sectors AS sector
            LEFT JOIN participant_result_cell_observations AS source
              ON source.id = sector.source_cell_observation_id
            LEFT JOIN tracker_passing_observations AS tracker_start
              ON tracker_start.id = sector.tracker_start_passing_observation_id
            LEFT JOIN tracker_passing_observations AS tracker_finish
              ON tracker_finish.id = sector.tracker_finish_passing_observation_id
            WHERE sector.canonical_lap_id IN ({placeholders})
            ORDER BY sector.canonical_lap_id,sector.sector_number
            """,
            tuple(lap_ids),
        ).fetchall()
        for row in sector_rows:
            sectors_by_lap[str(row["canonical_lap_id"])].append(
                {
                    "sector_number": int(row["sector_number"]),
                    "source_duration_raw": row["source_duration_raw"],
                    "source_duration_us": row["source_duration_us"],
                    "source_duration_ms": row["source_duration_ms"],
                    "source_cell_observation_id": row["source_cell_observation_id"],
                    "source_raw_value": _json_value(
                        row["source_raw_value_json"], context="canonical sector source raw"
                    )
                    if row["source_raw_value_json"] is not None
                    else None,
                    "tracker_started_at_provider_us": row["tracker_started_at_provider_us"],
                    "tracker_finished_at_provider_us": row["tracker_finished_at_provider_us"],
                    "tracker_duration_us": row["tracker_duration_us"],
                    "tracker_duration_ms": row["tracker_duration_ms"],
                    "tracker_start_passing_observation_id": row[
                        "tracker_start_passing_observation_id"
                    ],
                    "tracker_finish_passing_observation_id": row[
                        "tracker_finish_passing_observation_id"
                    ],
                    "tracker_start_raw": _json_value(
                        row["tracker_start_raw_json"], context="canonical sector tracker start raw"
                    )
                    if row["tracker_start_raw_json"] is not None
                    else None,
                    "tracker_finish_raw": _json_value(
                        row["tracker_finish_raw_json"], context="canonical sector tracker finish raw"
                    )
                    if row["tracker_finish_raw_json"] is not None
                    else None,
                    "reconciliation": row["duration_reconciliation"],
                }
            )
        passing_rows = connection.execute(
            f"""
            SELECT link.canonical_lap_id,link.passing_ordinal,link.role,observation.*
            FROM canonical_lap_tracker_passings AS link
            JOIN tracker_passing_observations AS observation
              ON observation.id = link.passing_observation_id
            WHERE link.canonical_lap_id IN ({placeholders})
            ORDER BY link.canonical_lap_id,link.passing_ordinal
            """,
            tuple(lap_ids),
        ).fetchall()
        for row in passing_rows:
            passings_by_lap[str(row["canonical_lap_id"])].append(
                {
                    "passing_observation_id": int(row["id"]),
                    "ordinal": int(row["passing_ordinal"]),
                    "role": row["role"],
                    "provider_passed_at_raw": row["provider_passed_at_raw"],
                    "provider_passed_at_provider_us": row["provider_passed_at_provider_us"],
                    "passed_at_us": row["passed_at_us"],
                    "observed_at_us": int(row["observed_at_us"]),
                    "start_distance_mm": row["start_distance_mm"],
                    "stop_distance_mm": row["stop_distance_mm"],
                    "sector_id": row["sector_id"],
                    "is_in_pit": bool(row["is_in_pit"]) if row["is_in_pit"] is not None else None,
                    "raw": _json_value(row["raw_passing_json"], context="canonical tracker passing raw"),
                    "source": {"message_id": row["source_message_id"], "key": row["source_key"]},
                }
            )
    items: list[dict[str, Any]] = []
    for row in chronological:
        lap_id = str(row["id"])
        sector_facts = sectors_by_lap[lap_id]
        items.append(
            {
                "lap_id": lap_id,
                "participant_id": row["participant_id"],
                "start_number": row["start_number"],
                "team_name": row["team_name"],
                "car_name": row["car_name"],
                "class_name": row["class_name"],
                "lap_ordinal": int(row["lap_ordinal"]),
                "lap_number": int(row["lap_number"]) if row["lap_number"] is not None else None,
                "coverage_complete": bool(row["coverage_complete"]),
                "started_at_us": row["started_at_us"],
                "completed_at_us": row["finished_at_us"],
                "started_at_provider_us": int(row["started_at_provider_us"]),
                "finished_at_provider_us": int(row["finished_at_provider_us"]),
                "tracker_duration_us": int(row["tracker_duration_us"]),
                "tracker_duration_ms": int(row["tracker_duration_ms"]),
                "duration_ms": row["source_duration_ms"],
                "source_duration_raw": row["source_duration_raw"],
                "source_duration_us": row["source_duration_us"],
                "duration_reconciliation": row["duration_reconciliation"],
                "sectors": {
                    f"sector_{sector['sector_number']}": sector["source_duration_raw"]
                    for sector in sector_facts
                },
                "sector_facts": sector_facts,
                "tracker_passings": passings_by_lap[lap_id],
                "flag": row["flag"],
                "is_in_lap": row["finish_boundary_kind"] == "PIT_FINISH",
                "is_out_lap": row["start_boundary_kind"] == "PIT_FINISH"
                and row["finish_boundary_kind"] == "MAIN_FINISH",
                "crosses_pit": bool(row["is_pit_lap"]),
                "is_clean": None,
                "start_boundary": {
                    "boundary_id": row["start_boundary_id"],
                    "kind": row["start_boundary_kind"],
                    "source_kind": row["start_source_kind"],
                    "provider_passed_at_raw": row["start_provider_raw"],
                    "provider_passed_at_provider_us": int(row["started_at_provider_us"]),
                    "passed_at_us": row["started_at_us"],
                    "passing_observation_id": row["start_passing_observation_id"],
                    "corroborating_passing_observation_id": row["start_corroborating_passing_id"],
                    "source": {
                        "message_id": row["start_source_message_id"],
                        "key": row["start_source_key"],
                    },
                },
                "finish_boundary": {
                    "boundary_id": row["finish_boundary_id"],
                    "kind": row["finish_boundary_kind"],
                    "source_kind": row["finish_source_kind"],
                    "provider_passed_at_raw": row["finish_provider_raw"],
                    "provider_passed_at_provider_us": int(row["finished_at_provider_us"]),
                    "passed_at_us": row["finished_at_us"],
                    "passing_observation_id": row["finish_passing_observation_id"],
                    "source": {
                        "message_id": row["finish_source_message_id"],
                        "key": row["finish_source_key"],
                    },
                },
                "source": {
                    "last_cell_observation_id": row["source_last_cell_observation_id"],
                    "last_raw_value": _json_value(
                        row["source_last_raw_json"], context="canonical LAST source raw"
                    )
                    if row["source_last_raw_json"] is not None
                    else None,
                    "last_value_text": row["source_last_value_text"],
                    "message_id": row["source_last_message_id"],
                    "key": row["source_last_key"],
                },
                "provenance": "source_last_with_tracker_chronology",
            }
        )
    return {
        "session_id": session_id,
        "heat": _heat_payload(heat),
        "participant_id": participant_id,
        "limit": limit,
        "lap_counts": _canonical_lap_counts(connection, heat_id),
        "items": items,
        "semantics": {
            "lap_number": "exact only when Tracker coverage is corroborated from the official green flag",
            "duration_ms": "authoritative result-grid LAST; never calculated from Tracker",
            "tracker_duration_ms": "difference between raw provider boundary timestamps",
            "sectors": "authoritative result-grid SECT values with independent Tracker reconciliation",
        },
        "provenance_contract": _provenance_contract(),
    }


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
        canonical_available = connection.execute(
            "SELECT 1 FROM canonical_laps WHERE source_heat_id = ? LIMIT 1", (heat_id,)
        ).fetchone()
        if canonical_available is not None:
            return _read_canonical_laps(
                connection,
                session_id=session_id,
                heat=heat,
                participant_id=participant_id,
                from_at_us=from_at_us,
                to_at_us=to_at_us,
                limit=limit,
            )
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


def _require_boolean(name: str, value: bool) -> bool:
    """Reject truthy values: public filters must remain explicit booleans."""

    if type(value) is not bool:
        raise ReadValidationError(f"{name} must be a boolean")
    return value


def _race_control_source(row: sqlite3.Row, *, prefix: str) -> dict[str, Any]:
    """Format durable SignalR provenance without inventing a provider time."""

    column_prefix = f"{prefix}_" if prefix else ""
    return {
        "message_id": row[f"{column_prefix}source_message_id"],
        "key": row[f"{column_prefix}source_key"],
        "message_ordinal": row[f"{column_prefix}source_message_ordinal"],
        "source_change_ordinal": row[f"{column_prefix}source_change_ordinal"],
    }


def _race_control_content(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "text_raw": row["text_raw"],
        "line": row["line"],
        "modality": row["modality"],
        "background_color_raw": row["background_color_raw"],
        "font_color_raw": row["font_color_raw"],
    }


def _race_control_current_item(row: sqlite3.Row) -> dict[str, Any]:
    """Expose one current Race Control board message and both evidence edges."""

    return {
        "message_id": row["message_id_raw"],
        **_race_control_content(row),
        "raw_record": _json_value(row["raw_record_json"], context="Race Control current record"),
        "is_active": bool(row["is_active"]),
        # The current Time Service ScreenMessagesList payload does not include
        # a message creation/display instant. This is intentionally null until
        # such a source value exists; it must never be inferred from receive
        # time.
        "provider_occurred_at_us": row["provider_occurred_at_us"],
        "first_observation": {
            "kind": row["first_observation_kind"],
            "observed_at_us": row["first_observed_at_us"],
            "source": _race_control_source(row, prefix="first"),
        },
        "last_observation": {
            "action": row["last_action"],
            "observed_at_us": row["last_observed_at_us"],
            "source": _race_control_source(row, prefix="last"),
        },
        "removed_at_us": row["removed_at_us"],
        "provenance": "measured",
    }


def _race_control_observation_item(row: sqlite3.Row) -> dict[str, Any]:
    """Expose one immutable board observation in chronological evidence form."""

    return {
        "observation_id": int(row["id"]),
        "message_id": row["message_id_raw"],
        "action": row["action"],
        **_race_control_content(row),
        "raw_payload": _json_value(row["raw_payload_json"], context="Race Control observation payload"),
        # The recorder has an exact receive timestamp. The provider currently
        # supplies no event timestamp in m_* payloads, so keep the distinction
        # explicit for subsequent LLM and audit consumers.
        "observed_at_us": int(row["observed_at_us"]),
        "provider_occurred_at_us": row["provider_occurred_at_us"],
        "source": _race_control_source(row, prefix=""),
        "provenance": "measured",
    }


def read_race_control_messages(
    session_id: str,
    *,
    database: str | Path | None = None,
    active_only: bool = False,
    limit: int = DEFAULT_FACT_LIMIT,
    observation_limit: int = DEFAULT_FACT_LIMIT,
) -> dict[str, Any]:
    """Read a bounded Race Control board plus its append-only source ledger.

    This endpoint deliberately does not require an active session. An
    engineer, archive reader, or later LLM pipeline can inspect the exact
    capture provenance of a stopped session using the same contract.
    """

    session_id = _require_session_id(session_id)
    active_only = _require_boolean("active_only", active_only)
    limit = _require_limit(limit)
    observation_limit = _require_limit(observation_limit)
    with _readonly_snapshot(database) as connection:
        _session_row(connection, session_id)
        heat = _latest_heat_row(connection, session_id)
        if heat is None:
            return {
                "schema_version": LIVE_SCHEMA_VERSION,
                "session_id": session_id,
                "heat": None,
                "active_only": active_only,
                "limit": limit,
                "observation_limit": observation_limit,
                "current_source_count": 0,
                "observation_source_count": 0,
                "items": [],
                "observations": [],
                "semantics": _race_control_semantics(),
                "provenance_contract": _provenance_contract(),
            }

        heat_id = int(heat["id"])
        current_count_filter = " AND is_active = 1" if active_only else ""
        current_row_filter = " AND current.is_active = 1" if active_only else ""
        current_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM race_control_messages_current WHERE source_heat_id = ?{current_count_filter}",
                (heat_id,),
            ).fetchone()[0]
        )
        current_rows = connection.execute(
            f"""
            SELECT current.message_id_raw,current.text_raw,current.line,current.modality,
                   current.background_color_raw,current.font_color_raw,current.raw_record_json,
                   current.is_active,current.first_observed_at_us,current.last_observed_at_us,
                   current.removed_at_us,current.provider_occurred_at_us,
                   current.first_observation_kind,current.last_action,
                   current.first_source_message_id,current.first_source_key,
                   first_message.ordinal AS first_source_message_ordinal,
                   current.first_source_change_ordinal,current.last_source_message_id,
                   current.last_source_key,last_message.ordinal AS last_source_message_ordinal,
                   current.last_source_change_ordinal
            FROM race_control_messages_current AS current
            LEFT JOIN feed_messages AS first_message ON first_message.id = current.first_source_message_id
            LEFT JOIN feed_messages AS last_message ON last_message.id = current.last_source_message_id
            WHERE current.source_heat_id = ?{current_row_filter}
            ORDER BY current.is_active DESC,current.last_observed_at_us DESC,current.message_id_raw ASC
            LIMIT ?
            """,
            (heat_id, limit),
        ).fetchall()

        observation_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM race_control_message_observations WHERE source_heat_id = ?",
                (heat_id,),
            ).fetchone()[0]
        )
        observation_rows = connection.execute(
            """
            SELECT id,message_id_raw,operation AS action,text_raw,line,modality,background_color_raw,font_color_raw,
                   raw_payload_json,provider_occurred_at_us,source_message_id,source_key,
                   source_message_ordinal,source_change_ordinal,observed_at_us
            FROM race_control_message_observations
            WHERE source_heat_id = ?
            ORDER BY observed_at_us DESC,source_message_id DESC,source_change_ordinal DESC,id DESC
            LIMIT ?
            """,
            (heat_id, observation_limit),
        ).fetchall()

        # SQL selects the newest bounded window efficiently. Publishing it in
        # chronological order keeps a message timeline readable and preserves
        # source-change ordering when several operations share one frame.
        return {
            "schema_version": LIVE_SCHEMA_VERSION,
            "session_id": session_id,
            "heat": _heat_payload(heat),
            "active_only": active_only,
            "limit": limit,
            "observation_limit": observation_limit,
            "current_source_count": current_count,
            "observation_source_count": observation_count,
            "items": [_race_control_current_item(row) for row in current_rows],
            "observations": [_race_control_observation_item(row) for row in reversed(observation_rows)],
            "semantics": _race_control_semantics(),
            "provenance_contract": _provenance_contract(),
        }


def _race_control_semantics() -> dict[str, str]:
    """Explain the timestamp boundary once, rather than making UI infer it."""

    return {
        "items": "Current provider ScreenMessagesList materialization for the latest source heat.",
        "observations": "Immutable m_i/m_c/m_d/m_a source operations in recorder-observed order.",
        "observed_at_us": "Exact time the recorder received the SignalR source message.",
        "provider_occurred_at_us": "Only a provider-supplied message occurrence time; null when absent.",
    }
