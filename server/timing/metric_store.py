"""Immutable metric inputs and sparse history materialization for timing.db.

The metrics engine owns formulas.  This module only gives it a coherent
read-only view of normalized facts and a deterministic way to retain chart
points without copying unchanged derived state every second.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from .config import now_us
from .normalization import OPEN_ENDED_TS_TIME, parse_result_state, parse_ts_time
from .stream_events import StreamEventCandidate, StreamEventError, append_stream_events


METRIC_SAMPLE_INTERVAL_US = 5_000_000
"""Normal chart cadence; event boundaries may be persisted between buckets."""

PLAYBACK_SNAPSHOT_INTERVAL_US = METRIC_SAMPLE_INTERVAL_US
"""Archive projection cadence; relevant domain boundaries are retained too."""

PLAYBACK_PROJECTION_VERSION = 1
PLAYBACK_PAYLOAD_CODEC = "gzip-json-v1"

_SCOPE_KINDS = frozenset({"participant", "class", "session"})


class MetricStoreError(RuntimeError):
    """A metric input or durable materialization request is invalid."""


@dataclass(frozen=True)
class MetricSessionInput:
    """The only operator-selected parameters available to metric formulas."""

    id: str
    mode: str
    lifecycle: str
    race_duration_s: int | None
    required_pits: int | None
    started_at_us: int | None
    stopped_at_us: int | None
    our_participant_id: str | None
    our_class_name: str | None
    identity_state: str


@dataclass(frozen=True)
class StateTickInput:
    observed_at_us: int
    freshness_ms: int
    source_key: str


@dataclass(frozen=True)
class TrackFlagInput:
    flag: str
    provider_code: str | None
    provider_label: str | None
    started_at_us: int
    observed_started_at_us: int | None
    calibrated_started_at_us: int | None
    start_provider_ts_raw: str | None
    source_message_id: int | None
    source_key: str
    updated_at_us: int


@dataclass(frozen=True)
class HeatStatisticsInput:
    heat_name: str | None
    green_flag_at_us: int | None
    finish_flag_at_us: int | None
    participants_started: int | None
    participants_classified: int | None
    participants_not_classified: int | None
    participants_on_track: int | None
    participants_in_pit_zone: int | None
    participants_in_tank_zone: int | None
    total_laps: int | None
    total_pitstops: int | None
    leader_laps_green: int | None
    leader_laps_safety_car: int | None
    leader_laps_code_60: int | None
    leader_laps_full_course_yellow: int | None
    safety_car_count: int | None
    code_60_count: int | None
    full_course_yellow_count: int | None
    observed_at_us: int
    source_message_id: int | None
    source_key: str


@dataclass(frozen=True)
class IngestGapInput:
    started_at_us: int
    reason: str
    connection_id: str | None


@dataclass(frozen=True)
class ParticipantStateInput:
    position_overall: int | None
    position_class: int | None
    marker: str | None
    laps: int | None
    state: str | None
    state_raw: str | None
    state_kind: str | None
    current_driver_name: str | None
    current_driver_stint_raw: str | None
    last_lap_ms: int | None
    last_lap_number: int | None
    best_lap_ms: int | None
    best_lap_number: int | None
    last_sectors_json: str | None
    best_sectors_json: str | None
    last_speeds_json: str | None
    gap_ms: int | None
    gap_raw: str | None
    gap_kind: str | None
    diff_ms: int | None
    diff_raw: str | None
    diff_kind: str | None
    sector_json: str | None
    speed_kph: float | None
    pit_time_raw: str | None
    provider_pit_count: int | None
    source_message_id: int | None
    source_key: str
    updated_at_us: int


@dataclass(frozen=True)
class LapInput:
    lap_number: int | None
    completed_at_us: int | None
    duration_ms: int | None
    sectors_json: str | None
    flag: str | None
    is_in_lap: bool
    is_out_lap: bool
    crosses_pit: bool
    is_clean: bool
    source_message_id: int | None
    source_key: str
    # A raw r_c LAST fact has no official LAPS value.  Its immutable source
    # cell id and capture-local ordinal retain chronology without pretending
    # that the ordinal is a provider lap number.
    timing_event_id: int | None = None
    capture_sequence: int | None = None
    # Tracker passings still drive tyre/stint chronology when a result grid
    # omits LAPS. They are not timing evidence unless a same-frame LAST fact
    # proves their duration. Keep that distinction per row so a later layout
    # with explicit LAPS does not erase earlier or later valid grid timing.
    timing_eligible: bool = True
    source_frame_id: int | None = None
    source_message_ordinal: int | None = None
    source_change_ordinal: int | None = None


@dataclass(frozen=True)
class PitStopInput:
    stop_number: int
    entered_at_us: int
    exited_at_us: int | None
    entered_lap: int | None
    exited_lap: int | None
    pit_lane_ms: int | None
    completed: bool
    entered_source_message_id: int | None
    entered_source_key: str
    exited_source_message_id: int | None
    exited_source_key: str | None
    pit_lane_duration_source_kind: str | None = None


@dataclass(frozen=True)
class TireStintInput:
    stint_number: int
    started_at_us: int
    ended_at_us: int | None
    started_lap: int | None
    ended_lap: int | None
    completed_laps: int
    source_message_id: int | None
    source_key: str


@dataclass(frozen=True)
class ParticipantMetricInput:
    id: str
    external_key: str
    transponder_id: str | None
    start_number: str | None
    team_name: str | None
    car_name: str | None
    class_name: str | None
    class_key: str | None
    is_ours: bool
    active: bool
    first_seen_at_us: int
    last_seen_at_us: int
    state: ParticipantStateInput | None
    laps: tuple[LapInput, ...]
    pit_stops: tuple[PitStopInput, ...]
    tire_stints: tuple[TireStintInput, ...]
    latest_timing_event_id: int | None = None

    @property
    def active_tire_stint(self) -> TireStintInput | None:
        """Return the observed open tyre stint, if the source has one."""

        return next((stint for stint in reversed(self.tire_stints) if stint.ended_at_us is None), None)


@dataclass(frozen=True)
class ClassScopeInput:
    """One automatically discovered class and its current competitors."""

    key: str
    display_name: str
    class_best_lap_ms: int | None
    class_best_start_number: str | None
    participants: tuple[ParticipantMetricInput, ...]


@dataclass(frozen=True)
class HeatMetricInput:
    """A single coherent, immutable input snapshot for formula evaluation."""

    source_heat_id: int
    generation: int
    external_name: str | None
    provider_started_at_us: int | None
    provider_finished_at_us: int | None
    created_at_us: int
    observed_at_us: int
    session: MetricSessionInput
    latest_tick: StateTickInput | None
    current_flag: TrackFlagInput | None
    statistics: HeatStatisticsInput | None
    open_ingest_gap: IngestGapInput | None
    participants: tuple[ParticipantMetricInput, ...]
    class_scopes: tuple[ClassScopeInput, ...]

    @property
    def our_participant(self) -> ParticipantMetricInput | None:
        """Use persisted source identity; never accept a dashboard selection here."""

        if self.session.our_participant_id is not None:
            for participant in self.participants:
                if participant.id == self.session.our_participant_id:
                    return participant
        return next((participant for participant in self.participants if participant.is_ours), None)

    @property
    def current_class_scope(self) -> ClassScopeInput | None:
        """Return the class of the automatically resolved Balchug entry."""

        participant = self.our_participant
        key = participant.class_key if participant is not None else _class_key(self.session.our_class_name)
        if key is None:
            return None
        return next((scope for scope in self.class_scopes if scope.key == key), None)

    def class_scope(self, key: str) -> ClassScopeInput | None:
        """Look up a class by its normalized source-derived key."""

        return next((scope for scope in self.class_scopes if scope.key == key), None)


@dataclass(frozen=True)
class MetricSampleCandidate:
    """One calculated scope state offered for sparse durable history."""

    scope_kind: str
    scope_key: str
    values: Mapping[str, Any]
    event_boundary: bool = False
    history_values: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class MetricScope:
    scope_kind: str
    scope_key: str


@dataclass(frozen=True)
class MetricHistoryPoint:
    """One validated sparse chart point supplied to a pure metric formula."""

    observed_at_us: int
    metric_version: int
    values: Mapping[str, Any]


@dataclass(frozen=True)
class MetricRunnerState:
    """Last durable derived boundary for restart-safe event detection."""

    source_heat_id: int
    observed_at_us: int
    source_frame_id: int
    source_message_id: int | None
    source_key: str
    metric_version: int
    boundary_state_json: str


@dataclass(frozen=True)
class MetricRunnerStateCandidate:
    """State/tick provenance committed with one metric materialization."""

    source_frame_id: int
    state_hash: str
    boundary_state_json: str


@dataclass(frozen=True)
class PlaybackSnapshotCandidate:
    """A compact, public archive state produced with one metric evaluation."""

    payload: Mapping[str, Any]
    event_boundary: bool = False


@dataclass(frozen=True)
class MetricMaterializationResult:
    """Outcome of writing sparse history and the current dashboard state.

    ``inserted``/``updated``/``skipped`` describe ``metric_samples`` and
    retain the original public meaning.  The ``current_*`` fields describe
    the one-row-per-scope materialization used by the live dashboard.
    """

    inserted: tuple[MetricScope, ...]
    updated: tuple[MetricScope, ...]
    skipped: tuple[MetricScope, ...]
    current_inserted: tuple[MetricScope, ...] = ()
    current_updated: tuple[MetricScope, ...] = ()
    current_skipped: tuple[MetricScope, ...] = ()
    runner_state_written: bool = False
    stream_events_written: int = 0
    playback_snapshot_written: bool = False

    @property
    def written(self) -> tuple[MetricScope, ...]:
        return self.inserted + self.updated

    @property
    def current_written(self) -> tuple[MetricScope, ...]:
        return self.current_inserted + self.current_updated


@contextmanager
def _read_snapshot(connection: sqlite3.Connection) -> Iterator[None]:
    """Pin all read queries to one SQLite snapshot without owning a caller tx."""

    owns_transaction = not connection.in_transaction
    if owns_transaction:
        connection.execute("BEGIN")
    try:
        yield
    finally:
        if owns_transaction:
            connection.rollback()


@contextmanager
def _write_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    if connection.in_transaction:
        raise MetricStoreError("Metric materialization requires a connection without an open transaction")
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _class_key(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.casefold().split())
    return normalized or None


def _int(value: Any) -> int | None:
    return int(value) if value is not None else None


@dataclass(frozen=True)
class _ResultLastFact:
    """One durable r_c LAST cell from a layout without the LAPS column."""

    timing_event_id: int
    participant_id: str
    value_text: str | None
    source_message_id: int
    source_key: str
    source_change_ordinal: int
    observed_at_us: int
    frame_id: int
    message_ordinal: int


@dataclass(frozen=True)
class _ResultStateFact:
    participant_id: str
    state_kind: str
    frame_id: int
    message_ordinal: int
    source_change_ordinal: int
    cell_id: int


def _result_last_duration_ms(value: Any) -> int | None:
    """Parse only a real Time Service LAST duration, never a sentinel."""

    source_us = parse_ts_time(value)
    if source_us is None or not 1_000_000 <= source_us < OPEN_ENDED_TS_TIME:
        return None
    return source_us // 1_000


def _interval_overlaps(
    started_at_us: int,
    ended_at_us: int,
    candidate_started_at_us: int,
    candidate_ended_at_us: int | None,
) -> bool:
    return candidate_started_at_us < ended_at_us and (
        candidate_ended_at_us is None or candidate_ended_at_us > started_at_us
    )


def _load_result_last_facts(
    connection: sqlite3.Connection, *, source_heat_id: int
) -> tuple[_ResultLastFact, ...]:
    """Read source LAST deltas without converting reconnect snapshots to laps.

    The raw-cell table is already immutable and idempotent.  ``r_i`` is a
    snapshot/reconnect baseline, so only an explicit ``r_c`` cell is evidence
    of a new no-LAPS timing event.  A valid preceding r_i for the same layout
    is required to keep schema-pending deltas out of tactical calculations.
    """

    rows = connection.execute(
        """
        SELECT observation.id AS timing_event_id,observation.participant_id,observation.value_text,
               observation.source_message_id,observation.source_key,observation.source_change_ordinal,
               observation.observed_at_us,frame.id AS frame_id,message.ordinal AS message_ordinal
        FROM participant_result_cell_observations AS observation
        JOIN result_column_definitions AS definition
          ON definition.layout_version_id = observation.layout_version_id
         AND definition.column_index = observation.column_index
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        JOIN feed_frames AS frame ON frame.id = message.frame_id
        WHERE observation.source_heat_id = ?
          AND observation.participant_id IS NOT NULL
          AND definition.canonical_key = 'last_lap'
          AND message.handle = 'r_c'
          AND NOT EXISTS (
            SELECT 1
            FROM result_column_definitions AS laps_definition
            WHERE laps_definition.layout_version_id = observation.layout_version_id
              AND laps_definition.canonical_key = 'laps'
          )
          AND NOT EXISTS (
            SELECT 1
            FROM result_column_definitions AS duplicate_last
            WHERE duplicate_last.layout_version_id = observation.layout_version_id
              AND duplicate_last.canonical_key = 'last_lap'
              AND duplicate_last.column_index <> observation.column_index
          )
          AND EXISTS (
            SELECT 1
            FROM participant_result_cell_observations AS snapshot_cell
            JOIN feed_messages AS snapshot_message ON snapshot_message.id = snapshot_cell.source_message_id
            JOIN feed_frames AS snapshot_frame ON snapshot_frame.id = snapshot_message.frame_id
            WHERE snapshot_cell.source_heat_id = observation.source_heat_id
              AND snapshot_cell.layout_version_id = observation.layout_version_id
              AND snapshot_message.handle = 'r_i'
              -- A reconnect needs its own authoritative grid snapshot. An
              -- r_i from a previous socket cannot make an early r_c from the
              -- new connection a trustworthy timing event.
              AND snapshot_frame.ingest_connection_id = frame.ingest_connection_id
              AND (
                snapshot_frame.id < frame.id
                OR (snapshot_frame.id = frame.id AND snapshot_message.ordinal < message.ordinal)
              )
          )
        ORDER BY observation.participant_id,frame.id,message.ordinal,
                 observation.source_change_ordinal,observation.id
        """,
        (source_heat_id,),
    ).fetchall()
    return tuple(
        _ResultLastFact(
            timing_event_id=int(row["timing_event_id"]),
            participant_id=row["participant_id"],
            value_text=row["value_text"],
            source_message_id=int(row["source_message_id"]),
            source_key=row["source_key"],
            source_change_ordinal=int(row["source_change_ordinal"]),
            observed_at_us=int(row["observed_at_us"]),
            frame_id=int(row["frame_id"]),
            message_ordinal=int(row["message_ordinal"]),
        )
        for row in rows
    )


def _load_result_state_facts(
    connection: sqlite3.Connection, *, source_heat_id: int
) -> dict[str, tuple[_ResultStateFact, ...]]:
    """Load exact STATE cells; do not use synthetic UNKNOWN state observations."""

    rows = connection.execute(
        """
        SELECT observation.id,observation.participant_id,observation.value_text,
               observation.source_change_ordinal,frame.id AS frame_id,message.ordinal AS message_ordinal
        FROM participant_result_cell_observations AS observation
        JOIN result_column_definitions AS definition
          ON definition.layout_version_id = observation.layout_version_id
         AND definition.column_index = observation.column_index
        JOIN feed_messages AS message ON message.id = observation.source_message_id
        JOIN feed_frames AS frame ON frame.id = message.frame_id
        WHERE observation.source_heat_id = ?
          AND observation.participant_id IS NOT NULL
          AND definition.canonical_key = 'state'
          AND NOT EXISTS (
            SELECT 1
            FROM result_column_definitions AS duplicate_state
            WHERE duplicate_state.layout_version_id = observation.layout_version_id
              AND duplicate_state.canonical_key = 'state'
              AND duplicate_state.column_index <> observation.column_index
          )
        ORDER BY observation.participant_id,frame.id,message.ordinal,
                 observation.source_change_ordinal,observation.id
        """,
        (source_heat_id,),
    ).fetchall()
    grouped: dict[str, list[_ResultStateFact]] = defaultdict(list)
    for row in rows:
        participant_id = row["participant_id"]
        if not isinstance(participant_id, str):
            continue
        grouped[participant_id].append(
            _ResultStateFact(
                participant_id=participant_id,
                state_kind=parse_result_state(row["value_text"]).kind,
                frame_id=int(row["frame_id"]),
                message_ordinal=int(row["message_ordinal"]),
                source_change_ordinal=int(row["source_change_ordinal"]),
                cell_id=int(row["id"]),
            )
        )
    return {participant_id: tuple(facts) for participant_id, facts in grouped.items()}


def _load_flag_periods(
    connection: sqlite3.Connection, *, source_heat_id: int
) -> tuple[tuple[str, int, int | None], ...]:
    return tuple(
        (row["flag"], int(row["started_at_us"]), _int(row["ended_at_us"]))
        for row in connection.execute(
            """
            SELECT flag,started_at_us,ended_at_us
            FROM track_flag_periods
            WHERE source_heat_id = ?
            ORDER BY started_at_us,id
            """,
            (source_heat_id,),
        )
    )


def _green_covers_interval(
    periods: Sequence[tuple[str, int, int | None]], *, started_at_us: int, ended_at_us: int
) -> bool:
    """Require positive Green coverage for the entire observed LAST interval."""

    if ended_at_us <= started_at_us:
        return False
    # Period reconciliation can temporarily retain an old open GREEN period
    # beside a newer caution period. A matching GREEN interval alone is not
    # sufficient: any non-Green overlap fails the lap closed.
    if any(
        flag != "GREEN" and _interval_overlaps(started_at_us, ended_at_us, period_started_at_us, period_ended_at_us)
        for flag, period_started_at_us, period_ended_at_us in periods
    ):
        return False
    cursor = started_at_us
    for flag, period_started_at_us, period_ended_at_us in periods:
        if flag != "GREEN" or period_started_at_us > cursor:
            continue
        if period_ended_at_us is not None and period_ended_at_us <= cursor:
            continue
        cursor = max(cursor, period_ended_at_us if period_ended_at_us is not None else ended_at_us)
        if cursor >= ended_at_us:
            return True
    return False


def _flag_at(
    periods: Sequence[tuple[str, int, int | None]], *, observed_at_us: int
) -> str | None:
    active = [
        (flag, started_at_us)
        for flag, started_at_us, ended_at_us in periods
        if started_at_us <= observed_at_us and (ended_at_us is None or ended_at_us > observed_at_us)
    ]
    return max(active, key=lambda item: item[1])[0] if active else None


def _load_raw_result_last_laps(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    pits_by_participant: Mapping[str, Sequence[PitStopInput]],
) -> tuple[dict[str, list[LapInput]], dict[str, int], set[int]]:
    """Build no-LAPS timing inputs directly from immutable r_c LAST cells."""

    facts = _load_result_last_facts(connection, source_heat_id=source_heat_id)
    state_by_participant = _load_result_state_facts(connection, source_heat_id=source_heat_id)
    periods = _load_flag_periods(connection, source_heat_id=source_heat_id)
    gaps = tuple(
        (int(row["started_at_us"]), _int(row["ended_at_us"]))
        for row in connection.execute(
            """
            SELECT started_at_us,ended_at_us
            FROM ingest_gaps
            WHERE source_heat_id = ?
               OR (
                 source_heat_id IS NULL
                 AND analysis_session_id = (
                   SELECT analysis_session_id FROM source_heats WHERE id = ?
                 )
               )
            ORDER BY started_at_us,id
            """,
            (source_heat_id, source_heat_id),
        )
    )
    grouped: dict[str, list[LapInput]] = defaultdict(list)
    latest_event_id: dict[str, int] = {}
    source_cell_ids: set[int] = set()
    previous_at_us: dict[str, int] = {}
    capture_sequence: dict[str, int] = defaultdict(int)
    state_index: dict[str, int] = defaultdict(int)
    current_state_kind: dict[str, str | None] = {}

    for event in facts:
        duration_ms = _result_last_duration_ms(event.value_text)
        if duration_ms is None:
            continue
        participant_id = event.participant_id
        previous = previous_at_us.get(participant_id)
        state_facts = state_by_participant.get(participant_id, ())
        index = state_index[participant_id]
        # Events are ordered by participant/frame/source ordinal. Consume each
        # STATE cell once rather than rescanning the entire history per LAST.
        # A frame is atomic, so a later STATE message in the same frame applies
        # to its LAST too and removes handle-order dependence.
        while index < len(state_facts) and state_facts[index].frame_id <= event.frame_id:
            current_state_kind[participant_id] = state_facts[index].state_kind
            index += 1
        state_index[participant_id] = index
        state_kind = current_state_kind.get(participant_id)
        is_in_lap = state_kind == "IN_PIT"
        is_out_lap = state_kind == "OUT_LAP"
        flag = _flag_at(periods, observed_at_us=event.observed_at_us)
        crosses_pit = is_in_lap
        has_gap = False
        if previous is not None:
            crosses_pit = crosses_pit or any(
                stop.completed
                and _interval_overlaps(previous, event.observed_at_us, stop.entered_at_us, stop.exited_at_us)
                for stop in pits_by_participant.get(participant_id, ())
            )
            has_gap = any(
                _interval_overlaps(previous, event.observed_at_us, gap_started_at_us, gap_ended_at_us)
                for gap_started_at_us, gap_ended_at_us in gaps
            )
        is_clean = bool(
            previous is not None
            and state_kind == "ON_TRACK"
            and not crosses_pit
            and not has_gap
            and _green_covers_interval(periods, started_at_us=previous, ended_at_us=event.observed_at_us)
        )
        capture_sequence[participant_id] += 1
        grouped[participant_id].append(
            LapInput(
                lap_number=None,
                completed_at_us=event.observed_at_us,
                duration_ms=duration_ms,
                sectors_json=None,
                flag=flag,
                is_in_lap=is_in_lap,
                is_out_lap=is_out_lap,
                crosses_pit=crosses_pit,
                is_clean=is_clean,
                source_message_id=event.source_message_id,
                source_key=event.source_key,
                timing_event_id=event.timing_event_id,
                capture_sequence=capture_sequence[participant_id],
                timing_eligible=True,
                source_frame_id=event.frame_id,
                source_message_ordinal=event.message_ordinal,
                source_change_ordinal=event.source_change_ordinal,
            )
        )
        previous_at_us[participant_id] = event.observed_at_us
        latest_event_id[participant_id] = event.timing_event_id
        source_cell_ids.add(event.timing_event_id)
    return grouped, latest_event_id, source_cell_ids


def _state_input(row: sqlite3.Row) -> ParticipantStateInput | None:
    if row["state_source_key"] is None:
        return None
    return ParticipantStateInput(
        position_overall=_int(row["position_overall"]),
        position_class=_int(row["position_class"]),
        marker=row["marker"],
        laps=_int(row["state_laps"]),
        state=row["state"],
        state_raw=row["state_raw"],
        state_kind=row["state_kind"],
        current_driver_name=row["current_driver_name"],
        current_driver_stint_raw=row["current_driver_stint_raw"],
        last_lap_ms=_int(row["last_lap_ms"]),
        last_lap_number=_int(row["last_lap_number"]),
        best_lap_ms=_int(row["best_lap_ms"]),
        best_lap_number=_int(row["best_lap_number"]),
        last_sectors_json=row["last_sectors_json"],
        best_sectors_json=row["best_sectors_json"],
        last_speeds_json=row["last_speeds_json"],
        gap_ms=_int(row["gap_ms"]),
        gap_raw=row["gap_raw"],
        gap_kind=row["gap_kind"],
        diff_ms=_int(row["diff_ms"]),
        diff_raw=row["diff_raw"],
        diff_kind=row["diff_kind"],
        sector_json=row["sector_json"],
        speed_kph=float(row["speed_kph"]) if row["speed_kph"] is not None else None,
        pit_time_raw=row["pit_time_raw"],
        provider_pit_count=_int(row["provider_pit_count"]),
        source_message_id=_int(row["state_source_message_id"]),
        source_key=row["state_source_key"],
        updated_at_us=int(row["state_updated_at_us"]),
    )


def _participant_sort_key(participant: ParticipantMetricInput) -> tuple[int, int, int, str, str]:
    state = participant.state
    return (
        0 if state is not None and state.position_class is not None else 1,
        state.position_class if state is not None and state.position_class is not None else 2_147_483_647,
        state.position_overall if state is not None and state.position_overall is not None else 2_147_483_647,
        participant.start_number or "",
        participant.id,
    )


def load_heat_metric_input(connection: sqlite3.Connection, source_heat_id: int) -> HeatMetricInput:
    """Read one source heat into facts only; no formula or dashboard choice leaks in."""

    if type(source_heat_id) is not int or source_heat_id <= 0:
        raise MetricStoreError("source_heat_id must be a positive integer")
    with _read_snapshot(connection):
        heat = connection.execute(
            """
            SELECT h.id,h.generation,h.external_name,h.provider_started_at_us,h.provider_finished_at_us,h.created_at_us,
                   s.id AS session_id,s.mode,s.lifecycle,s.race_duration_s,s.required_pits,s.started_at_us,
                   s.stopped_at_us,s.our_participant_id,s.our_class,s.identity_state
            FROM source_heats h
            JOIN analysis_sessions s ON s.id = h.analysis_session_id
            WHERE h.id = ?
            """,
            (source_heat_id,),
        ).fetchone()
        if heat is None:
            raise MetricStoreError(f"Source heat does not exist: {source_heat_id}")

        tick_row = connection.execute(
            """
            SELECT observed_at_us,freshness_ms,source_key
            FROM state_ticks
            WHERE source_heat_id = ?
            ORDER BY observed_second DESC
            LIMIT 1
            """,
            (source_heat_id,),
        ).fetchone()
        tick = (
            StateTickInput(
                observed_at_us=int(tick_row["observed_at_us"]),
                freshness_ms=int(tick_row["freshness_ms"]),
                source_key=tick_row["source_key"],
            )
            if tick_row is not None
            else None
        )

        flag_row = connection.execute(
            """
            SELECT flag,provider_code,provider_label,started_at_us,observed_started_at_us,
                   calibrated_started_at_us,start_provider_ts_raw,source_message_id,source_key,updated_at_us
            FROM track_flag_current
            WHERE source_heat_id = ?
            """,
            (source_heat_id,),
        ).fetchone()
        flag = (
            TrackFlagInput(
                flag=flag_row["flag"],
                provider_code=flag_row["provider_code"],
                provider_label=flag_row["provider_label"],
                started_at_us=int(flag_row["started_at_us"]),
                observed_started_at_us=_int(flag_row["observed_started_at_us"]),
                calibrated_started_at_us=_int(flag_row["calibrated_started_at_us"]),
                start_provider_ts_raw=flag_row["start_provider_ts_raw"],
                source_message_id=_int(flag_row["source_message_id"]),
                source_key=flag_row["source_key"],
                updated_at_us=int(flag_row["updated_at_us"]),
            )
            if flag_row is not None
            else None
        )

        statistics_row = connection.execute(
            """
            SELECT heat_name_raw,green_flag_at_us,finish_flag_at_us,participants_started,
                   participants_classified,participants_not_classified,participants_on_track,
                   participants_in_pit_zone,participants_in_tank_zone,total_laps,total_pitstops,
                   leader_laps_green,leader_laps_safety_car,leader_laps_code_60,
                   leader_laps_full_course_yellow,safety_car_count,code_60_count,
                   full_course_yellow_count,observed_at_us,source_message_id,source_key
            FROM heat_statistics_current
            WHERE source_heat_id = ?
            """,
            (source_heat_id,),
        ).fetchone()
        statistics = (
            HeatStatisticsInput(
                heat_name=statistics_row["heat_name_raw"],
                green_flag_at_us=_int(statistics_row["green_flag_at_us"]),
                finish_flag_at_us=_int(statistics_row["finish_flag_at_us"]),
                participants_started=_int(statistics_row["participants_started"]),
                participants_classified=_int(statistics_row["participants_classified"]),
                participants_not_classified=_int(statistics_row["participants_not_classified"]),
                participants_on_track=_int(statistics_row["participants_on_track"]),
                participants_in_pit_zone=_int(statistics_row["participants_in_pit_zone"]),
                participants_in_tank_zone=_int(statistics_row["participants_in_tank_zone"]),
                total_laps=_int(statistics_row["total_laps"]),
                total_pitstops=_int(statistics_row["total_pitstops"]),
                leader_laps_green=_int(statistics_row["leader_laps_green"]),
                leader_laps_safety_car=_int(statistics_row["leader_laps_safety_car"]),
                leader_laps_code_60=_int(statistics_row["leader_laps_code_60"]),
                leader_laps_full_course_yellow=_int(statistics_row["leader_laps_full_course_yellow"]),
                safety_car_count=_int(statistics_row["safety_car_count"]),
                code_60_count=_int(statistics_row["code_60_count"]),
                full_course_yellow_count=_int(statistics_row["full_course_yellow_count"]),
                observed_at_us=int(statistics_row["observed_at_us"]),
                source_message_id=_int(statistics_row["source_message_id"]),
                source_key=statistics_row["source_key"],
            )
            if statistics_row is not None
            else None
        )

        gap_row = connection.execute(
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
            (source_heat_id, source_heat_id),
        ).fetchone()
        open_gap = (
            IngestGapInput(
                started_at_us=int(gap_row["started_at_us"]),
                reason=gap_row["reason"],
                connection_id=gap_row["ingest_connection_id"],
            )
            if gap_row is not None
            else None
        )

        participant_rows = connection.execute(
            """
            SELECT p.id AS participant_id,p.external_key,p.transponder_id,p.start_number,p.team_name,
                   p.car_name,p.class_name,p.class_name_key,p.is_ours,p.active,p.first_seen_at_us,p.last_seen_at_us,
                   c.position_overall,c.position_class,c.marker,c.laps AS state_laps,c.state,c.state_raw,
                   c.state_kind,c.current_driver_name,c.current_driver_stint_raw,c.last_lap_ms,
                   c.last_lap_number,c.best_lap_ms,c.best_lap_number,c.last_sectors_json,
                   c.best_sectors_json,c.last_speeds_json,c.gap_ms,c.gap_raw,c.gap_kind,c.diff_ms,
                   c.diff_raw,c.diff_kind,c.sector_json,c.speed_kph,c.pit_time_raw,c.provider_pit_count,
                   c.source_message_id AS state_source_message_id,c.source_key AS state_source_key,
                   c.updated_at_us AS state_updated_at_us
            FROM participants p
            LEFT JOIN participant_state_current c
              ON c.source_heat_id = p.source_heat_id AND c.participant_id = p.id
            WHERE p.source_heat_id = ?
            ORDER BY p.id
            """,
            (source_heat_id,),
        ).fetchall()

        legacy_laps: list[tuple[str, int | None, bool, bool, LapInput]] = []
        for row in connection.execute(
            """
            SELECT lap.participant_id,lap.lap_number,lap.completed_at_us,lap.duration_ms,lap.sectors_json,lap.flag,
                   lap.is_in_lap,lap.is_out_lap,lap.crosses_pit,lap.is_clean,lap.source_message_id,lap.source_key,
                   lap.duration_source_cell_observation_id,lap.completion_passing_observation_id,
                   message.frame_id AS source_frame_id,message.ordinal AS source_message_ordinal,
                   EXISTS (
                     SELECT 1
                     FROM result_column_definitions AS explicit_laps
                     WHERE explicit_laps.layout_version_id = duration_cell.layout_version_id
                       AND explicit_laps.canonical_key = 'laps'
                   ) AS duration_from_explicit_laps_layout
            FROM laps AS lap
            LEFT JOIN feed_messages AS message ON message.id = lap.source_message_id
            LEFT JOIN participant_result_cell_observations AS duration_cell
              ON duration_cell.id = lap.duration_source_cell_observation_id
            WHERE lap.source_heat_id = ?
            ORDER BY lap.participant_id,lap.lap_number
            """,
            (source_heat_id,),
        ):
            legacy_laps.append(
                (
                    row["participant_id"],
                    _int(row["duration_source_cell_observation_id"]),
                    row["completion_passing_observation_id"] is not None,
                    bool(row["duration_from_explicit_laps_layout"]),
                    LapInput(
                        lap_number=int(row["lap_number"]),
                        completed_at_us=_int(row["completed_at_us"]),
                        duration_ms=_int(row["duration_ms"]),
                        sectors_json=row["sectors_json"],
                        flag=row["flag"],
                        is_in_lap=bool(row["is_in_lap"]),
                        is_out_lap=bool(row["is_out_lap"]),
                        crosses_pit=bool(row["crosses_pit"]),
                        is_clean=bool(row["is_clean"]),
                        source_message_id=_int(row["source_message_id"]),
                        source_key=row["source_key"],
                        source_frame_id=_int(row["source_frame_id"]),
                        source_message_ordinal=_int(row["source_message_ordinal"]),
                    ),
                )
            )

        pits_by_participant: dict[str, list[PitStopInput]] = defaultdict(list)
        for row in connection.execute(
            """
            SELECT participant_id,stop_number,entered_at_us,exited_at_us,entered_lap,exited_lap,
                   pit_lane_ms,pit_lane_duration_source_kind,completed,entered_source_message_id,entered_source_key,
                   exited_source_message_id,exited_source_key
            FROM pit_stops
            WHERE source_heat_id = ?
            ORDER BY participant_id,stop_number
            """,
            (source_heat_id,),
        ):
            pits_by_participant[row["participant_id"]].append(
                PitStopInput(
                    stop_number=int(row["stop_number"]),
                    entered_at_us=int(row["entered_at_us"]),
                    exited_at_us=_int(row["exited_at_us"]),
                    entered_lap=_int(row["entered_lap"]),
                    exited_lap=_int(row["exited_lap"]),
                    pit_lane_ms=_int(row["pit_lane_ms"]),
                    pit_lane_duration_source_kind=row["pit_lane_duration_source_kind"],
                    completed=bool(row["completed"]),
                    entered_source_message_id=_int(row["entered_source_message_id"]),
                    entered_source_key=row["entered_source_key"],
                    exited_source_message_id=_int(row["exited_source_message_id"]),
                    exited_source_key=row["exited_source_key"],
                )
            )

        raw_laps_by_participant, latest_timing_event_ids, raw_last_cell_ids = _load_raw_result_last_laps(
            connection,
            source_heat_id=source_heat_id,
            pits_by_participant=pits_by_participant,
        )
        laps_by_participant: dict[str, list[LapInput]] = defaultdict(list)
        for participant_id, duration_source_cell_id, tracker_boundary, explicit_grid_duration, lap in legacy_laps:
            # A same-frame tracker lap can already link to this r_c LAST cell.
            # The raw cell is the canonical timing sample, so retain it once.
            if duration_source_cell_id is not None and duration_source_cell_id in raw_last_cell_ids:
                continue
            # A raw no-LAPS LAST stream makes historical derived rows unsafe
            # timing evidence unless their source cell proves an explicit-LAPS
            # layout. This includes tracker-only crossings and pre-0009 rows
            # with no duration provenance. Keep them for tyre/stint chronology
            # but fail closed for pace, preventing a replayed raw LAST from
            # being double-counted with an old projection.
            if participant_id in raw_laps_by_participant and not explicit_grid_duration:
                lap = LapInput(
                    lap_number=lap.lap_number,
                    completed_at_us=lap.completed_at_us,
                    duration_ms=lap.duration_ms,
                    sectors_json=lap.sectors_json,
                    flag=lap.flag,
                    is_in_lap=lap.is_in_lap,
                    is_out_lap=lap.is_out_lap,
                    crosses_pit=lap.crosses_pit,
                    is_clean=lap.is_clean,
                    source_message_id=lap.source_message_id,
                    source_key=lap.source_key,
                    timing_eligible=False,
                    source_frame_id=lap.source_frame_id,
                    source_message_ordinal=lap.source_message_ordinal,
                    source_change_ordinal=lap.source_change_ordinal,
                )
            laps_by_participant[participant_id].append(lap)
        for participant_id, raw_laps in raw_laps_by_participant.items():
            laps_by_participant[participant_id].extend(raw_laps)

        stints_by_participant: dict[str, list[TireStintInput]] = defaultdict(list)
        for row in connection.execute(
            """
            SELECT participant_id,stint_number,started_at_us,ended_at_us,started_lap,ended_lap,
                   completed_laps,source_message_id,source_key
            FROM tire_stints
            WHERE source_heat_id = ?
            ORDER BY participant_id,stint_number
            """,
            (source_heat_id,),
        ):
            stints_by_participant[row["participant_id"]].append(
                TireStintInput(
                    stint_number=int(row["stint_number"]),
                    started_at_us=int(row["started_at_us"]),
                    ended_at_us=_int(row["ended_at_us"]),
                    started_lap=_int(row["started_lap"]),
                    ended_lap=_int(row["ended_lap"]),
                    completed_laps=int(row["completed_laps"]),
                    source_message_id=_int(row["source_message_id"]),
                    source_key=row["source_key"],
                )
            )

        class_bests: dict[str, tuple[int | None, str | None]] = {}
        for row in connection.execute(
            """
            SELECT class_name_key,lap_time_us,start_number_raw
            FROM statistics_class_best_laps
            WHERE source_heat_id = ?
            """,
            (source_heat_id,),
        ):
            lap_time_us = _int(row["lap_time_us"])
            class_bests[row["class_name_key"]] = (
                lap_time_us // 1_000 if lap_time_us is not None else None,
                row["start_number_raw"],
            )

        participants: list[ParticipantMetricInput] = []
        for row in participant_rows:
            participant_id = row["participant_id"]
            participants.append(
                ParticipantMetricInput(
                    id=participant_id,
                    external_key=row["external_key"],
                    transponder_id=row["transponder_id"],
                    start_number=row["start_number"],
                    team_name=row["team_name"],
                    car_name=row["car_name"],
                    class_name=row["class_name"],
                    class_key=row["class_name_key"] or _class_key(row["class_name"]),
                    is_ours=bool(row["is_ours"]),
                    active=bool(row["active"]),
                    first_seen_at_us=int(row["first_seen_at_us"]),
                    last_seen_at_us=int(row["last_seen_at_us"]),
                    state=_state_input(row),
                    laps=tuple(laps_by_participant[participant_id]),
                    pit_stops=tuple(pits_by_participant[participant_id]),
                    tire_stints=tuple(stints_by_participant[participant_id]),
                    latest_timing_event_id=latest_timing_event_ids.get(participant_id),
                )
            )
        participants.sort(key=_participant_sort_key)

        classes: dict[str, list[ParticipantMetricInput]] = defaultdict(list)
        class_names: dict[str, str] = {}
        for participant in participants:
            if participant.class_key is None:
                continue
            classes[participant.class_key].append(participant)
            class_names.setdefault(participant.class_key, participant.class_name or participant.class_key)
        class_scopes = tuple(
            ClassScopeInput(
                key=key,
                display_name=class_names[key],
                class_best_lap_ms=class_bests.get(key, (None, None))[0],
                class_best_start_number=class_bests.get(key, (None, None))[1],
                participants=tuple(sorted(members, key=_participant_sort_key)),
            )
            for key, members in sorted(classes.items())
        )

        observed_candidates = [int(heat["created_at_us"])]
        if tick is not None:
            observed_candidates.append(tick.observed_at_us)
        if flag is not None:
            observed_candidates.append(flag.updated_at_us)
        if statistics is not None:
            observed_candidates.append(statistics.observed_at_us)
        observed_candidates.extend(
            participant.state.updated_at_us for participant in participants if participant.state is not None
        )
        return HeatMetricInput(
            source_heat_id=int(heat["id"]),
            generation=int(heat["generation"]),
            external_name=heat["external_name"],
            provider_started_at_us=_int(heat["provider_started_at_us"]),
            provider_finished_at_us=_int(heat["provider_finished_at_us"]),
            created_at_us=int(heat["created_at_us"]),
            observed_at_us=max(observed_candidates),
            session=MetricSessionInput(
                id=heat["session_id"],
                mode=heat["mode"],
                lifecycle=heat["lifecycle"],
                race_duration_s=_int(heat["race_duration_s"]),
                required_pits=_int(heat["required_pits"]),
                started_at_us=_int(heat["started_at_us"]),
                stopped_at_us=_int(heat["stopped_at_us"]),
                our_participant_id=heat["our_participant_id"],
                our_class_name=heat["our_class"],
                identity_state=heat["identity_state"],
            ),
            latest_tick=tick,
            current_flag=flag,
            statistics=statistics,
            open_ingest_gap=open_gap,
            participants=tuple(participants),
            class_scopes=class_scopes,
        )


def _canonical_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise MetricStoreError("Metric values cannot contain NaN or infinity")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise MetricStoreError("Metric value object keys must be strings")
            normalized[key] = _canonical_json_value(child)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical_json_value(child) for child in value]
    raise MetricStoreError(f"Unsupported metric value type: {type(value).__name__}")


def _canonical_values_json(values: Mapping[str, Any]) -> str:
    if not isinstance(values, Mapping):
        raise MetricStoreError("Metric sample values must be an object")
    normalized = _canonical_json_value(values)
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _encoded_playback_payload(payload: Mapping[str, Any]) -> tuple[str, bytes, str]:
    """Encode a portable archive keyframe without retaining provider raw data."""

    encoded = _canonical_values_json(payload).encode("utf-8")
    return (
        PLAYBACK_PAYLOAD_CODEC,
        gzip.compress(encoded, compresslevel=6, mtime=0),
        hashlib.sha256(encoded).hexdigest(),
    )


def load_metric_history(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    scope_kind: str,
    scope_key: str,
    since_at_us: int | None = None,
    metric_version: int | None = None,
) -> tuple[MetricHistoryPoint, ...]:
    """Load ordered chart evidence without leaking SQLite into formulas."""
    if type(source_heat_id) is not int or source_heat_id <= 0:
        raise MetricStoreError("source_heat_id must be a positive integer")
    if scope_kind not in _SCOPE_KINDS:
        raise MetricStoreError(f"Unsupported metric scope: {scope_kind!r}")
    if not isinstance(scope_key, str) or not scope_key.strip():
        raise MetricStoreError("Metric scope_key must be a non-empty string")
    if since_at_us is not None and (type(since_at_us) is not int or since_at_us < 0):
        raise MetricStoreError("since_at_us must be a non-negative integer or None")
    if metric_version is not None and (type(metric_version) is not int or metric_version < 1):
        raise MetricStoreError("metric_version must be a positive integer or None")
    where = ["source_heat_id = ?", "scope_kind = ?", "scope_key = ?"]
    parameters: list[Any] = [source_heat_id, scope_kind, scope_key]
    if since_at_us is not None:
        where.append("observed_at_us >= ?")
        parameters.append(since_at_us)
    if metric_version is not None:
        where.append("metric_version = ?")
        parameters.append(metric_version)
    with _read_snapshot(connection):
        rows = connection.execute(
            f"""
            SELECT observed_at_us,metric_version,values_json
            FROM metric_samples
            WHERE {' AND '.join(where)}
            ORDER BY observed_at_us,observed_second
            """,
            tuple(parameters),
        ).fetchall()
    points: list[MetricHistoryPoint] = []
    for row in rows:
        try:
            values = json.loads(row["values_json"])
        except (TypeError, json.JSONDecodeError) as error:
            raise MetricStoreError("Stored metric history has invalid JSON") from error
        if not isinstance(values, Mapping):
            raise MetricStoreError("Stored metric history values must be an object")
        points.append(
            MetricHistoryPoint(
                observed_at_us=int(row["observed_at_us"]),
                metric_version=int(row["metric_version"]),
                values=values,
            )
        )
    return tuple(points)


def load_metric_runner_state(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    metric_version: int | None = None,
) -> MetricRunnerState | None:
    """Load the latest derived boundary without reconstructing it from facts."""

    if type(source_heat_id) is not int or source_heat_id <= 0:
        raise MetricStoreError("source_heat_id must be a positive integer")
    if metric_version is not None and (type(metric_version) is not int or metric_version < 1):
        raise MetricStoreError("metric_version must be a positive integer or None")
    where = ["source_heat_id = ?"]
    parameters: list[Any] = [source_heat_id]
    if metric_version is not None:
        where.append("metric_version = ?")
        parameters.append(metric_version)
    with _read_snapshot(connection):
        row = connection.execute(
            f"""
            SELECT source_heat_id,observed_at_us,source_frame_id,source_message_id,source_key,
                   metric_version,boundary_state_json
            FROM metric_runner_state
            WHERE {' AND '.join(where)}
            """,
            tuple(parameters),
        ).fetchone()
    if row is None:
        return None
    if not isinstance(row["boundary_state_json"], str) or not row["boundary_state_json"]:
        raise MetricStoreError("Stored metric runner boundary state is invalid")
    return MetricRunnerState(
        source_heat_id=int(row["source_heat_id"]),
        observed_at_us=int(row["observed_at_us"]),
        source_frame_id=int(row["source_frame_id"]),
        source_message_id=_int(row["source_message_id"]),
        source_key=row["source_key"],
        metric_version=int(row["metric_version"]),
        boundary_state_json=row["boundary_state_json"],
    )


def _validated_scope(candidate: MetricSampleCandidate) -> MetricScope:
    if candidate.scope_kind not in _SCOPE_KINDS:
        raise MetricStoreError(f"Unsupported metric scope: {candidate.scope_kind!r}")
    if not isinstance(candidate.scope_key, str) or not candidate.scope_key.strip():
        raise MetricStoreError("Metric scope_key must be a non-empty string")
    return MetricScope(candidate.scope_kind, candidate.scope_key)


def _observation_rank(
    observed_at_us: int,
    source_message_id: int | None,
    source_key: str,
) -> tuple[int, int, str]:
    """Order competing current-state writes deterministically.

    Receive time is primary.  Source message IDs then preserve order for
    multiple decoded messages from the same received frame; the source key is
    the stable fallback for imports that have no persisted message ID.
    """

    return (observed_at_us, source_message_id if source_message_id is not None else -1, source_key)


def materialize_metric_samples(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    observed_at_us: int,
    metric_version: int,
    source_key: str,
    samples: Iterable[MetricSampleCandidate],
    source_message_id: int | None = None,
    event_boundary: bool = False,
    interval_us: int = METRIC_SAMPLE_INTERVAL_US,
    runner_state: MetricRunnerStateCandidate | None = None,
    stream_events: Sequence[StreamEventCandidate] = (),
    playback_snapshot: PlaybackSnapshotCandidate | None = None,
) -> MetricMaterializationResult:
    """Materialize current state and sparse chart history from one metric tick.

    ``metric_current`` advances for every newer observation, including an
    unchanged value inside a chart interval.  ``metric_samples`` stays sparse:
    it permits one point per second and only retains a changed scope on a five
    second bucket, formula-version change, or source event boundary.  A newer
    event in the same second replaces that chart point deterministically;
    retrying the same event is a no-op in both materializations.
    """

    if type(source_heat_id) is not int or source_heat_id <= 0:
        raise MetricStoreError("source_heat_id must be a positive integer")
    if type(observed_at_us) is not int or observed_at_us < 0:
        raise MetricStoreError("observed_at_us must be a non-negative integer")
    if type(metric_version) is not int or metric_version < 1:
        raise MetricStoreError("metric_version must be a positive integer")
    if not isinstance(source_key, str) or not source_key.strip():
        raise MetricStoreError("source_key must be a non-empty string")
    if source_message_id is not None and (type(source_message_id) is not int or source_message_id <= 0):
        raise MetricStoreError("source_message_id must be a positive integer or None")
    if type(interval_us) is not int or interval_us <= 0:
        raise MetricStoreError("interval_us must be a positive integer")
    if runner_state is not None:
        if not isinstance(runner_state, MetricRunnerStateCandidate):
            raise MetricStoreError("runner_state must be MetricRunnerStateCandidate or None")
        if type(runner_state.source_frame_id) is not int or runner_state.source_frame_id <= 0:
            raise MetricStoreError("runner_state source_frame_id must be a positive integer")
        if not isinstance(runner_state.state_hash, str) or not runner_state.state_hash:
            raise MetricStoreError("runner_state state_hash must be a non-empty string")
        if not isinstance(runner_state.boundary_state_json, str) or not runner_state.boundary_state_json:
            raise MetricStoreError("runner_state boundary_state_json must be a non-empty string")
    if playback_snapshot is not None:
        if not isinstance(playback_snapshot, PlaybackSnapshotCandidate):
            raise MetricStoreError("playback_snapshot must be PlaybackSnapshotCandidate or None")
        if runner_state is None:
            raise MetricStoreError("playback snapshots require a durable metric runner state")
        _encoded_playback_payload(playback_snapshot.payload)
    try:
        prepared_stream_events = tuple(stream_events)
    except TypeError as error:
        raise MetricStoreError("stream_events must be an iterable of StreamEventCandidate values") from error
    if prepared_stream_events and runner_state is None:
        raise MetricStoreError("stream events require a durable metric runner state")

    prepared: list[tuple[MetricScope, str, str, bool]] = []
    seen_scopes: set[MetricScope] = set()
    for candidate in samples:
        if not isinstance(candidate, MetricSampleCandidate):
            raise MetricStoreError("samples must contain MetricSampleCandidate values")
        scope = _validated_scope(candidate)
        if scope in seen_scopes:
            raise MetricStoreError(f"Duplicate metric scope in one write: {scope.scope_kind}/{scope.scope_key}")
        seen_scopes.add(scope)
        current_values_json = _canonical_values_json(candidate.values)
        history_values_json = _canonical_values_json(
            candidate.values if candidate.history_values is None else candidate.history_values
        )
        prepared.append((scope, current_values_json, history_values_json, bool(candidate.event_boundary)))

    if not prepared:
        return MetricMaterializationResult((), (), ())

    observed_second = observed_at_us // 1_000_000
    bucket = observed_at_us // interval_us
    inserted: list[MetricScope] = []
    updated: list[MetricScope] = []
    skipped: list[MetricScope] = []
    current_inserted: list[MetricScope] = []
    current_updated: list[MetricScope] = []
    current_skipped: list[MetricScope] = []
    runner_state_written = False
    stream_events_written = 0
    playback_snapshot_written = False
    with _write_transaction(connection):
        heat = connection.execute(
            "SELECT analysis_session_id FROM source_heats WHERE id = ?", (source_heat_id,)
        ).fetchone()
        if heat is None:
            raise MetricStoreError(f"Source heat does not exist: {source_heat_id}")
        created_at_us = now_us()
        for scope, values_json, history_values_json, candidate_event in prepared:
            current = connection.execute(
                """
                SELECT observed_at_us,metric_version,values_json,source_message_id,source_key
                FROM metric_current
                WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ?
                """,
                (source_heat_id, scope.scope_kind, scope.scope_key),
            ).fetchone()
            if current is None:
                connection.execute(
                    """
                    INSERT INTO metric_current(
                      source_heat_id,scope_kind,scope_key,observed_at_us,metric_version,
                      values_json,source_message_id,source_key,created_at_us,updated_at_us
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        source_heat_id,
                        scope.scope_kind,
                        scope.scope_key,
                        observed_at_us,
                        metric_version,
                        values_json,
                        source_message_id,
                        source_key,
                        created_at_us,
                        created_at_us,
                    ),
                )
                current_inserted.append(scope)
            else:
                current_rank = _observation_rank(
                    int(current["observed_at_us"]),
                    _int(current["source_message_id"]),
                    current["source_key"],
                )
                incoming_rank = _observation_rank(observed_at_us, source_message_id, source_key)
                version_upgrade = (
                    incoming_rank == current_rank and metric_version > int(current["metric_version"])
                )
                if incoming_rank > current_rank or version_upgrade:
                    connection.execute(
                        """
                        UPDATE metric_current
                        SET observed_at_us = ?, metric_version = ?, values_json = ?,
                            source_message_id = ?, source_key = ?, updated_at_us = ?
                        WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ?
                        """,
                        (
                            observed_at_us,
                            metric_version,
                            values_json,
                            source_message_id,
                            source_key,
                            created_at_us,
                            source_heat_id,
                            scope.scope_kind,
                            scope.scope_key,
                        ),
                    )
                    current_updated.append(scope)
                else:
                    current_skipped.append(scope)

            exact = connection.execute(
                """
                SELECT observed_at_us,metric_version,values_json,source_key
                FROM metric_samples
                WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ? AND observed_second = ?
                """,
                (source_heat_id, scope.scope_kind, scope.scope_key, observed_second),
            ).fetchone()
            previous = connection.execute(
                """
                SELECT observed_at_us,metric_version,values_json
                FROM metric_samples
                WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ? AND observed_second < ?
                ORDER BY observed_second DESC
                LIMIT 1
                """,
                (source_heat_id, scope.scope_kind, scope.scope_key, observed_second),
            ).fetchone()
            unchanged = (
                (
                    exact is not None
                    and int(exact["metric_version"]) == metric_version
                    and exact["values_json"] == history_values_json
                )
                or (
                    exact is None
                    and previous is not None
                    and int(previous["metric_version"]) == metric_version
                    and previous["values_json"] == history_values_json
                )
            )
            if unchanged:
                skipped.append(scope)
                continue

            boundary = bool(event_boundary or candidate_event)
            previous_bucket = int(previous["observed_at_us"]) // interval_us if previous is not None else None
            is_periodic = previous is None or bucket > previous_bucket
            previous_version = int(previous["metric_version"]) if previous is not None else None
            version_changed = previous_version is not None and previous_version != metric_version
            if not boundary and not is_periodic and not version_changed:
                skipped.append(scope)
                continue

            if exact is None:
                connection.execute(
                    """
                    INSERT INTO metric_samples(
                      source_heat_id,scope_kind,scope_key,observed_second,observed_at_us,metric_version,
                      values_json,source_message_id,source_key,created_at_us
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        source_heat_id,
                        scope.scope_kind,
                        scope.scope_key,
                        observed_second,
                        observed_at_us,
                        metric_version,
                        history_values_json,
                        source_message_id,
                        source_key,
                        created_at_us,
                    ),
                )
                inserted.append(scope)
                continue

            existing_rank = (int(exact["observed_at_us"]), exact["source_key"])
            incoming_rank = (observed_at_us, source_key)
            if incoming_rank <= existing_rank:
                skipped.append(scope)
                continue
            connection.execute(
                """
                UPDATE metric_samples
                SET observed_at_us = ?, metric_version = ?, values_json = ?,
                    source_message_id = ?, source_key = ?
                WHERE source_heat_id = ? AND scope_kind = ? AND scope_key = ? AND observed_second = ?
                """,
                (
                    observed_at_us,
                    metric_version,
                    history_values_json,
                    source_message_id,
                    source_key,
                    source_heat_id,
                    scope.scope_kind,
                    scope.scope_key,
                    observed_second,
                ),
            )
            updated.append(scope)
        if runner_state is not None:
            tick_current = connection.execute(
                """
                SELECT observed_at_us,source_frame_id,source_key
                FROM state_ticks
                WHERE source_heat_id = ? AND observed_second = ?
                """,
                (source_heat_id, observed_second),
            ).fetchone()
            tick_rank = (
                (
                    int(tick_current["observed_at_us"]),
                    int(tick_current["source_frame_id"]) if tick_current["source_frame_id"] is not None else -1,
                    tick_current["source_key"],
                )
                if tick_current is not None
                else None
            )
            incoming_tick_rank = (observed_at_us, runner_state.source_frame_id, source_key)
            if tick_rank is None:
                connection.execute(
                    """
                    INSERT INTO state_ticks(
                      source_heat_id,observed_second,observed_at_us,source_frame_id,source_key,state_hash,
                      freshness_ms,created_at_us
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        source_heat_id,
                        observed_second,
                        observed_at_us,
                        runner_state.source_frame_id,
                        source_key,
                        runner_state.state_hash,
                        0,
                        created_at_us,
                    ),
                )
            elif incoming_tick_rank > tick_rank:
                connection.execute(
                    """
                    UPDATE state_ticks
                    SET observed_at_us = ?, source_frame_id = ?, source_key = ?, state_hash = ?, freshness_ms = ?
                    WHERE source_heat_id = ? AND observed_second = ?
                    """,
                    (
                        observed_at_us,
                        runner_state.source_frame_id,
                        source_key,
                        runner_state.state_hash,
                        0,
                        source_heat_id,
                        observed_second,
                    ),
                )

            state_current = connection.execute(
                """
                SELECT observed_at_us,source_frame_id,source_key,metric_version
                FROM metric_runner_state
                WHERE source_heat_id = ?
                """,
                (source_heat_id,),
            ).fetchone()
            state_rank = (
                (
                    int(state_current["observed_at_us"]),
                    int(state_current["source_frame_id"]),
                    state_current["source_key"],
                )
                if state_current is not None
                else None
            )
            version_upgrade = (
                state_current is not None
                and incoming_tick_rank == state_rank
                and metric_version > int(state_current["metric_version"])
            )
            if state_current is None:
                connection.execute(
                    """
                    INSERT INTO metric_runner_state(
                      source_heat_id,observed_at_us,source_frame_id,source_message_id,source_key,
                      metric_version,boundary_state_json,updated_at_us
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        source_heat_id,
                        observed_at_us,
                        runner_state.source_frame_id,
                        source_message_id,
                        source_key,
                        metric_version,
                        runner_state.boundary_state_json,
                        created_at_us,
                    ),
                )
                runner_state_written = True
            elif incoming_tick_rank > state_rank or version_upgrade:
                connection.execute(
                    """
                    UPDATE metric_runner_state
                    SET observed_at_us = ?, source_frame_id = ?, source_message_id = ?, source_key = ?,
                        metric_version = ?, boundary_state_json = ?, updated_at_us = ?
                    WHERE source_heat_id = ?
                    """,
                    (
                        observed_at_us,
                        runner_state.source_frame_id,
                        source_message_id,
                        source_key,
                        metric_version,
                        runner_state.boundary_state_json,
                        created_at_us,
                        source_heat_id,
                    ),
                )
                runner_state_written = True
        if runner_state_written and prepared_stream_events:
            try:
                stream_events_written = append_stream_events(
                    connection,
                    analysis_session_id=heat["analysis_session_id"],
                    source_heat_id=source_heat_id,
                    source_frame_id=runner_state.source_frame_id,
                    source_message_id=source_message_id,
                    source_key=source_key,
                    observed_at_us=observed_at_us,
                    events=prepared_stream_events,
                )
            except StreamEventError as error:
                raise MetricStoreError(f"Invalid stream event: {error}") from error
        if runner_state_written and playback_snapshot is not None:
            payload_codec, payload, payload_sha256 = _encoded_playback_payload(playback_snapshot.payload)
            exact = connection.execute(
                """
                SELECT observed_at_us,source_frame_id,source_key,metric_version,is_event_boundary,payload_sha256
                FROM playback_snapshots
                WHERE source_heat_id = ? AND observed_second = ?
                """,
                (source_heat_id, observed_second),
            ).fetchone()
            previous = connection.execute(
                """
                SELECT observed_at_us
                FROM playback_snapshots
                WHERE source_heat_id = ? AND observed_second < ?
                ORDER BY observed_second DESC
                LIMIT 1
                """,
                (source_heat_id, observed_second),
            ).fetchone()
            previous_bucket = (
                int(previous["observed_at_us"]) // PLAYBACK_SNAPSHOT_INTERVAL_US if previous is not None else None
            )
            periodic = previous_bucket is None or observed_at_us // PLAYBACK_SNAPSHOT_INTERVAL_US > previous_bucket
            if playback_snapshot.event_boundary or periodic:
                incoming_rank = (observed_at_us, runner_state.source_frame_id, source_key)
                exact_rank = (
                    (
                        int(exact["observed_at_us"]),
                        int(exact["source_frame_id"]) if exact["source_frame_id"] is not None else -1,
                        exact["source_key"],
                    )
                    if exact is not None
                    else None
                )
                if exact is None:
                    connection.execute(
                        """
                        INSERT INTO playback_snapshots(
                          source_heat_id,observed_second,observed_at_us,source_frame_id,source_message_id,
                          source_key,projection_version,metric_version,is_event_boundary,payload_codec,payload,
                          payload_sha256,created_at_us,updated_at_us
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            source_heat_id,
                            observed_second,
                            observed_at_us,
                            runner_state.source_frame_id,
                            source_message_id,
                            source_key,
                            PLAYBACK_PROJECTION_VERSION,
                            metric_version,
                            int(playback_snapshot.event_boundary),
                            payload_codec,
                            payload,
                            payload_sha256,
                            created_at_us,
                            created_at_us,
                        ),
                    )
                    playback_snapshot_written = True
                elif incoming_rank > exact_rank or (
                    incoming_rank == exact_rank and metric_version > int(exact["metric_version"])
                ):
                    connection.execute(
                        """
                        UPDATE playback_snapshots
                        SET observed_at_us = ?, source_frame_id = ?, source_message_id = ?, source_key = ?,
                            projection_version = ?, metric_version = ?,
                            is_event_boundary = ?, payload_codec = ?, payload = ?, payload_sha256 = ?, updated_at_us = ?
                        WHERE source_heat_id = ? AND observed_second = ?
                        """,
                        (
                            observed_at_us,
                            runner_state.source_frame_id,
                            source_message_id,
                            source_key,
                            PLAYBACK_PROJECTION_VERSION,
                            metric_version,
                            int(playback_snapshot.event_boundary or bool(exact["is_event_boundary"])),
                            payload_codec,
                            payload,
                            payload_sha256,
                            created_at_us,
                            source_heat_id,
                            observed_second,
                        ),
                    )
                    playback_snapshot_written = True
                elif incoming_rank == exact_rank and payload_sha256 != exact["payload_sha256"]:
                    raise MetricStoreError("Playback snapshot conflicts at an identical source rank")
    return MetricMaterializationResult(
        tuple(inserted),
        tuple(updated),
        tuple(skipped),
        tuple(current_inserted),
        tuple(current_updated),
        tuple(current_skipped),
        runner_state_written,
        stream_events_written,
        playback_snapshot_written,
    )
