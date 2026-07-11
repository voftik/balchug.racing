"""Deterministic tactical metric evaluation over normalized timing facts.

This module deliberately has no database or clock dependency.  The caller
loads one :class:`~timing.metric_store.HeatMetricInput` snapshot, evaluates it,
and may materialize the returned candidates.  Passing the prior immutable
snapshot/result enables event-boundary writes without letting replay speed or
browser time influence the calculation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
from math import ceil, floor
from typing import Any

from .metric_store import (
    ClassScopeInput,
    HeatMetricInput,
    LapInput,
    MetricHistoryPoint,
    MetricSampleCandidate,
    ParticipantMetricInput,
    PitStopInput,
    TireStintInput,
)
from .normalization import OPEN_ENDED_TS_TIME
from .metrics import (
    GREEN_FLAG,
    GAP_DIRECTION_LABEL_RU,
    GAP_RELATION_AHEAD,
    GAP_RELATION_BEHIND,
    GAP_WINDOWS_S,
    GapSample,
    LapSample,
    PaceMetrics,
    PitStop,
    RacePlan,
    calculate_catch_range,
    calculate_gap_trends,
    calculate_pace_metrics,
    calculate_pit_obligations,
    class_median_pace_ms,
    completed_pit_stops,
    is_clean_lap,
    pace_delta_ms,
    pace_rank,
)


METRIC_ENGINE_VERSION = 4
"""Version of the deterministic value schema emitted by this module."""

FRESHNESS_STALE_MS = 3_000
"""A persisted tick older than this is retained as STALE rather than LIVE."""

CHANNEL_LIVE = "LIVE"
CHANNEL_STALE = "STALE"
CHANNEL_OFFLINE = "OFFLINE"

CATCH_ALERT_LAPS = 5.0
SCHEDULE_DEVIATION_ALERT_S = 900.0
STINT_TREND_MAX_POINTS = 60
"""Bound robust degradation regression to the tactically current tyre window."""
SLOW_LAP_MAX_CLEAN_LAPS = 120
"""Retain current anomaly evidence without rescanning a 24-hour session."""

IntervalFactPointer = tuple[int | None, int | None, int | None, str | None, int | None, str | None]
"""Stable identity of one GAP/DIFF source cell, independent from row updates."""


@dataclass(frozen=True)
class ParticipantBoundaryState:
    """The small semantic participant state used for sparse-history events."""

    participant_id: str
    class_key: str | None
    identity: tuple[str | None, ...]
    driver_name: str | None
    position_overall: int | None
    position_class: int | None
    completed_laps: int | None
    state_kind: str | None
    last_lap_ms: int | None
    latest_timing_event_id: int | None
    best_lap_ms: int | None
    gap_interval_fact_pointer: IntervalFactPointer | None
    diff_interval_fact_pointer: IntervalFactPointer | None
    pits: tuple[tuple[int | None, int | None, int | None, bool | None], ...]
    active_stint: tuple[int | None, int | None, int | None] | None


@dataclass(frozen=True)
class MetricBoundaryState:
    """Facts whose changes are domain events, not ordinary chart ticks."""

    source_heat_id: int
    session: tuple[str | None, ...]
    flag: tuple[str | None, int | None, str | None, str | None] | None
    source_gap: tuple[int, str] | None
    ours_participant_id: str | None
    participants: tuple[ParticipantBoundaryState, ...]
    class_order_members: tuple[tuple[str, tuple[str, ...]], ...]


def serialize_metric_boundary_state(state: MetricBoundaryState) -> str:
    """Encode the minimal event cursor used to resume a runner after restart."""

    if not isinstance(state, MetricBoundaryState):
        raise TypeError("state must be MetricBoundaryState")
    payload = {
        "source_heat_id": state.source_heat_id,
        "session": list(state.session),
        "flag": list(state.flag) if state.flag is not None else None,
        "source_gap": list(state.source_gap) if state.source_gap is not None else None,
        "ours_participant_id": state.ours_participant_id,
        "participants": [
            {
                "participant_id": participant.participant_id,
                "class_key": participant.class_key,
                "identity": list(participant.identity),
                "driver_name": participant.driver_name,
                "position_overall": participant.position_overall,
                "position_class": participant.position_class,
                "completed_laps": participant.completed_laps,
                "state_kind": participant.state_kind,
                "last_lap_ms": participant.last_lap_ms,
                "latest_timing_event_id": participant.latest_timing_event_id,
                "best_lap_ms": participant.best_lap_ms,
                "gap_interval_fact_pointer": (
                    list(participant.gap_interval_fact_pointer)
                    if participant.gap_interval_fact_pointer is not None
                    else None
                ),
                "diff_interval_fact_pointer": (
                    list(participant.diff_interval_fact_pointer)
                    if participant.diff_interval_fact_pointer is not None
                    else None
                ),
                "pits": [list(pit) for pit in participant.pits],
                "active_stint": list(participant.active_stint) if participant.active_stint is not None else None,
            }
            for participant in state.participants
        ],
        "class_order_members": [
            [class_key, list(member_ids)]
            for class_key, member_ids in state.class_order_members
        ],
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def deserialize_metric_boundary_state(value: str) -> MetricBoundaryState:
    """Decode a durable runner cursor, rejecting malformed state explicitly."""

    try:
        payload = json.loads(value)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("Metric runner boundary state is not valid JSON") from error
    if not isinstance(payload, Mapping) or not _is_int(payload.get("source_heat_id"), minimum=1):
        raise ValueError("Metric runner boundary state has no valid source heat")

    def optional_text(item: Any) -> str | None:
        return item if isinstance(item, str) else None

    def optional_integer(item: Any) -> int | None:
        return item if _is_int(item) else None

    def interval_fact_pointer(item: Any) -> IntervalFactPointer | None:
        """Read a backward-compatible, field-level source fact cursor."""

        if item is None:
            return None
        if not isinstance(item, list) or len(item) != 6:
            raise ValueError("Metric runner boundary state has an invalid interval fact pointer")
        pointer: IntervalFactPointer = (
            item[0] if _is_int(item[0], minimum=1) else None,
            item[1] if _is_int(item[1], minimum=1) else None,
            item[2] if _is_int(item[2], minimum=1) else None,
            optional_text(item[3]),
            item[4] if _is_int(item[4], minimum=0) else None,
            optional_text(item[5]),
        )
        return pointer if any(value is not None for value in pointer) else None

    session_raw = payload.get("session")
    if not isinstance(session_raw, list) or len(session_raw) != 5:
        raise ValueError("Metric runner boundary state has an invalid session")
    session = tuple(optional_text(item) for item in session_raw)

    flag_raw = payload.get("flag")
    flag: tuple[str | None, int | None, str | None, str | None] | None
    if flag_raw is None:
        flag = None
    elif isinstance(flag_raw, list) and len(flag_raw) == 4:
        flag = (
            optional_text(flag_raw[0]),
            optional_integer(flag_raw[1]),
            optional_text(flag_raw[2]),
            optional_text(flag_raw[3]),
        )
    else:
        raise ValueError("Metric runner boundary state has an invalid flag")

    source_gap_raw = payload.get("source_gap")
    source_gap: tuple[int, str] | None
    if source_gap_raw is None:
        source_gap = None
    elif (
        isinstance(source_gap_raw, list)
        and len(source_gap_raw) == 2
        and _is_int(source_gap_raw[0], minimum=0)
        and isinstance(source_gap_raw[1], str)
    ):
        source_gap = (source_gap_raw[0], source_gap_raw[1])
    else:
        raise ValueError("Metric runner boundary state has an invalid source gap")

    participants_raw = payload.get("participants")
    if not isinstance(participants_raw, list):
        raise ValueError("Metric runner boundary state has invalid participants")
    participants: list[ParticipantBoundaryState] = []
    for item in participants_raw:
        if not isinstance(item, Mapping) or not isinstance(item.get("participant_id"), str):
            raise ValueError("Metric runner boundary state has an invalid participant")
        identity_raw = item.get("identity")
        pits_raw = item.get("pits")
        active_raw = item.get("active_stint")
        if not isinstance(identity_raw, list) or len(identity_raw) != 4 or not isinstance(pits_raw, list):
            raise ValueError("Metric runner boundary state has invalid participant detail")
        pits: list[tuple[int | None, int | None, int | None, bool | None]] = []
        for pit in pits_raw:
            if not isinstance(pit, list) or len(pit) != 4:
                raise ValueError("Metric runner boundary state has an invalid pit")
            completed = pit[3] if type(pit[3]) is bool else None
            pits.append((optional_integer(pit[0]), optional_integer(pit[1]), optional_integer(pit[2]), completed))
        if active_raw is None:
            active_stint = None
        elif isinstance(active_raw, list) and len(active_raw) == 3:
            active_stint = (optional_integer(active_raw[0]), optional_integer(active_raw[1]), optional_integer(active_raw[2]))
        else:
            raise ValueError("Metric runner boundary state has an invalid active stint")
        participants.append(
            ParticipantBoundaryState(
                participant_id=item["participant_id"],
                class_key=optional_text(item.get("class_key")),
                identity=tuple(optional_text(part) for part in identity_raw),
                driver_name=optional_text(item.get("driver_name")),
                position_overall=optional_integer(item.get("position_overall")),
                position_class=optional_integer(item.get("position_class")),
                completed_laps=optional_integer(item.get("completed_laps")),
                state_kind=optional_text(item.get("state_kind")),
                last_lap_ms=optional_integer(item.get("last_lap_ms")),
                latest_timing_event_id=optional_integer(item.get("latest_timing_event_id")),
                best_lap_ms=optional_integer(item.get("best_lap_ms")),
                gap_interval_fact_pointer=interval_fact_pointer(item.get("gap_interval_fact_pointer")),
                diff_interval_fact_pointer=interval_fact_pointer(item.get("diff_interval_fact_pointer")),
                pits=tuple(pits),
                active_stint=active_stint,
            )
        )

    class_order_raw = payload.get("class_order_members")
    if not isinstance(class_order_raw, list):
        raise ValueError("Metric runner boundary state has invalid class order")
    class_order_members: list[tuple[str, tuple[str, ...]]] = []
    for item in class_order_raw:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not isinstance(item[1], list)
            or not all(isinstance(member, str) for member in item[1])
        ):
            raise ValueError("Metric runner boundary state has an invalid class order entry")
        class_order_members.append((item[0], tuple(item[1])))
    return MetricBoundaryState(
        source_heat_id=payload["source_heat_id"],
        session=session,
        flag=flag,
        source_gap=source_gap,
        ours_participant_id=optional_text(payload.get("ours_participant_id")),
        participants=tuple(participants),
        class_order_members=tuple(class_order_members),
    )


@dataclass(frozen=True)
class MetricEngineResult:
    """One deterministic evaluation and the state needed for its successor."""

    source_heat_id: int
    observed_at_us: int
    candidates: tuple[MetricSampleCandidate, ...]
    event_keys: tuple[str, ...]
    boundary_state: MetricBoundaryState

    @property
    def event_boundary(self) -> bool:
        return bool(self.event_keys)


@dataclass(frozen=True)
class _ClassOrder:
    members: tuple[ParticipantMetricInput, ...]
    basis: str


def _is_int(value: Any, *, minimum: int | None = None) -> bool:
    return type(value) is int and (minimum is None or value >= minimum)


def _duration_ms(value: int | None) -> int | None:
    return value if _is_int(value, minimum=1) else None


def _rank(value: int | None) -> int | None:
    return value if _is_int(value, minimum=1) else None


def _lap_count(value: int | None) -> int | None:
    return value if _is_int(value, minimum=0) else None


def _elapsed_s(observed_at_us: int, started_at_us: int | None) -> float | None:
    if not _is_int(observed_at_us, minimum=0) or not _is_int(started_at_us, minimum=0):
        return None
    return max(0, observed_at_us - started_at_us) / 1_000_000.0


def _source_time_ms(value: int | None, kind: str | None) -> int | None:
    return value if kind == "TIME" and _is_int(value, minimum=0) else None


def _completed_laps(participant: ParticipantMetricInput) -> int | None:
    if participant.state is not None and _lap_count(participant.state.laps) is not None:
        return participant.state.laps
    observed = [lap.lap_number for lap in participant.laps if _lap_count(lap.lap_number) is not None]
    return max(observed) if observed else None


def _source_lap_count(participant: ParticipantMetricInput) -> int | None:
    """Return only an explicit current source LAPS value.

    Tracker passings are useful to reconstruct a local stint after capture
    starts, but a partial capture cannot promote them to the provider's
    official class-lap total.  Position gaps and lap deltas therefore use this
    narrower helper rather than ``_completed_laps``.
    """

    state = participant.state
    return _lap_count(state.laps) if state is not None else None


def _timing_laps(participant: ParticipantMetricInput) -> tuple[LapInput, ...]:
    """Return only facts that are eligible for timing calculations.

    Eligibility is assigned per normalized row. This is deliberately not a
    participant-wide "raw-or-legacy" switch: a feed can change layout during
    one heat, so raw no-LAPS LAST events and later explicit LAPS rows may both
    be valid timing evidence. Tracker-only rows remain available elsewhere
    for tyre-age and stint chronology.
    """

    return tuple(lap for lap in participant.laps if lap.timing_eligible)


def _last_lap_ms(participant: ParticipantMetricInput) -> int | None:
    if participant.state is not None and _duration_ms(participant.state.last_lap_ms) is not None:
        return participant.state.last_lap_ms
    for lap in reversed(tuple(sorted(_timing_laps(participant), key=_lap_sort_key))):
        if _duration_ms(lap.duration_ms) is not None:
            return lap.duration_ms
    return None


def _best_lap_ms(participant: ParticipantMetricInput) -> int | None:
    if participant.state is not None and _duration_ms(participant.state.best_lap_ms) is not None:
        return participant.state.best_lap_ms
    values = [lap.duration_ms for lap in _timing_laps(participant) if _duration_ms(lap.duration_ms) is not None]
    return min(values) if values else None


def _lap_sort_key(lap: LapInput) -> tuple[int, int, int, int, int, int]:
    # Raw LAST observations intentionally have no provider lap number.  Their
    # frame/message/change order is authoritative even if receipt timestamps
    # tie or regress across reconnect/replay.
    if _is_int(lap.source_frame_id, minimum=1):
        return (
            0,
            lap.source_frame_id,
            lap.source_message_ordinal if _is_int(lap.source_message_ordinal, minimum=0) else 2_147_483_647,
            lap.source_change_ordinal if _is_int(lap.source_change_ordinal, minimum=0) else 0,
            lap.timing_event_id if _is_int(lap.timing_event_id, minimum=1) else 0,
            lap.lap_number if _lap_count(lap.lap_number) is not None else 2_147_483_647,
        )
    return (
        1,
        lap.completed_at_us if _is_int(lap.completed_at_us, minimum=0) else 2_147_483_647,
        lap.capture_sequence if _is_int(lap.capture_sequence, minimum=1) else 2_147_483_647,
        lap.lap_number if _lap_count(lap.lap_number) is not None else 2_147_483_647,
        lap.timing_event_id if _is_int(lap.timing_event_id, minimum=1) else 2_147_483_647,
        lap.duration_ms if _duration_ms(lap.duration_ms) is not None else 2_147_483_647,
    )


def _lap_samples(participant: ParticipantMetricInput) -> tuple[LapSample, ...]:
    """Use the normalizer's persisted clean-lap decision without re-guessing it."""

    samples: list[LapSample] = []
    for lap in sorted(_timing_laps(participant), key=_lap_sort_key):
        # ``is_clean`` already includes the full flag/pit/feed-gap interval.
        # Supplying an explicit feed gap for a rejected row keeps a current
        # Green flag from accidentally promoting historical non-clean laps.
        samples.append(
            LapSample(
                participant_id=participant.id,
                lap_number=lap.lap_number,
                completed_at_us=lap.completed_at_us,
                duration_ms=lap.duration_ms,
                flag_kinds=(GREEN_FLAG,) if lap.is_clean else ((lap.flag,) if lap.flag else ()),
                is_in_lap=False if lap.is_clean else lap.is_in_lap,
                is_out_lap=False if lap.is_clean else lap.is_out_lap,
                crosses_pit=False if lap.is_clean else lap.crosses_pit,
                has_feed_gap=False if lap.is_clean else True,
            )
        )
    return tuple(samples)


def _pit_stop(value: PitStopInput) -> PitStop:
    return PitStop(
        stop_number=value.stop_number,
        entered_at_us=value.entered_at_us,
        exited_at_us=value.exited_at_us,
        entered_lap=value.entered_lap,
        exited_lap=value.exited_lap,
        pit_lane_ms=value.pit_lane_ms,
        pit_lane_duration_source_kind=value.pit_lane_duration_source_kind,
        completed=value.completed,
    )


def _completed_pits(participant: ParticipantMetricInput) -> tuple[PitStop, ...]:
    return completed_pit_stops(tuple(_pit_stop(stop) for stop in participant.pit_stops))


def _pit_history(participant: ParticipantMetricInput) -> tuple[dict[str, int | None], ...]:
    history: list[dict[str, int | None]] = []
    for stop in _completed_pits(participant):
        assert stop.entered_at_us is not None and stop.exited_at_us is not None
        # The observed pit-in/pit-out times are chronology only. Time Service
        # exposes the measured lane duration independently in L-PIT; deriving
        # it from observation timestamps turns stale/cached cells into a false
        # tactical fact.
        duration_ms = (
            stop.pit_lane_ms
            if stop.pit_lane_ms is not None and stop.pit_lane_duration_source_kind == "RESULT_L_PIT"
            else None
        )
        history.append(
            {
                "stop_number": stop.stop_number,
                "pit_in_at_us": stop.entered_at_us,
                "pit_out_at_us": stop.exited_at_us,
                "pit_in_lap": stop.entered_lap,
                "pit_out_lap": stop.exited_lap,
                "pit_lane_duration_ms": duration_ms,
            }
        )
    return tuple(history)


def _median_non_negative(values: Sequence[int]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    return float(ordered[middle]) if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2.0


def _laps_for_stint(participant: ParticipantMetricInput, stint: TireStintInput) -> tuple[LapInput, ...]:
    stint_laps: list[LapInput] = []
    for lap in participant.laps:
        if stint.started_at_us is not None and lap.completed_at_us is not None:
            if lap.completed_at_us < stint.started_at_us:
                continue
        elif stint.started_lap is not None:
            if lap.lap_number is None or lap.lap_number <= stint.started_lap:
                continue
        if stint.ended_at_us is not None and lap.completed_at_us is not None and lap.completed_at_us >= stint.ended_at_us:
            continue
        if stint.ended_lap is not None:
            if lap.lap_number is None or lap.lap_number > stint.ended_lap:
                continue
        stint_laps.append(lap)
    return tuple(stint_laps)


def _samples_for_stint(samples: Sequence[LapSample], stint: TireStintInput) -> tuple[LapSample, ...]:
    """Select an already-normalized participant sample stream for one stint."""

    result: list[LapSample] = []
    for lap in samples:
        if stint.started_at_us is not None and lap.completed_at_us is not None:
            if lap.completed_at_us < stint.started_at_us:
                continue
        elif stint.started_lap is not None:
            if lap.lap_number is None or lap.lap_number <= stint.started_lap:
                continue
        if stint.ended_at_us is not None and lap.completed_at_us is not None and lap.completed_at_us >= stint.ended_at_us:
            continue
        if stint.ended_lap is not None:
            if lap.lap_number is None or lap.lap_number > stint.ended_lap:
                continue
        result.append(lap)
    return tuple(result)


def _participant_with_laps(participant: ParticipantMetricInput, laps: tuple[LapInput, ...]) -> ParticipantMetricInput:
    stint_participant = ParticipantMetricInput(
        id=participant.id,
        external_key=participant.external_key,
        transponder_id=participant.transponder_id,
        start_number=participant.start_number,
        team_name=participant.team_name,
        car_name=participant.car_name,
        class_name=participant.class_name,
        class_key=participant.class_key,
        is_ours=participant.is_ours,
        active=participant.active,
        first_seen_at_us=participant.first_seen_at_us,
        last_seen_at_us=participant.last_seen_at_us,
        state=participant.state,
        laps=laps,
        pit_stops=participant.pit_stops,
        tire_stints=participant.tire_stints,
    )
    return stint_participant


def _stint_pace_values(
    participant: ParticipantMetricInput,
    stint: TireStintInput,
    *,
    all_samples: Sequence[LapSample] | None = None,
) -> tuple[PaceMetrics, tuple[LapSample, ...]]:
    samples = _samples_for_stint(
        all_samples if all_samples is not None else _lap_samples(participant),
        stint,
    )
    # Full slow-lap history is produced once for the participant's overall
    # stream. A stint's rolling pace needs only the current windows/counts.
    return calculate_pace_metrics(samples, include_slow_lap_history=False), samples


def _theil_sen_slope(points: Sequence[tuple[int, int]]) -> float | None:
    # Full 24-hour stints make the O(n²) pairwise estimator unsuitable for a
    # one-second feed. Recent tyre behaviour is the actionable signal, so the
    # stable trailing window is deliberately bounded.
    points = points[-STINT_TREND_MAX_POINTS:]
    slopes: list[float] = []
    for left_index, (left_age, left_duration) in enumerate(points):
        for right_age, right_duration in points[left_index + 1 :]:
            delta_age = right_age - left_age
            if delta_age:
                slopes.append((right_duration - left_duration) / delta_age)
    if not slopes:
        return None
    ordered = sorted(slopes)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2.0


def _slow_lap_events(
    samples: Sequence[LapSample], *, max_recent_clean_laps: int = SLOW_LAP_MAX_CLEAN_LAPS
) -> tuple[dict[str, int | float], ...]:
    clean = [sample for sample in samples if is_clean_lap(sample) and sample.duration_ms is not None]
    if max_recent_clean_laps > 0 and len(clean) > max_recent_clean_laps + 10:
        clean = clean[-(max_recent_clean_laps + 10) :]
    events: list[dict[str, int | float]] = []
    for index, sample in enumerate(clean):
        prior = [candidate.duration_ms for candidate in clean[max(0, index - 10) : index] if candidate.duration_ms is not None]
        if len(prior) != 10 or sample.lap_number is None:
            continue
        median = _median_non_negative(prior)
        if median is None:
            continue
        mad = _median_non_negative([int(abs(duration - median)) for duration in prior])
        if mad is None:
            continue
        threshold = median + max(2_000.0, 3.0 * mad)
        if sample.duration_ms > threshold:
            events.append(
                {
                    "lap_number": sample.lap_number,
                    "threshold_ms": threshold,
                    "excess_ms": sample.duration_ms - threshold,
                }
            )
    return tuple(events)


def _stint_age_duration_points_from_samples(
    samples: Sequence[LapSample], stint: TireStintInput
) -> tuple[tuple[int, int], ...]:
    if stint.started_lap is None:
        return ()
    result: list[tuple[int, int]] = []
    for lap in samples:
        if not is_clean_lap(lap) or lap.lap_number is None or lap.duration_ms is None:
            continue
        age = lap.lap_number - stint.started_lap
        if age >= 0:
            result.append((age, lap.duration_ms))
    return tuple(result)


def _stint_age_duration_points(
    participant: ParticipantMetricInput,
    stint: TireStintInput,
    *,
    all_samples: Sequence[LapSample] | None = None,
) -> tuple[tuple[int, int], ...]:
    return _stint_age_duration_points_from_samples(
        _stint_pace_values(participant, stint, all_samples=all_samples)[1],
        stint,
    )


def _near_tyre_age_pace_delta(
    ours: ParticipantMetricInput,
    scope: ClassScopeInput | None,
    *,
    samples_by_participant: Mapping[str, Sequence[LapSample]] | None = None,
) -> dict[str, float | None] | None:
    ours_stint = ours.active_tire_stint
    if scope is None or ours_stint is None:
        return None
    ours_age = ours_stint.completed_laps
    ours_band = [
        duration
        for age, duration in _stint_age_duration_points(
            ours,
            ours_stint,
            all_samples=samples_by_participant.get(ours.id) if samples_by_participant is not None else None,
        )
        if abs(age - ours_age) <= 2
    ]
    ours_median = _median_non_negative(ours_band)
    if ours_median is None or len(ours_band) < 3:
        return None
    deltas: dict[str, float | None] = {}
    for competitor in scope.participants:
        if competitor.id == ours.id:
            continue
        stint = competitor.active_tire_stint
        if stint is None or abs(stint.completed_laps - ours_age) > 2:
            deltas[competitor.id] = None
            continue
        band = [
            duration
            for age, duration in _stint_age_duration_points(
                competitor,
                stint,
                all_samples=samples_by_participant.get(competitor.id) if samples_by_participant is not None else None,
            )
            if abs(age - ours_age) <= 2
        ]
        competitor_median = _median_non_negative(band)
        deltas[competitor.id] = ours_median - competitor_median if competitor_median is not None and len(band) >= 3 else None
    return deltas


def _stint_summary(
    participant: ParticipantMetricInput,
    observed_at_us: int,
    *,
    active_stint: TireStintInput | None = None,
    active_pace: PaceMetrics | None = None,
    active_samples: Sequence[LapSample] | None = None,
    all_samples: Sequence[LapSample] | None = None,
) -> tuple[dict[str, Any], ...]:
    summaries: list[dict[str, Any]] = []
    for stint in participant.tire_stints:
        if stint is active_stint and active_pace is not None and active_samples is not None:
            pace, samples = active_pace, active_samples
        else:
            pace, samples = _stint_pace_values(participant, stint, all_samples=all_samples)
        clean_durations = [sample.duration_ms for sample in samples if is_clean_lap(sample) and sample.duration_ms is not None]
        summaries.append(
            {
                "stint_number": stint.stint_number,
                "completed_laps": stint.completed_laps,
                "elapsed_s": _elapsed_s(stint.ended_at_us or observed_at_us, stint.started_at_us),
                "pace_5_ms": pace.pace5_ms,
                "best_lap_ms": min(clean_durations) if clean_durations else None,
                "consistency_10_ms": pace.consistency10_ms,
            }
        )
    return tuple(summaries)


def _active_stint_values(
    participant: ParticipantMetricInput,
    observed_at_us: int,
    *,
    all_samples: Sequence[LapSample] | None = None,
) -> dict[str, Any]:
    stint = participant.active_tire_stint
    if stint is None:
        return {
            "stint_number": None,
            "stint_started_at_us": None,
            "stint_elapsed_s": None,
            "tyre_age_laps": None,
            "stint_pace_5_ms": None,
            "stint_best_lap_ms": None,
            "stint_consistency_10_ms": None,
            "stint_trend_ms_per_lap": None,
            "stint_cumulative_pace_change_ms": None,
            "stint_pace_delta_previous_ms": None,
            "stint_abrupt_deterioration": None,
            "stint_clean_lap_count": None,
            "clean_lap_ratio_current_stint": None,
            "stint_summary": None,
        }

    stint_pace, samples = _stint_pace_values(participant, stint, all_samples=all_samples)
    clean_durations = [sample.duration_ms for sample in samples if is_clean_lap(sample) and sample.duration_ms is not None]
    trend_points = _stint_age_duration_points_from_samples(samples, stint)
    trend = _theil_sen_slope(trend_points) if len({age for age, _ in trend_points}) >= 6 else None
    prior_stints = [candidate for candidate in participant.tire_stints if candidate.stint_number < stint.stint_number and candidate.ended_at_us is not None]
    previous_pace = (
        _stint_pace_values(participant, prior_stints[-1], all_samples=all_samples)[0].pace5_ms
        if prior_stints
        else None
    )
    minimum_age = min((age for age, _ in trend_points), default=None)
    slow_events = _slow_lap_events(samples)
    abrupt = (
        {"laps": [slow_events[-2], slow_events[-1]]}
        if len(slow_events) >= 2
        and int(slow_events[-1]["lap_number"]) == int(slow_events[-2]["lap_number"]) + 1
        else None
    )
    return {
        "stint_number": stint.stint_number,
        "stint_started_at_us": stint.started_at_us,
        "stint_elapsed_s": _elapsed_s(observed_at_us, stint.started_at_us),
        "tyre_age_laps": stint.completed_laps,
        "stint_pace_5_ms": stint_pace.pace5_ms,
        "stint_best_lap_ms": min(clean_durations) if clean_durations else None,
        "stint_consistency_10_ms": stint_pace.consistency10_ms,
        "stint_trend_ms_per_lap": trend,
        "stint_cumulative_pace_change_ms": (
            trend * (stint.completed_laps - minimum_age)
            if trend is not None and minimum_age is not None
            else None
        ),
        "stint_pace_delta_previous_ms": (
            stint_pace.pace5_ms - previous_pace
            if stint_pace.pace5_ms is not None and previous_pace is not None
            else None
        ),
        "stint_abrupt_deterioration": abrupt,
        "stint_clean_lap_count": stint_pace.clean_lap_count,
        "clean_lap_ratio_current_stint": stint_pace.clean_lap_ratio,
        "stint_summary": _stint_summary(
            participant,
            observed_at_us,
            active_stint=stint,
            active_pace=stint_pace,
            active_samples=samples,
            all_samples=all_samples,
        ),
    }


def _participant_values(
    participant: ParticipantMetricInput,
    *,
    observed_at_us: int,
    pace: PaceMetrics,
    lap_samples: Sequence[LapSample] | None = None,
) -> dict[str, Any]:
    state = participant.state
    completed_pits = _completed_pits(participant)
    history = _pit_history(participant)
    durations = [record["pit_lane_duration_ms"] for record in history if record["pit_lane_duration_ms"] is not None]
    last_lap = _last_lap_ms(participant)
    best_lap = _best_lap_ms(participant)
    samples = tuple(lap_samples) if lap_samples is not None else _lap_samples(participant)
    slow_events = _slow_lap_events(samples)
    return {
        "participant_id": participant.id,
        "start_number": participant.start_number,
        "team_name": participant.team_name,
        "car_name": participant.car_name,
        "class_name": participant.class_name,
        "class_key": participant.class_key,
        "is_ours": participant.is_ours,
        "active": participant.active,
        "current_driver_name": state.current_driver_name if state is not None else None,
        "position_overall": _rank(state.position_overall) if state is not None else None,
        "position_class": _rank(state.position_class) if state is not None else None,
        "completed_laps": _source_lap_count(participant),
        "current_state": state.state_kind if state is not None else None,
        "last_lap_ms": last_lap,
        "best_lap_ms": best_lap,
        "last_to_best_delta_ms": last_lap - best_lap if last_lap is not None and best_lap is not None and last_lap >= best_lap else None,
        "source_gap_ms": _source_time_ms(state.gap_ms, state.gap_kind) if state is not None else None,
        "source_diff_ms": _source_time_ms(state.diff_ms, state.diff_kind) if state is not None else None,
        "source_gap_fact": _source_fact_payload(state.gap_interval_fact) if state is not None else None,
        "source_diff_fact": _source_fact_payload(state.diff_interval_fact) if state is not None else None,
        "pace_3_ms": pace.pace3_ms,
        "pace_5_ms": pace.pace5_ms,
        "pace_10_ms": pace.pace10_ms,
        "consistency_10_ms": pace.consistency10_ms,
        "clean_lap_p10_p90_ms": (
            {
                "p10_ms": None,
                "p90_ms": None,
                "spread_ms": pace.p10_p90_range_ms,
            }
            if pace.p10_p90_range_ms is None
            else _p10_p90_values(participant, pace, lap_samples=samples)
        ),
        "clean_lap_count": pace.clean_lap_count,
        "observed_lap_count": pace.observed_lap_count,
        "clean_lap_ratio": pace.clean_lap_ratio,
        "slow_lap_numbers": pace.slow_lap_numbers,
        "slow_lap_anomaly": slow_events[-1] if slow_events else None,
        **_participant_sector_values(participant),
        **_active_stint_values(participant, observed_at_us, all_samples=samples),
        "pits_completed": len(completed_pits) if participant.tire_stints or state is not None else None,
        "pit_history": history,
        "total_pit_lane_time_ms": sum(durations) if durations else None,
        "median_pit_lane_time_ms": _median_non_negative(durations),
    }


def _sector_duration_ms(value: Any) -> int | None:
    if type(value) is int:
        raw = value
    elif isinstance(value, str) and value.strip().isdigit():
        raw = int(value.strip())
    else:
        return None
    # Time Service result-grid timing fields are microseconds. A lower value
    # could be an unrelated layout marker, while Int64.MaxValue explicitly
    # means an open/unavailable timing field. Both stay unavailable rather
    # than becoming a false sector duration.
    return raw // 1_000 if 1_000_000 <= raw < OPEN_ENDED_TS_TIME else None


def _sector_map(raw_json: str | None) -> dict[str, int]:
    if not raw_json:
        return {}
    try:
        raw = json.loads(raw_json)
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, Mapping):
        return {}
    values: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key.startswith("sector_"):
            continue
        duration = _sector_duration_ms(value)
        if duration is not None:
            values[key] = duration
    return values


def _participant_sector_values(participant: ParticipantMetricInput) -> dict[str, Any]:
    # Result-grid sector cells arrive sparsely throughout a lap. Until LAST
    # closes that lap they can be a mix of the previous and current lap, so
    # they must not improve a personal/class sector benchmark. The normalizer
    # admits a sectors_json only after it has linked each value to the source
    # LAST boundary and its exact result-cell observation.
    all_maps = [_sector_map(lap.sectors_json) for lap in participant.laps]
    confirmed = [values for values in all_maps if values]
    last = confirmed[-1] if confirmed else {}
    keys = sorted({key for values in all_maps for key in values})
    personal = {
        key: min(values[key] for values in all_maps if key in values)
        for key in keys
    }
    return {
        "last_sector_ms": last or None,
        "personal_best_sector_ms": personal or None,
    }


def _sector_metrics(ours: ParticipantMetricInput, scope: ClassScopeInput | None) -> dict[str, Any]:
    ours_values = _participant_sector_values(ours)
    ours_best = ours_values["personal_best_sector_ms"] or {}
    if scope is None:
        return {
            "last_sector_ms": ours_values["last_sector_ms"],
            "personal_best_sector_ms": ours_best or None,
            "class_best_sector_ms": None,
            "ideal_lap_ms": None,
            "potential_to_best_ms": None,
            "sector_delta_to_competitor_ms": None,
            "largest_sector_loss": None,
        }
    best_by_participant = {
        participant.id: _participant_sector_values(participant)["personal_best_sector_ms"] or {}
        for participant in scope.participants
    }
    active_keys = sorted({key for values in best_by_participant.values() for key in values})
    class_best = {
        key: min(values[key] for values in best_by_participant.values() if key in values)
        for key in active_keys
    }
    ideal = sum(ours_best[key] for key in active_keys) if active_keys and all(key in ours_best for key in active_keys) else None
    best_lap = _best_lap_ms(ours)
    potential = best_lap - ideal if best_lap is not None and ideal is not None and best_lap >= ideal else None
    deltas: dict[str, dict[str, int] | None] = {}
    losses: dict[str, dict[str, Any] | None] = {}
    for participant_id, values in best_by_participant.items():
        if participant_id == ours.id:
            continue
        delta = {key: ours_best[key] - values[key] for key in active_keys if key in ours_best and key in values}
        deltas[participant_id] = delta or None
        positive = [(key, value) for key, value in delta.items() if value > 0]
        if positive:
            key, value = max(positive, key=lambda item: item[1])
            losses[participant_id] = {
                "sector_index": key,
                "delta_ms": value,
                "competitor_id": participant_id,
            }
        else:
            losses[participant_id] = None
    return {
        "last_sector_ms": ours_values["last_sector_ms"],
        "personal_best_sector_ms": ours_best or None,
        "class_best_sector_ms": class_best or None,
        "ideal_lap_ms": ideal,
        "potential_to_best_ms": potential,
        "sector_delta_to_competitor_ms": deltas or None,
        "largest_sector_loss": losses or None,
    }


_HISTORY_KEYS_BY_SCOPE: dict[str, tuple[str, ...]] = {
    "session": (
        "metric_version",
        "channel_status",
        "track_flag",
        "track_flag_provider_code",
        "flag_phase_started_at_us",
        "session_elapsed_s",
        "session_remaining_s",
        "position_overall",
        "position_class",
        "completed_laps",
        "current_state",
        "class_leader_id",
        "class_ahead_id",
        "class_behind_id",
        "lap_delta_to_class_leader",
        "lap_delta_to_ahead",
        "lap_delta_to_behind",
        "class_ahead_completed_laps",
        "class_behind_completed_laps",
        "class_ahead_state",
        "class_behind_state",
        "gap_to_class_leader_ms",
        "gap_to_ahead_ms",
        "gap_to_behind_ms",
        "relation_intervals",
        "pace_3_ms",
        "pace_5_ms",
        "pace_10_ms",
        "consistency_10_ms",
        "class_pace_5_ms",
        "pace_rank_class",
        "position_change",
        "track_evolution_class_ms",
        "tyre_age_laps",
        "pits_completed",
        "pits_required",
        "pits_remaining",
        "session_remaining_s",
        "next_equal_pit_in_s",
        "stint_schedule_deviation_s",
        "closure_ahead",
        "closure_behind",
        "projected_gap_ms",
    ),
    "class": (
        "class_key",
        "class_best_lap_ms",
        "class_pace_5_ms",
        "class_leader_id",
        "class_order_basis",
        "total_completed_pits",
        "median_pit_lane_time_ms",
    ),
    "participant": (
        "participant_id",
        "position_overall",
        "position_class",
        "completed_laps",
        "current_state",
        "last_lap_ms",
        "best_lap_ms",
        "last_to_best_delta_ms",
        "source_gap_ms",
        "source_diff_ms",
        "pace_3_ms",
        "pace_5_ms",
        "pace_10_ms",
        "consistency_10_ms",
        "clean_lap_count",
        "observed_lap_count",
        "clean_lap_ratio",
        "stint_number",
        "stint_started_at_us",
        "stint_elapsed_s",
        "tyre_age_laps",
        "stint_pace_5_ms",
        "stint_best_lap_ms",
        "stint_consistency_10_ms",
        "stint_trend_ms_per_lap",
        "stint_cumulative_pace_change_ms",
        "stint_pace_delta_previous_ms",
        "stint_abrupt_deterioration",
        "stint_clean_lap_count",
        "clean_lap_ratio_current_stint",
        "pits_completed",
        "total_pit_lane_time_ms",
        "median_pit_lane_time_ms",
    ),
}


def _history_values(scope_kind: str, values: Mapping[str, Any]) -> dict[str, Any]:
    """Keep only time-series fields in the sparse chart store.

    Identity text, static class rosters and complete pit records belong in
    ``metric_current`` or their normalized source tables. Repeating them every
    chart point would turn a 24-hour race into a large series of duplicate JSON
    blobs without improving a graph.
    """
    # A missing point is semantically null for the history API. Omitting it
    # avoids repeating wide arrays of unavailable fields throughout a 24-hour
    # race without forward-filling or inventing a zero in the consumer.
    return {
        key: value
        for key in _HISTORY_KEYS_BY_SCOPE[scope_kind]
        if (value := values.get(key)) is not None
    }


def _p10_p90_values(
    participant: ParticipantMetricInput,
    pace: PaceMetrics,
    *,
    lap_samples: Sequence[LapSample] | None = None,
) -> dict[str, float | None]:
    """Return the tail distribution from the caller's normalized lap stream."""

    samples = lap_samples if lap_samples is not None else _lap_samples(participant)
    durations = [float(lap.duration_ms) for lap in samples if is_clean_lap(lap)][-10:]
    if len(durations) != 10:
        return {"p10_ms": None, "p90_ms": None, "spread_ms": None}
    ordered = sorted(durations)
    p10 = ordered[0]
    p90 = ordered[8]
    return {"p10_ms": p10, "p90_ms": p90, "spread_ms": pace.p10_p90_range_ms}


def _class_order(scope: ClassScopeInput) -> _ClassOrder | None:
    members = tuple(sorted((member for member in scope.participants if member.active), key=lambda member: member.id))
    if not members or any(member.state is None for member in members):
        return None
    class_positions = [_rank(member.state.position_class) for member in members]
    if all(position is not None for position in class_positions):
        positions = tuple(position for position in class_positions if position is not None)
        if len(set(positions)) != len(positions) or 1 not in positions:
            return None
        return _ClassOrder(
            tuple(sorted(members, key=lambda member: (member.state.position_class, member.id))),
            "PIC",
        )

    # POS is absolute order.  It must never be used to infer class neighbours:
    # without a complete unique PIC column, the whole class tactical order is
    # deliberately unavailable.
    return None


def _class_best_lap_ms(scope: ClassScopeInput) -> int | None:
    if _duration_ms(scope.class_best_lap_ms) is not None:
        return scope.class_best_lap_ms
    values = [_best_lap_ms(participant) for participant in scope.participants]
    available = [value for value in values if value is not None]
    return min(available) if available else None


_INTERVAL_STATUS_VALID = "VALID"
_INTERVAL_STATUS_SELF = "SELF"
_INTERVAL_STATUS_NO_TARGET = "NO_TARGET"
_INTERVAL_STATUS_NO_STATE = "NO_STATE"
_INTERVAL_STATUS_NON_RACING_STATE = "NON_RACING_STATE"
_INTERVAL_STATUS_LAPPED = "LAPPED"
_INTERVAL_STATUS_NO_SOURCE_FACT = "NO_SOURCE_FACT"
_INTERVAL_STATUS_INVALID_SOURCE_FACT = "INVALID_SOURCE_FACT"
_INTERVAL_STATUS_SOURCE_TARGET_MISMATCH = "SOURCE_TARGET_MISMATCH"
_INTERVAL_STATUS_SOURCE_POSITION_MISMATCH = "SOURCE_POSITION_MISMATCH"
_INTERVAL_STATUS_SOURCE_STATE_MISMATCH = "SOURCE_STATE_MISMATCH"
_INTERVAL_STATUS_SOURCE_LAPPED = "SOURCE_LAPPED"
_INTERVAL_STATUS_STALE_LAP_CONTEXT = "STALE_LAP_CONTEXT"
_INTERVAL_STATUS_NO_COHERENT_SOURCE_PAIR = "NO_COHERENT_SOURCE_PAIR"

_GAP_FACT_RELATIONS = frozenset({"OVERALL_LEADER", "GAP_TO_OVERALL_LEADER"})
_DIFF_FACT_RELATIONS = frozenset({"OVERALL_AHEAD", "DIFF_TO_OVERALL_AHEAD"})
_INTERVAL_RACING_STATE_KINDS = frozenset({"ON_TRACK", "OUT_LAP"})


def _source_fact_field(fact: Any, key: str) -> Any:
    """Read an additive interval-provenance field without coupling storage shape.

    The normalizer will provide a typed fact object, while fixture and replay
    callers may use mappings during the migration.  This evaluator must remain
    fail-closed when either representation is incomplete.
    """

    aliases = {
        "field_kind": ("interval_kind",),
        "value_ms": ("interval_ms",),
        "cell_observation_id": ("source_cell_observation_id",),
        "subject_position_overall": ("source_position_overall",),
        "subject_state_kind": ("source_state_kind",),
        "subject_laps": ("source_laps",),
    }
    names = (key, *aliases.get(key, ()))
    for name in names:
        value = fact.get(name) if isinstance(fact, Mapping) else getattr(fact, name, None)
        if value is not None:
            return value
    return None


def _source_fact_text(fact: Any, key: str) -> str | None:
    value = _source_fact_field(fact, key)
    return value.strip() if isinstance(value, str) and value.strip() else None


def _source_fact_int(fact: Any, key: str, *, minimum: int | None = None) -> int | None:
    value = _source_fact_field(fact, key)
    return value if _is_int(value, minimum=minimum) else None


def _source_interval_fact(state: Any, field_kind: str) -> Any | None:
    """Return a field-specific fact, never the row's generic source pointer."""

    if state is None:
        return None
    attribute = "gap_interval_fact" if field_kind == "GAP" else "diff_interval_fact"
    return getattr(state, attribute, None)


def _interval_fact_pointer(fact: Any) -> IntervalFactPointer | None:
    """Return only immutable GAP/DIFF provenance, not its derived value.

    A result row can be re-written by STATE, LAST, or driver changes while its
    cached interval cell still points to an older source observation.  The
    boundary cursor therefore tracks the fact identity and source ordering,
    rather than the row timestamp or the displayed number.
    """

    if fact is None:
        return None
    pointer: IntervalFactPointer = (
        _source_fact_int(fact, "id", minimum=1),
        _source_fact_int(fact, "cell_observation_id", minimum=1),
        _source_fact_int(fact, "source_message_id", minimum=1),
        _source_fact_text(fact, "source_key"),
        _source_fact_int(fact, "source_change_ordinal", minimum=0),
        _source_fact_text(fact, "field_kind"),
    )
    return pointer if any(value is not None for value in pointer) else None


def _source_fact_payload(fact: Any) -> dict[str, Any] | None:
    """Produce the public, compact provenance carried by a tactical relation."""

    if fact is None:
        return None
    payload = {
        "id": _source_fact_field(fact, "id"),
        "field_kind": _source_fact_text(fact, "field_kind"),
        "raw_value": _source_fact_text(fact, "raw_value"),
        "value_ms": _source_fact_int(fact, "value_ms", minimum=0),
        "value_kind": _source_fact_text(fact, "value_kind"),
        "cell_observation_id": _source_fact_int(fact, "cell_observation_id", minimum=1),
        "source_message_id": _source_fact_int(fact, "source_message_id", minimum=1),
        "source_key": _source_fact_text(fact, "source_key"),
        "source_change_ordinal": _source_fact_int(fact, "source_change_ordinal", minimum=0),
        "observed_at_us": _source_fact_int(fact, "observed_at_us", minimum=0),
        "source_handle": _source_fact_text(fact, "source_handle"),
        "observation_kind": _source_fact_text(fact, "observation_kind"),
        "subject_position_overall": _source_fact_int(fact, "subject_position_overall", minimum=1),
        "subject_state_kind": _source_fact_text(fact, "subject_state_kind"),
        "subject_laps": _source_fact_int(fact, "subject_laps", minimum=0),
        "target_participant_id": _source_fact_text(fact, "target_participant_id"),
        "target_position_overall": _source_fact_int(fact, "target_position_overall", minimum=1),
        "target_state_kind": _source_fact_text(fact, "target_state_kind"),
        "target_laps": _source_fact_int(fact, "target_laps", minimum=0),
        "relation_kind": _source_fact_text(fact, "relation_kind"),
    }
    return payload


def _participant_state_kind(participant: ParticipantMetricInput | None) -> str | None:
    state = participant.state if participant is not None else None
    return state.state_kind if state is not None and isinstance(state.state_kind, str) else None


def _relation_result(
    ours: ParticipantMetricInput,
    target: ParticipantMetricInput | None,
    *,
    status: str,
    value_ms: int | None = None,
    relation_kind: str | None = None,
    source_facts: Sequence[dict[str, Any] | None] = (),
    evaluated_at_us: int | None = None,
) -> dict[str, Any]:
    """Return one uniform relation object; scalar consumers use only VALID/SELF."""

    facts = [fact for fact in source_facts if fact is not None]
    observed = [fact["observed_at_us"] for fact in facts if _is_int(fact.get("observed_at_us"), minimum=0)]
    latest_at_us = max(observed) if observed else None
    age_ms = (
        max(0, evaluated_at_us - latest_at_us) // 1_000
        if _is_int(evaluated_at_us, minimum=0) and latest_at_us is not None
        else None
    )
    return {
        "target_participant_id": target.id if target is not None else None,
        "status": status,
        "value_ms": value_ms if status in {_INTERVAL_STATUS_VALID, _INTERVAL_STATUS_SELF} else None,
        "relation_kind": relation_kind,
        "source_facts": facts,
        "source_observed_at_us": latest_at_us,
        "source_age_ms": age_ms,
        "ours_state_kind": _participant_state_kind(ours),
        "target_state_kind": _participant_state_kind(target),
        "ours_laps": _source_lap_count(ours),
        "target_laps": _source_lap_count(target) if target is not None else None,
    }


def _relation_value_ms(relation: Mapping[str, Any]) -> int | None:
    value = relation.get("value_ms")
    return value if relation.get("status") in {_INTERVAL_STATUS_VALID, _INTERVAL_STATUS_SELF} and _is_int(value, minimum=0) else None


def _fact_context_status(
    fact: Any,
    *,
    holder: ParticipantMetricInput,
    target: ParticipantMetricInput,
    field_kind: str,
    allowed_relations: frozenset[str],
) -> tuple[str, dict[str, Any] | None]:
    """Validate one source fact against both its source and current relation.

    A GAP/DIFF number alone is deliberately insufficient: the source target,
    positions and states must still describe the exact pair being displayed.
    """

    payload = _source_fact_payload(fact)
    if payload is None:
        return _INTERVAL_STATUS_NO_SOURCE_FACT, None
    if (
        payload["field_kind"] != field_kind
        or payload["value_kind"] != "TIME"
        or payload["value_ms"] is None
        or payload["cell_observation_id"] is None
        or payload["source_message_id"] is None
        or payload["source_key"] is None
        or payload["source_change_ordinal"] is None
        or payload["observed_at_us"] is None
        or payload["source_handle"] is None
        or payload["observation_kind"] is None
    ):
        return _INTERVAL_STATUS_INVALID_SOURCE_FACT, payload
    if payload["relation_kind"] not in allowed_relations or payload["target_participant_id"] != target.id:
        return _INTERVAL_STATUS_SOURCE_TARGET_MISMATCH, payload

    holder_state = holder.state
    target_state = target.state
    if holder_state is None or target_state is None:
        return _INTERVAL_STATUS_NO_STATE, payload
    if (
        holder_state.state_kind not in _INTERVAL_RACING_STATE_KINDS
        or target_state.state_kind not in _INTERVAL_RACING_STATE_KINDS
    ):
        return _INTERVAL_STATUS_NON_RACING_STATE, payload
    holder_position = _rank(holder_state.position_overall)
    target_position = _rank(target_state.position_overall)
    if (
        holder_position is None
        or target_position is None
        or payload["subject_position_overall"] != holder_position
        or payload["target_position_overall"] != target_position
    ):
        return _INTERVAL_STATUS_SOURCE_POSITION_MISMATCH, payload
    # The interpretation comes from the provider column, not merely the
    # numeric duration.  GAP is explicitly to P1; DIFF is explicitly to the
    # immediately preceding overall position.
    if (
        (field_kind == "GAP" and target_position != 1)
        or (field_kind == "DIFF" and holder_position != target_position + 1)
    ):
        return _INTERVAL_STATUS_SOURCE_POSITION_MISMATCH, payload
    if (
        payload["subject_state_kind"] not in _INTERVAL_RACING_STATE_KINDS
        or payload["target_state_kind"] not in _INTERVAL_RACING_STATE_KINDS
    ):
        return _INTERVAL_STATUS_SOURCE_STATE_MISMATCH, payload

    holder_laps = _source_lap_count(holder)
    target_laps = _source_lap_count(target)
    if holder_laps is not None and target_laps is not None and holder_laps != target_laps:
        return _INTERVAL_STATUS_LAPPED, payload
    if payload["subject_laps"] is not None and payload["target_laps"] is not None and payload["subject_laps"] != payload["target_laps"]:
        return _INTERVAL_STATUS_SOURCE_LAPPED, payload
    if (
        (payload["subject_laps"] is not None and holder_laps is not None and payload["subject_laps"] != holder_laps)
        or (payload["target_laps"] is not None and target_laps is not None and payload["target_laps"] != target_laps)
    ):
        return _INTERVAL_STATUS_STALE_LAP_CONTEXT, payload
    return _INTERVAL_STATUS_VALID, payload


def _same_source_message(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """Only one atomic table message may support a derived GAP-pair distance."""

    return (
        left.get("source_message_id") == right.get("source_message_id")
        and left.get("source_key") == right.get("source_key")
        and left.get("observed_at_us") == right.get("observed_at_us")
        and left.get("source_handle") == right.get("source_handle")
    )


def _relative_interval(
    ours: ParticipantMetricInput,
    target: ParticipantMetricInput | None,
    *,
    participants: Mapping[str, ParticipantMetricInput] | None = None,
    evaluated_at_us: int | None = None,
) -> dict[str, Any]:
    """Resolve one tactical interval solely from source-proven GAP/DIFF facts.

    Direct GAP binds its row to the source's absolute leader; direct DIFF binds
    its row to the immediately preceding absolute position.  A GAP pair is
    accepted only when both cells are from the same atomic result message and
    share the same current overall leader.  No lap-time calculation is used.
    """

    if target is None:
        return _relation_result(ours, None, status=_INTERVAL_STATUS_NO_TARGET, evaluated_at_us=evaluated_at_us)
    if target.id == ours.id:
        return _relation_result(
            ours,
            target,
            status=_INTERVAL_STATUS_SELF,
            value_ms=0,
            relation_kind="SELF",
            evaluated_at_us=evaluated_at_us,
        )

    ours_state = ours.state
    target_state = target.state
    if ours_state is None or target_state is None:
        return _relation_result(ours, target, status=_INTERVAL_STATUS_NO_STATE, evaluated_at_us=evaluated_at_us)
    if (
        ours_state.state_kind not in _INTERVAL_RACING_STATE_KINDS
        or target_state.state_kind not in _INTERVAL_RACING_STATE_KINDS
    ):
        return _relation_result(
            ours,
            target,
            status=_INTERVAL_STATUS_NON_RACING_STATE,
            source_facts=(
                _source_fact_payload(_source_interval_fact(ours_state, "GAP")),
                _source_fact_payload(_source_interval_fact(ours_state, "DIFF")),
                _source_fact_payload(_source_interval_fact(target_state, "GAP")),
                _source_fact_payload(_source_interval_fact(target_state, "DIFF")),
            ),
            evaluated_at_us=evaluated_at_us,
        )
    ours_laps = _source_lap_count(ours)
    target_laps = _source_lap_count(target)
    if ours_laps is not None and target_laps is not None and ours_laps != target_laps:
        return _relation_result(ours, target, status=_INTERVAL_STATUS_LAPPED, evaluated_at_us=evaluated_at_us)

    candidates: list[tuple[str, dict[str, Any] | None]] = []

    def direct(
        holder: ParticipantMetricInput,
        direct_target: ParticipantMetricInput,
        field_kind: str,
        allowed_relations: frozenset[str],
        relation_kind: str,
    ) -> dict[str, Any] | None:
        fact = _source_interval_fact(holder.state, field_kind)
        status, payload = _fact_context_status(
            fact,
            holder=holder,
            target=direct_target,
            field_kind=field_kind,
            allowed_relations=allowed_relations,
        )
        candidates.append((status, payload))
        if status != _INTERVAL_STATUS_VALID or payload is None:
            return None
        return _relation_result(
            ours,
            target,
            status=_INTERVAL_STATUS_VALID,
            value_ms=payload["value_ms"],
            relation_kind=relation_kind,
            source_facts=(payload,),
            evaluated_at_us=evaluated_at_us,
        )

    # A direct GAP is valid only if the stored leader target is the tactical
    # target. The reverse form covers our car being the overall leader.
    result = direct(ours, target, "GAP", _GAP_FACT_RELATIONS, "GAP_TO_OVERALL_LEADER")
    if result is not None:
        return result
    result = direct(target, ours, "GAP", _GAP_FACT_RELATIONS, "GAP_TO_OVERALL_LEADER")
    if result is not None:
        return result
    result = direct(ours, target, "DIFF", _DIFF_FACT_RELATIONS, "DIFF_TO_OVERALL_AHEAD")
    if result is not None:
        return result
    result = direct(target, ours, "DIFF", _DIFF_FACT_RELATIONS, "DIFF_TO_OVERALL_AHEAD")
    if result is not None:
        return result

    # Two GAP cells may be compared only when they were emitted atomically and
    # point to the same actual leader. Otherwise subtraction is a synthetic
    # relation between unrelated moments and must be rejected.
    ours_fact = _source_interval_fact(ours_state, "GAP")
    target_fact = _source_interval_fact(target_state, "GAP")
    ours_payload = _source_fact_payload(ours_fact)
    target_payload = _source_fact_payload(target_fact)
    common_id = (
        ours_payload.get("target_participant_id")
        if ours_payload is not None and target_payload is not None
        and ours_payload.get("target_participant_id") == target_payload.get("target_participant_id")
        else None
    )
    roster = participants or {}
    common = roster.get(common_id) if isinstance(common_id, str) else None
    if common is not None and common.id not in {ours.id, target.id}:
        ours_status, valid_ours = _fact_context_status(
            ours_fact,
            holder=ours,
            target=common,
            field_kind="GAP",
            allowed_relations=_GAP_FACT_RELATIONS,
        )
        target_status, valid_target = _fact_context_status(
            target_fact,
            holder=target,
            target=common,
            field_kind="GAP",
            allowed_relations=_GAP_FACT_RELATIONS,
        )
        candidates.extend(((ours_status, valid_ours), (target_status, valid_target)))
        if (
            ours_status == _INTERVAL_STATUS_VALID
            and target_status == _INTERVAL_STATUS_VALID
            and valid_ours is not None
            and valid_target is not None
            and _same_source_message(valid_ours, valid_target)
            and (
                valid_ours["subject_laps"] is None
                or valid_target["subject_laps"] is None
                or valid_ours["subject_laps"] == valid_target["subject_laps"]
            )
        ):
            return _relation_result(
                ours,
                target,
                status=_INTERVAL_STATUS_VALID,
                value_ms=abs(valid_ours["value_ms"] - valid_target["value_ms"]),
                relation_kind="GAP_PAIR_COMMON_OVERALL_LEADER",
                source_facts=(valid_ours, valid_target),
                evaluated_at_us=evaluated_at_us,
            )
        if ours_status == _INTERVAL_STATUS_VALID and target_status == _INTERVAL_STATUS_VALID:
            return _relation_result(
                ours,
                target,
                status=_INTERVAL_STATUS_NO_COHERENT_SOURCE_PAIR,
                source_facts=(valid_ours, valid_target),
                evaluated_at_us=evaluated_at_us,
            )

    non_missing = next(
        (status for status, _ in candidates if status not in {_INTERVAL_STATUS_NO_SOURCE_FACT, _INTERVAL_STATUS_VALID}),
        _INTERVAL_STATUS_NO_SOURCE_FACT,
    )
    return _relation_result(
        ours,
        target,
        status=non_missing,
        source_facts=tuple(payload for _, payload in candidates),
        evaluated_at_us=evaluated_at_us,
    )


def _relative_gap_ms(
    ours: ParticipantMetricInput,
    target: ParticipantMetricInput,
    *,
    participants: Mapping[str, ParticipantMetricInput] | None = None,
    evaluated_at_us: int | None = None,
) -> int | None:
    """Compatibility scalar, deliberately derived only from a valid relation."""

    return _relation_value_ms(
        _relative_interval(ours, target, participants=participants, evaluated_at_us=evaluated_at_us)
    )


def _lap_delta(ours: ParticipantMetricInput, target: ParticipantMetricInput | None) -> int | None:
    if target is None:
        return None
    ours_laps = _source_lap_count(ours)
    target_laps = _source_lap_count(target)
    return ours_laps - target_laps if ours_laps is not None and target_laps is not None else None


def _channel_status(heat: HeatMetricInput) -> str:
    if heat.session.lifecycle != "active" or heat.open_ingest_gap is not None:
        return CHANNEL_OFFLINE
    # A Finish flag is a source-derived terminal state. The ingest connection
    # may still deliver aggregate snapshots, but no live tactical state exists.
    if heat.current_flag is not None and heat.current_flag.flag == "FINISH":
        return CHANNEL_OFFLINE
    # The integration runner evaluates a durable frame before it writes that
    # frame's state tick.  An active, gap-free frame is therefore live even on
    # the first evaluation of a connection.
    if heat.latest_tick is None:
        return CHANNEL_LIVE
    return CHANNEL_STALE if heat.latest_tick.freshness_ms > FRESHNESS_STALE_MS else CHANNEL_LIVE


def _flag_start_at_us(heat: HeatMetricInput) -> int | None:
    flag = heat.current_flag
    if flag is None:
        return None
    return flag.calibrated_started_at_us if flag.calibrated_started_at_us is not None else flag.started_at_us


def _session_values(heat: HeatMetricInput, *, observed_at_us: int) -> dict[str, Any]:
    flag = heat.current_flag
    session_start = (
        heat.provider_started_at_us
        if heat.provider_started_at_us is not None
        else heat.session.started_at_us
    )
    elapsed = _elapsed_s(observed_at_us, session_start)
    remaining = None
    if heat.session.mode == "race" and heat.session.race_duration_s is not None and elapsed is not None:
        remaining = max(0.0, heat.session.race_duration_s - elapsed)
    statistics = heat.statistics
    return {
        "metric_version": METRIC_ENGINE_VERSION,
        "channel_status": _channel_status(heat),
        "track_flag": flag.flag if flag is not None else None,
        "track_flag_provider_code": flag.provider_code if flag is not None else None,
        "track_flag_provider_label": flag.provider_label if flag is not None else None,
        "flag_phase_started_at_us": _flag_start_at_us(heat),
        "flag_phase_elapsed_s": _elapsed_s(observed_at_us, _flag_start_at_us(heat)),
        "heat_name": (statistics.heat_name if statistics is not None and statistics.heat_name else heat.external_name),
        "session_mode": heat.session.mode,
        "session_lifecycle": heat.session.lifecycle,
        "session_elapsed_s": elapsed,
        "session_remaining_s": remaining,
        "statistics": {
            "participants_started": statistics.participants_started if statistics is not None else None,
            "participants_classified": statistics.participants_classified if statistics is not None else None,
            "participants_not_classified": statistics.participants_not_classified if statistics is not None else None,
            "participants_on_track": statistics.participants_on_track if statistics is not None else None,
            "participants_in_pit_zone": statistics.participants_in_pit_zone if statistics is not None else None,
            "participants_in_tank_zone": statistics.participants_in_tank_zone if statistics is not None else None,
            "total_laps": statistics.total_laps if statistics is not None else None,
            "total_pitstops": statistics.total_pitstops if statistics is not None else None,
            "safety_car_count": statistics.safety_car_count if statistics is not None else None,
            "full_course_yellow_count": statistics.full_course_yellow_count if statistics is not None else None,
            "code_60_count": statistics.code_60_count if statistics is not None else None,
        },
    }


def _identity(participant: ParticipantMetricInput) -> dict[str, str | None]:
    state = participant.state
    return {
        "participant_id": participant.id,
        "start_number": participant.start_number,
        "team_name": participant.team_name,
        "driver_name": state.current_driver_name if state is not None else None,
        "car_name": participant.car_name,
        "class_name": participant.class_name,
    }


def _empty_race_obligation_values() -> dict[str, Any]:
    return {
        "pits_required": None,
        "pits_remaining": None,
        "initial_equal_stint_target_s": None,
        "remaining_equal_stint_s": None,
        "stop_load_per_hour": None,
        "next_equal_pit_target_elapsed_s": None,
        "next_equal_pit_in_s": None,
        "stint_schedule_deviation_s": None,
        "expected_remaining_laps_range": None,
    }


def _race_obligation_values(
    heat: HeatMetricInput,
    ours: ParticipantMetricInput,
    *,
    session_elapsed_s: float | None,
    pace: PaceMetrics,
) -> dict[str, Any]:
    empty = _empty_race_obligation_values()
    if heat.session.mode != "race":
        return empty
    obligations = calculate_pit_obligations(
        RacePlan(heat.session.race_duration_s, heat.session.required_pits),
        _completed_pits(ours),
        elapsed_s=session_elapsed_s,
    )
    if obligations is None:
        return empty
    expected_laps = None
    if pace.pace5_ms is not None and pace.consistency10_ms is not None:
        remaining_ms = obligations.remaining_time_s * 1_000.0
        slow_pace = pace.pace5_ms + pace.consistency10_ms
        fast_pace = max(1.0, pace.pace5_ms - pace.consistency10_ms)
        expected_laps = {
            "min_laps": floor(remaining_ms / slow_pace),
            "max_laps": ceil(remaining_ms / fast_pace),
        }
    return {
        "pits_required": obligations.required_pits,
        "pits_remaining": obligations.remaining_pits,
        "initial_equal_stint_target_s": obligations.initial_equal_stint_target_s,
        "remaining_equal_stint_s": obligations.remaining_equal_stint_target_s,
        "stop_load_per_hour": obligations.stop_load_per_hour,
        "next_equal_pit_target_elapsed_s": obligations.next_equal_pit_elapsed_s,
        "next_equal_pit_in_s": obligations.next_equal_pit_in_s,
        "stint_schedule_deviation_s": obligations.schedule_deviation_s,
        "expected_remaining_laps_range": expected_laps,
    }


def _mapping_int(values: Mapping[str, Any], key: str) -> int | None:
    value = values.get(key)
    return value if _is_int(value) else None


def _gap_sample_from_values(
    observed_at_us: int,
    values: Mapping[str, Any],
    *,
    relation: str,
) -> GapSample:
    suffix = "ahead" if relation == GAP_RELATION_AHEAD else "behind"
    relation_key = f"class_{suffix}"
    relation_values = values.get("relation_intervals")
    interval = (
        relation_values.get(relation_key)
        if isinstance(relation_values, Mapping) and isinstance(relation_values.get(relation_key), Mapping)
        else None
    )
    # New materializations carry the full source relation.  A baseline table
    # snapshot is useful for a KPI, but it is not a new gap measurement and
    # therefore cannot extend a closure/catch trend. Legacy history has no
    # relation object and remains readable until its source heat is rebuilt.
    if interval is not None:
        source_facts = interval.get("source_facts")
        has_delta = (
            isinstance(source_facts, Sequence)
            and not isinstance(source_facts, (str, bytes, bytearray))
            and bool(source_facts)
            and all(
                isinstance(fact, Mapping) and fact.get("observation_kind") == "DELTA"
                for fact in source_facts
            )
        )
        gap_ms = _relation_value_ms(interval) if has_delta else None
        source_at_us = _mapping_int(interval, "source_observed_at_us")
        sample_at_us = source_at_us if source_at_us is not None else observed_at_us
        target_id = interval.get("target_participant_id") if isinstance(interval.get("target_participant_id"), str) else None
        ours_state = interval.get("ours_state_kind") if isinstance(interval.get("ours_state_kind"), str) else None
        target_state = interval.get("target_state_kind") if isinstance(interval.get("target_state_kind"), str) else None
        ours_laps = _mapping_int(interval, "ours_laps")
        target_laps = _mapping_int(interval, "target_laps")
    else:
        # A pre-provenance history point can still be inspected as raw
        # archive state, but it must never turn its flattened scalar into a
        # tactical closure, catch or forecast input.
        gap_ms = None
        sample_at_us = observed_at_us
        target_id = None
        ours_state = None
        target_state = None
        ours_laps = None
        target_laps = None
    return GapSample(
        target_participant_id=target_id,
        observed_at_us=sample_at_us,
        gap_ms=gap_ms,
        our_lap_number=ours_laps,
        target_lap_number=target_laps,
        flag_kind=values.get("track_flag") if isinstance(values.get("track_flag"), str) else None,
        our_state_kind=ours_state,
        target_state_kind=target_state,
        has_feed_gap=values.get("channel_status") != CHANNEL_LIVE,
        # ``gap_ms`` can only be non-null after _relative_gap_ms accepted an
        # explicit provider TIME GAP/DIFF.  When LAPS is absent, it permits a
        # time-based closure trend but intentionally never invents a per-lap
        # rate or a lap-based catch projection.
        source_time_interval=gap_ms is not None,
    )


def _trend_record(trend: Any) -> dict[str, Any] | None:
    if trend is None:
        return None
    return {
        "window_s": trend.window_s,
        "covered_s": trend.covered_s,
        "started_gap_ms": trend.started_gap_ms,
        "ended_gap_ms": trend.ended_gap_ms,
        "closure_ms_per_min": trend.closure_ms_per_min,
        "closure_ms_per_lap": trend.closure_ms_per_lap,
        "direction": trend.direction,
        "label": GAP_DIRECTION_LABEL_RU[trend.direction],
    }


def _catch_record(catch: Any) -> dict[str, Any] | None:
    if catch is None:
        return None
    return {
        "min_laps": catch.minimum_laps,
        "max_laps": catch.maximum_laps,
        "min_time_ms": catch.minimum_time_ms,
        "max_time_ms": catch.maximum_time_ms,
        "source_windows_s": list(catch.source_windows_s),
    }


def _required_pace_ms(
    *,
    relation: str,
    ours_pace_ms: float | None,
    target_pace_ms: float | None,
    gap_ms: int | None,
    remaining_s: float | None,
) -> float | None:
    if (
        ours_pace_ms is None
        or target_pace_ms is None
        or gap_ms is None
        or remaining_s is None
        or remaining_s <= 0
    ):
        return None
    horizon_laps = remaining_s * 1_000.0 / ours_pace_ms
    if horizon_laps <= 0:
        return None
    if relation == GAP_RELATION_AHEAD:
        return target_pace_ms - gap_ms / horizon_laps
    return target_pace_ms + gap_ms / horizon_laps


def _projected_gaps(
    *,
    relation: str,
    gap_ms: int | None,
    trend: Any,
) -> dict[str, float | None] | None:
    if gap_ms is None or trend is None or trend.closure_ms_per_lap is None:
        return None
    sign = -1.0 if relation == GAP_RELATION_AHEAD else 1.0
    return {
        "5": gap_ms + sign * trend.closure_ms_per_lap * 5,
        "10": gap_ms + sign * trend.closure_ms_per_lap * 10,
    }


def _class_density(
    ours: ParticipantMetricInput,
    scope: ClassScopeInput | None,
    *,
    observed_at_us: int,
) -> dict[str, int] | None:
    if scope is None:
        return None
    participants = {participant.id: participant for participant in scope.participants}
    gaps = [
        _relative_gap_ms(
            ours,
            participant,
            participants=participants,
            evaluated_at_us=observed_at_us,
        )
        for participant in scope.participants
        if participant.id != ours.id
    ]
    known = [gap for gap in gaps if gap is not None]
    if not known:
        return None
    return {
        "5000": sum(gap <= 5_000 for gap in known),
        "10000": sum(gap <= 10_000 for gap in known),
        "30000": sum(gap <= 30_000 for gap in known),
    }


def _pit_anomaly(participant: ParticipantMetricInput) -> dict[str, float | int] | None:
    durations = [
        record["pit_lane_duration_ms"]
        for record in _pit_history(participant)
        if record["pit_lane_duration_ms"] is not None
    ]
    if len(durations) < 4:
        return None
    candidate = durations[-1]
    baseline = durations[:-1]
    median = _median_non_negative(baseline)
    if median is None:
        return None
    deviations = [abs(duration - median) for duration in baseline]
    mad = _median_non_negative([int(deviation) for deviation in deviations])
    if mad is None or abs(candidate - median) <= 3 * mad:
        return None
    return {
        "pit_lane_duration_ms": candidate,
        "median_ms": median,
        "mad_ms": mad,
        "excess_ms": abs(candidate - median) - 3 * mad,
    }


def _competitor_stint_and_pit_values(
    ours: ParticipantMetricInput,
    scope: ClassScopeInput | None,
    observed_at_us: int,
) -> tuple[dict[str, dict[str, int | float | None]] | None, dict[str, int | None] | None]:
    if scope is None:
        return None, None
    ours_pits = len(_completed_pits(ours))
    stint_state: dict[str, dict[str, int | float | None]] = {}
    pit_offset: dict[str, int | None] = {}
    for competitor in scope.participants:
        if competitor.id == ours.id:
            continue
        stint = competitor.active_tire_stint
        completed = len(_completed_pits(competitor)) if competitor.tire_stints or competitor.state is not None else None
        stint_state[competitor.id] = {
            "stint_number": stint.stint_number if stint is not None else None,
            "stint_elapsed_s": _elapsed_s(observed_at_us, stint.started_at_us) if stint is not None else None,
            "tyre_age_laps": stint.completed_laps if stint is not None else None,
            "pits_completed": completed,
        }
        pit_offset[competitor.id] = completed - ours_pits if isinstance(completed, int) else None
    return stint_state, pit_offset


def _latest_history_at_or_before(
    history: Sequence[MetricHistoryPoint], cutoff_at_us: int
) -> MetricHistoryPoint | None:
    eligible = [point for point in history if point.observed_at_us <= cutoff_at_us]
    return eligible[-1] if eligible else None


def _track_evolution_ms(
    *,
    history: Sequence[MetricHistoryPoint],
    observed_at_us: int,
    current_class_pace_ms: float | None,
) -> float | None:
    if current_class_pace_ms is None:
        return None
    baseline = _latest_history_at_or_before(history, observed_at_us - 600_000_000)
    if baseline is None or not isinstance(baseline.values.get("class_pace_5_ms"), (int, float)):
        return None
    relevant = [point for point in history if point.observed_at_us >= baseline.observed_at_us]
    if any(point.values.get("channel_status") != CHANNEL_LIVE for point in relevant):
        return None
    return current_class_pace_ms - float(baseline.values["class_pace_5_ms"])


def _battle_values(
    heat: HeatMetricInput,
    *,
    observed_at_us: int,
    session_values: Mapping[str, Any],
    tactical_values: Mapping[str, Any],
    pace_by_participant: Mapping[str, PaceMetrics],
    history: Sequence[MetricHistoryPoint],
) -> dict[str, Any]:
    empty = {
        "closure_ahead": None,
        "closure_behind": None,
        "catch_range": {"ahead": None, "behind": None},
        "required_pace_to_catch_ahead_ms": None,
        "required_pace_to_defend_behind_ms": None,
        "projected_gap_ms": {"ahead": None, "behind": None},
        "class_density": None,
        "position_change": None,
        "track_evolution_class_ms": None,
        "pit_lane_anomaly": None,
        "competitor_stint_state": None,
        "pit_offset": None,
    }
    ours = heat.our_participant
    scope = heat.current_class_scope
    if ours is None or scope is None or heat.session.identity_state != "resolved":
        return empty
    current_values = {**session_values, **tactical_values}
    ours_pace = pace_by_participant.get(ours.id)
    if ours_pace is None:
        return empty
    samples_by_relation: dict[str, tuple[GapSample, ...]] = {}
    trends_by_relation: dict[str, dict[int, Any]] = {}
    for relation in (GAP_RELATION_AHEAD, GAP_RELATION_BEHIND):
        samples = tuple(
            _gap_sample_from_values(point.observed_at_us, point.values, relation=relation)
            for point in history
        ) + (_gap_sample_from_values(observed_at_us, current_values, relation=relation),)
        samples_by_relation[relation] = samples
        trends_by_relation[relation] = calculate_gap_trends(samples, relation=relation, windows_s=GAP_WINDOWS_S)
    ahead_trends = trends_by_relation[GAP_RELATION_AHEAD]
    behind_trends = trends_by_relation[GAP_RELATION_BEHIND]
    current_ahead = samples_by_relation[GAP_RELATION_AHEAD][-1]
    current_behind = samples_by_relation[GAP_RELATION_BEHIND][-1]
    ahead_target = next(
        (participant for participant in scope.participants if participant.id == tactical_values.get("class_ahead_id")),
        None,
    )
    behind_target = next(
        (participant for participant in scope.participants if participant.id == tactical_values.get("class_behind_id")),
        None,
    )
    ahead_pace = pace_by_participant.get(ahead_target.id).pace5_ms if ahead_target is not None else None
    behind_pace = pace_by_participant.get(behind_target.id).pace5_ms if behind_target is not None else None
    prior_position = next(
        (
            _mapping_int(point.values, "position_overall")
            for point in reversed(history)
            if _mapping_int(point.values, "position_overall") is not None
        ),
        None,
    )
    current_position = _mapping_int(tactical_values, "position_overall")
    competitor_stint_state, pit_offset = _competitor_stint_and_pit_values(ours, scope, observed_at_us)
    return {
        "closure_ahead": {
            str(window): _trend_record(ahead_trends[window]) for window in GAP_WINDOWS_S
        },
        "closure_behind": {
            str(window): _trend_record(behind_trends[window]) for window in GAP_WINDOWS_S
        },
        "catch_range": {
            "ahead": _catch_record(
                calculate_catch_range(
                    current_ahead,
                    tuple(ahead_trends.values()),
                    relation=GAP_RELATION_AHEAD,
                    reference_pace_ms=ours_pace.pace5_ms,
                )
            ),
            "behind": _catch_record(
                calculate_catch_range(
                    current_behind,
                    tuple(behind_trends.values()),
                    relation=GAP_RELATION_BEHIND,
                    reference_pace_ms=ours_pace.pace5_ms,
                )
            ),
        },
        "required_pace_to_catch_ahead_ms": _required_pace_ms(
            relation=GAP_RELATION_AHEAD,
            ours_pace_ms=ours_pace.pace5_ms,
            target_pace_ms=ahead_pace,
            gap_ms=current_ahead.gap_ms,
            remaining_s=session_values.get("session_remaining_s"),
        ),
        "required_pace_to_defend_behind_ms": _required_pace_ms(
            relation=GAP_RELATION_BEHIND,
            ours_pace_ms=ours_pace.pace5_ms,
            target_pace_ms=behind_pace,
            gap_ms=current_behind.gap_ms,
            remaining_s=session_values.get("session_remaining_s"),
        ),
        "projected_gap_ms": {
            "ahead": _projected_gaps(
                relation=GAP_RELATION_AHEAD,
                gap_ms=current_ahead.gap_ms,
                trend=ahead_trends[60],
            ),
            "behind": _projected_gaps(
                relation=GAP_RELATION_BEHIND,
                gap_ms=current_behind.gap_ms,
                trend=behind_trends[60],
            ),
        },
        "class_density": _class_density(ours, scope, observed_at_us=observed_at_us),
        "position_change": prior_position - current_position
        if prior_position is not None and current_position is not None
        else None,
        "track_evolution_class_ms": _track_evolution_ms(
            history=history,
            observed_at_us=observed_at_us,
            current_class_pace_ms=tactical_values.get("class_pace_5_ms"),
        ),
        "pit_lane_anomaly": _pit_anomaly(ours),
        "competitor_stint_state": competitor_stint_state,
        "pit_offset": pit_offset,
    }


def _class_pit_durations(scope: ClassScopeInput | None) -> list[int]:
    if scope is None:
        return []
    return [
        int(record["pit_lane_duration_ms"])
        for participant in scope.participants
        for record in _pit_history(participant)
        if record["pit_lane_duration_ms"] is not None
    ]


def _event_alert(
    key: str,
    severity: str,
    observed_at_us: int,
    fact: Mapping[str, Any],
    numeric_consequence: int | float | None,
) -> dict[str, Any]:
    return {
        "key": key,
        "severity": severity,
        "at_us": observed_at_us,
        "fact": dict(fact),
        "numeric_consequence": numeric_consequence,
    }


def _metric_alerts(
    heat: HeatMetricInput,
    *,
    previous: HeatMetricInput | MetricEngineResult | MetricBoundaryState | None,
    observed_at_us: int,
    session_values: Mapping[str, Any],
    tactical_values: Mapping[str, Any],
    battle_values: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Emit contract alerts only on fact transitions or a newly active crossing."""
    prior = _previous_boundary_state(previous)
    now = build_boundary_state(heat)
    ours = heat.our_participant
    scope = heat.current_class_scope
    alerts: list[dict[str, Any]] = []
    if session_values.get("channel_status") == CHANNEL_OFFLINE and (
        prior is None or prior.source_gap != now.source_gap or prior.ours_participant_id != now.ours_participant_id
    ):
        alerts.append(
            _event_alert(
                "source_offline_or_ours_missing",
                "critical",
                observed_at_us,
                {"channel_status": CHANNEL_OFFLINE, "ours_present": ours is not None},
                session_values.get("flag_phase_elapsed_s"),
            )
        )
    if prior is not None and prior.flag != now.flag:
        alerts.append(
            _event_alert(
                "flag_changed",
                "action",
                observed_at_us,
                {"track_flag": session_values.get("track_flag")},
                session_values.get("flag_phase_elapsed_s"),
            )
        )
        if session_values.get("track_flag") == "RED":
            alerts.append(
                _event_alert(
                    "red_flag_or_session_reset",
                    "critical",
                    observed_at_us,
                    {"track_flag": "RED"},
                    session_values.get("flag_phase_elapsed_s"),
                )
            )
    if prior is not None and prior.source_heat_id != now.source_heat_id:
        alerts.append(
            _event_alert("red_flag_or_session_reset", "critical", observed_at_us, {"source_heat_reset": True}, None)
        )
    if ours is not None and heat.session.mode == "race":
        remaining_pits = tactical_values.get("pits_remaining")
        remaining_s = session_values.get("session_remaining_s")
        class_median = _median_non_negative(_class_pit_durations(scope))
        if isinstance(remaining_pits, int) and remaining_pits > 0 and isinstance(remaining_s, (int, float)) and class_median is not None:
            required_s = remaining_pits * class_median / 1_000.0
            if remaining_s < required_s:
                alerts.append(
                    _event_alert(
                        "mandatory_pits_infeasible",
                        "critical",
                        observed_at_us,
                        {"remaining_pits": remaining_pits, "observed_pit_cycle_ms": class_median},
                        required_s - remaining_s,
                    )
                )
        for direction in ("ahead", "behind"):
            catch = battle_values.get("catch_range", {}).get(direction)
            if catch is not None and catch.get("min_laps") is not None and catch["min_laps"] <= CATCH_ALERT_LAPS:
                alerts.append(
                    _event_alert(
                        "catch_or_threat_crossing",
                        "action",
                        observed_at_us,
                        {"direction": direction, "threshold_laps": CATCH_ALERT_LAPS},
                        catch["min_laps"],
                    )
                )
        deviation = tactical_values.get("stint_schedule_deviation_s")
        if isinstance(deviation, (int, float)) and abs(deviation) >= SCHEDULE_DEVIATION_ALERT_S:
            alerts.append(
                _event_alert(
                    "even_schedule_deviation",
                    "action",
                    observed_at_us,
                    {"threshold_s": SCHEDULE_DEVIATION_ALERT_S},
                    deviation,
                )
            )
    if ours is not None:
        open_pit = next(
            (stop for stop in reversed(ours.pit_stops) if not stop.completed and stop.exited_at_us is None),
            None,
        )
        baseline = _class_pit_durations(scope)
        if open_pit is not None and len(baseline) >= 3:
            median = _median_non_negative(baseline)
            mad = _median_non_negative([int(abs(duration - median)) for duration in baseline]) if median is not None else None
            current_duration = (observed_at_us - open_pit.entered_at_us) // 1_000
            if median is not None and mad is not None and current_duration > median + 3 * mad:
                alerts.append(
                    _event_alert(
                        "ours_pit_too_long",
                        "critical",
                        observed_at_us,
                        {"median_ms": median, "mad_ms": mad, "stop_number": open_pit.stop_number},
                        current_duration - (median + 3 * mad),
                    )
                )
    if tactical_values.get("stint_abrupt_deterioration") is not None:
        alerts.append(
            _event_alert(
                "stint_pace_deteriorated",
                "action",
                observed_at_us,
                tactical_values["stint_abrupt_deterioration"],
                tactical_values["stint_abrupt_deterioration"]["laps"][-1]["excess_ms"],
            )
        )
    if ours is not None and prior is not None:
        old_ours = next((participant for participant in prior.participants if participant.participant_id == ours.id), None)
        if old_ours is not None and old_ours.best_lap_ms != _best_lap_ms(ours):
            alerts.append(
                _event_alert(
                    "new_best_lap",
                    "information",
                    observed_at_us,
                    {"participant_id": ours.id, "previous_best_ms": old_ours.best_lap_ms},
                    _best_lap_ms(ours),
                )
            )
        current_position_class = _rank(ours.state.position_class) if ours.state is not None else None
        if old_ours is not None and old_ours.position_class != current_position_class:
            alerts.append(
                _event_alert(
                    "class_position_changed",
                    "information",
                    observed_at_us,
                    {"previous_position": old_ours.position_class, "current_position": current_position_class},
                    tactical_values.get("position_change"),
                )
            )
        if old_ours is not None and old_ours.pits != _participant_boundary_state(ours).pits:
            alerts.append(
                _event_alert(
                    "pit_completed_new_stint",
                    "information",
                    observed_at_us,
                    {"participant_id": ours.id, "stint_number": tactical_values.get("stint_number")},
                    tactical_values.get("total_pit_lane_time_ms"),
                )
            )
        prior_participants = {participant.participant_id: participant for participant in prior.participants}
        nearest_ids = {
            participant_id
            for participant_id in (tactical_values.get("class_ahead_id"), tactical_values.get("class_behind_id"))
            if isinstance(participant_id, str)
        }
        for competitor in (member for member in (scope.participants if scope is not None else ()) if member.id in nearest_ids):
            old = prior_participants.get(competitor.id)
            current = _participant_boundary_state(competitor)
            if old is None:
                continue
            if old.state_kind != current.state_kind and current.state_kind in {"IN_PIT", "OUT_LAP"}:
                alerts.append(
                    _event_alert(
                        "competitor_pit_transition",
                        "action",
                        observed_at_us,
                        {"participant_id": competitor.id, "state": current.state_kind},
                        len(_completed_pits(competitor)),
                    )
                )
            if old.pits != current.pits:
                alerts.append(
                    _event_alert(
                        "pit_completed_new_stint",
                        "information",
                        observed_at_us,
                        {"participant_id": competitor.id, "stint_number": competitor.active_tire_stint.stint_number if competitor.active_tire_stint else None},
                        _pit_history(competitor)[-1]["pit_lane_duration_ms"] if _pit_history(competitor) else None,
                    )
                )
    if prior is not None and scope is not None:
        before_order = dict(prior.class_order_members).get(scope.key, ())
        current_order = tuple(member.id for member in (_class_order(scope).members if _class_order(scope) is not None else ()))
        if before_order != current_order:
            alerts.append(
                _event_alert(
                    "class_leader_or_neighbor_changed",
                    "information",
                    observed_at_us,
                    {
                        "class_leader_id": tactical_values.get("class_leader_id"),
                        "class_ahead_id": tactical_values.get("class_ahead_id"),
                        "class_behind_id": tactical_values.get("class_behind_id"),
                    },
                    None,
                )
            )
    return tuple(alerts)


def _ours_tactical_values(
    heat: HeatMetricInput,
    *,
    observed_at_us: int,
    session_values: Mapping[str, Any],
    pace_by_participant: Mapping[str, PaceMetrics],
    class_orders: Mapping[str, _ClassOrder | None],
    lap_samples_by_participant: Mapping[str, Sequence[LapSample]],
) -> dict[str, Any]:
    empty = {
        "ours_identity": None,
        "ours_participant_id": None,
        "ours_class_key": None,
        "position_overall": None,
        "position_class": None,
        "completed_laps": None,
        "current_state": None,
        "last_lap_ms": None,
        "best_lap_ms": None,
        "last_to_best_delta_ms": None,
        "delta_to_class_best_ms": None,
        "class_leader_id": None,
        "class_ahead_id": None,
        "class_behind_id": None,
        "class_leader_completed_laps": None,
        "class_ahead_completed_laps": None,
        "class_behind_completed_laps": None,
        "class_leader_state": None,
        "class_ahead_state": None,
        "class_behind_state": None,
        "lap_delta_to_class_leader": None,
        "lap_delta_to_ahead": None,
        "lap_delta_to_behind": None,
        "gap_to_class_leader_ms": None,
        "gap_to_ahead_ms": None,
        "gap_to_behind_ms": None,
        "relation_intervals": {
            "class_leader": None,
            "class_ahead": None,
            "class_behind": None,
        },
        "pace_3_ms": None,
        "pace_5_ms": None,
        "pace_10_ms": None,
        "consistency_10_ms": None,
        "class_pace_5_ms": None,
        "pace_rank_class": None,
        "pace_delta_to_reference_ms": None,
        "stint_number": None,
        "stint_started_at_us": None,
        "stint_elapsed_s": None,
        "tyre_age_laps": None,
        "stint_pace_5_ms": None,
        "stint_best_lap_ms": None,
        "stint_consistency_10_ms": None,
        "stint_trend_ms_per_lap": None,
        "stint_cumulative_pace_change_ms": None,
        "stint_pace_delta_previous_ms": None,
        "stint_abrupt_deterioration": None,
        "pace_delta_near_tyre_age_ms": None,
        "stint_clean_lap_count": None,
        "clean_lap_ratio_current_stint": None,
        "stint_summary": None,
        "pits_completed": None,
        "pit_history": None,
        "total_pit_lane_time_ms": None,
        "median_pit_lane_time_ms": None,
        "slow_lap_anomaly": None,
        "last_sector_ms": None,
        "personal_best_sector_ms": None,
        "class_best_sector_ms": None,
        "ideal_lap_ms": None,
        "potential_to_best_ms": None,
        "sector_delta_to_competitor_ms": None,
        "largest_sector_loss": None,
        **_empty_race_obligation_values(),
    }
    ours = heat.our_participant
    if ours is None or heat.session.identity_state != "resolved":
        return empty
    ours_pace = pace_by_participant.get(ours.id)
    if ours_pace is None:
        return empty
    values = _participant_values(
        ours,
        observed_at_us=observed_at_us,
        pace=ours_pace,
        lap_samples=lap_samples_by_participant.get(ours.id),
    )
    scope = heat.current_class_scope
    order = class_orders.get(scope.key) if scope is not None else None
    leader = ahead = behind = None
    class_pace = None
    class_pace_by_id: dict[str, float | None] = {}
    if scope is not None:
        class_pace_by_id = {
            participant.id: pace_by_participant.get(participant.id).pace5_ms
            if pace_by_participant.get(participant.id) is not None
            else None
            for participant in scope.participants
        }
        class_pace = class_median_pace_ms(class_pace_by_id)
    if order is not None:
        try:
            ours_index = next(index for index, member in enumerate(order.members) if member.id == ours.id)
        except StopIteration:
            ours_index = -1
        if ours_index >= 0:
            leader = order.members[0]
            ahead = order.members[ours_index - 1] if ours_index > 0 else None
            behind = order.members[ours_index + 1] if ours_index + 1 < len(order.members) else None
    references = {
        "class_leader": pace_delta_ms(ours_pace.pace5_ms, pace_by_participant.get(leader.id).pace5_ms)
        if leader is not None and pace_by_participant.get(leader.id) is not None
        else None,
        "class_ahead": pace_delta_ms(ours_pace.pace5_ms, pace_by_participant.get(ahead.id).pace5_ms)
        if ahead is not None and pace_by_participant.get(ahead.id) is not None
        else None,
        "class_behind": pace_delta_ms(ours_pace.pace5_ms, pace_by_participant.get(behind.id).pace5_ms)
        if behind is not None and pace_by_participant.get(behind.id) is not None
        else None,
        "class_median": pace_delta_ms(ours_pace.pace5_ms, class_pace),
        "competitors": {
            participant_id: pace_delta_ms(ours_pace.pace5_ms, candidate_pace)
            for participant_id, candidate_pace in sorted(class_pace_by_id.items())
            if participant_id != ours.id
        },
    }
    class_best = _class_best_lap_ms(scope) if scope is not None else None
    sector_values = _sector_metrics(ours, scope)
    participants_by_id = {
        participant.id: participant
        for participant in (scope.participants if scope is not None else ())
    }
    relation_intervals = {
        "class_leader": _relative_interval(
            ours,
            leader,
            participants=participants_by_id,
            evaluated_at_us=observed_at_us,
        ),
        "class_ahead": _relative_interval(
            ours,
            ahead,
            participants=participants_by_id,
            evaluated_at_us=observed_at_us,
        ),
        "class_behind": _relative_interval(
            ours,
            behind,
            participants=participants_by_id,
            evaluated_at_us=observed_at_us,
        ),
    }
    obligations = _race_obligation_values(
        heat,
        ours,
        session_elapsed_s=session_values["session_elapsed_s"],
        pace=ours_pace,
    )
    return {
        **empty,
        "ours_identity": _identity(ours),
        "ours_participant_id": ours.id,
        "ours_class_key": ours.class_key,
        "position_overall": values["position_overall"],
        "position_class": values["position_class"],
        "completed_laps": values["completed_laps"],
        "current_state": values["current_state"],
        "last_lap_ms": values["last_lap_ms"],
        "best_lap_ms": values["best_lap_ms"],
        "last_to_best_delta_ms": values["last_to_best_delta_ms"],
        "delta_to_class_best_ms": values["best_lap_ms"] - class_best
        if values["best_lap_ms"] is not None and class_best is not None
        else None,
        "class_leader_id": leader.id if leader is not None else None,
        "class_ahead_id": ahead.id if ahead is not None else None,
        "class_behind_id": behind.id if behind is not None else None,
        "class_leader_completed_laps": _source_lap_count(leader) if leader is not None else None,
        "class_ahead_completed_laps": _source_lap_count(ahead) if ahead is not None else None,
        "class_behind_completed_laps": _source_lap_count(behind) if behind is not None else None,
        "class_leader_state": leader.state.state_kind if leader is not None and leader.state is not None else None,
        "class_ahead_state": ahead.state.state_kind if ahead is not None and ahead.state is not None else None,
        "class_behind_state": behind.state.state_kind if behind is not None and behind.state is not None else None,
        "lap_delta_to_class_leader": _lap_delta(ours, leader),
        "lap_delta_to_ahead": _lap_delta(ours, ahead),
        "lap_delta_to_behind": _lap_delta(ours, behind),
        "gap_to_class_leader_ms": _relation_value_ms(relation_intervals["class_leader"]),
        "gap_to_ahead_ms": _relation_value_ms(relation_intervals["class_ahead"]),
        "gap_to_behind_ms": _relation_value_ms(relation_intervals["class_behind"]),
        "relation_intervals": relation_intervals,
        "pace_3_ms": ours_pace.pace3_ms,
        "pace_5_ms": ours_pace.pace5_ms,
        "pace_10_ms": ours_pace.pace10_ms,
        "consistency_10_ms": ours_pace.consistency10_ms,
        "class_pace_5_ms": class_pace,
        "pace_rank_class": pace_rank(ours.id, class_pace_by_id) if class_pace_by_id else None,
        "pace_delta_to_reference_ms": references,
        "stint_number": values["stint_number"],
        "stint_started_at_us": values["stint_started_at_us"],
        "stint_elapsed_s": values["stint_elapsed_s"],
        "tyre_age_laps": values["tyre_age_laps"],
        "stint_pace_5_ms": values["stint_pace_5_ms"],
        "stint_best_lap_ms": values["stint_best_lap_ms"],
        "stint_consistency_10_ms": values["stint_consistency_10_ms"],
        "stint_trend_ms_per_lap": values["stint_trend_ms_per_lap"],
        "stint_cumulative_pace_change_ms": values["stint_cumulative_pace_change_ms"],
        "stint_pace_delta_previous_ms": values["stint_pace_delta_previous_ms"],
        "stint_abrupt_deterioration": values["stint_abrupt_deterioration"],
        "pace_delta_near_tyre_age_ms": _near_tyre_age_pace_delta(
            ours,
            scope,
            samples_by_participant=lap_samples_by_participant,
        ),
        "stint_clean_lap_count": values["stint_clean_lap_count"],
        "clean_lap_ratio_current_stint": values["clean_lap_ratio_current_stint"],
        "stint_summary": values["stint_summary"],
        "pits_completed": values["pits_completed"],
        "pit_history": values["pit_history"],
        "total_pit_lane_time_ms": values["total_pit_lane_time_ms"],
        "median_pit_lane_time_ms": values["median_pit_lane_time_ms"],
        "slow_lap_anomaly": values["slow_lap_anomaly"],
        **sector_values,
        **obligations,
    }


def _class_values(
    scope: ClassScopeInput,
    *,
    pace_by_participant: Mapping[str, PaceMetrics],
    order: _ClassOrder | None,
) -> dict[str, Any]:
    pace_by_id = {
        participant.id: pace_by_participant.get(participant.id).pace5_ms
        if pace_by_participant.get(participant.id) is not None
        else None
        for participant in scope.participants
    }
    completed_pit_durations: list[int] = []
    for participant in scope.participants:
        for record in _pit_history(participant):
            duration = record["pit_lane_duration_ms"]
            if duration is not None:
                completed_pit_durations.append(duration)
    return {
        "class_key": scope.key,
        "class_name": scope.display_name,
        "participant_count": len(scope.participants),
        "class_best_lap_ms": _class_best_lap_ms(scope),
        "class_best_start_number": scope.class_best_start_number,
        "class_pace_5_ms": class_median_pace_ms(pace_by_id),
        "pace_5_by_participant": {participant_id: pace_by_id[participant_id] for participant_id in sorted(pace_by_id)},
        "class_leader_id": order.members[0].id if order is not None else None,
        "class_order_basis": order.basis if order is not None else None,
        "class_order_participant_ids": [member.id for member in order.members] if order is not None else None,
        "median_pit_lane_time_ms": _median_non_negative(completed_pit_durations),
        "total_completed_pits": sum(len(_completed_pits(participant)) for participant in scope.participants),
    }


def _participant_boundary_state(participant: ParticipantMetricInput) -> ParticipantBoundaryState:
    state = participant.state
    active_stint = participant.active_tire_stint
    return ParticipantBoundaryState(
        participant_id=participant.id,
        class_key=participant.class_key,
        identity=(
            participant.start_number,
            participant.team_name,
            participant.car_name,
            participant.class_name,
        ),
        driver_name=state.current_driver_name if state is not None else None,
        position_overall=_rank(state.position_overall) if state is not None else None,
        position_class=_rank(state.position_class) if state is not None else None,
        completed_laps=_completed_laps(participant),
        state_kind=state.state_kind if state is not None else None,
        last_lap_ms=_last_lap_ms(participant),
        latest_timing_event_id=participant.latest_timing_event_id,
        best_lap_ms=_best_lap_ms(participant),
        gap_interval_fact_pointer=_interval_fact_pointer(_source_interval_fact(state, "GAP")),
        diff_interval_fact_pointer=_interval_fact_pointer(_source_interval_fact(state, "DIFF")),
        pits=tuple(
            (stop.stop_number, stop.entered_at_us, stop.exited_at_us, stop.completed)
            for stop in sorted(participant.pit_stops, key=lambda stop: (stop.stop_number, stop.entered_at_us))
        ),
        active_stint=(active_stint.stint_number, active_stint.started_at_us, active_stint.completed_laps)
        if active_stint is not None
        else None,
    )


def build_boundary_state(heat: HeatMetricInput) -> MetricBoundaryState:
    """Capture only semantic changes which warrant a between-bucket point."""

    flag = heat.current_flag
    flag_signature = (
        (flag.flag, _flag_start_at_us(heat), flag.provider_code, flag.provider_label)
        if flag is not None
        else None
    )
    gap = heat.open_ingest_gap
    class_orders: list[tuple[str, tuple[str, ...]]] = []
    for scope in sorted(heat.class_scopes, key=lambda scope: scope.key):
        order = _class_order(scope)
        class_orders.append((scope.key, tuple(member.id for member in order.members) if order is not None else ()))
    ours = heat.our_participant
    return MetricBoundaryState(
        source_heat_id=heat.source_heat_id,
        session=(
            heat.session.id,
            heat.session.mode,
            heat.session.lifecycle,
            str(heat.session.started_at_us) if heat.session.started_at_us is not None else None,
            str(heat.session.stopped_at_us) if heat.session.stopped_at_us is not None else None,
        ),
        flag=flag_signature,
        source_gap=(gap.started_at_us, gap.reason) if gap is not None else None,
        ours_participant_id=ours.id if ours is not None else None,
        participants=tuple(
            _participant_boundary_state(participant)
            for participant in sorted(heat.participants, key=lambda participant: participant.id)
        ),
        class_order_members=tuple(class_orders),
    )


def _previous_boundary_state(
    previous: HeatMetricInput | MetricEngineResult | MetricBoundaryState | None,
) -> MetricBoundaryState | None:
    if previous is None:
        return None
    if isinstance(previous, MetricBoundaryState):
        return previous
    if isinstance(previous, MetricEngineResult):
        return previous.boundary_state
    if isinstance(previous, HeatMetricInput):
        return build_boundary_state(previous)
    raise TypeError("previous must be HeatMetricInput, MetricEngineResult, MetricBoundaryState, or None")


def detect_event_boundaries(
    previous: HeatMetricInput | MetricEngineResult | MetricBoundaryState | None,
    current: HeatMetricInput,
) -> tuple[str, ...]:
    """Return stable source-domain events without treating ordinary gaps as events."""

    prior = _previous_boundary_state(previous)
    now = build_boundary_state(current)
    if prior is None:
        return ("initial_snapshot",)
    if prior.source_heat_id != now.source_heat_id:
        return ("source_heat_changed",)

    events: list[str] = []
    if prior.session != now.session:
        events.append("session_lifecycle")
    if prior.flag != now.flag:
        events.append("track_flag")
    if prior.source_gap != now.source_gap:
        events.append("source_gap")
    if prior.ours_participant_id != now.ours_participant_id:
        events.append("ours_identity")
    if prior.class_order_members != now.class_order_members:
        for key, _ in now.class_order_members:
            events.append(f"class_order:{key}")

    before = {participant.participant_id: participant for participant in prior.participants}
    after = {participant.participant_id: participant for participant in now.participants}
    for participant_id in sorted(before.keys() | after.keys()):
        left = before.get(participant_id)
        right = after.get(participant_id)
        if left is None or right is None:
            events.append(f"participant_roster:{participant_id}")
            continue
        if left.identity != right.identity or left.driver_name != right.driver_name or left.class_key != right.class_key:
            events.append(f"identity:{participant_id}")
        if left.gap_interval_fact_pointer != right.gap_interval_fact_pointer:
            events.append(f"interval_fact:{participant_id}:GAP")
        if left.diff_interval_fact_pointer != right.diff_interval_fact_pointer:
            events.append(f"interval_fact:{participant_id}:DIFF")
        if left.state_kind != right.state_kind:
            events.append(f"state:{participant_id}")
        if (
            left.completed_laps != right.completed_laps
            or left.last_lap_ms != right.last_lap_ms
            or left.latest_timing_event_id != right.latest_timing_event_id
            or left.best_lap_ms != right.best_lap_ms
        ):
            events.append(f"lap:{participant_id}")
        if left.pits != right.pits or left.active_stint != right.active_stint:
            events.append(f"pit_or_stint:{participant_id}")
        if left.position_overall != right.position_overall or left.position_class != right.position_class:
            events.append(f"position:{participant_id}")
    return tuple(events)


def _event_scopes(
    previous: HeatMetricInput | MetricEngineResult | MetricBoundaryState | None,
    current: HeatMetricInput,
) -> set[tuple[str, str]]:
    """Return only scopes whose chart history must retain this domain event.

    A lap by one competitor is an event for that participant, their class and
    the Balchug session when it changes a same-class tactical reference.  It is
    not an event for every other participant.  Current values still advance for
    every scope on every frame through ``metric_current``.
    """
    all_scopes = {
        ("session", current.session.id),
        *(("class", scope.key) for scope in current.class_scopes),
        *(("participant", participant.id) for participant in current.participants),
    }
    prior = _previous_boundary_state(previous)
    now = build_boundary_state(current)
    if prior is None or prior.source_heat_id != now.source_heat_id:
        return all_scopes

    scopes: set[tuple[str, str]] = set()
    session_scope = ("session", current.session.id)
    ours = current.our_participant
    ours_class_key = ours.class_key if ours is not None else None
    members = {participant.id: participant for participant in current.participants}
    before = {participant.participant_id: participant for participant in prior.participants}
    after = {participant.participant_id: participant for participant in now.participants}

    if prior.session != now.session or prior.flag != now.flag or prior.source_gap != now.source_gap:
        scopes.add(session_scope)
    if prior.ours_participant_id != now.ours_participant_id:
        scopes.add(session_scope)

    before_orders = dict(prior.class_order_members)
    after_orders = dict(now.class_order_members)
    for class_key in sorted(before_orders.keys() | after_orders.keys()):
        if before_orders.get(class_key) != after_orders.get(class_key):
            if any(scope.key == class_key for scope in current.class_scopes):
                scopes.add(("class", class_key))
            if class_key == ours_class_key:
                scopes.add(session_scope)

    for participant_id in sorted(before.keys() | after.keys()):
        left = before.get(participant_id)
        right = after.get(participant_id)
        participant = members.get(participant_id)
        if left == right:
            continue
        if participant is not None:
            scopes.add(("participant", participant_id))
            if participant.class_key is not None:
                scopes.add(("class", participant.class_key))
            if participant_id == (ours.id if ours is not None else None) or participant.class_key == ours_class_key:
                scopes.add(session_scope)
        elif left is not None and left.class_key is not None and any(
            scope.key == left.class_key for scope in current.class_scopes
        ):
            scopes.add(("class", left.class_key))
            if left.class_key == ours_class_key:
                scopes.add(session_scope)
    return scopes


def evaluate_heat_metrics(
    heat: HeatMetricInput,
    *,
    previous: HeatMetricInput | MetricEngineResult | MetricBoundaryState | None = None,
    history: Sequence[MetricHistoryPoint] = (),
) -> MetricEngineResult:
    """Evaluate P0/P1 tactical facts into session, class, and participant scopes.

    Values are deliberately null when a source prerequisite is absent.  In
    particular, class intervals are only produced for same-lap pairs and a
    missing/partial PIC column never falls back to POS.
    """

    if not isinstance(heat, HeatMetricInput):
        raise TypeError("heat must be HeatMetricInput")
    observed_at_us = heat.observed_at_us
    session_values = _session_values(heat, observed_at_us=observed_at_us)
    lap_samples_by_participant = {
        participant.id: _lap_samples(participant)
        for participant in heat.participants
    }
    pace_by_participant = {
        participant.id: calculate_pace_metrics(
            lap_samples_by_participant[participant.id],
            slow_lap_window=SLOW_LAP_MAX_CLEAN_LAPS,
        )
        for participant in heat.participants
    }
    class_orders = {scope.key: _class_order(scope) for scope in heat.class_scopes}
    tactical_values = _ours_tactical_values(
        heat,
        observed_at_us=observed_at_us,
        session_values=session_values,
        pace_by_participant=pace_by_participant,
        class_orders=class_orders,
        lap_samples_by_participant=lap_samples_by_participant,
    )
    battle_values = _battle_values(
        heat,
        observed_at_us=observed_at_us,
        session_values=session_values,
        tactical_values=tactical_values,
        pace_by_participant=pace_by_participant,
        history=history,
    )
    alerts = _metric_alerts(
        heat,
        previous=previous,
        observed_at_us=observed_at_us,
        session_values=session_values,
        tactical_values=tactical_values,
        battle_values=battle_values,
    )
    event_keys = detect_event_boundaries(previous, heat)
    event_scopes = _event_scopes(previous, heat)
    session_candidate_values = {**session_values, **tactical_values, **battle_values, "alerts": alerts}

    candidates: list[MetricSampleCandidate] = [
        MetricSampleCandidate(
            scope_kind="session",
            scope_key=heat.session.id,
            values=session_candidate_values,
            event_boundary=("session", heat.session.id) in event_scopes,
            history_values=_history_values("session", session_candidate_values),
        )
    ]
    for scope in sorted(heat.class_scopes, key=lambda scope: scope.key):
        class_candidate_values = _class_values(
            scope,
            pace_by_participant=pace_by_participant,
            order=class_orders[scope.key],
        )
        candidates.append(
            MetricSampleCandidate(
                scope_kind="class",
                scope_key=scope.key,
                values=class_candidate_values,
                event_boundary=("class", scope.key) in event_scopes,
                history_values=_history_values("class", class_candidate_values),
            )
        )
    for participant in sorted(heat.participants, key=lambda participant: participant.id):
        participant_candidate_values = _participant_values(
            participant,
            observed_at_us=observed_at_us,
            pace=pace_by_participant[participant.id],
            lap_samples=lap_samples_by_participant[participant.id],
        )
        candidates.append(
            MetricSampleCandidate(
                scope_kind="participant",
                scope_key=participant.id,
                values=participant_candidate_values,
                event_boundary=("participant", participant.id) in event_scopes,
                history_values=_history_values("participant", participant_candidate_values),
            )
        )
    return MetricEngineResult(
        source_heat_id=heat.source_heat_id,
        observed_at_us=observed_at_us,
        candidates=tuple(candidates),
        event_keys=event_keys,
        boundary_state=build_boundary_state(heat),
    )
