"""Pure deterministic primitives for tactical timing metrics.

The live normalizer owns source interpretation and persistence.  This module
only receives immutable, already-normalized observations and returns derived
values in milliseconds, laps, and seconds.  It never reads the clock, opens a
database, or accepts manual tyre, driver, fuel, or service inputs.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import ceil, isfinite
from typing import Any


GREEN_FLAG = "GREEN"
ON_TRACK_STATE = "ON_TRACK"

GAP_RELATION_AHEAD = "AHEAD"
GAP_RELATION_BEHIND = "BEHIND"
GAP_RELATIONS = frozenset((GAP_RELATION_AHEAD, GAP_RELATION_BEHIND))
GAP_WINDOWS_S = (30, 60, 180)

GAP_DIRECTION_CLOSING = "CLOSING"
GAP_DIRECTION_LOSING_GROUND = "LOSING_GROUND"
GAP_DIRECTION_PULLING_AWAY = "PULLING_AWAY"
GAP_DIRECTION_BEING_CAUGHT = "BEING_CAUGHT"
GAP_DIRECTION_STABLE = "STABLE"

# These labels are presentation-ready so a UI never needs to reverse a sign.
GAP_DIRECTION_LABEL_RU = {
    GAP_DIRECTION_CLOSING: "догоняем",
    GAP_DIRECTION_LOSING_GROUND: "соперник отрывается",
    GAP_DIRECTION_PULLING_AWAY: "отрываемся",
    GAP_DIRECTION_BEING_CAUGHT: "нас догоняют",
    GAP_DIRECTION_STABLE: "стабильно",
}


def _is_int(value: Any, *, minimum: int | None = None) -> bool:
    return type(value) is int and (minimum is None or value >= minimum)


def _positive_number(value: Any) -> float | None:
    if type(value) not in {int, float}:
        return None
    number = float(value)
    return number if isfinite(number) and number > 0 else None


def _non_negative_number(value: Any) -> float | None:
    if type(value) not in {int, float}:
        return None
    number = float(value)
    return number if isfinite(number) and number >= 0 else None


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values or not 0.0 <= quantile <= 1.0:
        return None
    ordered = sorted(values)
    # The product contract fixes P10/P90 to nearest-rank rather than an
    # interpolated percentile: rank = ceil(percentile * population size).
    rank = max(1, min(len(ordered), ceil(quantile * len(ordered))))
    return ordered[rank - 1]


@dataclass(frozen=True)
class LapSample:
    """One completed lap with the evidence needed for clean-lap selection.

    ``flag_kinds`` must describe every flag phase crossed by the lap.  An empty
    sequence means that coverage is unknown, so the lap is deliberately not
    admitted to pace analytics.
    """

    participant_id: str | None = None
    lap_number: int | None = None
    completed_at_us: int | None = None
    duration_ms: int | None = None
    flag_kinds: tuple[str, ...] = ()
    is_in_lap: bool | None = None
    is_out_lap: bool | None = None
    crosses_pit: bool | None = None
    has_feed_gap: bool | None = None

    def __post_init__(self) -> None:
        flags = (self.flag_kinds,) if isinstance(self.flag_kinds, str) else tuple(self.flag_kinds or ())
        object.__setattr__(self, "flag_kinds", flags)


def is_clean_lap(lap: LapSample) -> bool:
    """Return whether a lap fully satisfies the analytics clean-lap contract."""

    if _positive_number(lap.duration_ms) is None:
        return False
    if not lap.flag_kinds or any(flag != GREEN_FLAG for flag in lap.flag_kinds):
        return False
    return (
        lap.is_in_lap is False
        and lap.is_out_lap is False
        and lap.crosses_pit is False
        and lap.has_feed_gap is False
    )


def select_clean_laps(laps: Sequence[LapSample]) -> tuple[LapSample, ...]:
    """Keep source-ordered laps that are valid for tactical pace calculations."""

    return tuple(lap for lap in laps if is_clean_lap(lap))


def median_ms(values: Sequence[int | float | None]) -> float | None:
    """Return a median duration in ms, excluding unavailable or invalid values."""

    normalized = [number for value in values if (number := _positive_number(value)) is not None]
    return _median(normalized)


def mad_ms(values: Sequence[int | float | None]) -> float | None:
    """Return the unscaled median absolute deviation in ms."""

    normalized = [number for value in values if (number := _positive_number(value)) is not None]
    center = _median(normalized)
    if center is None:
        return None
    return _median([abs(value - center) for value in normalized])


@dataclass(frozen=True)
class PaceMetrics:
    """Rolling clean-lap pace values for one participant or one stint."""

    pace3_ms: float | None
    pace5_ms: float | None
    pace10_ms: float | None
    consistency10_ms: float | None
    p10_p90_range_ms: float | None
    clean_lap_count: int
    observed_lap_count: int
    clean_lap_ratio: float | None
    slow_lap_numbers: tuple[int, ...] | None


def calculate_pace_metrics(
    laps: Sequence[LapSample],
    *,
    include_slow_lap_history: bool = True,
    slow_lap_window: int | None = None,
) -> PaceMetrics:
    """Calculate Pace3/5/10, robust consistency, and clean-lap anomalies.

    Pace windows use the last N clean laps in source order.  A slow-lap event
    compares a candidate with its *prior* ten clean laps, never with a baseline
    contaminated by the candidate itself.  ``slow_lap_numbers`` stays ``None``
    until that event baseline is available; an empty tuple then means that no
    observed candidate exceeds its specified threshold.
    """

    observed = tuple(laps)
    clean = select_clean_laps(observed)
    durations = [float(lap.duration_ms) for lap in clean]

    def pace(window: int) -> float | None:
        return _median(durations[-window:]) if len(durations) >= window else None

    pace3 = pace(3)
    pace5 = pace(5)
    pace10 = pace(10)
    last10 = durations[-10:]
    mad10 = mad_ms(last10) if len(last10) == 10 else None
    consistency10 = 1.4826 * mad10 if mad10 is not None else None
    p10 = _percentile(last10, 0.10) if len(last10) == 10 else None
    p90 = _percentile(last10, 0.90) if len(last10) == 10 else None
    range10 = p90 - p10 if p10 is not None and p90 is not None else None
    if slow_lap_window is not None and (type(slow_lap_window) is not int or slow_lap_window < 1):
        raise ValueError("slow_lap_window must be a positive integer or None")
    if not include_slow_lap_history or len(clean) < 11:
        slow_lap_numbers: tuple[int, ...] | None = None
    else:
        # Slow-lap alerts are operationally relevant near the live tail. Keep
        # ten baseline laps before the configured evaluation window so the
        # first retained candidate still has an unchanged robust threshold.
        start_index = max(10, len(clean) - (slow_lap_window or len(clean)))
        anomalies: list[int] = []
        for index in range(start_index, len(clean)):
            lap = clean[index]
            prior = [float(previous.duration_ms) for previous in clean[index - 10 : index]]
            if lap.lap_number is None:
                continue
            prior_pace = _median(prior)
            prior_mad = mad_ms(prior)
            if prior_pace is None or prior_mad is None:
                continue
            threshold = prior_pace + max(2_000.0, 3.0 * prior_mad)
            if float(lap.duration_ms) > threshold:
                anomalies.append(lap.lap_number)
        slow_lap_numbers = tuple(anomalies)
    ratio = len(clean) / len(observed) if observed else None
    return PaceMetrics(
        pace3_ms=pace3,
        pace5_ms=pace5,
        pace10_ms=pace10,
        consistency10_ms=consistency10,
        p10_p90_range_ms=range10,
        clean_lap_count=len(clean),
        observed_lap_count=len(observed),
        clean_lap_ratio=ratio,
        slow_lap_numbers=slow_lap_numbers,
    )


def pace_delta_ms(ours_pace_ms: int | float | None, reference_pace_ms: int | float | None) -> float | None:
    """Return ours minus reference; a negative result means Balchug is faster."""

    ours = _positive_number(ours_pace_ms)
    reference = _positive_number(reference_pace_ms)
    return ours - reference if ours is not None and reference is not None else None


def class_median_pace_ms(pace_by_participant: Mapping[str, int | float | None]) -> float | None:
    """Return ClassPace5 from available participant Pace5 values only."""

    values = [number for value in pace_by_participant.values() if (number := _positive_number(value)) is not None]
    return _median(values)


def pace_rank(participant_id: str, pace_by_participant: Mapping[str, int | float | None]) -> int | None:
    """Return competition pace rank, where tied equal pace values share a rank."""

    ours = _positive_number(pace_by_participant.get(participant_id))
    if ours is None:
        return None
    population = [number for value in pace_by_participant.values() if (number := _positive_number(value)) is not None]
    return 1 + sum(value < ours for value in population)


@dataclass(frozen=True)
class GapSample:
    """One normalized gap observation between our car and a selected target."""

    target_participant_id: str | None = None
    observed_at_us: int | None = None
    gap_ms: int | None = None
    our_lap_number: int | None = None
    target_lap_number: int | None = None
    flag_kind: str | None = None
    our_state_kind: str | None = None
    target_state_kind: str | None = None
    has_feed_gap: bool | None = None


def is_eligible_gap_sample(sample: GapSample) -> bool:
    """Whether an observation is safe for an interval/catch calculation."""

    return (
        isinstance(sample.target_participant_id, str)
        and bool(sample.target_participant_id)
        and _is_int(sample.observed_at_us, minimum=0)
        and _is_int(sample.gap_ms, minimum=0)
        and _is_int(sample.our_lap_number, minimum=0)
        and sample.our_lap_number == sample.target_lap_number
        and sample.flag_kind == GREEN_FLAG
        and sample.our_state_kind == ON_TRACK_STATE
        and sample.target_state_kind == ON_TRACK_STATE
        and sample.has_feed_gap is False
    )


@dataclass(frozen=True)
class GapTrend:
    """A Green, same-lap rolling trend with relation-specific signed closure."""

    relation: str
    window_s: int
    covered_s: float
    started_at_us: int
    ended_at_us: int
    started_gap_ms: int
    ended_gap_ms: int
    gap_change_ms: int
    closure_ms_per_min: float
    closure_ms_per_lap: float | None
    direction: str


def _gap_direction(relation: str, gap_change_ms: int) -> str:
    if gap_change_ms == 0:
        return GAP_DIRECTION_STABLE
    if relation == GAP_RELATION_AHEAD:
        return GAP_DIRECTION_CLOSING if gap_change_ms < 0 else GAP_DIRECTION_LOSING_GROUND
    return GAP_DIRECTION_PULLING_AWAY if gap_change_ms > 0 else GAP_DIRECTION_BEING_CAUGHT


def _ordered_gap_samples(samples: Sequence[GapSample]) -> tuple[GapSample, ...]:
    indexed = tuple(enumerate(samples))
    return tuple(
        sample
        for _, sample in sorted(
            indexed,
            key=lambda item: (
                item[1].observed_at_us if _is_int(item[1].observed_at_us, minimum=0) else float("inf"),
                item[0],
            ),
        )
    )


def calculate_gap_trend(
    samples: Sequence[GapSample], *, relation: str, window_s: int
) -> GapTrend | None:
    """Calculate one rolling gap trend, or ``None`` when its safety gate fails.

    A full contiguous eligible span must cover the requested window.  Therefore
    a Red/SC phase, a pit state, a lapped target, or a feed gap cannot leak a
    stale trend into a new Green period.
    """

    if relation not in GAP_RELATIONS or not _is_int(window_s, minimum=1):
        return None
    ordered = _ordered_gap_samples(samples)
    if len(ordered) < 2 or not is_eligible_gap_sample(ordered[-1]):
        return None

    latest_target = ordered[-1].target_participant_id
    suffix_start = len(ordered) - 1
    while (
        suffix_start > 0
        and is_eligible_gap_sample(ordered[suffix_start - 1])
        and ordered[suffix_start - 1].target_participant_id == latest_target
    ):
        suffix_start -= 1
    suffix = ordered[suffix_start:]
    latest = suffix[-1]
    assert latest.observed_at_us is not None
    cutoff_us = latest.observed_at_us - window_s * 1_000_000
    candidates = [sample for sample in suffix[:-1] if sample.observed_at_us is not None and sample.observed_at_us <= cutoff_us]
    if not candidates:
        return None
    first = candidates[-1]
    assert first.observed_at_us is not None
    assert first.gap_ms is not None and latest.gap_ms is not None
    elapsed_us = latest.observed_at_us - first.observed_at_us
    if elapsed_us <= 0:
        return None
    gap_change = latest.gap_ms - first.gap_ms
    # Ahead: a shrinking interval is positive (we are closing).  Behind: an
    # expanding interval is positive (we are pulling away).  The semantic
    # labels below make this sign safe for the dashboard and catch helper.
    closure = first.gap_ms - latest.gap_ms if relation == GAP_RELATION_AHEAD else latest.gap_ms - first.gap_ms
    lap_delta = latest.our_lap_number - first.our_lap_number
    return GapTrend(
        relation=relation,
        window_s=window_s,
        covered_s=elapsed_us / 1_000_000.0,
        started_at_us=first.observed_at_us,
        ended_at_us=latest.observed_at_us,
        started_gap_ms=first.gap_ms,
        ended_gap_ms=latest.gap_ms,
        gap_change_ms=gap_change,
        closure_ms_per_min=closure * 60_000_000.0 / elapsed_us,
        closure_ms_per_lap=closure / lap_delta if lap_delta > 0 else None,
        direction=_gap_direction(relation, gap_change),
    )


def calculate_gap_trends(
    samples: Sequence[GapSample], *, relation: str, windows_s: Sequence[int] = GAP_WINDOWS_S
) -> dict[int, GapTrend | None]:
    """Calculate the standard 30/60/180-second trend set in a stable order."""

    return {window_s: calculate_gap_trend(samples, relation=relation, window_s=window_s) for window_s in windows_s}


@dataclass(frozen=True)
class CatchRange:
    """A contact forecast range derived from one or more valid closure trends."""

    relation: str
    gap_ms: int
    minimum_laps: float
    maximum_laps: float
    minimum_time_ms: float
    maximum_time_ms: float
    source_windows_s: tuple[int, ...]


def calculate_catch_range(
    current: GapSample,
    trends: Sequence[GapTrend | None],
    *,
    relation: str,
    reference_pace_ms: int | float | None,
) -> CatchRange | None:
    """Return a Green/same-lap catch range, or ``None`` without valid inputs.

    For an ahead target this means Balchug catches the target.  For a behind
    target this means the target catches Balchug.  The source contract uses a
    positive closure sign for a closing target ahead and a negative closure
    sign for a target behind that is catching us.
    """

    if relation not in GAP_RELATIONS or not is_eligible_gap_sample(current):
        return None
    pace = _positive_number(reference_pace_ms)
    if pace is None or current.gap_ms is None:
        return None
    rates: list[tuple[int, float]] = []
    expected_direction = GAP_DIRECTION_CLOSING if relation == GAP_RELATION_AHEAD else GAP_DIRECTION_BEING_CAUGHT
    for trend in trends:
        if trend is None or trend.relation != relation or trend.direction != expected_direction:
            continue
        closure = trend.closure_ms_per_lap
        if relation == GAP_RELATION_AHEAD:
            rate = _positive_number(closure)
        else:
            rate = _positive_number(-closure) if closure is not None else None
        if rate is not None:
            rates.append((trend.window_s, rate))
    if len(rates) < 2:
        return None
    fastest_rate = max(rate for _, rate in rates)
    slowest_rate = min(rate for _, rate in rates)
    minimum_laps = current.gap_ms / fastest_rate
    maximum_laps = current.gap_ms / slowest_rate
    return CatchRange(
        relation=relation,
        gap_ms=current.gap_ms,
        minimum_laps=minimum_laps,
        maximum_laps=maximum_laps,
        minimum_time_ms=minimum_laps * pace,
        maximum_time_ms=maximum_laps * pace,
        source_windows_s=tuple(sorted(window for window, _ in rates)),
    )


@dataclass(frozen=True)
class PitStop:
    """A source-derived pit event; an incomplete stop never satisfies an obligation."""

    stop_number: int | None = None
    entered_at_us: int | None = None
    exited_at_us: int | None = None
    entered_lap: int | None = None
    exited_lap: int | None = None
    pit_lane_ms: int | None = None
    completed: bool | None = None


def is_completed_pit_stop(stop: PitStop) -> bool:
    """Require an observed pit in and pit out before a stop is counted."""

    return (
        stop.completed is True
        and _is_int(stop.entered_at_us, minimum=0)
        and _is_int(stop.exited_at_us, minimum=0)
        and stop.exited_at_us >= stop.entered_at_us
    )


def completed_pit_stops(stops: Sequence[PitStop]) -> tuple[PitStop, ...]:
    """Deduplicate source updates and return completed pit in-to-out events."""

    selected: dict[tuple[Any, ...], tuple[int, PitStop]] = {}
    for index, stop in enumerate(stops):
        if not is_completed_pit_stop(stop):
            continue
        if _is_int(stop.stop_number, minimum=1):
            key: tuple[Any, ...] = ("stop_number", stop.stop_number)
        else:
            key = ("timestamps", stop.entered_at_us, stop.exited_at_us)
        existing = selected.get(key)
        if existing is None or (stop.exited_at_us, index) >= (existing[1].exited_at_us, existing[0]):
            selected[key] = (index, stop)
    ordered = sorted(
        selected.values(),
        key=lambda item: (item[1].exited_at_us, item[1].entered_at_us, item[1].stop_number or 0, item[0]),
    )
    return tuple(stop for _, stop in ordered)


@dataclass(frozen=True)
class TireStint:
    """Automatically reconstructed tyre stint; age is completed laps only."""

    stint_number: int
    started_at_us: int | None
    ended_at_us: int | None
    started_lap: int | None
    ended_lap: int | None
    completed_laps: int
    is_partial: bool
    is_current: bool


@dataclass(frozen=True)
class TireLedger:
    """A participant's source-derived tyre ledger with no manual overrides."""

    stints: tuple[TireStint, ...]
    completed_pit_count: int

    @property
    def current_stint(self) -> TireStint | None:
        return self.stints[-1] if self.stints else None

    @property
    def current_tire_age_laps(self) -> int | None:
        current = self.current_stint
        return current.completed_laps if current is not None else None


def _max_lap_number(laps: Sequence[LapSample]) -> int | None:
    numbers = [lap.lap_number for lap in laps if _is_int(lap.lap_number, minimum=0)]
    return max(numbers) if numbers else None


def _observed_laps_in_interval(
    laps: Sequence[LapSample], *, start_at_us: int | None, end_at_us: int | None
) -> int:
    seen: set[tuple[Any, ...]] = set()
    for index, lap in enumerate(laps):
        completed_at = lap.completed_at_us
        if _is_int(completed_at, minimum=0):
            if start_at_us is not None and completed_at < start_at_us:
                continue
            if end_at_us is not None and completed_at >= end_at_us:
                continue
            key: tuple[Any, ...] = ("lap", lap.lap_number) if _is_int(lap.lap_number, minimum=0) else ("time", completed_at, index)
        else:
            continue
        seen.add(key)
    return len(seen)


def derive_tire_ledger(
    laps: Sequence[LapSample], stops: Sequence[PitStop], *, activation_at_us: int | None = None
) -> TireLedger:
    """Rebuild tyre stints from completed pit events and completed-lap evidence.

    The first captured stint is explicitly marked partial because recording can
    begin after the heat has already started.  Every completed pit out creates
    the next stint at age zero; subsequent completed laps increase its age.
    """

    completed = completed_pit_stops(stops)
    observations = tuple(laps)
    max_lap = _max_lap_number(observations)
    stints: list[TireStint] = []
    for stint_index in range(len(completed) + 1):
        previous = completed[stint_index - 1] if stint_index > 0 else None
        following = completed[stint_index] if stint_index < len(completed) else None
        started_at = previous.exited_at_us if previous is not None else (activation_at_us if _is_int(activation_at_us, minimum=0) else None)
        ended_at = following.entered_at_us if following is not None else None
        started_lap = previous.exited_lap if previous is not None else None
        ended_lap = following.entered_lap if following is not None else None
        if started_lap is not None:
            upper_lap = ended_lap if ended_lap is not None else max_lap
            completed_laps = max(0, upper_lap - started_lap) if upper_lap is not None else 0
        else:
            completed_laps = _observed_laps_in_interval(
                observations,
                start_at_us=started_at,
                end_at_us=ended_at,
            )
        stints.append(
            TireStint(
                stint_number=stint_index + 1,
                started_at_us=started_at,
                ended_at_us=ended_at,
                started_lap=started_lap,
                ended_lap=ended_lap,
                completed_laps=completed_laps,
                is_partial=stint_index == 0,
                is_current=following is None,
            )
        )
    return TireLedger(stints=tuple(stints), completed_pit_count=len(completed))


@dataclass(frozen=True)
class RacePlan:
    """The only Race-mode configuration accepted by the calculation layer."""

    duration_s: int | None = None
    required_pits: int | None = None


@dataclass(frozen=True)
class PitObligations:
    """Automatic mandatory-stop and equal-cadence values for Race mode."""

    completed_pits: int
    required_pits: int
    remaining_pits: int
    remaining_time_s: float
    initial_equal_stint_target_s: float
    remaining_equal_stint_target_s: float
    stop_load_per_hour: float | None
    next_equal_pit_elapsed_s: float | None
    next_equal_pit_in_s: float | None
    schedule_deviation_s: float | None


def calculate_pit_obligations(
    plan: RacePlan, stops: Sequence[PitStop], *, elapsed_s: int | float | None
) -> PitObligations | None:
    """Calculate race pit debt from completed source events only.

    The function intentionally has no fuel, compound, service, or manually
    entered tyre parameters.  It returns ``None`` until valid Race parameters
    and elapsed session time are present.
    """

    if plan.duration_s not in {14_400, 21_600, 43_200, 86_400} or plan.required_pits not in set(range(2, 9)):
        return None
    elapsed = _non_negative_number(elapsed_s)
    if elapsed is None:
        return None
    completed = len(completed_pit_stops(stops))
    remaining_pits = max(0, plan.required_pits - completed)
    remaining_time = max(0.0, float(plan.duration_s) - elapsed)
    initial_target = plan.duration_s / (plan.required_pits + 1)
    remaining_target = remaining_time / (remaining_pits + 1)
    if remaining_pits == 0:
        next_elapsed = None
        next_in = None
        schedule_deviation = None
        stop_load = None
    else:
        next_elapsed = (completed + 1) * initial_target
        next_in = max(0.0, next_elapsed - elapsed)
        schedule_deviation = elapsed - next_elapsed
        remaining_hours = remaining_time / 3_600.0
        stop_load = remaining_pits / remaining_hours if remaining_hours > 0 else None
    return PitObligations(
        completed_pits=completed,
        required_pits=plan.required_pits,
        remaining_pits=remaining_pits,
        remaining_time_s=remaining_time,
        initial_equal_stint_target_s=initial_target,
        remaining_equal_stint_target_s=remaining_target,
        stop_load_per_hour=stop_load,
        next_equal_pit_elapsed_s=next_elapsed,
        next_equal_pit_in_s=next_in,
        schedule_deviation_s=schedule_deviation,
    )
