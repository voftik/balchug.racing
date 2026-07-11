"""Pure normalization helpers for the Time Service SignalR feed.

The recorder deliberately persists raw frames first.  This module is the
second, deterministic step: it translates known wire shapes into typed values
without opening sockets, reading a database, or deciding race strategy.

Provider ``TsTime`` values are microseconds since 2000-01-01.  They are *not*
UTC timestamps by themselves.  ``ConnectionClockCalibrator`` makes the
per-connection offset explicit, while records such as flag history retain their
raw TsTime boundaries until an ingest worker chooses to resolve them.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any


TIME_SERVICE_EPOCH_UNIX_US = 946_684_800_000_000
"""Unix microseconds for the Time Service epoch, 2000-01-01T00:00:00Z."""

OPEN_ENDED_TS_TIME = 9_223_372_036_854_775_807
"""Provider ``Int64.MaxValue`` sentinel used for an unfinished interval."""


def parse_ts_time(value: Any) -> int | None:
    """Return a non-negative integral Time Service timestamp, if valid.

    JSON snapshots use both numbers and strings.  Floats, booleans, empty
    strings and signed negative values are rejected so a malformed value can
    remain raw instead of being silently converted into a false timestamp.
    """

    if type(value) is int:
        return value if value >= 0 else None
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate or not candidate.isascii() or not candidate.isdigit():
        return None
    return int(candidate)


def is_open_ended_ts_time(value: Any) -> bool:
    """Whether ``value`` is the provider's open-interval sentinel."""

    return parse_ts_time(value) == OPEN_ENDED_TS_TIME


def time_service_to_unix_us(value: Any) -> int | None:
    """Convert a raw TsTime to epoch microseconds without claiming it is UTC.

    The return value is only an epoch-coordinate value.  Callers must add a
    connection calibration before presenting it as a UTC instant.
    """

    timestamp = parse_ts_time(value)
    if timestamp is None or timestamp == OPEN_ENDED_TS_TIME:
        return None
    return TIME_SERVICE_EPOCH_UNIX_US + timestamp


def received_at_to_unix_us(value: Any) -> int | None:
    """Parse recorder receive time into Unix microseconds.

    Recordings use RFC3339 text, while callers that already have a UTC integer
    can pass it through unchanged.  Naive datetimes are intentionally rejected.
    """

    if type(value) is int:
        return value
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        candidate = value.strip()
        if candidate.isdigit():
            return int(candidate)
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            moment = datetime.fromisoformat(candidate)
        except ValueError:
            return None
    else:
        return None
    if moment.tzinfo is None or moment.utcoffset() is None:
        return None
    utc_moment = moment.astimezone(timezone.utc)
    unix_epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = utc_moment - unix_epoch
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000) + delta.microseconds


def _message_payload(args: Any) -> Any:
    if isinstance(args, Sequence) and not isinstance(args, (str, bytes, bytearray)):
        return args[0] if len(args) == 1 else args
    return args


def _server_time_from_payload(value: Any) -> int | None:
    direct = parse_ts_time(value)
    if direct is not None:
        return direct
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return parse_ts_time(value[0]) if len(value) == 1 else None
    if isinstance(value, Mapping):
        # These are explicit server-clock names, not arbitrary nested values.
        for key in ("t", "time", "serverTime", "server_time", "timestamp"):
            if key in value:
                parsed = parse_ts_time(value[key])
                if parsed is not None:
                    return parsed
    return None


@dataclass
class ConnectionClockCalibrator:
    """Median provider-clock offset derived from one SignalR connection.

    A reconnect must use a new instance.  That avoids applying an old source
    clock to a new connection when the provider's timezone or clock changes.
    """

    max_samples: int = 31
    _offsets_us: list[int] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        if type(self.max_samples) is not int or self.max_samples < 1:
            raise ValueError("max_samples must be a positive integer")

    @property
    def sample_count(self) -> int:
        return len(self._offsets_us)

    @property
    def offset_us(self) -> int | None:
        """The median receive-minus-provider offset, or ``None`` before input."""

        if not self._offsets_us:
            return None
        ordered = sorted(self._offsets_us)
        middle = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[middle]
        return (ordered[middle - 1] + ordered[middle]) // 2

    def observe(self, provider_ts_time: Any, received_at: Any) -> int | None:
        """Add one explicit server-time observation and return its offset."""

        provider_us = time_service_to_unix_us(provider_ts_time)
        received_us = received_at_to_unix_us(received_at)
        if provider_us is None or received_us is None:
            return None
        offset = received_us - provider_us
        self._offsets_us.append(offset)
        if len(self._offsets_us) > self.max_samples:
            del self._offsets_us[: len(self._offsets_us) - self.max_samples]
        return offset

    def observe_server_time(self, handle: str, args: Any, received_at: Any) -> int | None:
        """Observe ``s_i``/``s_t`` only; other handles cannot calibrate a clock."""

        if handle not in {"s_i", "s_t"}:
            return None
        return self.observe(_server_time_from_payload(_message_payload(args)), received_at)

    def to_utc_us(self, provider_ts_time: Any) -> int | None:
        """Resolve a raw provider TsTime only after this connection is calibrated."""

        provider_us = time_service_to_unix_us(provider_ts_time)
        offset = self.offset_us
        return provider_us + offset if provider_us is not None and offset is not None else None

    def snapshot(self) -> dict[str, Any]:
        """Return the ordered offset window needed for deterministic restore."""

        return {
            "max_samples": self.max_samples,
            "offsets_us": list(self._offsets_us),
        }

    @classmethod
    def from_snapshot(cls, state: Any) -> "ConnectionClockCalibrator":
        """Restore a validated offset window without observing a false clock sample."""

        if not isinstance(state, Mapping):
            raise ValueError("clock checkpoint must be an object")
        max_samples = state.get("max_samples")
        offsets = state.get("offsets_us")
        if type(max_samples) is not int or max_samples < 1:
            raise ValueError("clock checkpoint max_samples must be positive")
        if not isinstance(offsets, list) or len(offsets) > max_samples:
            raise ValueError("clock checkpoint offsets are invalid")
        if any(type(offset) is not int for offset in offsets):
            raise ValueError("clock checkpoint offsets must be integers")
        calibrator = cls(max_samples=max_samples)
        calibrator._offsets_us = list(offsets)
        return calibrator


@dataclass(frozen=True)
class ResultColumn:
    """One dynamic result-table column and its recognized stable key."""

    index: int
    source_name: str
    source_parameter: str | None
    key: str | None


@dataclass(frozen=True)
class ResultSchemaBinding:
    """One source-name binding in the fixed current Time Service layout."""

    key: str
    source_name: str
    source_parameter: str | None = None


@dataclass(frozen=True)
class ResultSchemaContractValidation:
    """Diagnostic result for the known production result-table contract.

    The result grid remains header-based.  This contract deliberately records
    a drift instead of moving the meaning of a field to a fixed column index.
    """

    contract_name: str
    status: str
    required_keys: tuple[str, ...]
    present_keys: tuple[str, ...]
    missing_required_keys: tuple[str, ...]
    binding_mismatches: tuple[dict[str, Any], ...]
    optional_present_keys: tuple[str, ...]
    unknown_columns: tuple[dict[str, Any], ...]


CURRENT_RESULT_SCHEMA_CONTRACT = "time-service-result-grid-v1"
"""Exact header contract observed on the current live Igora table."""


CURRENT_RESULT_SCHEMA_REQUIRED: tuple[ResultSchemaBinding, ...] = (
    ResultSchemaBinding("position_overall", "position"),
    ResultSchemaBinding("start_number", "startnumber"),
    ResultSchemaBinding("state", "State"),
    ResultSchemaBinding("team_name", "Team name"),
    ResultSchemaBinding("current_driver", "CurrentDriver"),
    ResultSchemaBinding("class_name", "class"),
    ResultSchemaBinding("position_class", "position_in_class"),
    ResultSchemaBinding("gap", "hole"),
    ResultSchemaBinding("best_lap", "fastestRoundTime"),
    ResultSchemaBinding("last_lap", "lastRoundTime"),
    ResultSchemaBinding("driver_stint", "CurrentDriverStintTime"),
    ResultSchemaBinding("pit_time", "PitTime"),
    ResultSchemaBinding("pit_stops", "pitstops"),
    ResultSchemaBinding("sector_1", "SectorTimes", "1"),
    ResultSchemaBinding("sector_2", "SectorTimes", "2"),
    ResultSchemaBinding("sector_3", "SectorTimes", "3"),
)


CURRENT_RESULT_SCHEMA_OPTIONAL: tuple[ResultSchemaBinding, ...] = (
    ResultSchemaBinding("car_name", "car"),
    ResultSchemaBinding("laps", "laps"),
    ResultSchemaBinding("diff", "diff"),
    ResultSchemaBinding("section_marker", "sectionMarker"),
)


def _header_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


_RESULT_FIELD_ALIASES = {
    "position": "position_overall",
    "pos": "position_overall",
    "marker": "marker",
    "startnumber": "start_number",
    "startno": "start_number",
    "nr": "start_number",
    "number": "start_number",
    "nbr": "start_number",
    "state": "state",
    "team": "team_name",
    "teamname": "team_name",
    "driver": "current_driver",
    "currentdriver": "current_driver",
    "driverincar": "current_driver",
    "class": "class_name",
    "cls": "class_name",
    "positioninclass": "position_class",
    "pic": "position_class",
    "hole": "gap",
    "gap": "gap",
    "diff": "diff",
    "fastestroundtime": "best_lap",
    "best": "best_lap",
    "lastroundtime": "last_lap",
    "last": "last_lap",
    "laps": "laps",
    "lap": "laps",
    "currentdriverstinttime": "driver_stint",
    "stint": "driver_stint",
    "pittime": "pit_time",
    "lpit": "pit_time",
    "pitstops": "pit_stops",
    "pit": "pit_stops",
    "sectionmarker": "section_marker",
    "section": "section_marker",
    "s": "section_marker",
    "car": "car_name",
    "vehicle": "car_name",
    "speed": "speed",
}


def _result_field_key(source_name: str, source_parameter: str | None = None) -> str | None:
    token = _header_token(source_name)
    known = _RESULT_FIELD_ALIASES.get(token)
    if known is not None:
        return known
    sector = re.fullmatch(r"(?:sectortimes?|sector|sect)([1-9][0-9]*)", token)
    if sector is not None:
        return f"sector_{sector.group(1)}"
    # The live Time Service layout names each sector column ``SectorTimes``
    # and carries its ordinal separately in the header parameter (``p``).
    # A generic sector header without a positive numeric ordinal remains raw
    # rather than being assigned an invented timing dimension.
    if token in {"sectortime", "sectortimes", "sector", "sect"} and source_parameter is not None:
        parameter = source_parameter.strip()
        if re.fullmatch(r"[1-9][0-9]*", parameter):
            return f"sector_{parameter}"
    return None


def result_columns(layout: Any) -> dict[int, ResultColumn]:
    """Read the current dynamic ``r_i``/``r_l`` header layout.

    Unknown headers are returned with ``key=None``.  The caller can retain the
    raw cell and report schema drift instead of assigning a guessed meaning.
    """

    if not isinstance(layout, Mapping):
        return {}
    candidate = layout.get("h")
    if candidate is None and isinstance(layout.get("l"), Mapping):
        candidate = layout["l"].get("h")
    if not isinstance(candidate, list):
        return {}
    columns: dict[int, ResultColumn] = {}
    for index, header in enumerate(candidate):
        if isinstance(header, Mapping):
            raw_name = header.get("n", index)
            raw_parameter = header.get("p")
        else:
            raw_name = header
            raw_parameter = None
        source_name = str(raw_name).strip() if raw_name is not None else str(index)
        source_parameter = str(raw_parameter) if raw_parameter not in (None, "") else None
        columns[index] = ResultColumn(
            index=index,
            source_name=source_name,
            source_parameter=source_parameter,
            key=_result_field_key(source_name, source_parameter),
        )
    return columns


def validate_current_result_schema(
    columns: Mapping[int, ResultColumn],
) -> ResultSchemaContractValidation:
    """Validate the stable live layout without relying on column indexes.

    A result remains ``DEGRADED`` when a familiar alias such as ``POS`` is
    supplied in place of the provider's current wire header.  The generic
    normalizer may still retain that alias as a raw/header-based fact, while
    the durable diagnostic makes a production contract change explicit.
    """

    bindings = (*CURRENT_RESULT_SCHEMA_REQUIRED, *CURRENT_RESULT_SCHEMA_OPTIONAL)
    required_keys = tuple(binding.key for binding in CURRENT_RESULT_SCHEMA_REQUIRED)
    optional_keys = {binding.key for binding in CURRENT_RESULT_SCHEMA_OPTIONAL}
    present_keys = tuple(sorted({column.key for column in columns.values() if column.key is not None}))
    missing_required: list[str] = []
    mismatches: list[dict[str, Any]] = []

    for binding in bindings:
        observed = tuple(
            column
            for _, column in sorted(columns.items())
            if column.key == binding.key
        )
        exact = tuple(
            column
            for column in observed
            if _header_token(column.source_name) == _header_token(binding.source_name)
            and column.source_parameter == binding.source_parameter
        )
        if binding.key in required_keys and not exact:
            missing_required.append(binding.key)
        if not observed:
            continue
        if len(observed) != 1 or len(exact) != 1:
            mismatches.append(
                {
                    "key": binding.key,
                    "expected_source_name": binding.source_name,
                    "expected_source_parameter": binding.source_parameter,
                    "observed": [
                        {
                            "index": column.index,
                            "source_name": column.source_name,
                            "source_parameter": column.source_parameter,
                            "canonical_key": column.key,
                        }
                        for column in observed
                    ],
                }
            )

    unknown_columns = tuple(
        {
            "index": column.index,
            "source_name": column.source_name,
            "source_parameter": column.source_parameter,
        }
        for _, column in sorted(columns.items())
        if column.key is None
    )
    optional_present = tuple(key for key in sorted(optional_keys) if key in present_keys)
    return ResultSchemaContractValidation(
        contract_name=CURRENT_RESULT_SCHEMA_CONTRACT,
        status="CURRENT" if not missing_required and not mismatches else "DEGRADED",
        required_keys=required_keys,
        present_keys=present_keys,
        missing_required_keys=tuple(missing_required),
        binding_mismatches=tuple(mismatches),
        optional_present_keys=optional_present,
        unknown_columns=unknown_columns,
    )


@dataclass(frozen=True)
class ResultState:
    """A lossless interpretation of a result-grid ``STATE`` cell."""

    raw: Any
    kind: str
    literal: str | None = None
    timer_target_raw: str | None = None
    timer_target_ts_time: int | None = None


_STATE_LITERALS = {
    "inpit": "IN_PIT",
    "outlap": "OUT_LAP",
}


def _literal_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def parse_result_state(value: Any) -> ResultState:
    """Parse source ``STATE`` values without treating timers as durations.

    ``E<TsTime>`` represents a provider-clock target while a car is on track.
    ``S<literal>`` is a source status; currently only ``In Pit`` and ``OutLap``
    have canonical meanings.  A new literal stays ``UNKNOWN`` with its source
    text intact for future schema work.
    """

    if not isinstance(value, str):
        return ResultState(raw=value, kind="UNKNOWN")
    raw = value
    candidate = raw.strip()
    if not candidate:
        return ResultState(raw=raw, kind="UNKNOWN")
    prefix = candidate[0].upper()
    if prefix == "E":
        timer_raw = candidate[1:].strip()
        timer = parse_ts_time(timer_raw)
        if timer is not None and timer != OPEN_ENDED_TS_TIME:
            return ResultState(
                raw=raw,
                kind="ON_TRACK",
                timer_target_raw=timer_raw,
                timer_target_ts_time=timer,
            )
        return ResultState(raw=raw, kind="UNKNOWN", timer_target_raw=timer_raw or None)
    literal = candidate[1:].strip() if prefix == "S" else candidate
    return ResultState(raw=raw, kind=_STATE_LITERALS.get(_literal_token(literal), "UNKNOWN"), literal=literal)


@dataclass(frozen=True)
class FlagState:
    """Canonical track-flag state while retaining provider code/label."""

    raw: Any
    kind: str
    provider_code: int | None
    provider_label: str | None


_FLAG_BY_CODE: dict[int, tuple[str, str]] = {
    -1: ("NOT_STARTED", "Not started"),
    0: ("NOT_STARTED", "Not started"),
    1: ("READY", "Ready"),
    2: ("RED", "Red flag"),
    3: ("SAFETY_CAR", "Safety car"),
    4: ("CODE_60", "Code 60"),
    5: ("FINISH", "Finish flag"),
    6: ("GREEN", "Green flag"),
    7: ("FULL_COURSE_YELLOW", "Full course yellow"),
}

_FLAG_CODE_BY_LABEL = {
    "notstarted": 0,
    "ready": 1,
    "warmupflag": 1,
    "redflag": 2,
    "yellowflag": 3,
    "safetycar": 3,
    "purpleflag": 4,
    "code60": 4,
    "finishflag": 5,
    "greenflag": 6,
    "fullcourseyellow": 7,
    "fcy": 7,
}


def _flag_code(value: Any) -> int | None:
    if type(value) is int:
        return value
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if re.fullmatch(r"-?[0-9]+", candidate):
        return int(candidate)
    return None


def canonical_flag(value: Any) -> FlagState:
    """Map a provider code or history label to a stable flag kind.

    Code ``3`` is intentionally ``SAFETY_CAR`` and code ``7`` is
    ``FULL_COURSE_YELLOW``.  A string history label retains its exact source
    spelling as ``provider_label``; a numeric current flag uses the known label.
    """

    code = _flag_code(value)
    if code is not None:
        known = _FLAG_BY_CODE.get(code)
        if known is not None:
            return FlagState(raw=value, kind=known[0], provider_code=code, provider_label=known[1])
        return FlagState(raw=value, kind="UNKNOWN", provider_code=code, provider_label=None)
    if isinstance(value, str):
        source_label = value.strip()
        label_code = _FLAG_CODE_BY_LABEL.get(_literal_token(source_label))
        if label_code is not None:
            return FlagState(raw=value, kind=_FLAG_BY_CODE[label_code][0], provider_code=label_code, provider_label=source_label)
        return FlagState(raw=value, kind="UNKNOWN", provider_code=None, provider_label=source_label or None)
    return FlagState(raw=value, kind="UNKNOWN", provider_code=None, provider_label=None)


def _as_int(value: Any, *, minimum: int | None = None) -> int | None:
    if type(value) is int:
        parsed = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        parsed = int(value.strip())
    else:
        return None
    return parsed if minimum is None or parsed >= minimum else None


def _as_float(value: Any, *, minimum: float | None = None) -> float | None:
    if type(value) in {int, float}:
        parsed = float(value)
    elif isinstance(value, str):
        try:
            parsed = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return None
    return parsed if minimum is None or parsed >= minimum else None


def _as_bool(value: Any) -> bool | None:
    if type(value) is bool:
        return value
    if type(value) is int and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        token = value.strip().casefold()
        if token in {"true", "1"}:
            return True
        if token in {"false", "0"}:
            return False
    return None


def _as_text(value: Any) -> str | None:
    if value is None or type(value) is bool:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if type(value) in {int, float}:
        return str(value)
    return None


@dataclass(frozen=True)
class TrackerPassing:
    """Typed form of one provider ``t_p`` positional tuple."""

    raw: Any
    transponder_id: str | None
    start_number: str | None
    distance_mm: int | None
    stop_distance_mm: int | None
    sector_id: int | None
    speed_mm_s: int | None
    speed_kph: float | None
    is_in_pit: bool | None
    provider_passed_at_raw: Any
    passed_at_ts_time: int | None
    path_id: str | None
    errors: tuple[str, ...] = ()


def parse_tracker_passing(value: Any) -> TrackerPassing:
    """Parse ``t_p`` tuple values, including mm/s to km/h conversion."""

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return TrackerPassing(
            raw=value,
            transponder_id=None,
            start_number=None,
            distance_mm=None,
            stop_distance_mm=None,
            sector_id=None,
            speed_mm_s=None,
            speed_kph=None,
            is_in_pit=None,
            provider_passed_at_raw=None,
            passed_at_ts_time=None,
            path_id=None,
            errors=("expected_tuple",),
        )
    fields = list(value)
    errors: list[str] = []

    def item(index: int) -> Any:
        return fields[index] if index < len(fields) else None

    transponder_id = _as_text(item(0))
    start_number = _as_text(item(1))
    distance_mm = _as_int(item(2))
    stop_distance_mm = _as_int(item(3))
    sector_id = _as_int(item(4))
    speed_mm_s = _as_int(item(5), minimum=0)
    is_in_pit = _as_bool(item(6))
    passed_raw = item(7)
    passed_at = parse_ts_time(passed_raw)
    path_id = _as_text(item(8))
    if len(fields) < 8:
        errors.append("truncated_tuple")
    if transponder_id is None:
        errors.append("invalid_transponder_id")
    if speed_mm_s is None:
        errors.append("invalid_speed_mm_s")
    if is_in_pit is None:
        errors.append("invalid_is_in_pit")
    if passed_at is None:
        errors.append("invalid_passed_at")
    return TrackerPassing(
        raw=value,
        transponder_id=transponder_id,
        start_number=start_number,
        distance_mm=distance_mm,
        stop_distance_mm=stop_distance_mm,
        sector_id=sector_id,
        speed_mm_s=speed_mm_s,
        speed_kph=(speed_mm_s * 0.0036) if speed_mm_s is not None else None,
        is_in_pit=is_in_pit,
        provider_passed_at_raw=passed_raw,
        passed_at_ts_time=passed_at,
        path_id=path_id,
        errors=tuple(errors),
    )


@dataclass(frozen=True)
class BestLapRecord:
    """One compact ``a_u.b`` or ``a_u.q`` best-lap record."""

    provider_key: str
    lap_number: int | None
    lap_time_us: int | None
    occurred_at_ts_time: int | None
    average_speed_kph: float | None
    driver_name: str | None
    team_name: str | None
    vehicle_name: str | None
    start_number: str | None
    class_name: str | None
    class_order: int | None


@dataclass(frozen=True)
class CautionPeriod:
    """One ``a_u.i`` flag-history period with raw, uncalibrated boundaries."""

    provider_key: str
    flag: FlagState
    started_at_raw: Any
    started_at_ts_time: int | None
    ended_at_raw: Any
    ended_at_ts_time: int | None
    is_open: bool
    clock_stopped_raw: Any
    clock_stopped: bool | None
    remark: str | None


@dataclass(frozen=True)
class LeaderHistoryRecord:
    """One compact ``a_u.l`` leader-history record."""

    provider_key: str
    occurred_at_raw: Any
    occurred_at_ts_time: int | None
    lap_number: int | None
    start_number: str | None
    team_name: str | None
    driver_name: str | None
    vehicle_name: str | None


@dataclass(frozen=True)
class LeaderLapAggregate:
    """One compact ``a_u.d`` aggregate of laps led by an entry."""

    provider_key: str
    leader_laps: int | None
    team_name: str | None
    vehicle_name: str | None
    start_number: str | None


@dataclass(frozen=True)
class StatisticsUpdate:
    """Known portions of one ``a_i``/``a_u`` compact statistics patch."""

    summary: Mapping[str, Any]
    best_lap_history: tuple[BestLapRecord, ...] = ()
    best_lap_per_class: tuple[BestLapRecord, ...] = ()
    caution_periods: tuple[CautionPeriod, ...] = ()
    leader_history: tuple[LeaderHistoryRecord, ...] = ()
    leader_lap_aggregates: tuple[LeaderLapAggregate, ...] = ()
    truncations: Mapping[str, int] = field(default_factory=dict)
    unknown_keys: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


_STATISTICS_COUNTERS = {
    "p": "participants_started",
    "a": "participants_classified",
    "n": "participants_not_classified",
    "pt": "participants_on_track",
    "pp": "participants_in_pit_zone",
    "ptz": "participants_in_tank_zone",
    "o": "total_laps",
    "x": "total_pitstops",
    "e": "leader_laps_green",
    "y": "leader_laps_safety_car",
    "r": "leader_laps_code_60",
    "fy": "leader_laps_full_course_yellow",
    "c": "safety_car_count",
    "s": "code_60_count",
    "fc": "full_course_yellow_count",
}

_STATISTICS_DURATIONS_RAW = {
    "u": "safety_car_total_time_raw",
    "t": "code_60_total_time_raw",
    "fu": "full_course_yellow_total_time_raw",
}

_COLLECTIONS = frozenset({"b", "q", "i", "l", "d"})


def _ordered_records(value: Any) -> tuple[tuple[str, Mapping[str, Any]], ...]:
    if isinstance(value, Mapping):
        records = [(str(key), record) for key, record in value.items() if isinstance(record, Mapping)]
    elif isinstance(value, list):
        records = [(str(index), record) for index, record in enumerate(value, start=1) if isinstance(record, Mapping)]
    else:
        return ()

    def sort_key(item: tuple[str, Mapping[str, Any]]) -> tuple[int, int | str]:
        key = item[0]
        return (0, int(key)) if re.fullmatch(r"[0-9]+", key) else (1, key)

    return tuple(sorted(records, key=sort_key))


def _best_lap_records(value: Any) -> tuple[BestLapRecord, ...]:
    records: list[BestLapRecord] = []
    for key, record in _ordered_records(value):
        records.append(
            BestLapRecord(
                provider_key=key,
                lap_number=_as_int(record.get("r"), minimum=0),
                lap_time_us=_as_int(record.get("i"), minimum=0),
                occurred_at_ts_time=parse_ts_time(record.get("t")),
                average_speed_kph=_as_float(record.get("a"), minimum=0),
                driver_name=_as_text(record.get("d")),
                team_name=_as_text(record.get("n")),
                vehicle_name=_as_text(record.get("c")),
                start_number=_as_text(record.get("s")),
                class_name=_as_text(record.get("m")),
                class_order=_as_int(record.get("p"), minimum=0),
            )
        )
    return tuple(records)


def _caution_periods(value: Any) -> tuple[CautionPeriod, ...]:
    records: list[CautionPeriod] = []
    for key, record in _ordered_records(value):
        started_raw = record.get("f")
        ended_raw = record.get("t")
        records.append(
            CautionPeriod(
                provider_key=key,
                flag=canonical_flag(record.get("k")),
                started_at_raw=started_raw,
                started_at_ts_time=parse_ts_time(started_raw),
                ended_at_raw=ended_raw,
                ended_at_ts_time=None if is_open_ended_ts_time(ended_raw) else parse_ts_time(ended_raw),
                is_open=is_open_ended_ts_time(ended_raw),
                clock_stopped_raw=record.get("s"),
                clock_stopped=_as_bool(record.get("s")),
                remark=_as_text(record.get("r")),
            )
        )
    return tuple(records)


def _leader_history_records(value: Any) -> tuple[LeaderHistoryRecord, ...]:
    records: list[LeaderHistoryRecord] = []
    for key, record in _ordered_records(value):
        occurred_raw = record.get("f")
        records.append(
            LeaderHistoryRecord(
                provider_key=key,
                occurred_at_raw=occurred_raw,
                occurred_at_ts_time=parse_ts_time(occurred_raw),
                lap_number=_as_int(record.get("l"), minimum=0),
                start_number=_as_text(record.get("s")),
                team_name=_as_text(record.get("n")),
                driver_name=_as_text(record.get("d")),
                vehicle_name=_as_text(record.get("c")),
            )
        )
    return tuple(records)


def _leader_lap_aggregates(value: Any) -> tuple[LeaderLapAggregate, ...]:
    records: list[LeaderLapAggregate] = []
    for key, record in _ordered_records(value):
        records.append(
            LeaderLapAggregate(
                provider_key=key,
                leader_laps=_as_int(record.get("e"), minimum=0),
                team_name=_as_text(record.get("n")),
                vehicle_name=_as_text(record.get("c")),
                start_number=_as_text(record.get("s")),
            )
        )
    return tuple(records)


def normalize_statistics_update(payload: Any) -> StatisticsUpdate:
    """Normalize a compact ``a_i`` or ``a_u`` payload.

    This function does not merge patches or resolve timestamps.  In particular,
    ``a_u.i`` flag-boundary TsTime values remain raw/provider-clock values until
    the caller applies its per-connection ``ConnectionClockCalibrator``.
    """

    if not isinstance(payload, Mapping):
        return StatisticsUpdate(summary={}, errors=("expected_object",))

    summary: dict[str, Any] = {}
    errors: list[str] = []
    heat_name = payload.get("h")
    if "h" in payload:
        text = _as_text(heat_name)
        if text is None:
            errors.append("invalid_heat_name")
        else:
            summary["heat_name"] = text
    for key, field_name in _STATISTICS_COUNTERS.items():
        if key not in payload:
            continue
        parsed = _as_int(payload[key], minimum=0)
        if parsed is None:
            errors.append(f"invalid_{field_name}")
        else:
            summary[field_name] = parsed
    for key, field_name in _STATISTICS_DURATIONS_RAW.items():
        if key in payload:
            # Unit/clock semantics are provider-specific; retain it as supplied.
            summary[field_name] = payload[key]
    for key, field_name in (("g", "green_flag_ts_time"), ("f", "finish_flag_ts_time")):
        if key not in payload:
            continue
        parsed = parse_ts_time(payload[key])
        if parsed is None:
            errors.append(f"invalid_{field_name}")
        else:
            summary[field_name] = parsed

    truncations: dict[str, int] = {}
    for key in _COLLECTIONS:
        truncation_key = f"{key}C"
        if truncation_key not in payload:
            continue
        parsed = _as_int(payload[truncation_key], minimum=0)
        if parsed is None:
            errors.append(f"invalid_{truncation_key}")
        else:
            truncations[key] = parsed

    known = set(_STATISTICS_COUNTERS) | set(_STATISTICS_DURATIONS_RAW) | {"h", "g", "f"} | set(_COLLECTIONS)
    known.update(f"{key}C" for key in _COLLECTIONS)
    unknown_keys = tuple(sorted(str(key) for key in payload if key not in known))
    return StatisticsUpdate(
        summary=summary,
        best_lap_history=_best_lap_records(payload.get("b")),
        best_lap_per_class=_best_lap_records(payload.get("q")),
        caution_periods=_caution_periods(payload.get("i")),
        leader_history=_leader_history_records(payload.get("l")),
        leader_lap_aggregates=_leader_lap_aggregates(payload.get("d")),
        truncations=truncations,
        unknown_keys=unknown_keys,
        errors=tuple(errors),
    )
