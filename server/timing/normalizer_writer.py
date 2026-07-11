"""Database-backed normalizer for durable Time Service timing frames.

Raw SignalR frames and decoded handles are committed before this code runs.  The
normalizer turns only known shapes into query-ready rows while retaining source
values, receive-time provenance and per-connection calibrated timestamps.
Unknown handles/columns stay safely in the raw feed and result-cell history.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3
import uuid
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from .config import now_us
from .ingest_store import StoredFrame
from .lifecycle import OUR_START_NUMBER, OUR_TEAM_NAME
from .metric_runner import TimingMetricRunner
from .normalization import (
    ConnectionClockCalibrator,
    CautionPeriod,
    FlagState,
    OPEN_ENDED_TS_TIME,
    ResultState,
    StatisticsUpdate,
    canonical_flag,
    is_open_ended_ts_time,
    normalize_statistics_update,
    parse_result_state,
    parse_tracker_passing,
    parse_ts_time,
)
from .protocol import SignalRMessage
from .result_grid import ResultGrid


NORMALIZER_VERSION = "timeservice-normalizer-v1"
OUR_TEAM_KEY = " ".join(OUR_TEAM_NAME.casefold().split())
# A live `h_h.f` transition and the matching Statistics history entry normally
# arrive seconds apart. Older history must be inserted as its own event rather
# than being attached to the current live period.
FLAG_RECONCILIATION_WINDOW_US = 120 * 1_000_000


class NormalizerError(RuntimeError):
    """A normalized write cannot be safely committed."""


@dataclass(frozen=True)
class FrameMessage:
    """One decoded provider invocation with immutable feed provenance."""

    id: int
    ordinal: int
    handle: str
    args: tuple[Any, ...]
    compressed: bool
    source_key: str
    received_at_us: int
    connection_id: str

    @property
    def payload(self) -> Any:
        return self.args[0] if len(self.args) == 1 else list(self.args)


@dataclass(frozen=True)
class ResultCellFact:
    """One unambiguous result-grid cell tied to its durable source event."""

    id: int
    value_text: str | None
    source_message_id: int
    source_key: str


@dataclass(frozen=True)
class PendingPitEvent:
    """A source STATE/PIT event applied after every handle in one frame."""

    context: FrameMessage
    participant_id: str
    state_event: ResultState | None
    state_cell: ResultCellFact | None
    pit_count_event: int | None
    pit_count_cell: ResultCellFact | None
    lap_number: int | None
    pit_time_event: ResultCellFact | None
    previous: sqlite3.Row | None


@dataclass(frozen=True)
class DriverStintValue:
    """Typed but intentionally non-strategic interpretation of STINT."""

    raw: str | None
    kind: str
    provider_ts_time: int | None
    duration_ms: int | None


@contextmanager
def _write_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    if connection.in_transaction:
        raise NormalizerError("Normalizer writes require a connection without an open transaction")
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _text(value: Any) -> str | None:
    if value is None or type(value) is bool:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if type(value) in {int, float}:
        return str(value)
    return None


def _key(value: Any) -> str | None:
    text = _text(value)
    return " ".join(text.casefold().split()) if text is not None else None


def _integer(value: Any, *, minimum: int | None = None) -> int | None:
    if type(value) is int:
        result = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        result = int(value.strip())
    else:
        return None
    return result if minimum is None or result >= minimum else None


def _number(value: Any) -> float | None:
    if type(value) in {int, float}:
        result = float(value)
    elif isinstance(value, str):
        try:
            result = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return result if result == result and result not in {float("inf"), float("-inf")} else None


def _duration_us_to_ms(value: Any) -> int | None:
    source_us = parse_ts_time(value)
    if source_us is None or not 1_000_000 <= source_us < OPEN_ENDED_TS_TIME:
        return None
    return source_us // 1_000


def _parse_driver_stint(value: Any) -> DriverStintValue:
    """Parse Time Service STINT grammar without assigning race semantics.

    ``S`` and ``P`` carry Time Service instants; ``L`` carries a duration.
    They are stored for audit and future analysis only, never used by tactical
    calculations in this normalizer.
    """

    raw = _text(value)
    if raw is None:
        return DriverStintValue(raw=None, kind="UNKNOWN", provider_ts_time=None, duration_ms=None)
    prefix = raw[:1].upper()
    payload = raw[1:].strip()
    if prefix in {"S", "P"}:
        provider_ts_time = parse_ts_time(payload)
        if provider_ts_time is not None and provider_ts_time not in {0, OPEN_ENDED_TS_TIME}:
            return DriverStintValue(
                raw=raw,
                kind="START_TS" if prefix == "S" else "POINT_TS",
                provider_ts_time=provider_ts_time,
                duration_ms=None,
            )
    if prefix == "L":
        duration_ms = _duration_us_to_ms(payload)
        if duration_ms is not None:
            return DriverStintValue(raw=raw, kind="DURATION", provider_ts_time=None, duration_ms=duration_ms)
    return DriverStintValue(raw=raw, kind="UNKNOWN", provider_ts_time=None, duration_ms=None)


def _event_ts_time(value: Any) -> int | None:
    """Return a real provider event timestamp, excluding the unset zero."""

    timestamp = parse_ts_time(value)
    if timestamp is None or timestamp == 0 or is_open_ended_ts_time(value):
        return None
    return timestamp


def _gap_ms(value: Any) -> int | None:
    """The current GAP/DIFF cells are decimal seconds, never a TsTime."""
    number = _number(value)
    return round(number * 1_000) if number is not None and number >= 0 else None


def _payload_mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _deep_merge(current: dict[str, Any], patch: Mapping[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(current)
    for key, value in patch.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _changes(value: Any) -> tuple[tuple[Any, ...], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(
        tuple(item)
        for item in value
        if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray))
    )


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _upsert(
    connection: sqlite3.Connection,
    table: str,
    values: Mapping[str, Any],
    *,
    conflict_columns: Sequence[str],
    update_columns: Sequence[str] | None = None,
) -> None:
    """Use a small trusted SQL builder for internal fixed table/column names."""
    columns = tuple(values)
    if not columns:
        raise NormalizerError(f"Cannot upsert an empty row into {table}")
    updates = tuple(update_columns) if update_columns is not None else tuple(
        column for column in columns if column not in conflict_columns
    )
    quoted_columns = ",".join(f'"{column}"' for column in columns)
    placeholders = ",".join("?" for _ in columns)
    conflict = ",".join(f'"{column}"' for column in conflict_columns)
    if updates:
        update_sql = ",".join(f'"{column}"=excluded."{column}"' for column in updates)
        suffix = f" ON CONFLICT({conflict}) DO UPDATE SET {update_sql}"
    else:
        suffix = f" ON CONFLICT({conflict}) DO NOTHING"
    connection.execute(
        f'INSERT INTO "{table}" ({quoted_columns}) VALUES ({placeholders}){suffix}',
        tuple(values[column] for column in columns),
    )


def _insert_ignore(connection: sqlite3.Connection, table: str, values: Mapping[str, Any]) -> bool:
    columns = tuple(values)
    quoted_columns = ",".join(f'"{column}"' for column in columns)
    placeholders = ",".join("?" for _ in columns)
    cursor = connection.execute(
        f'INSERT OR IGNORE INTO "{table}" ({quoted_columns}) VALUES ({placeholders})',
        tuple(values[column] for column in columns),
    )
    return cursor.rowcount == 1


class TimingNormalizer:
    """Replay-safe reducer and writer for one engineer analysis session."""

    def __init__(self, analysis_session_id: str, *, replay_active: bool = False):
        if type(replay_active) is not bool:
            raise NormalizerError("replay_active must be a boolean")
        self.analysis_session_id = analysis_session_id
        self._replay_active = replay_active
        self.grid = ResultGrid()
        self.heat: dict[str, Any] = {}
        self.statistics: dict[str, Any] = {}
        self._calibrators: dict[str, ConnectionClockCalibrator] = {}
        self._heat_id: int | None = None
        self._layout_id: int | None = None
        self._provider_heat_start_ts: int | None = None
        self._finish_sector_ids: set[int] = set()
        self._pending_pit_events: list[PendingPitEvent] = []
        self._primed = False
        self._metric_runner = TimingMetricRunner()

    def __call__(
        self,
        connection: sqlite3.Connection,
        frame: StoredFrame,
        messages: tuple[SignalRMessage, ...],
    ) -> None:
        self.process_frame(connection, frame, messages)

    def process_frame(
        self,
        connection: sqlite3.Connection,
        frame: StoredFrame,
        _messages: tuple[SignalRMessage, ...],
    ) -> None:
        """Normalize one decoded durable frame in its own atomic transaction."""
        self._prime(connection)
        contexts = self._frame_messages(connection, frame)
        if not contexts:
            return
        self._pending_pit_events = []
        try:
            with _write_transaction(connection):
                self._ensure_heat(connection, contexts[0].received_at_us)
                for context in contexts:
                    self._apply_message(connection, context, write=True)
                # A finish-loop passing tells us when a car crossed the line, but
                # never how long its lap took. Pair it with a single LAST cell in
                # the same provider frame only after every handle in that frame
                # has been materialized, so r_c/t_p ordering cannot alter facts.
                self._reconcile_frame_tracker_lap_states(connection, frame.id)
                self._reconcile_tracker_lap_sources(connection, frame.id)
                # A t_p and an outbound STATE can be ordered either way in a
                # SignalR frame. Apply source pit boundaries only after tracker
                # chronology is complete, so tyre/stint ledgers are invariant.
                self._reconcile_frame_pit_events(connection)
        finally:
            self._pending_pit_events = []
        # Derived tactical state is deliberately downstream of the committed
        # normalized facts. A retry reuses the same frame time/source key and
        # is idempotent in metric_current and metric_samples.
        last = contexts[-1]
        self._metric_runner.process_frame(
            connection,
            source_heat_id=self.heat_id,
            source_frame_id=frame.id,
            observed_at_us=frame.received_at_us,
            source_message_id=last.id,
            source_key=last.source_key,
            replay_active=self._replay_active,
        )

    def _prime(self, connection: sqlite3.Connection) -> None:
        """Restore in-memory sparse state from already committed derived frames."""
        if self._primed:
            return
        rows = connection.execute(
            """
            SELECT id,ingest_connection_id,frame_sequence,received_at_us
            FROM feed_frames
            WHERE analysis_session_id = ? AND decode_state = 'decoded' AND processed_at_us IS NOT NULL
            ORDER BY id
            """,
            (self.analysis_session_id,),
        ).fetchall()
        for row in rows:
            frame = StoredFrame(
                id=int(row["id"]),
                connection_id=row["ingest_connection_id"],
                sequence=int(row["frame_sequence"]),
                received_at_us=int(row["received_at_us"]),
                source_key=f"{row['ingest_connection_id']}:{row['frame_sequence']}",
            )
            for context in self._frame_messages(connection, frame):
                self._apply_message(connection, context, write=False)
        self._primed = True

    def _frame_messages(self, connection: sqlite3.Connection, frame: StoredFrame) -> tuple[FrameMessage, ...]:
        rows = connection.execute(
            """
            SELECT id,ordinal,handle,args_json,compressed
            FROM feed_messages
            WHERE frame_id = ?
            ORDER BY ordinal
            """,
            (frame.id,),
        ).fetchall()
        result: list[FrameMessage] = []
        for row in rows:
            try:
                args = json.loads(row["args_json"])
            except (TypeError, json.JSONDecodeError) as error:
                raise NormalizerError(f"Stored feed message {row['id']} has invalid args JSON") from error
            if not isinstance(args, list):
                raise NormalizerError(f"Stored feed message {row['id']} args are not an array")
            ordinal = int(row["ordinal"])
            result.append(
                FrameMessage(
                    id=int(row["id"]),
                    ordinal=ordinal,
                    handle=row["handle"],
                    args=tuple(args),
                    compressed=bool(row["compressed"]),
                    source_key=f"{frame.source_key}:{ordinal}",
                    received_at_us=frame.received_at_us,
                    connection_id=frame.connection_id,
                )
            )
        return tuple(result)

    def _ensure_heat(self, connection: sqlite3.Connection, observed_at_us: int) -> int:
        if self._heat_id is not None:
            return self._heat_id
        row = connection.execute(
            """
            SELECT id FROM source_heats
            WHERE analysis_session_id = ?
            ORDER BY generation DESC
            LIMIT 1
            """,
            (self.analysis_session_id,),
        ).fetchone()
        if row is None:
            generation = 1
            connection.execute(
                """
                INSERT INTO source_heats(analysis_session_id,generation,created_at_us)
                VALUES (?,?,?)
                """,
                (self.analysis_session_id, generation, observed_at_us),
            )
            self._heat_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        else:
            self._heat_id = int(row["id"])
        return self._heat_id

    @property
    def heat_id(self) -> int:
        if self._heat_id is None:
            raise NormalizerError("source heat is required before a normalized write")
        return self._heat_id

    def _clock(self, connection_id: str) -> ConnectionClockCalibrator:
        return self._calibrators.setdefault(connection_id, ConnectionClockCalibrator())

    def _latest_calibration_id(self, connection: sqlite3.Connection, connection_id: str) -> int | None:
        row = connection.execute(
            """
            SELECT id FROM connection_clock_calibrations
            WHERE ingest_connection_id = ?
            ORDER BY sample_count DESC, id DESC
            LIMIT 1
            """,
            (connection_id,),
        ).fetchone()
        return int(row["id"]) if row is not None else None

    def _apply_message(self, connection: sqlite3.Connection, context: FrameMessage, *, write: bool) -> None:
        payload = context.payload
        handle = context.handle
        if handle in {"s_i", "s_t"}:
            self._observe_clock(connection, context, write=write)
            # h_i normally arrives before the first server-clock sample. Write
            # the heat again once calibrated so session elapsed time is based
            # on the provider clock rather than the recorder start time.
            if write and self.heat:
                self._write_heat(connection, context)
            if write and self.statistics:
                self._write_statistics(connection, context)
            return
        if handle in {"h_i", "h_h"}:
            patch = _payload_mapping(payload)
            if patch is None:
                return
            old_flag = canonical_flag(self.heat.get("f")) if "f" in self.heat else None
            if handle == "h_i":
                next_start = parse_ts_time(patch.get("s"))
                is_new_heat = (
                    next_start is not None
                    and self._provider_heat_start_ts is not None
                    and next_start != self._provider_heat_start_ts
                )
                if is_new_heat:
                    if write:
                        self._start_new_heat(connection, context)
                    self.grid = ResultGrid()
                    self.statistics = {}
                    self._layout_id = None
                    self._finish_sector_ids.clear()
                    old_flag = None
                if next_start is not None:
                    self._provider_heat_start_ts = next_start
            self.heat = _deep_merge(self.heat, patch) if handle == "h_h" else copy.deepcopy(dict(patch))
            if write:
                self._write_heat(connection, context)
                if "f" in patch:
                    new_flag = canonical_flag(self.heat.get("f"))
                    if old_flag is None or not self._same_flag_values(old_flag, new_flag):
                        self._write_immediate_flag(connection, context, new_flag)
            return
        if handle == "r_l":
            self.grid.set_layout(payload)
            self._layout_id = None
            if write:
                self._ensure_layout(connection, context)
            return
        if handle == "r_i":
            self.grid.apply_snapshot(payload)
            self._layout_id = None
            if write:
                self._write_result_changes(connection, context, _payload_mapping(payload).get("r") if _payload_mapping(payload) else None)
            return
        if handle == "r_c":
            self.grid.apply_changes(payload)
            if write:
                self._write_result_changes(connection, context, payload)
            return
        if handle == "r_d":
            self.grid.remove_rows(payload)
            return
        if handle == "t_i":
            tracker = _payload_mapping(payload)
            if tracker is not None:
                self._update_tracker_topology(tracker)
                if write:
                    self._write_tracker_passings(connection, context, tracker.get("d"), allow_lap_completion=False)
            return
        if handle == "t_p":
            if write:
                self._write_tracker_passings(connection, context, payload, allow_lap_completion=True)
            return
        if handle == "a_i":
            patch = _payload_mapping(payload)
            if patch is not None:
                self.statistics = copy.deepcopy(dict(patch))
                if write:
                    self._write_statistics(connection, context)
            return
        if handle == "a_u":
            patch = _payload_mapping(payload)
            if patch is not None:
                self.statistics = _deep_merge(self.statistics, patch)
                if write:
                    self._write_statistics(connection, context)
            return
        if handle == "a_r":
            self.statistics = {}
            if write:
                # ``a_r`` resets the provider's current Statistics view. Raw
                # feed messages and append-only history rows remain available
                # for replay; only materialized current snapshots are cleared.
                connection.execute("DELETE FROM heat_statistics_current WHERE source_heat_id = ?", (self.heat_id,))
                connection.execute("DELETE FROM source_statistics_current WHERE source_heat_id = ?", (self.heat_id,))

    def _start_new_heat(self, connection: sqlite3.Connection, context: FrameMessage) -> None:
        """Close an observed provider heat reset without treating a reconnect as one."""
        previous_id = self.heat_id
        previous_finish = self._clock(context.connection_id).to_utc_us(parse_ts_time(self.heat.get("e")))
        if previous_finish is not None:
            connection.execute(
                """
                UPDATE source_heats
                SET provider_finished_at_us = COALESCE(provider_finished_at_us, ?)
                WHERE id = ?
                """,
                (previous_finish, previous_id),
            )
        generation = int(
            connection.execute(
                "SELECT COALESCE(MAX(generation), 0) + 1 FROM source_heats WHERE analysis_session_id = ?",
                (self.analysis_session_id,),
            ).fetchone()[0]
        )
        cursor = connection.execute(
            "INSERT INTO source_heats(analysis_session_id,generation,created_at_us) VALUES (?,?,?)",
            (self.analysis_session_id, generation, context.received_at_us),
        )
        self._heat_id = int(cursor.lastrowid)

    def _observe_clock(self, connection: sqlite3.Connection, context: FrameMessage, *, write: bool) -> None:
        clock = self._clock(context.connection_id)
        offset = clock.observe_server_time(context.handle, list(context.args), context.received_at_us)
        if offset is None or not write:
            return
        raw = context.payload
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)) and len(raw) == 1:
            raw = raw[0]
        provider_ts = parse_ts_time(raw)
        if provider_ts is None:
            return
        event_key = f"{context.source_key}:clock"
        _insert_ignore(
            connection,
            "connection_clock_samples",
            {
                "ingest_connection_id": context.connection_id,
                "source_heat_id": self.heat_id,
                "provider_timestamp_raw": _text(raw) or "",
                "provider_timestamp_us": provider_ts,
                "provider_timestamp_kind": "ts_time",
                "received_at_us": context.received_at_us,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "source_event_key": event_key,
                "created_at_us": now_us(),
            },
        )
        calibration_key = f"median:{clock.sample_count}:{context.source_key}"
        _insert_ignore(
            connection,
            "connection_clock_calibrations",
            {
                "ingest_connection_id": context.connection_id,
                "source_heat_id": self.heat_id,
                "calibration_key": calibration_key,
                "provider_timestamp_kind": "ts_time",
                "offset_us": offset if clock.offset_us is None else clock.offset_us,
                "sample_count": clock.sample_count,
                "median_abs_deviation_us": None,
                "valid_from_provider_us": provider_ts,
                "valid_to_provider_us": None,
                "valid_from_observed_at_us": context.received_at_us,
                "valid_to_observed_at_us": None,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "created_at_us": now_us(),
            },
        )

    def _write_heat(self, connection: sqlite3.Connection, context: FrameMessage) -> None:
        provider_start = parse_ts_time(self.heat.get("s"))
        provider_finish = parse_ts_time(self.heat.get("e"))
        calibrated_start = self._clock(context.connection_id).to_utc_us(provider_start)
        calibrated_finish = self._clock(context.connection_id).to_utc_us(provider_finish)
        connection.execute(
            """
            UPDATE source_heats
            SET external_name = COALESCE(?, external_name),
                provider_started_at_us = COALESCE(?, provider_started_at_us),
                provider_finished_at_us = COALESCE(?, provider_finished_at_us)
            WHERE id = ?
            """,
            (_text(self.heat.get("n")), calibrated_start, calibrated_finish, self.heat_id),
        )

    def _ensure_layout(self, connection: sqlite3.Connection, context: FrameMessage) -> int | None:
        if self.grid.layout is None:
            return None
        raw_layout = _json(self.grid.layout)
        layout_fingerprint = _fingerprint(self.grid.layout)
        existing = connection.execute(
            """
            SELECT id FROM result_layout_versions
            WHERE source_heat_id = ? AND layout_fingerprint = ?
            """,
            (self.heat_id, layout_fingerprint),
        ).fetchone()
        if existing is not None:
            self._layout_id = int(existing["id"])
            return self._layout_id
        ordinal = int(
            connection.execute(
                "SELECT COALESCE(MAX(version_ordinal), -1) + 1 FROM result_layout_versions WHERE source_heat_id = ?",
                (self.heat_id,),
            ).fetchone()[0]
        )
        cursor = connection.execute(
            """
            INSERT INTO result_layout_versions(
              source_heat_id,version_ordinal,layout_fingerprint,raw_layout_json,source_message_id,
              source_key,observed_at_us,created_at_us
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                self.heat_id,
                ordinal,
                layout_fingerprint,
                raw_layout,
                context.id,
                context.source_key,
                context.received_at_us,
                now_us(),
            ),
        )
        self._layout_id = int(cursor.lastrowid)
        headers: Any = self.grid.layout.get("h") if isinstance(self.grid.layout, Mapping) else None
        if headers is None and isinstance(self.grid.layout, Mapping) and isinstance(self.grid.layout.get("l"), Mapping):
            headers = self.grid.layout["l"].get("h")
        for index, column in self.grid.columns.items():
            raw_definition = headers[index] if isinstance(headers, list) and index < len(headers) else None
            display_name = raw_definition.get("c") if isinstance(raw_definition, Mapping) else None
            connection.execute(
                """
                INSERT INTO result_column_definitions(
                  layout_version_id,column_index,source_name_raw,source_parameter_raw,display_name_raw,
                  canonical_key,raw_definition_json
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (
                    self._layout_id,
                    index,
                    column.source_name,
                    column.source_parameter,
                    _text(display_name),
                    column.key,
                    _json(raw_definition),
                ),
            )
        return self._layout_id

    def _write_result_changes(self, connection: sqlite3.Connection, context: FrameMessage, changes: Any) -> None:
        layout_id = self._ensure_layout(connection, context)
        if layout_id is None:
            return
        changed_rows: set[int] = set()
        for ordinal, change in enumerate(_changes(changes)):
            if len(change) < 3 or type(change[0]) is not int or type(change[1]) is not int:
                continue
            row_index, column_index = change[0], change[1]
            if row_index < 0 or column_index < 0:
                continue
            participant_id = self._participant_for_row(connection, context, row_index)
            raw_values = list(change[2:])
            _insert_ignore(
                connection,
                "participant_result_cell_observations",
                {
                    "source_heat_id": self.heat_id,
                    "participant_id": participant_id,
                    "layout_version_id": layout_id,
                    "provider_row_index": row_index,
                    "column_index": column_index,
                    "raw_value_json": _json(raw_values),
                    "value_text": _text(raw_values[0]) if raw_values else None,
                    "source_message_id": context.id,
                    "source_key": context.source_key,
                    "source_change_ordinal": ordinal,
                    "observed_at_us": context.received_at_us,
                    "created_at_us": now_us(),
                },
            )
            changed_rows.add(row_index)
        for row_index in sorted(changed_rows):
            self._write_row_state(connection, context, row_index, layout_id)

    def _row_event_cells(
        self,
        connection: sqlite3.Connection,
        *,
        participant_id: str,
        source_message_id: int,
    ) -> dict[str, ResultCellFact]:
        """Return only unambiguous timing cells changed by this source message.

        The materialized ResultGrid is deliberately not used as event evidence:
        it retains a prior sparse value until the provider sends a replacement.
        A duplicate canonical cell in one source message is ambiguous and is
        therefore omitted instead of guessed.
        """

        rows = connection.execute(
            """
            SELECT definition.canonical_key,observation.id,observation.value_text,
                   observation.source_message_id,observation.source_key
            FROM participant_result_cell_observations AS observation
            JOIN result_column_definitions AS definition
              ON definition.layout_version_id = observation.layout_version_id
             AND definition.column_index = observation.column_index
            WHERE observation.source_heat_id = ?
              AND observation.participant_id = ?
              AND observation.source_message_id = ?
              AND definition.canonical_key IN ('state','pit_stops','pit_time','driver_stint','laps')
            ORDER BY definition.canonical_key,observation.id
            """,
            (self.heat_id, participant_id, source_message_id),
        ).fetchall()
        candidates: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            key = row["canonical_key"]
            if isinstance(key, str):
                candidates.setdefault(key, []).append(row)
        result: dict[str, ResultCellFact] = {}
        for key, values in candidates.items():
            if len(values) != 1:
                continue
            value = values[0]
            result[key] = ResultCellFact(
                id=int(value["id"]),
                value_text=value["value_text"],
                source_message_id=int(value["source_message_id"]),
                source_key=value["source_key"],
            )
        return result

    def _participant_for_row(self, connection: sqlite3.Connection, context: FrameMessage, row_index: int) -> str | None:
        row = self.grid.row_values(row_index)
        start_number = _text(row.get("start_number"))
        team_name = _text(row.get("team_name"))
        class_name = _text(row.get("class_name"))
        car_name = _text(row.get("car_name"))
        driver_name = _text(row.get("current_driver"))
        if not any((start_number, team_name, class_name, car_name, driver_name)):
            return None
        team_key = _key(team_name)
        start_key = _key(start_number)
        car_key = _key(car_name)
        class_key = _key(class_name)
        existing_ours_conflict = None
        if start_key == OUR_START_NUMBER and team_key not in {None, OUR_TEAM_KEY}:
            existing_ours_conflict = connection.execute(
                """
                SELECT id FROM participants
                WHERE source_heat_id = ? AND start_number_key = ? AND team_name_key = ? AND is_ours = 1
                LIMIT 1
                """,
                (self.heat_id, start_key, OUR_TEAM_KEY),
            ).fetchone()
        matched_without_number = None
        if start_key is None and team_key is not None:
            clauses = ["source_heat_id = ?", "team_name_key = ?"]
            parameters: list[Any] = [self.heat_id, team_key]
            if class_key is not None:
                clauses.append("class_name_key = ?")
                parameters.append(class_key)
            matched_without_number = connection.execute(
                f"SELECT id,external_key FROM participants WHERE {' AND '.join(clauses)} "
                "ORDER BY is_ours DESC,last_seen_at_us DESC LIMIT 1",
                tuple(parameters),
            ).fetchone()
        if existing_ours_conflict is not None:
            external_key = f"conflict:nr:{start_key}:team:{team_key}"
        elif matched_without_number is not None:
            external_key = matched_without_number["external_key"]
        elif start_key:
            external_key = f"nr:{start_key}"
        elif team_key:
            external_key = f"team:{team_key}:class:{class_key or ''}"
        else:
            external_key = f"row:{row_index}"
        participant_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"balchug-racing:{self.heat_id}:{external_key}"))
        is_ours = int(
            existing_ours_conflict is None
            and (team_key == OUR_TEAM_KEY or (team_key is None and start_key == OUR_START_NUMBER))
        )
        participant_values = {
            "id": participant_id,
            "source_heat_id": self.heat_id,
            "external_key": external_key,
            "transponder_id": None,
            "start_number": start_number,
            "team_name": team_name,
            "car_name": car_name,
            "class_name": class_name,
            "is_ours": is_ours,
            "active": 1,
            "first_seen_at_us": context.received_at_us,
            "last_seen_at_us": context.received_at_us,
            "identity_key": external_key,
            "start_number_key": start_key,
            "team_name_key": team_key,
            "car_name_key": car_key,
            "class_name_key": class_key,
            "identity_source_message_id": context.id,
            "identity_source_key": context.source_key,
            "identity_observed_at_us": context.received_at_us,
        }
        participant_updates = [
            "is_ours",
            "active",
            "last_seen_at_us",
            "identity_key",
            "identity_source_message_id",
            "identity_source_key",
            "identity_observed_at_us",
        ]
        # A sparse layout can omit NR/CAR/CLS for one tick. Preserve the last
        # source observation instead of erasing identity and creating a second
        # participant when the header returns.
        for raw_column, key_column in (
            ("start_number", "start_number_key"),
            ("team_name", "team_name_key"),
            ("car_name", "car_name_key"),
            ("class_name", "class_name_key"),
        ):
            if participant_values[raw_column] is not None:
                participant_updates.extend((raw_column, key_column))
        _upsert(
            connection,
            "participants",
            participant_values,
            conflict_columns=("source_heat_id", "external_key"),
            update_columns=tuple(participant_updates),
        )
        # Re-read because the deterministic identifier is only a proposed key;
        # the unique source_heat/external_key constraint owns the actual row.
        resolved = connection.execute(
            "SELECT id FROM participants WHERE source_heat_id = ? AND external_key = ?",
            (self.heat_id, external_key),
        ).fetchone()
        if resolved is None:
            raise NormalizerError("Participant upsert did not produce a row")
        participant_id = resolved["id"]
        identity_fingerprint = _fingerprint(
            {
                "start_number": start_number,
                "team": team_name,
                "car": car_name,
                "class": class_name,
                "driver": driver_name,
            }
        )
        _insert_ignore(
            connection,
            "participant_identity_observations",
            {
                "source_heat_id": self.heat_id,
                "participant_id": participant_id,
                "external_key_raw": external_key,
                "transponder_id_raw": None,
                "start_number_raw": start_number,
                "start_number_key": start_key,
                "team_name_raw": team_name,
                "team_name_key": team_key,
                "car_name_raw": car_name,
                "car_name_key": car_key,
                "class_name_raw": class_name,
                "class_name_key": class_key,
                "driver_name_raw": driver_name,
                "driver_name_key": _key(driver_name),
                "identity_fingerprint": identity_fingerprint,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "source_event_key": f"{context.source_key}:identity:{row_index}",
                "observed_at_us": context.received_at_us,
                "created_at_us": now_us(),
            },
        )
        self._write_identity_segment(
            connection,
            context,
            participant_id,
            identity_fingerprint,
            start_number,
            team_name,
            car_name,
            class_name,
            driver_name,
        )
        if existing_ours_conflict is not None:
            self._emit_stream_event(
                connection,
                context,
                event_type="identity_conflict",
                payload={
                    "expected_start_number": OUR_START_NUMBER,
                    "expected_team": OUR_TEAM_NAME,
                    "observed_start_number": start_number,
                    "observed_team": team_name,
                    "observed_class": class_name,
                    "participant_id": participant_id,
                },
            )
        if team_key == OUR_TEAM_KEY:
            connection.execute(
                """
                UPDATE analysis_sessions
                SET our_participant_id = ?, our_class = ?, identity_state = 'resolved', updated_at_us = ?
                WHERE id = ?
                """,
                (participant_id, class_name, context.received_at_us, self.analysis_session_id),
            )
        elif start_key == OUR_START_NUMBER and team_key not in {None, OUR_TEAM_KEY}:
            connection.execute(
                """
                UPDATE analysis_sessions
                SET identity_state = CASE WHEN our_participant_id IS NULL THEN 'unresolved' ELSE identity_state END,
                    updated_at_us = ?
                WHERE id = ?
                """,
                (context.received_at_us, self.analysis_session_id),
            )
        return participant_id

    def _emit_stream_event(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> None:
        """Emit a replay-safe exceptional event without making it dashboard data."""
        exists = connection.execute(
            """
            SELECT 1 FROM stream_events
            WHERE analysis_session_id = ? AND source_message_id = ? AND event_type = ?
            LIMIT 1
            """,
            (self.analysis_session_id, context.id, event_type),
        ).fetchone()
        if exists is None:
            connection.execute(
                """
                INSERT INTO stream_events(
                  analysis_session_id,source_heat_id,source_message_id,source_key,event_type,payload_json,created_at_us
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (
                    self.analysis_session_id,
                    self.heat_id,
                    context.id,
                    context.source_key,
                    event_type,
                    _json(payload),
                    now_us(),
                ),
            )

    def _write_identity_segment(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        participant_id: str,
        identity_fingerprint: str,
        start_number: str | None,
        team_name: str | None,
        car_name: str | None,
        class_name: str | None,
        driver_name: str | None,
    ) -> None:
        current = connection.execute(
            """
            SELECT id,identity_fingerprint FROM participant_identity_segments
            WHERE source_heat_id = ? AND participant_id = ? AND ended_at_us IS NULL
            ORDER BY started_at_us DESC LIMIT 1
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        if current is not None and current["identity_fingerprint"] == identity_fingerprint:
            return
        if current is not None:
            connection.execute(
                """
                UPDATE participant_identity_segments
                SET ended_at_us = ?, ended_observed_at_us = ?, updated_at_us = ?
                WHERE id = ?
                """,
                (context.received_at_us, context.received_at_us, now_us(), current["id"]),
            )
        segment_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"balchug-racing:segment:{self.heat_id}:{participant_id}:{context.source_key}:{identity_fingerprint}",
            )
        )
        _insert_ignore(
            connection,
            "participant_identity_segments",
            {
                "id": segment_id,
                "source_heat_id": self.heat_id,
                "participant_id": participant_id,
                "team_name": team_name,
                "car_name": car_name,
                "class_name": class_name,
                "driver_name_raw": driver_name,
                "driver_name_key": _key(driver_name),
                "started_at_us": context.received_at_us,
                "ended_at_us": None,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "created_at_us": now_us(),
                "updated_at_us": now_us(),
                "start_number_raw": start_number,
                "start_number_key": _key(start_number),
                "team_name_key": _key(team_name),
                "car_name_key": _key(car_name),
                "class_name_key": _key(class_name),
                "identity_fingerprint": identity_fingerprint,
                "observed_at_us": context.received_at_us,
                "ended_observed_at_us": None,
            },
        )

    def _write_row_state(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        row_index: int,
        layout_id: int,
    ) -> None:
        participant_id = self._participant_for_row(connection, context, row_index)
        if participant_id is None:
            return
        row = self.grid.row_values(row_index)
        current_state = (
            parse_result_state(row.get("state")) if "state" in row else ResultState(raw=None, kind="UNKNOWN")
        )
        current_pit_count_raw = _text(row.get("pit_stops"))
        current_pit_count = _integer(row.get("pit_stops"), minimum=0)
        current_pit_time_raw = _text(row.get("pit_time"))
        old = connection.execute(
            """
            SELECT * FROM participant_state_current
            WHERE source_heat_id = ? AND participant_id = ?
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        event_cells = self._row_event_cells(
            connection,
            participant_id=participant_id,
            source_message_id=context.id,
        )
        state_cell = event_cells.get("state")
        pit_count_cell = event_cells.get("pit_stops")
        pit_time_cell = event_cells.get("pit_time")
        driver_stint_cell = event_cells.get("driver_stint")
        lap_cell = event_cells.get("laps")
        event_state = parse_result_state(state_cell.value_text) if state_cell is not None else None
        event_pit_count = _integer(pit_count_cell.value_text, minimum=0) if pit_count_cell is not None else None
        event_driver_stint = _parse_driver_stint(driver_stint_cell.value_text) if driver_stint_cell is not None else None
        current_driver_stint = _parse_driver_stint(row.get("driver_stint"))
        # A reconnect r_i repeats every visible cell. Treat an unchanged
        # L-PIT value as display state, not a new duration fact for an exit.
        # A repeated value in an explicit r_c is still a fresh source event:
        # two real pit stops can legitimately have the same duration.
        pit_time_event = (
            pit_time_cell
            if pit_time_cell is not None
            and (
                context.handle == "r_c"
                or old is None
                or pit_time_cell.value_text != old["pit_time_raw"]
            )
            else None
        )
        clock = self._clock(context.connection_id)
        timer_at_us = clock.to_utc_us(current_state.timer_target_ts_time)
        calibration_id = self._latest_calibration_id(connection, context.connection_id)
        sectors = {
            key: row[key]
            for key in sorted(row)
            if key.startswith("sector_")
        }
        state_source = state_cell if state_cell is not None else None
        pit_count_source = pit_count_cell if pit_count_cell is not None else None
        pit_time_source = pit_time_cell if pit_time_cell is not None else None
        driver_stint_source = driver_stint_cell if driver_stint_cell is not None else None
        state_event_key = f"{context.source_key}:state:{row_index}"
        observed = _insert_ignore(
            connection,
            "participant_state_observations",
            {
                "source_heat_id": self.heat_id,
                "participant_id": participant_id,
                "layout_version_id": layout_id,
                "provider_row_index": row_index,
                "state_raw": state_cell.value_text if state_cell is not None else None,
                "state_kind": event_state.kind if event_state is not None else "UNKNOWN",
                "state_timer_target_raw": event_state.timer_target_raw if event_state is not None else None,
                "state_timer_target_provider_us": event_state.timer_target_ts_time if event_state is not None else None,
                "state_timer_target_at_us": clock.to_utc_us(event_state.timer_target_ts_time)
                if event_state is not None
                else None,
                "state_timer_calibration_id": calibration_id if event_state is not None else None,
                "provider_pit_count_raw": pit_count_cell.value_text if pit_count_cell is not None else None,
                "provider_pit_count": event_pit_count,
                "state_cell_observation_id": state_cell.id if state_cell is not None else None,
                "provider_pit_count_cell_observation_id": pit_count_cell.id if pit_count_cell is not None else None,
                "pit_time_raw": pit_time_cell.value_text if pit_time_cell is not None else None,
                "pit_time_cell_observation_id": pit_time_cell.id if pit_time_cell is not None else None,
                "driver_stint_raw": driver_stint_cell.value_text if driver_stint_cell is not None else None,
                "driver_stint_kind": event_driver_stint.kind if event_driver_stint is not None else None,
                "driver_stint_provider_ts_time": event_driver_stint.provider_ts_time
                if event_driver_stint is not None
                else None,
                "driver_stint_at_us": clock.to_utc_us(event_driver_stint.provider_ts_time)
                if event_driver_stint is not None and event_driver_stint.provider_ts_time is not None
                else None,
                "driver_stint_calibration_id": calibration_id
                if event_driver_stint is not None and event_driver_stint.provider_ts_time is not None
                else None,
                "driver_stint_duration_ms": event_driver_stint.duration_ms if event_driver_stint is not None else None,
                "driver_stint_cell_observation_id": driver_stint_cell.id if driver_stint_cell is not None else None,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "source_event_key": state_event_key,
                "observed_at_us": context.received_at_us,
                "created_at_us": now_us(),
            },
        )
        _upsert(
            connection,
            "participant_state_current",
            {
                "source_heat_id": self.heat_id,
                "participant_id": participant_id,
                "position_overall": _integer(row.get("position_overall"), minimum=0),
                "position_class": _integer(row.get("position_class"), minimum=0),
                "marker": _text(row.get("marker")),
                "laps": _integer(row.get("laps"), minimum=0),
                "state": current_state.kind,
                "state_raw": _text(current_state.raw),
                "state_kind": current_state.kind,
                "current_driver_name": _text(row.get("current_driver")),
                "current_driver_stint_raw": current_driver_stint.raw,
                "last_lap_ms": _duration_us_to_ms(row.get("last_lap")),
                "last_lap_number": None,
                "best_lap_ms": _duration_us_to_ms(row.get("best_lap")),
                "best_lap_number": None,
                "last_sectors_json": _json(sectors),
                "best_sectors_json": None,
                "last_speeds_json": None,
                "gap_ms": _gap_ms(row.get("gap")),
                "gap_raw": _text(row.get("gap")),
                "gap_kind": "TIME" if _gap_ms(row.get("gap")) is not None else None,
                "diff_ms": _gap_ms(row.get("diff")),
                "diff_raw": _text(row.get("diff")),
                "diff_kind": "TIME" if _gap_ms(row.get("diff")) is not None else None,
                "sector_json": _json(sectors),
                "speed_kph": _number(row.get("speed")),
                "pit_time_raw": current_pit_time_raw,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "updated_at_us": context.received_at_us,
                "state_timer_target_raw": current_state.timer_target_raw
                if state_source is not None
                else (old["state_timer_target_raw"] if old is not None else None),
                "state_timer_target_provider_us": current_state.timer_target_ts_time
                if state_source is not None
                else (old["state_timer_target_provider_us"] if old is not None else None),
                "state_timer_target_at_us": timer_at_us
                if state_source is not None
                else (old["state_timer_target_at_us"] if old is not None else None),
                "state_timer_calibration_id": calibration_id
                if state_source is not None
                else (old["state_timer_calibration_id"] if old is not None else None),
                "state_timer_source_message_id": context.id
                if state_source is not None
                else (old["state_timer_source_message_id"] if old is not None else None),
                "state_timer_source_key": context.source_key
                if state_source is not None
                else (old["state_timer_source_key"] if old is not None else None),
                "state_timer_observed_at_us": context.received_at_us
                if state_source is not None
                else (old["state_timer_observed_at_us"] if old is not None else None),
                "state_source_cell_observation_id": state_source.id
                if state_source is not None
                else (old["state_source_cell_observation_id"] if old is not None else None),
                "provider_pit_count": current_pit_count,
                "provider_pit_count_raw": current_pit_count_raw,
                "provider_pit_count_source_message_id": context.id
                if pit_count_source is not None
                else (old["provider_pit_count_source_message_id"] if old is not None else None),
                "provider_pit_count_source_key": context.source_key
                if pit_count_source is not None
                else (old["provider_pit_count_source_key"] if old is not None else None),
                "provider_pit_count_observed_at_us": context.received_at_us
                if pit_count_source is not None
                else (old["provider_pit_count_observed_at_us"] if old is not None else None),
                "provider_pit_count_source_cell_observation_id": pit_count_source.id
                if pit_count_source is not None
                else (old["provider_pit_count_source_cell_observation_id"] if old is not None else None),
                "pit_time_source_cell_observation_id": pit_time_source.id
                if pit_time_source is not None
                else (old["pit_time_source_cell_observation_id"] if old is not None else None),
                "pit_time_source_message_id": context.id
                if pit_time_source is not None
                else (old["pit_time_source_message_id"] if old is not None else None),
                "pit_time_source_key": context.source_key
                if pit_time_source is not None
                else (old["pit_time_source_key"] if old is not None else None),
                "pit_time_observed_at_us": context.received_at_us
                if pit_time_source is not None
                else (old["pit_time_observed_at_us"] if old is not None else None),
                "driver_stint_kind": current_driver_stint.kind
                if driver_stint_source is not None
                else (old["driver_stint_kind"] if old is not None else None),
                "driver_stint_provider_ts_time": current_driver_stint.provider_ts_time
                if driver_stint_source is not None
                else (old["driver_stint_provider_ts_time"] if old is not None else None),
                "driver_stint_at_us": clock.to_utc_us(current_driver_stint.provider_ts_time)
                if driver_stint_source is not None and current_driver_stint.provider_ts_time is not None
                else (old["driver_stint_at_us"] if old is not None else None),
                "driver_stint_calibration_id": calibration_id
                if driver_stint_source is not None and current_driver_stint.provider_ts_time is not None
                else (old["driver_stint_calibration_id"] if old is not None else None),
                "driver_stint_duration_ms": current_driver_stint.duration_ms
                if driver_stint_source is not None
                else (old["driver_stint_duration_ms"] if old is not None else None),
                "driver_stint_source_cell_observation_id": driver_stint_source.id
                if driver_stint_source is not None
                else (old["driver_stint_source_cell_observation_id"] if old is not None else None),
                "driver_stint_source_message_id": context.id
                if driver_stint_source is not None
                else (old["driver_stint_source_message_id"] if old is not None else None),
                "driver_stint_source_key": context.source_key
                if driver_stint_source is not None
                else (old["driver_stint_source_key"] if old is not None else None),
                "driver_stint_observed_at_us": context.received_at_us
                if driver_stint_source is not None
                else (old["driver_stint_observed_at_us"] if old is not None else None),
            },
            conflict_columns=("source_heat_id", "participant_id"),
        )
        if observed:
            source_laps = _integer(lap_cell.value_text, minimum=0) if lap_cell is not None else None
            if (
                old is not None
                and old["laps"] is not None
                and source_laps is not None
                and source_laps > int(old["laps"])
            ):
                self._complete_laps_from_grid(
                    connection,
                    context,
                    participant_id,
                    previous_lap=int(old["laps"]),
                    current_lap=source_laps,
                    last_lap_ms=_duration_us_to_ms(row.get("last_lap")),
                    state_kind=current_state.kind,
                )
            # Keep an active stint available to same-frame tracker passings,
            # but defer pit boundary mutations until every frame handle has
            # supplied its chronology.
            self._ensure_tire_stint(connection, context, participant_id, _integer(row.get("laps"), minimum=0))
            self._pending_pit_events.append(
                PendingPitEvent(
                    context=context,
                    participant_id=participant_id,
                    state_event=event_state,
                    state_cell=state_cell,
                    pit_count_event=event_pit_count,
                    pit_count_cell=pit_count_cell,
                    lap_number=_integer(row.get("laps"), minimum=0),
                    pit_time_event=pit_time_event,
                    previous=old,
                )
            )

    def _complete_laps_from_grid(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        participant_id: str,
        *,
        previous_lap: int,
        current_lap: int,
        last_lap_ms: int | None,
        state_kind: str,
    ) -> None:
        """Create source-numbered lap rows for an explicit LAPS increase.

        If the provider skips numbers, the intermediate rows are retained with
        unknown duration/completion time rather than inventing individual laps.
        """
        last_cell = self._result_cell_observation(
            connection,
            participant_id=participant_id,
            source_message_id=context.id,
            canonical_key="last_lap",
        )
        source_sectors = self._result_sectors_for_lap(connection, participant_id=participant_id, last_cell=last_cell)
        for lap_number in range(previous_lap + 1, current_lap + 1):
            is_latest = lap_number == current_lap
            completed_at_us = context.received_at_us if is_latest else None
            has_source_duration = is_latest and last_lap_ms is not None and last_cell is not None
            has_source_sectors = is_latest and bool(source_sectors)
            _insert_ignore(
                connection,
                "laps",
                {
                    "id": str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"balchug-racing:grid-lap:{self.heat_id}:{participant_id}:{context.source_key}:{lap_number}",
                        )
                    ),
                    "source_heat_id": self.heat_id,
                    "participant_id": participant_id,
                    "lap_number": lap_number,
                    "completed_at_us": completed_at_us,
                    "duration_ms": last_lap_ms if has_source_duration else None,
                    "sectors_json": _json({key: value[0] for key, value in source_sectors.items()})
                    if has_source_sectors
                    else None,
                    "flag": self._current_flag_kind(connection),
                    "is_in_lap": int(is_latest and state_kind == "IN_PIT"),
                    "is_out_lap": int(is_latest and state_kind == "OUT_LAP"),
                    "crosses_pit": int(is_latest and state_kind == "IN_PIT"),
                    "is_clean": int(
                        is_latest
                        and self._is_clean_lap(connection, participant_id, completed_at_us, state_kind)
                    ),
                    "source_message_id": context.id,
                    "source_key": context.source_key,
                    "created_at_us": now_us(),
                    "completion_passing_observation_id": None,
                    "duration_source_cell_observation_id": int(last_cell["id"]) if has_source_duration else None,
                    "duration_source_message_id": context.id if has_source_duration else None,
                    "duration_source_key": context.source_key if has_source_duration else None,
                    "duration_source_kind": "RESULT_GRID_LAST" if has_source_duration else None,
                    "sectors_source_cell_observation_ids_json": _json(
                        {key: value[1] for key, value in source_sectors.items()}
                    )
                    if has_source_sectors
                    else None,
                },
            )

    def _result_cell_observation(
        self,
        connection: sqlite3.Connection,
        *,
        participant_id: str,
        source_message_id: int,
        canonical_key: str,
    ) -> sqlite3.Row | None:
        """Return one exact result-cell observation, otherwise fail closed."""

        rows = connection.execute(
            """
            SELECT observation.id,observation.value_text,observation.source_message_id,observation.source_key,
                   observation.source_change_ordinal,message.frame_id,message.ordinal AS message_ordinal
            FROM participant_result_cell_observations AS observation
            JOIN result_column_definitions AS definition
              ON definition.layout_version_id = observation.layout_version_id
             AND definition.column_index = observation.column_index
            JOIN feed_messages AS message ON message.id = observation.source_message_id
            WHERE observation.source_heat_id = ?
              AND observation.participant_id = ?
              AND observation.source_message_id = ?
              AND definition.canonical_key = ?
            ORDER BY observation.id
            LIMIT 2
            """,
            (self.heat_id, participant_id, source_message_id, canonical_key),
        ).fetchall()
        return rows[0] if len(rows) == 1 else None

    def _result_sectors_for_lap(
        self,
        connection: sqlite3.Connection,
        *,
        participant_id: str,
        last_cell: sqlite3.Row | None,
    ) -> dict[str, tuple[str | None, int]]:
        """Attach sectors observed since the preceding source LAST boundary.

        Sector cells often update independently from LAST. A value is usable
        only when it was observed after the prior LAST message and no later
        than this LAST message in source order. A result message is atomic:
        every sector in the current LAST message belongs to this boundary,
        even when the provider serializes it after the LAST cell. Without a
        preceding LAST, only sectors in the current message are provable.
        """

        if last_cell is None:
            return {}
        previous = connection.execute(
            """
            SELECT observation.id,observation.source_change_ordinal,
                   message.frame_id,message.ordinal AS message_ordinal
            FROM participant_result_cell_observations AS observation
            JOIN result_column_definitions AS definition
              ON definition.layout_version_id = observation.layout_version_id
             AND definition.column_index = observation.column_index
            JOIN feed_messages AS message ON message.id = observation.source_message_id
            WHERE observation.source_heat_id = ?
              AND observation.participant_id = ?
              AND definition.canonical_key = 'last_lap'
              AND (
                   message.frame_id < ?
                   OR (message.frame_id = ? AND message.ordinal < ?)
              )
            ORDER BY message.frame_id DESC,message.ordinal DESC,observation.source_change_ordinal DESC
            LIMIT 1
            """,
            (
                self.heat_id,
                participant_id,
                int(last_cell["frame_id"]),
                int(last_cell["frame_id"]),
                int(last_cell["message_ordinal"]),
            ),
        ).fetchone()
        if previous is None:
            rows = connection.execute(
                """
                SELECT definition.canonical_key,observation.id,observation.value_text,
                       message.frame_id,message.ordinal AS message_ordinal,observation.source_change_ordinal
                FROM participant_result_cell_observations AS observation
                JOIN result_column_definitions AS definition
                  ON definition.layout_version_id = observation.layout_version_id
                 AND definition.column_index = observation.column_index
                JOIN feed_messages AS message ON message.id = observation.source_message_id
                WHERE observation.source_heat_id = ?
                  AND observation.participant_id = ?
                  AND observation.source_message_id = ?
                  AND definition.canonical_key GLOB 'sector_[0-9]*'
                ORDER BY definition.canonical_key,observation.source_change_ordinal
                """,
                (self.heat_id, participant_id, int(last_cell["source_message_id"])),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT definition.canonical_key,observation.id,observation.value_text,
                       message.frame_id,message.ordinal AS message_ordinal,observation.source_change_ordinal
                FROM participant_result_cell_observations AS observation
                JOIN result_column_definitions AS definition
                  ON definition.layout_version_id = observation.layout_version_id
                 AND definition.column_index = observation.column_index
                JOIN feed_messages AS message ON message.id = observation.source_message_id
                WHERE observation.source_heat_id = ?
                  AND observation.participant_id = ?
                  AND definition.canonical_key GLOB 'sector_[0-9]*'
                  AND (
                       message.frame_id > ?
                       OR (message.frame_id = ? AND message.ordinal > ?)
                  )
                  AND (
                       message.frame_id < ?
                       OR (message.frame_id = ? AND message.ordinal <= ?)
                  )
                ORDER BY definition.canonical_key,message.frame_id,message.ordinal,observation.source_change_ordinal
                """,
                (
                    self.heat_id,
                    participant_id,
                    int(previous["frame_id"]),
                    int(previous["frame_id"]),
                    int(previous["message_ordinal"]),
                    int(last_cell["frame_id"]),
                    int(last_cell["frame_id"]),
                    int(last_cell["message_ordinal"]),
                ),
            ).fetchall()
        sectors: dict[str, tuple[str | None, int]] = {}
        for row in rows:
            key = row["canonical_key"]
            value = row["value_text"]
            if isinstance(key, str):
                # An unavailable source sector is deliberately represented as
                # null. It is not filled from tracker data or an older lap.
                sectors[key] = (value if _duration_us_to_ms(value) is not None else None, int(row["id"]))
        return sectors

    def _reconcile_frame_tracker_lap_states(self, connection: sqlite3.Connection, frame_id: int) -> None:
        """Make same-frame tracker lap state independent of handle ordering."""

        laps = connection.execute(
            """
            SELECT lap.id,lap.participant_id
            FROM laps AS lap
            JOIN tracker_passing_observations AS passing
              ON passing.id = lap.completion_passing_observation_id
            JOIN feed_messages AS message ON message.id = passing.source_message_id
            WHERE lap.source_heat_id = ? AND message.frame_id = ?
            """,
            (self.heat_id, frame_id),
        ).fetchall()
        for lap in laps:
            state = connection.execute(
                """
                SELECT state_kind FROM participant_state_current
                WHERE source_heat_id = ? AND participant_id = ?
                """,
                (self.heat_id, lap["participant_id"]),
            ).fetchone()
            state_kind = state["state_kind"] if state is not None else None
            if state_kind == "IN_PIT":
                connection.execute(
                    """
                    UPDATE laps
                    SET is_in_lap = 1,is_out_lap = 0,crosses_pit = 1,is_clean = 0
                    WHERE id = ?
                    """,
                    (lap["id"],),
                )
            elif state_kind == "OUT_LAP":
                connection.execute(
                    """
                    UPDATE laps
                    SET is_in_lap = 0,is_out_lap = 1,crosses_pit = 0,is_clean = 0
                    WHERE id = ?
                    """,
                    (lap["id"],),
                )

    def _reconcile_tracker_lap_sources(self, connection: sqlite3.Connection, frame_id: int) -> None:
        """Attach same-frame source LAST facts to no-LAPS tracker boundaries.

        A result-grid message and tracker passing can arrive in either order in
        one SignalR frame. The pair is valid only when the participant has one
        new finish boundary and one LAST-cell observation in that frame. This
        intentionally leaves duration/sector values NULL for any ambiguous or
        delayed source update instead of deriving them from tracker timestamps.
        """

        candidates = connection.execute(
            """
            SELECT lap.id,lap.participant_id,lap.is_clean,lap.crosses_pit
            FROM laps AS lap
            JOIN tracker_passing_observations AS passing
              ON passing.id = lap.completion_passing_observation_id
            JOIN feed_messages AS message ON message.id = passing.source_message_id
            WHERE lap.source_heat_id = ?
              AND message.frame_id = ?
              AND lap.duration_ms IS NULL
              AND lap.duration_source_cell_observation_id IS NULL
            ORDER BY lap.participant_id,lap.id
            """,
            (self.heat_id, frame_id),
        ).fetchall()
        if not candidates:
            return
        laps_by_participant: dict[str, list[sqlite3.Row]] = {}
        for lap in candidates:
            laps_by_participant.setdefault(lap["participant_id"], []).append(lap)

        last_rows = connection.execute(
            """
            SELECT observation.id,observation.participant_id,observation.value_text,
                   observation.source_message_id,observation.source_key,
                   observation.source_change_ordinal,message.frame_id,message.ordinal AS message_ordinal
            FROM participant_result_cell_observations AS observation
            JOIN result_column_definitions AS definition
              ON definition.layout_version_id = observation.layout_version_id
             AND definition.column_index = observation.column_index
            JOIN feed_messages AS message ON message.id = observation.source_message_id
            WHERE observation.source_heat_id = ?
              AND message.frame_id = ?
              AND definition.canonical_key = 'last_lap'
            ORDER BY observation.participant_id,observation.id
            """,
            (self.heat_id, frame_id),
        ).fetchall()
        last_by_participant: dict[str, list[sqlite3.Row]] = {}
        for last in last_rows:
            participant_id = last["participant_id"]
            if participant_id is not None:
                last_by_participant.setdefault(participant_id, []).append(last)

        for participant_id, laps in laps_by_participant.items():
            lasts = last_by_participant.get(participant_id, [])
            if len(laps) != 1 or len(lasts) != 1:
                continue
            lap = laps[0]
            last = lasts[0]
            duration_ms = _duration_us_to_ms(last["value_text"])
            if duration_ms is None:
                continue
            source_message_id = int(last["source_message_id"])
            source_sectors = self._result_sectors_for_lap(
                connection,
                participant_id=participant_id,
                last_cell=last,
            )
            state = connection.execute(
                """
                SELECT state_kind FROM participant_state_current
                WHERE source_heat_id = ? AND participant_id = ?
                """,
                (self.heat_id, participant_id),
            ).fetchone()
            state_kind = state["state_kind"] if state is not None else None
            is_in_lap = int(state_kind == "IN_PIT")
            is_out_lap = int(state_kind == "OUT_LAP")
            crosses_pit = int(bool(lap["crosses_pit"]) or state_kind == "IN_PIT")
            is_clean = 0 if state_kind in {"IN_PIT", "OUT_LAP"} else int(lap["is_clean"])
            connection.execute(
                """
                UPDATE laps
                SET duration_ms = ?, sectors_json = ?,
                    duration_source_cell_observation_id = ?,
                    duration_source_message_id = ?, duration_source_key = ?, duration_source_kind = 'RESULT_GRID_LAST',
                    sectors_source_cell_observation_ids_json = ?,
                    is_in_lap = ?, is_out_lap = ?, crosses_pit = ?, is_clean = ?
                WHERE id = ?
                  AND duration_ms IS NULL
                  AND duration_source_cell_observation_id IS NULL
                """,
                (
                    duration_ms,
                    _json({key: value[0] for key, value in source_sectors.items()}) if source_sectors else None,
                    int(last["id"]),
                    source_message_id,
                    last["source_key"],
                    _json({key: value[1] for key, value in source_sectors.items()}) if source_sectors else None,
                    is_in_lap,
                    is_out_lap,
                    crosses_pit,
                    is_clean,
                    lap["id"],
                ),
            )

    def _write_immediate_flag(self, connection: sqlite3.Connection, context: FrameMessage, flag: FlagState) -> None:
        current = connection.execute(
            """
            SELECT flag,provider_code,provider_label,source_flag_kind_raw,started_at_us,source_key
            FROM track_flag_current WHERE source_heat_id = ?
            """,
            (self.heat_id,),
        ).fetchone()
        if current is not None and self._same_flag_state(current, flag):
            return
        if current is not None:
            connection.execute(
                """
                UPDATE track_flag_periods
                SET ended_at_us = ?, observed_ended_at_us = ?, ended_source_message_id = ?, ended_source_key = ?
                WHERE source_heat_id = ? AND ended_at_us IS NULL
                """,
                (context.received_at_us, context.received_at_us, context.id, context.source_key, self.heat_id),
            )
        connection.execute(
            """
            INSERT INTO track_flag_periods(
              source_heat_id,flag,provider_code,provider_label,started_at_us,source_message_id,source_key,created_at_us,
              observed_started_at_us,source_flag_kind_raw
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                self.heat_id,
                flag.kind,
                str(flag.provider_code) if flag.provider_code is not None else None,
                flag.provider_label,
                context.received_at_us,
                context.id,
                context.source_key,
                now_us(),
                context.received_at_us,
                _text(flag.raw),
            ),
        )
        _upsert(
            connection,
            "track_flag_current",
            {
                "source_heat_id": self.heat_id,
                "flag": flag.kind,
                "provider_code": str(flag.provider_code) if flag.provider_code is not None else None,
                "provider_label": flag.provider_label,
                "started_at_us": context.received_at_us,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "updated_at_us": context.received_at_us,
                "start_provider_ts_raw": None,
                "start_provider_ts_us": None,
                "observed_started_at_us": context.received_at_us,
                "calibrated_started_at_us": None,
                "start_clock_calibration_id": None,
                "source_flag_kind_raw": _text(flag.raw),
                "reconciliation_key": None,
                "reconciliation_source_message_id": None,
                "reconciliation_source_key": None,
                "reconciled_at_us": None,
            },
            conflict_columns=("source_heat_id",),
        )

    @staticmethod
    def _same_flag_state(current: sqlite3.Row, flag: FlagState) -> bool:
        """Keep a period for each distinct source flag, including unknown ones."""
        return TimingNormalizer._same_flag_values(
            FlagState(
                raw=current["source_flag_kind_raw"],
                kind=current["flag"],
                provider_code=canonical_flag(current["provider_code"]).provider_code,
                provider_label=current["provider_label"],
            ),
            flag,
        )

    @staticmethod
    def _same_flag_values(left: FlagState, right: FlagState) -> bool:
        if left.kind != right.kind or left.provider_code != right.provider_code:
            return False
        if left.kind != "UNKNOWN":
            return True
        return _key(left.provider_label or _text(left.raw)) == _key(right.provider_label or _text(right.raw))

    def _pit_entered_at_us(self, pit_time_raw: str | None, context: FrameMessage) -> int:
        if pit_time_raw and pit_time_raw[:1].upper() == "S":
            provider_time = parse_ts_time(pit_time_raw[1:])
            calibrated = self._clock(context.connection_id).to_utc_us(provider_time)
            if calibrated is not None:
                return calibrated
        # The observation itself is durable provenance when the provider does
        # not expose a calibrated pit-entry timestamp. It is not shown as a
        # claimed source timestamp.
        return context.received_at_us

    @staticmethod
    def _pit_entry_time_source(fact: ResultCellFact | None) -> ResultCellFact | None:
        """Return a valid source L-PIT S<TsTime> cell, never a cached L value."""

        if fact is None or fact.value_text is None or fact.value_text[:1].upper() != "S":
            return None
        # `0` and Int64.MaxValue are provider sentinels, not pit-entry
        # instants. Treating either as a calibrated timestamp would place an
        # observed pit in 2000 or far beyond the session and contaminate the
        # timeline and mandatory-stop ledger.
        return fact if _event_ts_time(fact.value_text[1:]) is not None else None

    @staticmethod
    def _pit_duration_ms(pit_time_raw: str | None) -> int | None:
        if not pit_time_raw or pit_time_raw[:1].upper() != "L":
            return None
        return _duration_us_to_ms(pit_time_raw[1:])

    def _reconcile_frame_pit_events(self, connection: sqlite3.Connection) -> None:
        """Apply queued source pit transitions after a frame's tracker facts."""

        entry_sources_by_participant: dict[str, list[ResultCellFact]] = {}
        for event in self._pending_pit_events:
            source = self._pit_entry_time_source(event.pit_time_event)
            if source is not None:
                entry_sources_by_participant.setdefault(event.participant_id, []).append(source)
        for event in self._pending_pit_events:
            pit_time_event = event.pit_time_event
            is_entry = (
                event.state_event is not None
                and event.state_event.kind == "IN_PIT"
                and event.previous is not None
                and event.previous["state_kind"] != "IN_PIT"
            )
            candidates = entry_sources_by_participant.get(event.participant_id, [])
            if is_entry and len(candidates) == 1:
                # Time Service can send L-PIT=S<TsTime> in a separate r_c
                # handle from STATE=IN_PIT. The frame is the causal boundary.
                pit_time_event = candidates[0]
            self._reconcile_pit_and_tire_stint(
                connection,
                event.context,
                event.participant_id,
                event.state_event,
                event.state_cell,
                event.pit_count_event,
                event.pit_count_cell,
                event.lap_number,
                pit_time_event,
                event.previous,
            )

    def _reconcile_pit_and_tire_stint(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        participant_id: str,
        state_event: ResultState | None,
        state_cell: ResultCellFact | None,
        pit_count_event: int | None,
        pit_count_cell: ResultCellFact | None,
        lap_number: int | None,
        pit_time_event: ResultCellFact | None,
        previous: sqlite3.Row | None,
    ) -> None:
        """Create pit/tyre facts only from causally current source cells.

        A completed pit closes the current tyre stint and opens a new one. No
        manual tyre input or compound override exists in this data path. In
        particular, a previously materialized L-PIT display value is never
        reused as the duration of a later State/PIT transition.
        """
        previous_state = previous["state_kind"] if previous is not None else None
        previous_count = previous["provider_pit_count"] if previous is not None else None
        effective_lap_number = self._effective_lap_number(connection, participant_id, lap_number)
        current_state = state_event.kind if state_event is not None else previous_state
        now_in_pit = current_state == "IN_PIT"
        was_in_pit = previous_state == "IN_PIT"
        count_increased = (
            pit_count_event is not None
            and previous_count is not None
            and pit_count_event > previous_count
        )
        opened = connection.execute(
            """
            SELECT id,stop_number,entered_at_us,entered_lap
            FROM pit_stops
            WHERE source_heat_id = ? AND participant_id = ? AND completed = 0
            ORDER BY stop_number DESC LIMIT 1
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        had_opened_pit = opened is not None
        pit_facts_changed = False
        # The first source row is a baseline, not an observed transition. A
        # capture may begin while a crew is already in pit lane, with a pit
        # count inherited from time before the recording window.
        entered_from_state_transition = (
            state_event is not None and previous is not None and was_in_pit is False and now_in_pit
        )
        if entered_from_state_transition:
            max_stop = int(
                connection.execute(
                "SELECT COALESCE(MAX(stop_number), 0) FROM pit_stops WHERE source_heat_id = ? AND participant_id = ?",
                (self.heat_id, participant_id),
                ).fetchone()[0]
            )
            source_stop_number = (
                pit_count_event
                if pit_count_event is not None and pit_count_event > 0
                else (int(previous_count) if previous_count is not None and previous_count > 0 else None)
            )
            stop_number = source_stop_number if source_stop_number is not None else max_stop + 1
            if stop_number <= max_stop:
                stop_number = max_stop + 1
            entry_time_source = self._pit_entry_time_source(pit_time_event)
            entered_at_us = self._pit_entered_at_us(
                entry_time_source.value_text if entry_time_source is not None else None,
                context,
            )
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO pit_stops(
                  id,source_heat_id,participant_id,stop_number,entered_at_us,entered_lap,
                  completed,entered_source_message_id,entered_source_key,
                  entered_state_cell_observation_id,entered_pit_count_cell_observation_id,
                  entered_at_source_cell_observation_id,entered_at_source_message_id,
                  entered_at_source_key,entered_at_source_kind,created_at_us,updated_at_us
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(uuid.uuid5(uuid.NAMESPACE_URL, f"balchug-racing:pit:{self.heat_id}:{participant_id}:{stop_number}")),
                    self.heat_id,
                    participant_id,
                    stop_number,
                    entered_at_us,
                    effective_lap_number,
                    0,
                    context.id,
                    context.source_key,
                    state_cell.id if entered_from_state_transition and state_cell is not None else None,
                    pit_count_cell.id if count_increased and pit_count_cell is not None else None,
                    entry_time_source.id if entry_time_source is not None else None,
                    entry_time_source.source_message_id if entry_time_source is not None else None,
                    entry_time_source.source_key if entry_time_source is not None else None,
                    "RESULT_L_PIT_S" if entry_time_source is not None else None,
                    now_us(),
                    now_us(),
                ),
            )
            if cursor.rowcount:
                pit_facts_changed = True
                opened = connection.execute(
                    """
                    SELECT id,stop_number,entered_at_us,entered_lap
                    FROM pit_stops WHERE source_heat_id = ? AND participant_id = ? AND stop_number = ?
                    """,
                    (self.heat_id, participant_id, stop_number),
                ).fetchone()
        # A counter-only update is raw source evidence, not a pit boundary.
        # An open stop can close only after an explicit outbound STATE cell.
        exits_pit = (
            state_event is not None
            and state_event.kind in {"ON_TRACK", "OUT_LAP"}
            and (was_in_pit or had_opened_pit)
        )
        if exits_pit and opened is not None:
            duration_ms = self._pit_duration_ms(pit_time_event.value_text if pit_time_event is not None else None)
            duration_source = pit_time_event if duration_ms is not None else None
            cursor = connection.execute(
                """
                UPDATE pit_stops
                SET exited_at_us = ?, exited_lap = ?, pit_lane_ms = ?, completed = 1,
                    exited_source_message_id = ?, exited_source_key = ?,
                    exited_state_cell_observation_id = ?, exited_pit_count_cell_observation_id = ?,
                    pit_lane_duration_source_cell_observation_id = ?,
                    pit_lane_duration_source_message_id = ?, pit_lane_duration_source_key = ?,
                    pit_lane_duration_source_kind = ?, updated_at_us = ?
                WHERE id = ? AND completed = 0
                """,
                (
                    context.received_at_us,
                    effective_lap_number,
                    duration_ms,
                    context.id,
                    context.source_key,
                    state_cell.id if state_cell is not None else None,
                    pit_count_cell.id if pit_count_cell is not None else None,
                    duration_source.id if duration_source is not None else None,
                    duration_source.source_message_id if duration_source is not None else None,
                    duration_source.source_key if duration_source is not None else None,
                    "RESULT_L_PIT" if duration_source is not None else None,
                    now_us(),
                    opened["id"],
                ),
            )
            pit_facts_changed = bool(cursor.rowcount) or pit_facts_changed
            self._complete_tire_stint(connection, context, participant_id, effective_lap_number)
        else:
            self._ensure_tire_stint(connection, context, participant_id, effective_lap_number)
        if pit_facts_changed:
            self._invalidate_clean_laps_for_pit(connection, participant_id)

    def _ensure_tire_stint(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        participant_id: str,
        lap_number: int | None,
    ) -> None:
        lap_number = self._effective_lap_number(connection, participant_id, lap_number)
        current = connection.execute(
            """
            SELECT id,started_lap FROM tire_stints
            WHERE source_heat_id = ? AND participant_id = ? AND ended_at_us IS NULL
            ORDER BY stint_number DESC LIMIT 1
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        if current is None:
            stint_number = int(
                connection.execute(
                    "SELECT COALESCE(MAX(stint_number), 0) + 1 FROM tire_stints WHERE source_heat_id = ? AND participant_id = ?",
                    (self.heat_id, participant_id),
                ).fetchone()[0]
            )
            connection.execute(
                """
                INSERT INTO tire_stints(
                  id,source_heat_id,participant_id,stint_number,started_at_us,started_lap,completed_laps,
                  source_message_id,source_key,created_at_us,updated_at_us
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(uuid.uuid5(uuid.NAMESPACE_URL, f"balchug-racing:tyre:{self.heat_id}:{participant_id}:{stint_number}")),
                    self.heat_id,
                    participant_id,
                    stint_number,
                    context.received_at_us,
                    lap_number,
                    0,
                    context.id,
                    context.source_key,
                    now_us(),
                    now_us(),
                ),
            )
            return
        if lap_number is not None and current["started_lap"] is not None:
            connection.execute(
                """
                UPDATE tire_stints
                SET completed_laps = MAX(completed_laps, ?), updated_at_us = ?
                WHERE id = ?
                """,
                (max(0, lap_number - int(current["started_lap"])), now_us(), current["id"]),
            )

    def _complete_tire_stint(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        participant_id: str,
        lap_number: int | None,
    ) -> None:
        lap_number = self._effective_lap_number(connection, participant_id, lap_number)
        current = connection.execute(
            """
            SELECT id,stint_number,started_lap FROM tire_stints
            WHERE source_heat_id = ? AND participant_id = ? AND ended_at_us IS NULL
            ORDER BY stint_number DESC LIMIT 1
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        if current is not None:
            completed = (
                max(0, lap_number - int(current["started_lap"]))
                if lap_number is not None and current["started_lap"] is not None
                else 0
            )
            connection.execute(
                """
                UPDATE tire_stints
                SET ended_at_us = ?, ended_lap = ?, completed_laps = MAX(completed_laps, ?), updated_at_us = ?
                WHERE id = ? AND ended_at_us IS NULL
                """,
                (context.received_at_us, lap_number, completed, now_us(), current["id"]),
            )
            next_number = int(current["stint_number"]) + 1
        else:
            next_number = 1
        connection.execute(
            """
            INSERT OR IGNORE INTO tire_stints(
              id,source_heat_id,participant_id,stint_number,started_at_us,started_lap,completed_laps,
              source_message_id,source_key,created_at_us,updated_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(uuid.uuid5(uuid.NAMESPACE_URL, f"balchug-racing:tyre:{self.heat_id}:{participant_id}:{next_number}")),
                self.heat_id,
                participant_id,
                next_number,
                context.received_at_us,
                lap_number,
                0,
                context.id,
                context.source_key,
                now_us(),
                now_us(),
            ),
        )

    def _effective_lap_number(
        self, connection: sqlite3.Connection, participant_id: str, source_lap_number: int | None
    ) -> int | None:
        if source_lap_number is not None:
            return source_lap_number
        # Some live layouts omit LAPS. In that case the finish-loop reducer
        # supplies a source-derived count since analysis began for tyre ageing.
        row = connection.execute(
            "SELECT COALESCE(MAX(lap_number), 0) FROM laps WHERE source_heat_id = ? AND participant_id = ?",
            (self.heat_id, participant_id),
        ).fetchone()
        return int(row[0]) if row is not None and int(row[0]) > 0 else None

    def _update_tracker_topology(self, payload: Mapping[str, Any]) -> None:
        """Derive finish loops from source topology instead of hard-coding a track."""
        loops = payload.get("l")
        if not isinstance(loops, Sequence) or isinstance(loops, (str, bytes, bytearray)):
            return
        finish_sectors: set[int] = set()
        for loop in loops:
            if not isinstance(loop, Sequence) or isinstance(loop, (str, bytes, bytearray)) or len(loop) < 3:
                continue
            distance = _integer(loop[0])
            is_in_pit = loop[1] if type(loop[1]) is bool else None
            sector_id = _integer(loop[2])
            if distance == 0 and is_in_pit is False and sector_id is not None and sector_id >= 0:
                finish_sectors.add(sector_id)
        if finish_sectors:
            self._finish_sector_ids = finish_sectors

    def _participant_for_tracker(
        self,
        connection: sqlite3.Connection,
        start_number: str | None,
        transponder_id: str | None,
    ) -> str | None:
        if start_number is not None:
            row = connection.execute(
                """
                SELECT id FROM participants
                WHERE source_heat_id = ? AND start_number_key = ?
                ORDER BY is_ours DESC, last_seen_at_us DESC LIMIT 1
                """,
                (self.heat_id, _key(start_number)),
            ).fetchone()
            if row is not None:
                return row["id"]
        if transponder_id is not None:
            row = connection.execute(
                """
                SELECT id FROM participants
                WHERE source_heat_id = ? AND transponder_id = ?
                ORDER BY last_seen_at_us DESC LIMIT 1
                """,
                (self.heat_id, transponder_id),
            ).fetchone()
            if row is not None:
                return row["id"]
        return None

    def _write_tracker_passings(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        payload: Any,
        *,
        allow_lap_completion: bool,
    ) -> None:
        for ordinal, raw_passing in enumerate(_changes(payload)):
            passing = parse_tracker_passing(raw_passing)
            participant_id = self._participant_for_tracker(connection, passing.start_number, passing.transponder_id)
            calibration_id = self._latest_calibration_id(connection, context.connection_id)
            passed_at_us = self._clock(context.connection_id).to_utc_us(passing.passed_at_ts_time)
            event_fingerprint = _fingerprint(
                {
                    "transponder": passing.transponder_id,
                    "start_number": passing.start_number,
                    "distance": passing.distance_mm,
                    "stop_distance": passing.stop_distance_mm,
                    "sector": passing.sector_id,
                    "speed": passing.speed_mm_s,
                    "in_pit": passing.is_in_pit,
                    "provider_time": passing.passed_at_ts_time,
                    "path": passing.path_id,
                }
            )
            is_new_observation = _insert_ignore(
                connection,
                "tracker_passing_observations",
                {
                    "source_heat_id": self.heat_id,
                    "participant_id": participant_id,
                    "transponder_id_raw": passing.transponder_id,
                    "start_number_raw": passing.start_number,
                    "start_distance_mm": passing.distance_mm,
                    "stop_distance_mm": passing.stop_distance_mm,
                    "sector_id": passing.sector_id,
                    "raw_speed_mm_s": passing.speed_mm_s,
                    "is_in_pit": int(passing.is_in_pit) if passing.is_in_pit is not None else None,
                    "provider_passed_at_raw": _text(passing.provider_passed_at_raw),
                    "provider_passed_at_provider_us": passing.passed_at_ts_time,
                    "provider_passed_at_kind": "ts_time" if passing.passed_at_ts_time is not None else None,
                    "passed_at_us": passed_at_us,
                    "clock_calibration_id": calibration_id,
                    "event_fingerprint": event_fingerprint,
                    "raw_passing_json": _json(raw_passing),
                    "source_message_id": context.id,
                    "source_key": context.source_key,
                    "source_event_key": f"{context.source_key}:passing:{ordinal}",
                    "observed_at_us": context.received_at_us,
                    "created_at_us": now_us(),
                },
            )
            passing_observation_id: int | None = None
            if is_new_observation:
                observation = connection.execute(
                    """
                    SELECT id FROM tracker_passing_observations
                    WHERE source_heat_id = ? AND event_fingerprint = ?
                    """,
                    (self.heat_id, event_fingerprint),
                ).fetchone()
                if observation is None:
                    raise NormalizerError("Tracker passing observation was not persisted")
                passing_observation_id = int(observation["id"])
            if passing.transponder_id is None or passing.is_in_pit is None:
                continue
            _insert_ignore(
                connection,
                "tracker_passings",
                {
                    "source_heat_id": self.heat_id,
                    "participant_id": participant_id,
                    "transponder_id": passing.transponder_id,
                    "start_number": passing.start_number,
                    "distance_mm": passing.distance_mm,
                    "stop_distance_mm": passing.stop_distance_mm,
                    "sector_id": passing.sector_id,
                    "speed_kph": passing.speed_kph,
                    "is_in_pit": int(passing.is_in_pit),
                    "passed_at_us": passed_at_us,
                    "provider_passed_at_raw": _text(passing.provider_passed_at_raw),
                    "path_id": passing.path_id,
                    "source_message_id": context.id,
                    "source_key": context.source_key,
                    "message_ordinal": ordinal,
                    "created_at_us": now_us(),
                    "raw_speed_mm_s": passing.speed_mm_s,
                    "provider_passed_at_provider_us": passing.passed_at_ts_time,
                    "provider_passed_at_kind": "ts_time" if passing.passed_at_ts_time is not None else None,
                    "clock_calibration_id": calibration_id,
                    "event_fingerprint": event_fingerprint,
                    "observed_at_us": context.received_at_us,
                    "raw_passing_json": _json(raw_passing),
                },
            )
            if (
                allow_lap_completion
                and is_new_observation
                and participant_id is not None
                and passing.sector_id in self._finish_sector_ids
            ):
                self._complete_lap_from_tracker(
                    connection,
                    context,
                    participant_id,
                    event_fingerprint,
                    passed_at_us,
                    passing_observation_id,
                )

    def _complete_lap_from_tracker(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        participant_id: str,
        event_fingerprint: str,
        completed_at_us: int | None,
        completion_passing_observation_id: int | None,
    ) -> None:
        """Count a lap only when the active layout has no explicit LAPS value."""
        state = connection.execute(
            """
            SELECT laps,state_kind FROM participant_state_current
            WHERE source_heat_id = ? AND participant_id = ?
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        if state is None or state["laps"] is not None:
            return
        previous = connection.execute(
            """
            SELECT lap_number,completed_at_us FROM laps
            WHERE source_heat_id = ? AND participant_id = ?
            ORDER BY lap_number DESC LIMIT 1
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        lap_number = int(previous["lap_number"]) + 1 if previous is not None else 1
        source_completed_at_us = completed_at_us if completed_at_us is not None else context.received_at_us
        inserted = _insert_ignore(
            connection,
            "laps",
            {
                "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"balchug-racing:lap:{self.heat_id}:{participant_id}:{event_fingerprint}")),
                "source_heat_id": self.heat_id,
                "participant_id": participant_id,
                "lap_number": lap_number,
                "completed_at_us": source_completed_at_us,
                # The tracker is a finish-line clock only. It must never
                # manufacture a lap duration; _reconcile_tracker_lap_sources
                # can attach a same-frame result-grid LAST value later.
                "duration_ms": None,
                "sectors_json": None,
                "flag": self._current_flag_kind(connection),
                "is_in_lap": int(state["state_kind"] == "IN_PIT"),
                "is_out_lap": int(state["state_kind"] == "OUT_LAP"),
                "crosses_pit": int(state["state_kind"] == "IN_PIT"),
                "is_clean": int(
                    self._is_clean_lap(connection, participant_id, source_completed_at_us, state["state_kind"])
                ),
                "source_message_id": context.id,
                "source_key": context.source_key,
                "created_at_us": now_us(),
                "completion_passing_observation_id": completion_passing_observation_id,
                "duration_source_cell_observation_id": None,
                "duration_source_message_id": None,
                "duration_source_key": None,
                "duration_source_kind": None,
                "sectors_source_cell_observation_ids_json": None,
            },
        )
        if inserted:
            active_stint = connection.execute(
                """
                SELECT id FROM tire_stints
                WHERE source_heat_id = ? AND participant_id = ? AND ended_at_us IS NULL
                ORDER BY stint_number DESC LIMIT 1
                """,
                (self.heat_id, participant_id),
            ).fetchone()
            if active_stint is not None:
                connection.execute(
                    "UPDATE tire_stints SET completed_laps = completed_laps + 1, updated_at_us = ? WHERE id = ?",
                    (now_us(), active_stint["id"]),
                )

    def _is_clean_lap(
        self,
        connection: sqlite3.Connection,
        participant_id: str,
        completed_at_us: int | None,
        state_kind: str | None,
    ) -> bool:
        """Return true only when the complete observed lap interval is Green."""
        if completed_at_us is None or state_kind in {"IN_PIT", "OUT_LAP"}:
            return False
        current_flag = self._current_flag_kind(connection)
        if current_flag != "GREEN":
            return False
        previous = connection.execute(
            """
            SELECT completed_at_us FROM laps
            WHERE source_heat_id = ? AND participant_id = ? AND completed_at_us IS NOT NULL
            ORDER BY lap_number DESC LIMIT 1
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        if previous is None:
            # A recording that starts halfway through a lap cannot prove that
            # the entire lap was clean, even if its finish is green.
            return False
        started_at_us = int(previous["completed_at_us"])
        non_green = connection.execute(
            """
            SELECT 1 FROM track_flag_periods
            WHERE source_heat_id = ? AND flag <> 'GREEN' AND started_at_us < ?
              AND (ended_at_us IS NULL OR ended_at_us > ?)
            LIMIT 1
            """,
            (self.heat_id, completed_at_us, started_at_us),
        ).fetchone()
        if non_green is not None:
            return False
        pit_overlap = connection.execute(
            """
            SELECT 1 FROM pit_stops
            WHERE source_heat_id = ? AND participant_id = ?
              AND completed = 1
              AND entered_at_us < ?
              AND (exited_at_us IS NULL OR exited_at_us > ?)
            LIMIT 1
            """,
            (self.heat_id, participant_id, completed_at_us, started_at_us),
        ).fetchone()
        if pit_overlap is not None:
            return False
        gap = connection.execute(
            """
            SELECT 1 FROM ingest_gaps
            WHERE analysis_session_id = ? AND started_at_us < ?
              AND (ended_at_us IS NULL OR ended_at_us > ?)
            LIMIT 1
            """,
            (self.analysis_session_id, completed_at_us, started_at_us),
        ).fetchone()
        return gap is None

    def _invalidate_clean_laps_for_pit(self, connection: sqlite3.Connection, participant_id: str) -> None:
        """Remove a clean mark when a later pit fact intersects its full interval."""

        connection.execute(
            """
            WITH lap_intervals AS (
              SELECT id,completed_at_us,
                     LAG(completed_at_us) OVER (
                       PARTITION BY participant_id ORDER BY lap_number
                     ) AS lap_started_at_us
              FROM laps
              WHERE source_heat_id = ? AND participant_id = ? AND completed_at_us IS NOT NULL
            )
            UPDATE laps
            SET is_clean = 0, crosses_pit = 1
            WHERE id IN (
              SELECT lap_intervals.id
              FROM lap_intervals
              JOIN pit_stops AS pit
                ON pit.source_heat_id = ? AND pit.participant_id = ?
               AND pit.completed = 1
               AND pit.entered_at_us < lap_intervals.completed_at_us
               AND (pit.exited_at_us IS NULL OR pit.exited_at_us > lap_intervals.lap_started_at_us)
              WHERE lap_intervals.lap_started_at_us IS NOT NULL
            )
            """,
            (self.heat_id, participant_id, self.heat_id, participant_id),
        )

    def _current_flag_kind(self, connection: sqlite3.Connection) -> str | None:
        row = connection.execute(
            "SELECT flag FROM track_flag_current WHERE source_heat_id = ?", (self.heat_id,)
        ).fetchone()
        return row["flag"] if row is not None else None

    def _write_statistics(self, connection: sqlite3.Connection, context: FrameMessage) -> None:
        """Persist merged Statistics-tab facts and reconcile authoritative flags."""
        update = normalize_statistics_update(self.statistics)
        raw_payload_json = _json(self.statistics)
        clock = self._clock(context.connection_id)
        calibration_id = self._latest_calibration_id(connection, context.connection_id)
        green_raw = _text(self.statistics.get("g"))
        finish_raw = _text(self.statistics.get("f"))
        # Statistics uses ``0`` while a heat is still running. It is not a
        # Time Service instant and must never become a year-2000 boundary.
        green_provider = _event_ts_time(green_raw)
        finish_provider = _event_ts_time(finish_raw)
        green_at_us = clock.to_utc_us(green_provider)
        finish_at_us = clock.to_utc_us(finish_provider)
        summary = update.summary
        event_key = f"{context.source_key}:statistics"
        typed_values = {
            "source_heat_id": self.heat_id,
            "heat_name_raw": _text(summary.get("heat_name")),
            "green_flag_provider_ts_raw": green_raw,
            "green_flag_provider_ts_us": green_provider,
            "green_flag_at_us": green_at_us,
            "green_flag_calibration_id": calibration_id,
            "finish_flag_provider_ts_raw": finish_raw,
            "finish_flag_provider_ts_us": finish_provider,
            "finish_flag_at_us": finish_at_us,
            "finish_flag_calibration_id": calibration_id,
            "participants_started": summary.get("participants_started"),
            "participants_classified": summary.get("participants_classified"),
            "participants_not_classified": summary.get("participants_not_classified"),
            "participants_on_track": summary.get("participants_on_track"),
            "participants_in_pit_zone": summary.get("participants_in_pit_zone"),
            "participants_in_tank_zone": summary.get("participants_in_tank_zone"),
            "total_laps": summary.get("total_laps"),
            "total_pitstops": summary.get("total_pitstops"),
            "leader_laps_green": summary.get("leader_laps_green"),
            "leader_laps_safety_car": summary.get("leader_laps_safety_car"),
            "leader_laps_code_60": summary.get("leader_laps_code_60"),
            "leader_laps_full_course_yellow": summary.get("leader_laps_full_course_yellow"),
            "safety_car_count": summary.get("safety_car_count"),
            "code_60_count": summary.get("code_60_count"),
            "full_course_yellow_count": summary.get("full_course_yellow_count"),
            "safety_car_total_time_raw": _text(summary.get("safety_car_total_time_raw")),
            "code_60_total_time_raw": _text(summary.get("code_60_total_time_raw")),
            "full_course_yellow_total_time_raw": _text(summary.get("full_course_yellow_total_time_raw")),
            "raw_payload_json": raw_payload_json,
            "source_message_id": context.id,
            "source_key": context.source_key,
            "source_event_key": event_key,
            "observed_at_us": context.received_at_us,
            "updated_at_us": now_us(),
        }
        _upsert(
            connection,
            "heat_statistics_current",
            typed_values,
            conflict_columns=("source_heat_id",),
        )
        sample_values = {
            key: value
            for key, value in typed_values.items()
            if key not in {"updated_at_us", "green_flag_calibration_id", "finish_flag_calibration_id"}
        }
        sample_values["observed_second"] = context.received_at_us // 1_000_000
        sample_values["created_at_us"] = now_us()
        _upsert(
            connection,
            "heat_statistics_samples",
            sample_values,
            conflict_columns=("source_heat_id", "observed_second"),
        )
        _upsert(
            connection,
            "source_statistics_current",
            {
                "source_heat_id": self.heat_id,
                "observed_at_us": context.received_at_us,
                "payload_json": raw_payload_json,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "updated_at_us": now_us(),
            },
            conflict_columns=("source_heat_id",),
        )
        _upsert(
            connection,
            "source_statistics_samples",
            {
                "source_heat_id": self.heat_id,
                "observed_second": context.received_at_us // 1_000_000,
                "observed_at_us": context.received_at_us,
                "payload_json": raw_payload_json,
                "source_message_id": context.id,
                "source_key": context.source_key,
            },
            conflict_columns=("source_heat_id", "observed_second"),
        )
        for record in update.best_lap_history:
            self._write_best_lap_history(connection, context, record, calibration_id)
        for record in update.best_lap_per_class:
            self._write_class_best_lap(connection, context, record, calibration_id)
        for caution in update.caution_periods:
            self._write_caution_period(connection, context, caution, calibration_id)
        self._reconcile_initial_green_boundary(
            connection,
            context,
            provider_ts=green_provider,
            boundary_at_us=green_at_us,
            calibration_id=calibration_id,
        )
        self._reconcile_finish_boundary(
            connection,
            context,
            provider_ts=finish_provider,
            boundary_at_us=finish_at_us,
            calibration_id=calibration_id,
        )
        self._reconcile_exact_flag_adjacencies(connection, context)

    def _best_record_values(self, record: Any, context: FrameMessage, calibration_id: int | None) -> dict[str, Any]:
        occurred_at = self._clock(context.connection_id).to_utc_us(record.occurred_at_ts_time)
        return {
            "time_of_day_raw": str(record.occurred_at_ts_time) if record.occurred_at_ts_time is not None else None,
            "time_of_day_provider_us": record.occurred_at_ts_time,
            "time_of_day_at_us": occurred_at,
            "clock_calibration_id": calibration_id,
            "lap_time_raw": str(record.lap_time_us) if record.lap_time_us is not None else None,
            "lap_time_us": record.lap_time_us,
            "lap_number": record.lap_number,
            "start_number_raw": record.start_number,
            "start_number_key": _key(record.start_number),
            "team_name_raw": record.team_name,
            "team_name_key": _key(record.team_name),
            "driver_name_raw": record.driver_name,
            "driver_name_key": _key(record.driver_name),
            "car_name_raw": record.vehicle_name,
            "car_name_key": _key(record.vehicle_name),
            "average_speed_raw": str(record.average_speed_kph) if record.average_speed_kph is not None else None,
            "average_speed_kph": record.average_speed_kph,
        }

    def _write_best_lap_history(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        record: Any,
        calibration_id: int | None,
    ) -> None:
        values = self._best_record_values(record, context, calibration_id)
        fingerprint = _fingerprint(
            {
                "provider_key": record.provider_key,
                "occurred": record.occurred_at_ts_time,
                "lap": record.lap_number,
                "time": record.lap_time_us,
                "number": record.start_number,
                "team": record.team_name,
            }
        )
        _insert_ignore(
            connection,
            "statistics_best_lap_history",
            {
                "source_heat_id": self.heat_id,
                "event_fingerprint": fingerprint,
                **values,
                "provider_rank": _integer(record.provider_key, minimum=0),
                "raw_record_json": _json(record.__dict__),
                "source_message_id": context.id,
                "source_key": context.source_key,
                "source_event_key": f"{context.source_key}:best:{record.provider_key}",
                "observed_at_us": context.received_at_us,
                "created_at_us": now_us(),
            },
        )
        self._enrich_participant_car(connection, record)

    def _write_class_best_lap(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        record: Any,
        calibration_id: int | None,
    ) -> None:
        class_key = _key(record.class_name)
        if class_key is None:
            return
        values = self._best_record_values(record, context, calibration_id)
        fingerprint = _fingerprint(
            {
                "class": record.class_name,
                "occurred": record.occurred_at_ts_time,
                "lap": record.lap_number,
                "time": record.lap_time_us,
                "number": record.start_number,
                "team": record.team_name,
            }
        )
        _upsert(
            connection,
            "statistics_class_best_laps",
            {
                "source_heat_id": self.heat_id,
                "class_name_raw": record.class_name,
                "class_name_key": class_key,
                **values,
                "provider_class_order": record.class_order,
                "event_fingerprint": fingerprint,
                "raw_record_json": _json(record.__dict__),
                "source_message_id": context.id,
                "source_key": context.source_key,
                "source_event_key": f"{context.source_key}:class-best:{class_key}",
                "observed_at_us": context.received_at_us,
                "updated_at_us": now_us(),
            },
            conflict_columns=("source_heat_id", "class_name_key"),
        )
        self._enrich_participant_car(connection, record)

    def _enrich_participant_car(self, connection: sqlite3.Connection, record: Any) -> None:
        """Use Statistics car data only for the matching source entry identity."""
        start_key = _key(record.start_number)
        car_name = _text(record.vehicle_name)
        if start_key is None or car_name is None:
            return
        conditions = ["source_heat_id = ?", "start_number_key = ?"]
        parameters: list[Any] = [self.heat_id, start_key]
        if _key(record.team_name) is not None:
            conditions.append("team_name_key = ?")
            parameters.append(_key(record.team_name))
        if _key(record.class_name) is not None:
            conditions.append("class_name_key = ?")
            parameters.append(_key(record.class_name))
        connection.execute(
            f"UPDATE participants SET car_name = ?, car_name_key = ? WHERE {' AND '.join(conditions)}",
            (car_name, _key(car_name), *parameters),
        )

    def _write_caution_period(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        caution: CautionPeriod,
        calibration_id: int | None,
    ) -> None:
        reconciliation_key = f"{caution.flag.kind}:{_text(caution.started_at_raw) or caution.provider_key}"
        clock = self._clock(context.connection_id)
        started_at_us = clock.to_utc_us(caution.started_at_ts_time)
        ended_at_us = clock.to_utc_us(caution.ended_at_ts_time)
        values = {
            "source_heat_id": self.heat_id,
            "reconciliation_key": reconciliation_key,
            "flag_kind_raw": _text(caution.flag.raw),
            "start_provider_ts_raw": _text(caution.started_at_raw),
            "end_provider_ts_raw": _text(caution.ended_at_raw),
            "start_provider_ts_us": caution.started_at_ts_time,
            "end_provider_ts_us": caution.ended_at_ts_time,
            "started_at_us": started_at_us,
            "ended_at_us": ended_at_us,
            "start_clock_calibration_id": calibration_id,
            "end_clock_calibration_id": calibration_id if ended_at_us is not None else None,
            "clock_stopped_raw": _text(caution.clock_stopped_raw),
            "clock_stopped": int(caution.clock_stopped) if caution.clock_stopped is not None else None,
            "remark_raw": caution.remark,
            "raw_record_json": _json(
                {
                    "provider_key": caution.provider_key,
                    "flag": {
                        "raw": caution.flag.raw,
                        "kind": caution.flag.kind,
                        "provider_code": caution.flag.provider_code,
                        "provider_label": caution.flag.provider_label,
                    },
                    "started_at_raw": caution.started_at_raw,
                    "started_at_ts_time": caution.started_at_ts_time,
                    "ended_at_raw": caution.ended_at_raw,
                    "ended_at_ts_time": caution.ended_at_ts_time,
                    "is_open": caution.is_open,
                    "clock_stopped_raw": caution.clock_stopped_raw,
                    "clock_stopped": caution.clock_stopped,
                    "remark": caution.remark,
                }
            ),
            "source_message_id": context.id,
            "source_key": context.source_key,
            "source_event_key": f"{context.source_key}:caution:{reconciliation_key}",
            "observed_at_us": context.received_at_us,
            "created_at_us": now_us(),
            "updated_at_us": now_us(),
        }
        _upsert(
            connection,
            "statistics_caution_history",
            values,
            conflict_columns=("source_heat_id", "reconciliation_key"),
        )
        candidate_time = started_at_us if started_at_us is not None else context.received_at_us
        period = connection.execute(
            """
            SELECT id FROM track_flag_periods
            WHERE source_heat_id = ? AND reconciliation_key = ?
            """,
            (self.heat_id, reconciliation_key),
        ).fetchone()
        if period is None:
            period = connection.execute(
                """
                SELECT id FROM track_flag_periods
                WHERE source_heat_id = ? AND flag = ? AND reconciliation_key IS NULL
                  AND ABS(COALESCE(observed_started_at_us, started_at_us) - ?) <= ?
                ORDER BY ABS(COALESCE(observed_started_at_us, started_at_us) - ?), id
                LIMIT 1
                """,
                (self.heat_id, caution.flag.kind, candidate_time, FLAG_RECONCILIATION_WINDOW_US, candidate_time),
            ).fetchone()
        if period is None:
            cursor = connection.execute(
                """
                INSERT INTO track_flag_periods(
                  source_heat_id,flag,provider_code,provider_label,started_at_us,ended_at_us,
                  source_message_id,source_key,created_at_us,start_provider_ts_raw,end_provider_ts_raw,
                  start_provider_ts_us,end_provider_ts_us,observed_started_at_us,observed_ended_at_us,
                  calibrated_started_at_us,calibrated_ended_at_us,start_clock_calibration_id,
                  end_clock_calibration_id,source_flag_kind_raw,clock_stopped_raw,remark_raw,
                  reconciliation_key,reconciliation_source_message_id,reconciliation_source_key,reconciled_at_us
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    self.heat_id,
                    caution.flag.kind,
                    str(caution.flag.provider_code) if caution.flag.provider_code is not None else None,
                    caution.flag.provider_label,
                    started_at_us if started_at_us is not None else context.received_at_us,
                    ended_at_us,
                    context.id,
                    f"{context.source_key}:caution:{reconciliation_key}",
                    now_us(),
                    _text(caution.started_at_raw),
                    _text(caution.ended_at_raw),
                    caution.started_at_ts_time,
                    caution.ended_at_ts_time,
                    context.received_at_us,
                    context.received_at_us if ended_at_us is not None else None,
                    started_at_us,
                    ended_at_us,
                    calibration_id,
                    calibration_id if ended_at_us is not None else None,
                    _text(caution.flag.raw),
                    _text(caution.clock_stopped_raw),
                    caution.remark,
                    reconciliation_key,
                    context.id,
                    context.source_key,
                    now_us(),
                ),
            )
            period_id = int(cursor.lastrowid)
        else:
            period_id = int(period["id"])
            connection.execute(
                """
                UPDATE track_flag_periods
                SET start_provider_ts_raw = ?,
                    end_provider_ts_raw = CASE
                        WHEN ? IS NOT NULL THEN ?
                        WHEN end_provider_ts_raw IS NULL THEN ?
                        ELSE end_provider_ts_raw
                    END,
                    start_provider_ts_us = COALESCE(?, start_provider_ts_us),
                    end_provider_ts_us = COALESCE(?, end_provider_ts_us),
                    calibrated_started_at_us = COALESCE(?, calibrated_started_at_us),
                    calibrated_ended_at_us = COALESCE(?, calibrated_ended_at_us),
                    started_at_us = COALESCE(?, started_at_us),
                    ended_at_us = COALESCE(?, ended_at_us),
                    observed_ended_at_us = COALESCE(
                        observed_ended_at_us,
                        CASE WHEN ? IS NULL THEN NULL ELSE ? END
                    ),
                    start_clock_calibration_id = COALESCE(?, start_clock_calibration_id),
                    end_clock_calibration_id = CASE WHEN ? IS NULL THEN end_clock_calibration_id ELSE ? END,
                    source_flag_kind_raw = ?,
                    clock_stopped_raw = ?, remark_raw = ?, reconciliation_key = ?,
                    reconciliation_source_message_id = ?, reconciliation_source_key = ?, reconciled_at_us = ?
                WHERE id = ?
                """,
                (
                    _text(caution.started_at_raw),
                    ended_at_us,
                    _text(caution.ended_at_raw),
                    _text(caution.ended_at_raw),
                    caution.started_at_ts_time,
                    caution.ended_at_ts_time,
                    started_at_us,
                    ended_at_us,
                    started_at_us,
                    ended_at_us,
                    ended_at_us,
                    context.received_at_us,
                    calibration_id,
                    ended_at_us,
                    calibration_id if ended_at_us is not None else None,
                    _text(caution.flag.raw),
                    _text(caution.clock_stopped_raw),
                    caution.remark,
                    reconciliation_key,
                    context.id,
                    context.source_key,
                    now_us(),
                    period_id,
                ),
            )
        current = connection.execute(
            """
            SELECT flag,provider_code,provider_label,source_flag_kind_raw
            FROM track_flag_current WHERE source_heat_id = ?
            """,
            (self.heat_id,),
        ).fetchone()
        if current is not None and self._same_flag_state(current, caution.flag) and ended_at_us is None:
            connection.execute(
                """
                UPDATE track_flag_current
                SET started_at_us = COALESCE(?, started_at_us), start_provider_ts_raw = ?,
                    start_provider_ts_us = ?, calibrated_started_at_us = ?, start_clock_calibration_id = ?,
                    source_flag_kind_raw = ?, reconciliation_key = ?, reconciliation_source_message_id = ?,
                    reconciliation_source_key = ?, reconciled_at_us = ?, updated_at_us = ?
                WHERE source_heat_id = ?
                """,
                (
                    started_at_us,
                    _text(caution.started_at_raw),
                    caution.started_at_ts_time,
                    started_at_us,
                    calibration_id,
                    _text(caution.flag.raw),
                    reconciliation_key,
                    context.id,
                    context.source_key,
                    now_us(),
                    context.received_at_us,
                    self.heat_id,
                ),
            )
        self._close_caution_if_live_flag_superseded(connection, period_id, caution.flag)
        persisted = connection.execute(
            """
            SELECT calibrated_started_at_us,calibrated_ended_at_us
            FROM track_flag_periods WHERE id = ?
            """,
            (period_id,),
        ).fetchone()
        self._reconcile_caution_timeline(
            connection,
            context,
            period_id=period_id,
            caution=caution,
            # A provider timestamp can be received again after the rolling
            # connection calibration changed. Keep one resolved boundary and
            # copy that exact value to its neighbours.
            started_at_us=(
                int(persisted["calibrated_started_at_us"])
                if persisted is not None and persisted["calibrated_started_at_us"] is not None
                else None
            ),
            ended_at_us=(
                int(persisted["calibrated_ended_at_us"])
                if persisted is not None and persisted["calibrated_ended_at_us"] is not None
                else None
            ),
            calibration_id=calibration_id,
        )
        self._invalidate_clean_laps_for_caution(connection, period_id)

    def _set_period_start_boundary(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        period: sqlite3.Row,
        boundary_at_us: int,
        provider_ts: int,
        calibration_id: int | None,
    ) -> None:
        """Replace a provisional status start with one provider boundary."""

        connection.execute(
            """
            UPDATE track_flag_periods
            SET started_at_us = ?, start_provider_ts_raw = ?, start_provider_ts_us = ?,
                calibrated_started_at_us = ?, start_clock_calibration_id = ?,
                reconciliation_source_message_id = ?, reconciliation_source_key = ?, reconciled_at_us = ?
            WHERE id = ?
            """,
            (
                boundary_at_us,
                str(provider_ts),
                provider_ts,
                boundary_at_us,
                calibration_id,
                context.id,
                context.source_key,
                now_us(),
                period["id"],
            ),
        )
        if period["source_key"] is None:
            return
        connection.execute(
            """
            UPDATE track_flag_current
            SET started_at_us = ?, start_provider_ts_raw = ?, start_provider_ts_us = ?,
                calibrated_started_at_us = ?, start_clock_calibration_id = ?,
                reconciliation_source_message_id = ?, reconciliation_source_key = ?, reconciled_at_us = ?,
                updated_at_us = ?
            WHERE source_heat_id = ? AND source_key = ?
            """,
            (
                boundary_at_us,
                str(provider_ts),
                provider_ts,
                boundary_at_us,
                calibration_id,
                context.id,
                context.source_key,
                now_us(),
                context.received_at_us,
                self.heat_id,
                period["source_key"],
            ),
        )

    def _set_period_end_boundary(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        period: sqlite3.Row,
        boundary_at_us: int,
        provider_ts: int,
        calibration_id: int | None,
    ) -> None:
        """Replace a provisional status end without discarding its observation."""

        connection.execute(
            """
            UPDATE track_flag_periods
            SET ended_at_us = ?, end_provider_ts_raw = ?, end_provider_ts_us = ?,
                calibrated_ended_at_us = ?, end_clock_calibration_id = ?,
                ended_source_message_id = COALESCE(ended_source_message_id, ?),
                ended_source_key = COALESCE(ended_source_key, ?),
                reconciliation_source_message_id = ?, reconciliation_source_key = ?, reconciled_at_us = ?
            WHERE id = ?
            """,
            (
                boundary_at_us,
                str(provider_ts),
                provider_ts,
                boundary_at_us,
                calibration_id,
                context.id,
                context.source_key,
                context.id,
                context.source_key,
                now_us(),
                period["id"],
            ),
        )

    def _align_preceding_status(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        excluded_period_id: int | None,
        boundary_at_us: int,
        provider_ts: int,
        calibration_id: int | None,
    ) -> sqlite3.Row | None:
        """Close the provisional status containing an authoritative transition."""

        conditions = [
            "source_heat_id = ?",
            "started_at_us < ?",
            "(ended_at_us IS NULL OR ended_at_us > ?)",
            "calibrated_ended_at_us IS NULL",
        ]
        parameters: list[Any] = [self.heat_id, boundary_at_us, boundary_at_us]
        if excluded_period_id is not None:
            conditions.append("id <> ?")
            parameters.append(excluded_period_id)
        period = connection.execute(
            f"""
            SELECT id,source_key FROM track_flag_periods
            WHERE {' AND '.join(conditions)}
            ORDER BY started_at_us DESC,id DESC
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if period is not None:
            self._set_period_end_boundary(
                connection,
                context,
                period=period,
                boundary_at_us=boundary_at_us,
                provider_ts=provider_ts,
                calibration_id=calibration_id,
            )
        return period

    def _align_following_status(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        period_id: int,
        period_flag: str,
        boundary_at_us: int,
        provider_ts: int,
        calibration_id: int | None,
    ) -> sqlite3.Row | None:
        """Move a directly observed successor onto an exact preceding end."""

        period = connection.execute(
            """
            SELECT id,source_key
            FROM track_flag_periods
            WHERE source_heat_id = ? AND id <> ? AND flag <> ?
              AND calibrated_started_at_us IS NULL
              AND started_at_us >= ?
              AND ABS(COALESCE(observed_started_at_us, started_at_us) - ?) <= ?
            ORDER BY ABS(COALESCE(observed_started_at_us, started_at_us) - ?),id
            LIMIT 1
            """,
            (
                self.heat_id,
                period_id,
                period_flag,
                boundary_at_us,
                boundary_at_us,
                FLAG_RECONCILIATION_WINDOW_US,
                boundary_at_us,
            ),
        ).fetchone()
        if period is not None:
            self._set_period_start_boundary(
                connection,
                context,
                period=period,
                boundary_at_us=boundary_at_us,
                provider_ts=provider_ts,
                calibration_id=calibration_id,
            )
        return period

    def _synthesize_following_status(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        preceding: sqlite3.Row | None,
        caution: CautionPeriod,
        boundary_at_us: int,
        calibration_id: int | None,
    ) -> None:
        """Split a long direct status when history arrives after a reconnect."""

        if preceding is None or caution.ended_at_ts_time is None:
            return
        current = connection.execute(
            """
            SELECT flag,provider_code,provider_label,source_flag_kind_raw,source_key
            FROM track_flag_current WHERE source_heat_id = ?
            """,
            (self.heat_id,),
        ).fetchone()
        if (
            current is None
            or current["source_key"] != preceding["source_key"]
            or self._same_flag_state(current, caution.flag)
        ):
            return
        source_key = (
            f"statistics-boundary:{self.heat_id}:{caution.provider_key}:"
            f"{caution.ended_at_ts_time}:{current['flag']}"
        )
        _insert_ignore(
            connection,
            "track_flag_periods",
            {
                "source_heat_id": self.heat_id,
                "flag": current["flag"],
                "provider_code": current["provider_code"],
                "provider_label": current["provider_label"],
                "started_at_us": boundary_at_us,
                "source_message_id": context.id,
                "source_key": source_key,
                "created_at_us": now_us(),
                "start_provider_ts_raw": str(caution.ended_at_ts_time),
                "start_provider_ts_us": caution.ended_at_ts_time,
                "observed_started_at_us": None,
                "calibrated_started_at_us": boundary_at_us,
                "start_clock_calibration_id": calibration_id,
                "source_flag_kind_raw": current["source_flag_kind_raw"],
                "reconciliation_source_message_id": context.id,
                "reconciliation_source_key": context.source_key,
                "reconciled_at_us": now_us(),
            },
        )
        _upsert(
            connection,
            "track_flag_current",
            {
                "source_heat_id": self.heat_id,
                "flag": current["flag"],
                "provider_code": current["provider_code"],
                "provider_label": current["provider_label"],
                "started_at_us": boundary_at_us,
                "source_message_id": context.id,
                "source_key": source_key,
                "updated_at_us": context.received_at_us,
                "start_provider_ts_raw": str(caution.ended_at_ts_time),
                "start_provider_ts_us": caution.ended_at_ts_time,
                "observed_started_at_us": None,
                "calibrated_started_at_us": boundary_at_us,
                "start_clock_calibration_id": calibration_id,
                "source_flag_kind_raw": current["source_flag_kind_raw"],
                "reconciliation_key": None,
                "reconciliation_source_message_id": context.id,
                "reconciliation_source_key": context.source_key,
                "reconciled_at_us": now_us(),
            },
            conflict_columns=("source_heat_id",),
        )

    def _reconcile_caution_timeline(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        period_id: int,
        caution: CautionPeriod,
        started_at_us: int | None,
        ended_at_us: int | None,
        calibration_id: int | None,
    ) -> None:
        """Make provider caution boundaries exclusive with their neighbour states."""

        preceding: sqlite3.Row | None = None
        if started_at_us is not None and caution.started_at_ts_time is not None:
            preceding = self._align_preceding_status(
                connection,
                context,
                excluded_period_id=period_id,
                boundary_at_us=started_at_us,
                provider_ts=caution.started_at_ts_time,
                calibration_id=calibration_id,
            )
        if ended_at_us is None or caution.ended_at_ts_time is None:
            return
        successor = self._align_following_status(
            connection,
            context,
            period_id=period_id,
            period_flag=caution.flag.kind,
            boundary_at_us=ended_at_us,
            provider_ts=caution.ended_at_ts_time,
            calibration_id=calibration_id,
        )
        if successor is None:
            self._synthesize_following_status(
                connection,
                context,
                preceding=preceding,
                caution=caution,
                boundary_at_us=ended_at_us,
                calibration_id=calibration_id,
            )

    def _reconcile_initial_green_boundary(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        provider_ts: int | None,
        boundary_at_us: int | None,
        calibration_id: int | None,
    ) -> None:
        """Use Statistics' heat-start Green time when the first state is Green."""

        if provider_ts is None or boundary_at_us is None:
            return
        period = connection.execute(
            """
            SELECT id,source_key,flag,started_at_us,calibrated_started_at_us
            FROM track_flag_periods
            WHERE source_heat_id = ?
            ORDER BY started_at_us,id
            LIMIT 1
            """,
            (self.heat_id,),
        ).fetchone()
        if period is None or period["flag"] != "GREEN" or period["started_at_us"] <= boundary_at_us:
            return
        if period["calibrated_started_at_us"] is not None:
            return
        self._set_period_start_boundary(
            connection,
            context,
            period=period,
            boundary_at_us=boundary_at_us,
            provider_ts=provider_ts,
            calibration_id=calibration_id,
        )

    def _reconcile_finish_boundary(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        *,
        provider_ts: int | None,
        boundary_at_us: int | None,
        calibration_id: int | None,
    ) -> None:
        """Treat Statistics' finish timestamp as the exact end-state transition."""

        if provider_ts is None or boundary_at_us is None:
            return
        current = connection.execute(
            """
            SELECT flag,provider_code,provider_label,source_flag_kind_raw,source_key
            FROM track_flag_current WHERE source_heat_id = ?
            """,
            (self.heat_id,),
        ).fetchone()
        period = None
        if current is not None and current["flag"] == "FINISH":
            period = connection.execute(
                """
                SELECT id,source_key,calibrated_started_at_us,start_provider_ts_us
                FROM track_flag_periods
                WHERE source_heat_id = ? AND source_key = ?
                """,
                (self.heat_id, current["source_key"]),
            ).fetchone()
        if period is None:
            period = connection.execute(
                """
                SELECT id,source_key,calibrated_started_at_us,start_provider_ts_us
                FROM track_flag_periods
                WHERE source_heat_id = ? AND flag = 'FINISH'
                  AND ABS(COALESCE(observed_started_at_us, started_at_us) - ?) <= ?
                ORDER BY ABS(COALESCE(observed_started_at_us, started_at_us) - ?),id
                LIMIT 1
                """,
                (self.heat_id, boundary_at_us, FLAG_RECONCILIATION_WINDOW_US, boundary_at_us),
            ).fetchone()
        if period is not None:
            canonical_boundary_at_us = (
                int(period["calibrated_started_at_us"])
                if period["calibrated_started_at_us"] is not None
                else boundary_at_us
            )
            canonical_provider_ts = (
                int(period["start_provider_ts_us"])
                if period["start_provider_ts_us"] is not None
                else provider_ts
            )
            self._align_preceding_status(
                connection,
                context,
                excluded_period_id=int(period["id"]),
                boundary_at_us=canonical_boundary_at_us,
                provider_ts=canonical_provider_ts,
                calibration_id=calibration_id,
            )
            if period["calibrated_started_at_us"] is None:
                self._set_period_start_boundary(
                    connection,
                    context,
                    period=period,
                    boundary_at_us=canonical_boundary_at_us,
                    provider_ts=canonical_provider_ts,
                    calibration_id=calibration_id,
                )
            return

        self._align_preceding_status(
            connection,
            context,
            excluded_period_id=None,
            boundary_at_us=boundary_at_us,
            provider_ts=provider_ts,
            calibration_id=calibration_id,
        )
        source_key = f"statistics-finish:{self.heat_id}:{provider_ts}"
        _insert_ignore(
            connection,
            "track_flag_periods",
            {
                "source_heat_id": self.heat_id,
                "flag": "FINISH",
                "provider_code": "5",
                "provider_label": "Finish flag",
                "started_at_us": boundary_at_us,
                "source_message_id": context.id,
                "source_key": source_key,
                "created_at_us": now_us(),
                "start_provider_ts_raw": str(provider_ts),
                "start_provider_ts_us": provider_ts,
                "observed_started_at_us": context.received_at_us,
                "calibrated_started_at_us": boundary_at_us,
                "start_clock_calibration_id": calibration_id,
                "source_flag_kind_raw": "5",
                "reconciliation_source_message_id": context.id,
                "reconciliation_source_key": context.source_key,
                "reconciled_at_us": now_us(),
            },
        )
        _upsert(
            connection,
            "track_flag_current",
            {
                "source_heat_id": self.heat_id,
                "flag": "FINISH",
                "provider_code": "5",
                "provider_label": "Finish flag",
                "started_at_us": boundary_at_us,
                "source_message_id": context.id,
                "source_key": source_key,
                "updated_at_us": context.received_at_us,
                "start_provider_ts_raw": str(provider_ts),
                "start_provider_ts_us": provider_ts,
                "observed_started_at_us": context.received_at_us,
                "calibrated_started_at_us": boundary_at_us,
                "start_clock_calibration_id": calibration_id,
                "source_flag_kind_raw": "5",
                "reconciliation_key": None,
                "reconciliation_source_message_id": context.id,
                "reconciliation_source_key": context.source_key,
                "reconciled_at_us": now_us(),
            },
            conflict_columns=("source_heat_id",),
        )

    def _reconcile_exact_flag_adjacencies(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
    ) -> None:
        """Use one calibrated instant for both sides of the same raw boundary."""

        rows = connection.execute(
            """
            SELECT id,flag,source_key,reconciliation_key,started_at_us,ended_at_us,
                   start_provider_ts_us,end_provider_ts_us,
                   calibrated_started_at_us,calibrated_ended_at_us,
                   start_clock_calibration_id,end_clock_calibration_id
            FROM track_flag_periods
            WHERE source_heat_id = ?
            ORDER BY started_at_us,id
            """,
            (self.heat_id,),
        ).fetchall()
        for left, right in zip(rows, rows[1:]):
            if (
                left["end_provider_ts_us"] is None
                or right["start_provider_ts_us"] is None
                or int(left["end_provider_ts_us"]) != int(right["start_provider_ts_us"])
            ):
                continue
            if right["flag"] == "FINISH" and right["calibrated_started_at_us"] is not None:
                boundary_at_us = int(right["calibrated_started_at_us"])
                calibration_id = right["start_clock_calibration_id"]
            elif right["reconciliation_key"] is not None and right["calibrated_started_at_us"] is not None:
                boundary_at_us = int(right["calibrated_started_at_us"])
                calibration_id = right["start_clock_calibration_id"]
            elif left["reconciliation_key"] is not None and left["calibrated_ended_at_us"] is not None:
                boundary_at_us = int(left["calibrated_ended_at_us"])
                calibration_id = left["end_clock_calibration_id"]
            else:
                continue
            if boundary_at_us < int(left["started_at_us"]):
                continue
            connection.execute(
                """
                UPDATE track_flag_periods
                SET ended_at_us = ?, calibrated_ended_at_us = ?, end_clock_calibration_id = ?,
                    reconciliation_source_message_id = ?, reconciliation_source_key = ?, reconciled_at_us = ?
                WHERE id = ?
                """,
                (
                    boundary_at_us,
                    boundary_at_us,
                    calibration_id,
                    context.id,
                    context.source_key,
                    now_us(),
                    left["id"],
                ),
            )
            connection.execute(
                """
                UPDATE track_flag_periods
                SET started_at_us = ?, calibrated_started_at_us = ?, start_clock_calibration_id = ?,
                    reconciliation_source_message_id = ?, reconciliation_source_key = ?, reconciled_at_us = ?
                WHERE id = ?
                """,
                (
                    boundary_at_us,
                    boundary_at_us,
                    calibration_id,
                    context.id,
                    context.source_key,
                    now_us(),
                    right["id"],
                ),
            )
            connection.execute(
                """
                UPDATE track_flag_current
                SET started_at_us = ?, calibrated_started_at_us = ?, start_clock_calibration_id = ?,
                    reconciliation_source_message_id = ?, reconciliation_source_key = ?, reconciled_at_us = ?,
                    updated_at_us = ?
                WHERE source_heat_id = ? AND source_key = ?
                """,
                (
                    boundary_at_us,
                    boundary_at_us,
                    calibration_id,
                    context.id,
                    context.source_key,
                    now_us(),
                    context.received_at_us,
                    self.heat_id,
                    right["source_key"],
                ),
            )

    def _invalidate_clean_laps_for_caution(self, connection: sqlite3.Connection, period_id: int) -> None:
        """Remove a clean mark if later flag history proves that a lap was interrupted."""
        period = connection.execute(
            "SELECT flag,started_at_us,ended_at_us FROM track_flag_periods WHERE id = ?",
            (period_id,),
        ).fetchone()
        if period is None or period["flag"] == "GREEN" or period["started_at_us"] is None:
            return
        if period["ended_at_us"] is None:
            connection.execute(
                """
                WITH lap_intervals AS (
                  SELECT id,completed_at_us,
                         LAG(completed_at_us) OVER (
                           PARTITION BY participant_id ORDER BY lap_number
                         ) AS lap_started_at_us
                  FROM laps
                  WHERE source_heat_id = ? AND completed_at_us IS NOT NULL
                )
                UPDATE laps SET is_clean = 0
                WHERE id IN (
                  SELECT id FROM lap_intervals
                  WHERE lap_started_at_us IS NOT NULL
                    AND completed_at_us > ?
                )
                """,
                (self.heat_id, period["started_at_us"]),
            )
            return
        connection.execute(
            """
            WITH lap_intervals AS (
              SELECT id,completed_at_us,
                     LAG(completed_at_us) OVER (
                       PARTITION BY participant_id ORDER BY lap_number
                     ) AS lap_started_at_us
              FROM laps
              WHERE source_heat_id = ? AND completed_at_us IS NOT NULL
            )
            UPDATE laps SET is_clean = 0
            WHERE id IN (
              SELECT id FROM lap_intervals
              WHERE lap_started_at_us IS NOT NULL
                AND lap_started_at_us < ? AND completed_at_us > ?
            )
            """,
            (self.heat_id, period["ended_at_us"], period["started_at_us"]),
        )

    def _close_caution_if_live_flag_superseded(
        self,
        connection: sqlite3.Connection,
        period_id: int,
        flag: FlagState,
    ) -> None:
        """Do not let a delayed open Statistics record reopen a live-closed flag."""
        current = connection.execute(
            """
            SELECT flag,provider_code,provider_label,source_flag_kind_raw,started_at_us,
                   observed_started_at_us,source_message_id,source_key
            FROM track_flag_current WHERE source_heat_id = ?
            """,
            (self.heat_id,),
        ).fetchone()
        if current is None or self._same_flag_state(current, flag):
            return
        period = connection.execute(
            "SELECT started_at_us,ended_at_us,observed_ended_at_us FROM track_flag_periods WHERE id = ?",
            (period_id,),
        ).fetchone()
        if period is None or period["ended_at_us"] is not None:
            return
        boundary_us = current["observed_started_at_us"] or current["started_at_us"]
        if boundary_us is None or (
            period["started_at_us"] is not None and int(boundary_us) < int(period["started_at_us"])
        ):
            return
        connection.execute(
            """
            UPDATE track_flag_periods
            SET ended_at_us = ?, observed_ended_at_us = COALESCE(observed_ended_at_us, ?),
                ended_source_message_id = COALESCE(ended_source_message_id, ?),
                ended_source_key = COALESCE(ended_source_key, ?)
            WHERE id = ? AND ended_at_us IS NULL
            """,
            (
                boundary_us,
                boundary_us,
                current["source_message_id"],
                current["source_key"],
                period_id,
            ),
        )


class TimingNormalizerRegistry:
    """Share one reducer per active analysis session inside the ingest process."""

    def __init__(self) -> None:
        self._normalizers: dict[str, TimingNormalizer] = {}

    def __call__(
        self,
        connection: sqlite3.Connection,
        frame: StoredFrame,
        messages: tuple[SignalRMessage, ...],
    ) -> None:
        row = connection.execute(
            "SELECT analysis_session_id FROM feed_frames WHERE id = ?", (frame.id,)
        ).fetchone()
        if row is None:
            raise NormalizerError(f"Cannot find analysis session for frame {frame.id}")
        session_id = row["analysis_session_id"]
        normalizer = self._normalizers.setdefault(session_id, TimingNormalizer(session_id))
        normalizer(connection, frame, messages)
