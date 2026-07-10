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
from .normalization import (
    ConnectionClockCalibrator,
    CautionPeriod,
    FlagState,
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
    if source_us is None or is_open_ended_ts_time(value):
        return None
    return source_us // 1_000


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

    def __init__(self, analysis_session_id: str):
        self.analysis_session_id = analysis_session_id
        self.grid = ResultGrid()
        self.heat: dict[str, Any] = {}
        self.statistics: dict[str, Any] = {}
        self._calibrators: dict[str, ConnectionClockCalibrator] = {}
        self._heat_id: int | None = None
        self._layout_id: int | None = None
        self._provider_heat_start_ts: int | None = None
        self._finish_sector_ids: set[int] = set()
        self._primed = False

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
        with _write_transaction(connection):
            self._ensure_heat(connection, contexts[0].received_at_us)
            for context in contexts:
                self._apply_message(connection, context, write=True)

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
                    if old_flag is None or (old_flag.kind, old_flag.provider_code) != (
                        new_flag.kind,
                        new_flag.provider_code,
                    ):
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
        calibrated_start = self._clock(context.connection_id).to_utc_us(provider_start)
        connection.execute(
            """
            UPDATE source_heats
            SET external_name = COALESCE(?, external_name),
                provider_started_at_us = COALESCE(?, provider_started_at_us)
            WHERE id = ?
            """,
            (_text(self.heat.get("n")), calibrated_start, self.heat_id),
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
        state = parse_result_state(row.get("state")) if "state" in row else ResultState(raw=None, kind="UNKNOWN")
        pit_count_raw = _text(row.get("pit_stops"))
        pit_count = _integer(row.get("pit_stops"), minimum=0)
        old = connection.execute(
            """
            SELECT state_kind,provider_pit_count,laps
            FROM participant_state_current
            WHERE source_heat_id = ? AND participant_id = ?
            """,
            (self.heat_id, participant_id),
        ).fetchone()
        clock = self._clock(context.connection_id)
        timer_at_us = clock.to_utc_us(state.timer_target_ts_time)
        calibration_id = self._latest_calibration_id(connection, context.connection_id)
        sectors = {
            key: row[key]
            for key in sorted(row)
            if key.startswith("sector_")
        }
        state_event_key = f"{context.source_key}:state:{row_index}"
        observed = _insert_ignore(
            connection,
            "participant_state_observations",
            {
                "source_heat_id": self.heat_id,
                "participant_id": participant_id,
                "layout_version_id": layout_id,
                "provider_row_index": row_index,
                "state_raw": _text(state.raw),
                "state_kind": state.kind,
                "state_timer_target_raw": state.timer_target_raw,
                "state_timer_target_provider_us": state.timer_target_ts_time,
                "state_timer_target_at_us": timer_at_us,
                "state_timer_calibration_id": calibration_id,
                "provider_pit_count_raw": pit_count_raw,
                "provider_pit_count": pit_count,
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
                "state": state.kind,
                "state_raw": _text(state.raw),
                "state_kind": state.kind,
                "current_driver_name": _text(row.get("current_driver")),
                "current_driver_stint_raw": _text(row.get("driver_stint")),
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
                "pit_time_raw": _text(row.get("pit_time")),
                "source_message_id": context.id,
                "source_key": context.source_key,
                "updated_at_us": context.received_at_us,
                "state_timer_target_raw": state.timer_target_raw,
                "state_timer_target_provider_us": state.timer_target_ts_time,
                "state_timer_target_at_us": timer_at_us,
                "state_timer_calibration_id": calibration_id,
                "state_timer_source_message_id": context.id,
                "state_timer_source_key": context.source_key,
                "state_timer_observed_at_us": context.received_at_us,
                "provider_pit_count": pit_count,
                "provider_pit_count_raw": pit_count_raw,
                "provider_pit_count_source_message_id": context.id,
                "provider_pit_count_source_key": context.source_key,
                "provider_pit_count_observed_at_us": context.received_at_us,
            },
            conflict_columns=("source_heat_id", "participant_id"),
        )
        if observed:
            source_laps = _integer(row.get("laps"), minimum=0)
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
                    state_kind=state.kind,
                )
            self._reconcile_pit_and_tire_stint(
                connection,
                context,
                participant_id,
                state,
                pit_count,
                _integer(row.get("laps"), minimum=0),
                _text(row.get("pit_time")),
                old,
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
        for lap_number in range(previous_lap + 1, current_lap + 1):
            is_latest = lap_number == current_lap
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
                    "completed_at_us": context.received_at_us if is_latest else None,
                    "duration_ms": last_lap_ms if is_latest else None,
                    "sectors_json": None,
                    "flag": self._current_flag_kind(connection),
                    "is_in_lap": 0,
                    "is_out_lap": int(is_latest and state_kind == "OUT_LAP"),
                    "crosses_pit": 0,
                    "is_clean": 0,
                    "source_message_id": context.id,
                    "source_key": context.source_key,
                    "created_at_us": now_us(),
                },
            )

    def _write_immediate_flag(self, connection: sqlite3.Connection, context: FrameMessage, flag: FlagState) -> None:
        current = connection.execute(
            "SELECT flag,provider_code,started_at_us,source_key FROM track_flag_current WHERE source_heat_id = ?",
            (self.heat_id,),
        ).fetchone()
        if current is not None and current["flag"] == flag.kind and str(current["provider_code"]) == str(flag.provider_code):
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
    def _pit_duration_ms(pit_time_raw: str | None) -> int | None:
        if not pit_time_raw or pit_time_raw[:1].upper() != "L":
            return None
        return _duration_us_to_ms(pit_time_raw[1:])

    def _reconcile_pit_and_tire_stint(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        participant_id: str,
        state: ResultState,
        pit_count: int | None,
        lap_number: int | None,
        pit_time_raw: str | None,
        previous: sqlite3.Row | None,
    ) -> None:
        """Create pit/tyre facts only from observed state/count transitions.

        A completed pit closes the current tyre stint and opens a new one. No
        manual tyre input or compound override exists in this data path.
        """
        previous_state = previous["state_kind"] if previous is not None else None
        previous_count = previous["provider_pit_count"] if previous is not None else None
        now_in_pit = state.kind == "IN_PIT"
        was_in_pit = previous_state == "IN_PIT"
        count_increased = (
            pit_count is not None
            and previous_count is not None
            and pit_count > previous_count
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
        if (was_in_pit is False and now_in_pit) or (count_increased and opened is None):
            max_stop = int(
                connection.execute(
                    "SELECT COALESCE(MAX(stop_number), 0) FROM pit_stops WHERE source_heat_id = ? AND participant_id = ?",
                    (self.heat_id, participant_id),
                ).fetchone()[0]
            )
            stop_number = pit_count if pit_count is not None and pit_count > 0 else max_stop + 1
            if stop_number <= max_stop:
                stop_number = max_stop + 1
            entered_at_us = self._pit_entered_at_us(pit_time_raw, context)
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO pit_stops(
                  id,source_heat_id,participant_id,stop_number,entered_at_us,entered_lap,
                  completed,entered_source_message_id,entered_source_key,created_at_us,updated_at_us
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(uuid.uuid5(uuid.NAMESPACE_URL, f"balchug-racing:pit:{self.heat_id}:{participant_id}:{stop_number}")),
                    self.heat_id,
                    participant_id,
                    stop_number,
                    entered_at_us,
                    lap_number,
                    0,
                    context.id,
                    context.source_key,
                    now_us(),
                    now_us(),
                ),
            )
            if cursor.rowcount:
                opened = connection.execute(
                    """
                    SELECT id,stop_number,entered_at_us,entered_lap
                    FROM pit_stops WHERE source_heat_id = ? AND participant_id = ? AND stop_number = ?
                    """,
                    (self.heat_id, participant_id, stop_number),
                ).fetchone()
        exits_pit = was_in_pit and not now_in_pit
        if exits_pit and opened is not None:
            duration_ms = self._pit_duration_ms(pit_time_raw)
            if duration_ms is None:
                duration_ms = max(0, (context.received_at_us - int(opened["entered_at_us"])) // 1_000)
            connection.execute(
                """
                UPDATE pit_stops
                SET exited_at_us = ?, exited_lap = ?, pit_lane_ms = ?, completed = 1,
                    exited_source_message_id = ?, exited_source_key = ?, updated_at_us = ?
                WHERE id = ? AND completed = 0
                """,
                (
                    context.received_at_us,
                    lap_number,
                    duration_ms,
                    context.id,
                    context.source_key,
                    now_us(),
                    opened["id"],
                ),
            )
            self._complete_tire_stint(connection, context, participant_id, lap_number)
        else:
            self._ensure_tire_stint(connection, context, participant_id, lap_number)

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
                and not passing.is_in_pit
                and passing.sector_id in self._finish_sector_ids
            ):
                self._complete_lap_from_tracker(
                    connection,
                    context,
                    participant_id,
                    event_fingerprint,
                    passed_at_us,
                )

    def _complete_lap_from_tracker(
        self,
        connection: sqlite3.Connection,
        context: FrameMessage,
        participant_id: str,
        event_fingerprint: str,
        completed_at_us: int | None,
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
        duration_ms = (
            max(0, (source_completed_at_us - int(previous["completed_at_us"])) // 1_000)
            if previous is not None and previous["completed_at_us"] is not None
            else None
        )
        inserted = _insert_ignore(
            connection,
            "laps",
            {
                "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"balchug-racing:lap:{self.heat_id}:{participant_id}:{event_fingerprint}")),
                "source_heat_id": self.heat_id,
                "participant_id": participant_id,
                "lap_number": lap_number,
                "completed_at_us": source_completed_at_us,
                "duration_ms": duration_ms,
                "sectors_json": None,
                "flag": self._current_flag_kind(connection),
                "is_in_lap": 0,
                "is_out_lap": int(state["state_kind"] == "OUT_LAP"),
                "crosses_pit": 0,
                "is_clean": 0,
                "source_message_id": context.id,
                "source_key": context.source_key,
                "created_at_us": now_us(),
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
        green_provider = parse_ts_time(green_raw)
        finish_provider = parse_ts_time(finish_raw)
        summary = update.summary
        event_key = f"{context.source_key}:statistics"
        typed_values = {
            "source_heat_id": self.heat_id,
            "heat_name_raw": _text(summary.get("heat_name")),
            "green_flag_provider_ts_raw": green_raw,
            "green_flag_provider_ts_us": green_provider,
            "green_flag_at_us": clock.to_utc_us(green_provider),
            "green_flag_calibration_id": calibration_id,
            "finish_flag_provider_ts_raw": finish_raw,
            "finish_flag_provider_ts_us": finish_provider,
            "finish_flag_at_us": clock.to_utc_us(finish_provider),
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
            "raw_record_json": _json(caution.__dict__),
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
                ORDER BY ABS(COALESCE(observed_started_at_us, started_at_us) - ?), id
                LIMIT 1
                """,
                (self.heat_id, caution.flag.kind, candidate_time),
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
                SET start_provider_ts_raw = ?, end_provider_ts_raw = ?, start_provider_ts_us = ?,
                    end_provider_ts_us = ?, calibrated_started_at_us = ?, calibrated_ended_at_us = ?,
                    started_at_us = COALESCE(?, started_at_us), ended_at_us = ?,
                    observed_ended_at_us = CASE WHEN ? IS NULL THEN observed_ended_at_us ELSE ? END,
                    start_clock_calibration_id = ?, end_clock_calibration_id = ?, source_flag_kind_raw = ?,
                    clock_stopped_raw = ?, remark_raw = ?, reconciliation_key = ?,
                    reconciliation_source_message_id = ?, reconciliation_source_key = ?, reconciled_at_us = ?
                WHERE id = ?
                """,
                (
                    _text(caution.started_at_raw),
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
            "SELECT flag FROM track_flag_current WHERE source_heat_id = ?",
            (self.heat_id,),
        ).fetchone()
        if current is not None and current["flag"] == caution.flag.kind and ended_at_us is None:
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
