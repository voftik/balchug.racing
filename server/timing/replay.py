"""Deterministic replay for a timing recorder NDJSON file."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


def _normalise(value: Any) -> Any:
    """Return JSON-safe state with deterministic key ordering for hashing."""
    if isinstance(value, dict):
        return {str(key): _normalise(value[key]) for key in sorted(value, key=lambda item: str(item))}
    if isinstance(value, tuple):
        return [_normalise(item) for item in value]
    if isinstance(value, list):
        return [_normalise(item) for item in value]
    return value


def _payload(args: list[Any]) -> Any:
    return args[0] if len(args) == 1 else args


def _changes(value: Any) -> list[list[Any]]:
    """Normalize a provider r_c/r_d payload into a list of change rows."""
    if not isinstance(value, list):
        return []
    if value and all(isinstance(item, list) for item in value):
        return value
    return []


@dataclass
class TimingReducer:
    """Sparse, schema-driven state reducer sufficient for recorder/replay.

    Result cell updates remain keyed by their provider row/column indexes until
    the normalizer learns the concrete heat layout. This prevents a premature
    hardcoded table schema from corrupting raw timing data.
    """

    handles: dict[str, Any] = field(default_factory=dict)
    result_layout: Any = None
    result_snapshot: Any = None
    result_cells: dict[str, Any] = field(default_factory=dict)
    result_meta_changes: list[Any] = field(default_factory=list)
    removed_rows: list[Any] = field(default_factory=list)
    tracker_passings: list[Any] = field(default_factory=list)
    latest_heat: Any = None
    latest_server_time: Any = None
    unknown_handles: dict[str, int] = field(default_factory=dict)
    messages_applied: int = 0

    def _apply_result_cells(self, changes: Any) -> None:
        for change in _changes(changes):
            if len(change) >= 3 and isinstance(change[0], int) and isinstance(change[1], int):
                if change[0] >= 0 and change[1] >= 0:
                    self.result_cells[f"{change[0]}:{change[1]}"] = copy.deepcopy(change[2:])
                else:
                    self.result_meta_changes.append(copy.deepcopy(change))
        self.result_meta_changes = self.result_meta_changes[-100:]

    def apply(self, handle: str, args: list[Any]) -> None:
        value = _payload(args)
        self.messages_applied += 1
        self.handles[handle] = copy.deepcopy(value)

        if handle == "r_l":
            self.result_layout = copy.deepcopy(value)
        elif handle == "r_i":
            self.result_snapshot = copy.deepcopy(value)
            self.result_cells.clear()
            self.result_meta_changes.clear()
            self.removed_rows.clear()
            if isinstance(value, dict):
                self.result_layout = copy.deepcopy(value.get("l", self.result_layout))
                self._apply_result_cells(value.get("r", []))
        elif handle == "r_c":
            self._apply_result_cells(value)
        elif handle == "r_d":
            for change in _changes(value):
                self.removed_rows.append(copy.deepcopy(change))
        elif handle == "t_p":
            for passing in _changes(value):
                self.tracker_passings.append(copy.deepcopy(passing))
            self.tracker_passings = self.tracker_passings[-1000:]
        elif handle in {"h_h", "h_i"}:
            # h_h is commonly a partial patch. Keep the h_i flag, heat name
            # and clock fields unless the delta explicitly replaces them.
            if isinstance(self.latest_heat, dict) and isinstance(value, dict):
                self.latest_heat = {**self.latest_heat, **copy.deepcopy(value)}
            else:
                self.latest_heat = copy.deepcopy(value)
        elif handle == "s_t":
            self.latest_server_time = copy.deepcopy(value)
        elif handle in {"a_i", "a_u", "t_i", "m_i", "s_i", "h_i"}:
            pass
        else:
            self.unknown_handles[handle] = self.unknown_handles.get(handle, 0) + 1

    def apply_record(self, record: dict[str, Any]) -> None:
        # v1 recordings write raw frames first and decoded handles separately.
        # The legacy combined frame shape is accepted for old short fixtures.
        if record.get("kind") not in {"decoded", "frame"}:
            return
        for message in record.get("messages", []):
            handle = message.get("handle")
            args = message.get("args", [])
            if isinstance(handle, str) and isinstance(args, list):
                self.apply(handle, args)

    def snapshot(self) -> dict[str, Any]:
        return _normalise(
            {
                "handles": self.handles,
                "result_layout": self.result_layout,
                "result_snapshot": self.result_snapshot,
                "result_cells": self.result_cells,
                "result_meta_changes": self.result_meta_changes,
                "removed_rows": self.removed_rows,
                "tracker_passings": self.tracker_passings,
                "latest_heat": self.latest_heat,
                "latest_server_time": self.latest_server_time,
                "unknown_handles": self.unknown_handles,
                "messages_applied": self.messages_applied,
            }
        )

    def result_columns(self) -> dict[int, str]:
        """Map dynamic provider column indexes to stable field labels."""
        if not isinstance(self.result_layout, dict):
            return {}
        headers = self.result_layout.get("h", [])
        if not isinstance(headers, list):
            return {}
        columns: dict[int, str] = {}
        for index, header in enumerate(headers):
            if not isinstance(header, dict):
                continue
            name = str(header.get("n", index))
            parameter = str(header.get("p", ""))
            columns[index] = f"{name}({parameter})" if parameter else name
        return columns

    def result_rows(self) -> dict[int, dict[str, Any]]:
        """Materialize sparse cells using only the current dynamic layout."""
        columns = self.result_columns()
        rows: dict[int, dict[str, Any]] = {}
        for key, values in self.result_cells.items():
            row_text, column_text = key.split(":", 1)
            row, column = int(row_text), int(column_text)
            rows.setdefault(row, {})[columns.get(column, f"column_{column}")] = values[0] if values else None
        return rows

    def state_hash(self) -> str:
        encoded = json.dumps(self.snapshot(), ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def iter_records(path: Path) -> Iterable[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid NDJSON at line {line_number}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"NDJSON line {line_number} was not an object")
            yield record


def replay_file(path: Path) -> TimingReducer:
    reducer = TimingReducer()
    for record in iter_records(path):
        reducer.apply_record(record)
    return reducer
