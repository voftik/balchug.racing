"""Stateful reducer for the provider's dynamic sparse result grid.

This stays separate from database writes so a restarted worker can rebuild the
same row state from raw ``r_i``/``r_c`` messages before emitting derived facts.
Unknown columns and cell styling metadata remain represented, rather than being
coerced into a guessed timing field.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .normalization import ResultColumn, result_columns


class ResultGridStateError(ValueError):
    """A checkpoint cannot safely reconstruct the sparse result-grid reducer."""


@dataclass(frozen=True)
class ResultCell:
    """The primary source value plus any provider rendering metadata."""

    value: Any
    presentation: tuple[Any, ...]


@dataclass
class ResultGrid:
    """Current result grid keyed by provider row and dynamic column indexes."""

    layout: Any = None
    columns: dict[int, ResultColumn] = field(default_factory=dict)
    rows: dict[int, dict[int, ResultCell]] = field(default_factory=dict)
    metadata_changes: list[tuple[Any, ...]] = field(default_factory=list)
    layout_generation: int = 0
    schema_pending: bool = True
    schema_conflicts: dict[str, tuple[int, ...]] = field(default_factory=dict)

    @staticmethod
    def _schema_conflicts(columns: Mapping[int, ResultColumn]) -> dict[str, tuple[int, ...]]:
        """Return duplicate canonical headers that would make a row ambiguous.

        The provider can move columns at runtime.  Two recognized aliases for
        the same canonical field are not safe to choose between, because the
        later sparse cell would silently overwrite the earlier one in
        :meth:`row_values`.  Unknown fields remain lossless and are therefore
        not a conflict.
        """

        indexes_by_key: dict[str, list[int]] = {}
        for index, column in columns.items():
            if column.key is not None:
                indexes_by_key.setdefault(column.key, []).append(index)
        return {
            key: tuple(indexes)
            for key, indexes in indexes_by_key.items()
            if len(indexes) > 1
        }

    @property
    def schema_ready(self) -> bool:
        """Whether cells can safely be materialized using the current layout."""

        return self.layout is not None and not self.schema_pending and not self.schema_conflicts

    def set_layout(self, layout: Any) -> None:
        """Install a new layout and require a fresh snapshot before deltas.

        ``r_l`` can remap an existing column index (notably ``POS``/``PIC``).
        Retaining old sparse cells across that boundary would reinterpret a
        factual absolute position as a class position, or vice versa.  Clear
        the materialized state and wait for the next authoritative ``r_i``.
        """

        self.layout = copy.deepcopy(layout)
        self.columns = result_columns(layout)
        self.rows.clear()
        self.schema_pending = True
        self.schema_conflicts = self._schema_conflicts(self.columns)
        self.layout_generation += 1

    @staticmethod
    def _change_rows(value: Any) -> tuple[Sequence[Any], ...]:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            return ()
        return tuple(item for item in value if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)))

    def apply_snapshot(self, payload: Any) -> None:
        """Replace grid cells from an initial ``r_i`` snapshot."""
        if not isinstance(payload, Mapping):
            return
        layout = payload.get("l")
        if layout is not None:
            self.set_layout(layout)
        self.rows.clear()
        self.metadata_changes.clear()
        if self.layout is None:
            self.schema_pending = True
            return
        # A snapshot is the only message that can make a new r_l schema live.
        # Conflicting canonical headers remain fail-closed even after r_i.
        self.schema_pending = False
        self.apply_changes(payload.get("r"))

    def apply_changes(self, payload: Any) -> None:
        """Merge sparse ``r_c`` cells without treating negative cells as data rows."""
        for change in self._change_rows(payload):
            if len(change) < 3 or type(change[0]) is not int or type(change[1]) is not int:
                self.metadata_changes.append(tuple(copy.deepcopy(change)))
                continue
            row_index, column_index = change[0], change[1]
            if row_index < 0 or column_index < 0:
                self.metadata_changes.append(tuple(copy.deepcopy(change)))
                continue
            # Do not retain a delta that arrived between a layout change and
            # its full r_i snapshot, or under an ambiguous duplicate header.
            # The writer still persists the raw message independently; this
            # reducer simply refuses to turn it into live tactical facts.
            if not self.schema_ready:
                continue
            values = tuple(copy.deepcopy(value) for value in change[2:])
            self.rows.setdefault(row_index, {})[column_index] = ResultCell(
                value=values[0] if values else None,
                presentation=values[1:],
            )
        self.metadata_changes = self.metadata_changes[-200:]

    def remove_rows(self, payload: Any) -> None:
        """Apply a conservative ``r_d`` removal without guessing its other fields."""
        for change in self._change_rows(payload):
            if not change or type(change[0]) is not int or change[0] < 0:
                continue
            self.rows.pop(change[0], None)

    def row_values(self, row_index: int) -> dict[str, Any]:
        """Materialize recognized keys and raw unknown columns for one source row."""
        if not self.schema_ready:
            return {}
        result: dict[str, Any] = {}
        for column_index, cell in self.rows.get(row_index, {}).items():
            column = self.columns.get(column_index)
            if column is None:
                result[f"column_{column_index}"] = cell.value
                continue
            key = column.key or f"unknown:{column.source_name}:{column.source_parameter or ''}"
            result[key] = cell.value
        return result

    def all_rows(self) -> dict[int, dict[str, Any]]:
        if not self.schema_ready:
            return {}
        return {row_index: self.row_values(row_index) for row_index in sorted(self.rows)}

    def snapshot(self) -> dict[str, Any]:
        """JSON-safe deterministic reducer state for a future checkpoint."""
        return {
            "layout": copy.deepcopy(self.layout),
            "rows": {
                str(row_index): {
                    str(column_index): [copy.deepcopy(cell.value), *copy.deepcopy(cell.presentation)]
                    for column_index, cell in sorted(cells.items())
                }
                for row_index, cells in sorted(self.rows.items())
            },
            "metadata_changes": [list(change) for change in self.metadata_changes],
            "layout_generation": self.layout_generation,
            "schema_pending": self.schema_pending,
            "schema_conflicts": {
                key: list(indexes) for key, indexes in sorted(self.schema_conflicts.items())
            },
        }

    def restore_snapshot(self, state: Any) -> None:
        """Restore a strict checkpoint without replaying provider mutations.

        Reconstructing through :meth:`set_layout` would increment the layout
        generation and make a previously accepted same-connection `r_c` look
        unsafe. The checkpoint therefore stores the generation while this
        method recomputes every derivable field from the raw layout and rejects
        a mismatch rather than accepting synthetic sparse state.
        """

        if not isinstance(state, Mapping):
            raise ResultGridStateError("result grid checkpoint must be an object")
        layout = copy.deepcopy(state.get("layout"))
        generation = _checkpoint_non_negative_integer(state.get("layout_generation"), "layout_generation")
        schema_pending = state.get("schema_pending")
        if type(schema_pending) is not bool:
            raise ResultGridStateError("schema_pending must be a boolean")
        columns = result_columns(layout)
        conflicts = self._schema_conflicts(columns)
        expected_conflicts = _checkpoint_conflicts(state.get("schema_conflicts"))
        if conflicts != expected_conflicts:
            raise ResultGridStateError("schema_conflicts do not match checkpoint layout")
        metadata = state.get("metadata_changes")
        if not isinstance(metadata, Sequence) or isinstance(metadata, (str, bytes, bytearray)):
            raise ResultGridStateError("metadata_changes must be an array")
        if len(metadata) > 200:
            raise ResultGridStateError("metadata_changes exceeds reducer bound")
        restored_metadata: list[tuple[Any, ...]] = []
        for item in metadata:
            if not isinstance(item, Sequence) or isinstance(item, (str, bytes, bytearray)):
                raise ResultGridStateError("metadata_changes item must be an array")
            restored_metadata.append(tuple(copy.deepcopy(value) for value in item))

        raw_rows = state.get("rows")
        if not isinstance(raw_rows, Mapping):
            raise ResultGridStateError("rows must be an object")
        restored_rows: dict[int, dict[int, ResultCell]] = {}
        for row_key, raw_cells in raw_rows.items():
            row_index = _checkpoint_non_negative_integer(row_key, "row index")
            if not isinstance(raw_cells, Mapping):
                raise ResultGridStateError("row cells must be an object")
            cells: dict[int, ResultCell] = {}
            for column_key, raw_cell in raw_cells.items():
                column_index = _checkpoint_non_negative_integer(column_key, "column index")
                if not isinstance(raw_cell, Sequence) or isinstance(raw_cell, (str, bytes, bytearray)) or not raw_cell:
                    raise ResultGridStateError("result cell must be a non-empty array")
                cells[column_index] = ResultCell(
                    value=copy.deepcopy(raw_cell[0]),
                    presentation=tuple(copy.deepcopy(value) for value in raw_cell[1:]),
                )
            restored_rows[row_index] = cells
        if layout is None and restored_rows:
            raise ResultGridStateError("rows require a result layout")
        if schema_pending and restored_rows:
            raise ResultGridStateError("schema-pending checkpoint cannot retain result rows")

        self.layout = layout
        self.columns = columns
        self.rows = restored_rows
        self.metadata_changes = restored_metadata
        self.layout_generation = generation
        self.schema_pending = schema_pending
        self.schema_conflicts = conflicts


def _checkpoint_non_negative_integer(value: Any, field_name: str) -> int:
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    raise ResultGridStateError(f"{field_name} must be a non-negative integer")


def _checkpoint_conflicts(value: Any) -> dict[str, tuple[int, ...]]:
    if not isinstance(value, Mapping):
        raise ResultGridStateError("schema_conflicts must be an object")
    result: dict[str, tuple[int, ...]] = {}
    for key, indexes in value.items():
        if not isinstance(key, str) or not key:
            raise ResultGridStateError("schema conflict key must be a non-empty string")
        if not isinstance(indexes, Sequence) or isinstance(indexes, (str, bytes, bytearray)):
            raise ResultGridStateError("schema conflict indexes must be an array")
        parsed = tuple(_checkpoint_non_negative_integer(index, "schema conflict index") for index in indexes)
        if len(parsed) < 2:
            raise ResultGridStateError("schema conflict must contain at least two indexes")
        result[key] = parsed
    return result
