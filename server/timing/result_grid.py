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

    def set_layout(self, layout: Any) -> None:
        """Install a new layout while retaining sparse cells only when explicit."""
        self.layout = copy.deepcopy(layout)
        self.columns = result_columns(layout)
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
        }
