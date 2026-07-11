"""Durable projection of Time Service Race Control screen messages.

The provider exposes the current Race Control board as a tiny mutable SignalR
collection.  This module makes that collection auditable without treating a
rendered message as a timestamped race fact: the exact available clock is the
recorder receive instant supplied by the caller.  The raw frame/message remains
the primary evidence; the two tables written here are an immutable operation
ledger and a separate current materialization.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .config import now_us
from .screen_messages import ScreenMessagePatch, parse_screen_message_update


class RaceControlStoreError(RuntimeError):
    """A caller supplied ambiguous source provenance for a board mutation."""


@dataclass(frozen=True)
class RaceControlSource:
    """Immutable provenance for one decoded SignalR invocation."""

    source_heat_id: int
    source_frame_id: int
    source_message_id: int
    source_message_ordinal: int
    source_key: str
    observed_at_us: int


@dataclass(frozen=True)
class RaceControlProjectionResult:
    """Small diagnostics useful to a bounded historical backfill."""

    action: str
    observations_written: int
    current_messages_affected: int


def project_screen_message(
    connection: sqlite3.Connection,
    *,
    source: RaceControlSource,
    handle: str,
    args: Any,
) -> RaceControlProjectionResult:
    """Apply one `m_*` invocation inside the caller's write transaction.

    The operation is replay-safe: an observation is uniquely identified by its
    heat/source key/change ordinal.  `m_i` uses the browser's documented
    reverse application order, while an incomplete snapshot can never remove a
    currently visible message.
    """

    _validate_source(source)
    update = parse_screen_message_update(handle, args)
    payload_json = _canonical_json(_unwrap_signalr_args(args))
    written = 0
    affected = 0

    if update.operation == "SNAPSHOT":
        if not update.patches:
            written += _insert_observation(
                connection,
                source=source,
                source_handle=handle,
                action="INITIAL_SNAPSHOT" if update.snapshot_complete else "UNKNOWN",
                source_change_ordinal=0,
                payload_json=payload_json,
            )[1]
        seen_ids: set[str] = set()
        for patch in update.patches:
            observation_id, inserted = _insert_observation(
                connection,
                source=source,
                source_handle=handle,
                action="INITIAL_SNAPSHOT",
                source_change_ordinal=patch.source_ordinal,
                payload_json=payload_json,
                patch=patch,
            )
            written += inserted
            seen_ids.add(patch.provider_message_id)
            if inserted:
                affected += _upsert_current(
                    connection,
                    source=source,
                    action="INITIAL_SNAPSHOT",
                    patch=patch,
                    observation_id=observation_id,
                )
        # A replayed older snapshot must not roll current state backwards.
        # The immutable insertion tells us whether this exact source operation
        # is new to this materialization transaction.
        if update.snapshot_complete and written:
            affected += _reconcile_snapshot_absences(
                connection,
                source=source,
                visible_message_ids=seen_ids,
            )
        return RaceControlProjectionResult(
            "INITIAL_SNAPSHOT" if update.snapshot_complete else "UNKNOWN",
            written,
            affected,
        )

    if update.operation == "UPSERT":
        patch = update.patches[0]
        observation_id, inserted = _insert_observation(
            connection,
            source=source,
            source_handle=handle,
            action="UPSERT",
            source_change_ordinal=0,
            payload_json=payload_json,
            patch=patch,
        )
        if inserted:
            affected += _upsert_current(
                connection,
                source=source,
                action="UPSERT",
                patch=patch,
                observation_id=observation_id,
            )
        return RaceControlProjectionResult("UPSERT", inserted, affected)

    if update.operation == "DELETE":
        observation_id, inserted = _insert_observation(
            connection,
            source=source,
            source_handle=handle,
            action="DELETE",
            source_change_ordinal=0,
            payload_json=payload_json,
            message_id=update.provider_message_id,
        )
        if inserted:
            affected += _mark_removed(
                connection,
                source=source,
                message_ids=(update.provider_message_id,) if update.provider_message_id is not None else (),
                action="DELETE",
                observation_id=observation_id,
                source_change_ordinal=0,
            )
        return RaceControlProjectionResult("DELETE", inserted, affected)

    if update.operation == "RESET":
        observation_id, inserted = _insert_observation(
            connection,
            source=source,
            source_handle=handle,
            action="CLEAR",
            source_change_ordinal=0,
            payload_json=payload_json,
        )
        if inserted:
            affected += _mark_removed(
                connection,
                source=source,
                message_ids=None,
                action="CLEAR",
                observation_id=observation_id,
                source_change_ordinal=0,
            )
        return RaceControlProjectionResult("CLEAR", inserted, affected)

    # Invalid known payloads and unknown m_* handles remain represented as
    # immutable raw observations, but cannot destructively mutate the board.
    _, inserted = _insert_observation(
        connection,
        source=source,
        source_handle=handle,
        action="UNKNOWN",
        source_change_ordinal=0,
        payload_json=payload_json,
    )
    return RaceControlProjectionResult("UNKNOWN", inserted, 0)


def _insert_observation(
    connection: sqlite3.Connection,
    *,
    source: RaceControlSource,
    source_handle: str,
    action: str,
    source_change_ordinal: int,
    payload_json: str,
    patch: ScreenMessagePatch | None = None,
    message_id: str | None = None,
) -> tuple[int, int]:
    """Insert one immutable ledger row and return its id plus insert count."""

    if patch is not None:
        message_id = patch.provider_message_id
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO race_control_message_observations(
          source_heat_id,source_handle,operation,message_id_raw,text_raw,line,modality,
          background_color_raw,font_color_raw,provider_occurred_at_us,raw_record_json,raw_payload_json,
          source_frame_id,source_message_id,source_message_ordinal,source_key,source_change_ordinal,
          observed_at_us,created_at_us
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source.source_heat_id,
            source_handle,
            action,
            message_id,
            patch.text if patch is not None and "text" in patch.changed_fields else None,
            patch.line if patch is not None and "line" in patch.changed_fields else None,
            patch.modality if patch is not None and "modality" in patch.changed_fields else None,
            patch.background_color if patch is not None and "background_color" in patch.changed_fields else None,
            patch.font_color if patch is not None and "font_color" in patch.changed_fields else None,
            None,
            patch.raw_payload_json if patch is not None else None,
            payload_json,
            source.source_frame_id,
            source.source_message_id,
            source.source_message_ordinal,
            source.source_key,
            source_change_ordinal,
            source.observed_at_us,
            now_us(),
        ),
    )
    row = connection.execute(
        """
        SELECT id FROM race_control_message_observations
        WHERE source_heat_id = ? AND source_key = ? AND source_change_ordinal = ?
        """,
        (source.source_heat_id, source.source_key, source_change_ordinal),
    ).fetchone()
    if row is None:  # pragma: no cover - protects a broken database constraint
        raise RaceControlStoreError("Race Control observation did not persist")
    return int(row["id"]), max(cursor.rowcount, 0)


def _upsert_current(
    connection: sqlite3.Connection,
    *,
    source: RaceControlSource,
    action: str,
    patch: ScreenMessagePatch,
    observation_id: int,
) -> int:
    """Apply only fields explicitly changed by the provider to current state."""

    existing = connection.execute(
        """
        SELECT text_raw,line,modality,background_color_raw,font_color_raw,raw_record_json
        FROM race_control_messages_current
        WHERE source_heat_id = ? AND message_id_raw = ?
        """,
        (source.source_heat_id, patch.provider_message_id),
    ).fetchone()
    timestamp_us = now_us()
    if existing is None:
        connection.execute(
            """
            INSERT INTO race_control_messages_current(
              source_heat_id,message_id_raw,text_raw,line,modality,background_color_raw,font_color_raw,
              provider_occurred_at_us,raw_record_json,is_active,first_observation_kind,first_observed_at_us,
              first_source_frame_id,first_source_message_id,first_source_key,first_source_change_ordinal,
              first_observation_id,last_action,last_observed_at_us,last_source_frame_id,last_source_message_id,
              last_source_key,last_source_change_ordinal,last_observation_id,removed_at_us,removal_action,
              removed_source_frame_id,removed_source_message_id,removed_source_key,removed_source_change_ordinal,
              removed_observation_id,created_at_us,updated_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                source.source_heat_id,
                patch.provider_message_id,
                patch.text if "text" in patch.changed_fields else None,
                patch.line if "line" in patch.changed_fields else None,
                patch.modality if "modality" in patch.changed_fields else None,
                patch.background_color if "background_color" in patch.changed_fields else None,
                patch.font_color if "font_color" in patch.changed_fields else None,
                None,
                patch.raw_payload_json,
                1,
                action,
                source.observed_at_us,
                source.source_frame_id,
                source.source_message_id,
                source.source_key,
                patch.source_ordinal,
                observation_id,
                action,
                source.observed_at_us,
                source.source_frame_id,
                source.source_message_id,
                source.source_key,
                patch.source_ordinal,
                observation_id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                timestamp_us,
                timestamp_us,
            ),
        )
        return 1

    current = {
        "text_raw": existing["text_raw"],
        "line": existing["line"],
        "modality": existing["modality"],
        "background_color_raw": existing["background_color_raw"],
        "font_color_raw": existing["font_color_raw"],
    }
    if "text" in patch.changed_fields:
        current["text_raw"] = patch.text
    if "line" in patch.changed_fields:
        current["line"] = patch.line
    if "modality" in patch.changed_fields:
        current["modality"] = patch.modality
    if "background_color" in patch.changed_fields:
        current["background_color_raw"] = patch.background_color
    if "font_color" in patch.changed_fields:
        current["font_color_raw"] = patch.font_color
    connection.execute(
        """
        UPDATE race_control_messages_current
        SET text_raw = ?, line = ?, modality = ?, background_color_raw = ?, font_color_raw = ?,
            raw_record_json = ?, is_active = 1, last_action = ?, last_observed_at_us = ?,
            last_source_frame_id = ?, last_source_message_id = ?, last_source_key = ?,
            last_source_change_ordinal = ?, last_observation_id = ?, removed_at_us = NULL,
            removal_action = NULL, removed_source_frame_id = NULL, removed_source_message_id = NULL,
            removed_source_key = NULL, removed_source_change_ordinal = NULL, removed_observation_id = NULL,
            updated_at_us = ?
        WHERE source_heat_id = ? AND message_id_raw = ?
        """,
        (
            current["text_raw"],
            current["line"],
            current["modality"],
            current["background_color_raw"],
            current["font_color_raw"],
            patch.raw_payload_json,
            action,
            source.observed_at_us,
            source.source_frame_id,
            source.source_message_id,
            source.source_key,
            patch.source_ordinal,
            observation_id,
            timestamp_us,
            source.source_heat_id,
            patch.provider_message_id,
        ),
    )
    return 1


def _mark_removed(
    connection: sqlite3.Connection,
    *,
    source: RaceControlSource,
    message_ids: Iterable[str] | None,
    action: str,
    observation_id: int | None,
    source_change_ordinal: int,
) -> int:
    """Inactivate known current rows while retaining complete removal evidence."""

    parameters: list[Any] = [
        action,
        source.observed_at_us,
        source.source_frame_id,
        source.source_message_id,
        source.source_key,
        source_change_ordinal,
        observation_id,
        source.observed_at_us,
        action,
        source.source_frame_id,
        source.source_message_id,
        source.source_key,
        source_change_ordinal,
        observation_id,
        now_us(),
        source.source_heat_id,
    ]
    where = "source_heat_id = ? AND is_active = 1"
    if message_ids is not None:
        values = tuple(message_id for message_id in message_ids if message_id is not None)
        if not values:
            return 0
        placeholders = ",".join("?" for _ in values)
        where += f" AND message_id_raw IN ({placeholders})"
        parameters.extend(values)
    cursor = connection.execute(
        f"""
        UPDATE race_control_messages_current
        SET is_active = 0, last_action = ?, last_observed_at_us = ?, last_source_frame_id = ?,
            last_source_message_id = ?, last_source_key = ?, last_source_change_ordinal = ?,
            last_observation_id = ?, removed_at_us = ?, removal_action = ?, removed_source_frame_id = ?,
            removed_source_message_id = ?, removed_source_key = ?, removed_source_change_ordinal = ?,
            removed_observation_id = ?, updated_at_us = ?
        WHERE {where}
        """,
        tuple(parameters),
    )
    return max(cursor.rowcount, 0)


def _reconcile_snapshot_absences(
    connection: sqlite3.Connection,
    *,
    source: RaceControlSource,
    visible_message_ids: set[str],
) -> int:
    """Apply an authoritative `m_i` clear to records absent from its payload."""

    if not visible_message_ids:
        return _mark_removed(
            connection,
            source=source,
            message_ids=None,
            action="SNAPSHOT_RECONCILIATION",
            observation_id=None,
            source_change_ordinal=0,
        )
    placeholders = ",".join("?" for _ in visible_message_ids)
    parameters: list[Any] = [
        "SNAPSHOT_RECONCILIATION",
        source.observed_at_us,
        source.source_frame_id,
        source.source_message_id,
        source.source_key,
        0,
        source.observed_at_us,
        "SNAPSHOT_RECONCILIATION",
        source.source_frame_id,
        source.source_message_id,
        source.source_key,
        0,
        now_us(),
        source.source_heat_id,
        *sorted(visible_message_ids),
    ]
    cursor = connection.execute(
        f"""
        UPDATE race_control_messages_current
        SET is_active = 0, last_action = ?, last_observed_at_us = ?, last_source_frame_id = ?,
            last_source_message_id = ?, last_source_key = ?, last_source_change_ordinal = ?,
            last_observation_id = NULL, removed_at_us = ?, removal_action = ?,
            removed_source_frame_id = ?, removed_source_message_id = ?, removed_source_key = ?,
            removed_source_change_ordinal = ?, removed_observation_id = NULL, updated_at_us = ?
        WHERE source_heat_id = ? AND is_active = 1 AND message_id_raw NOT IN ({placeholders})
        """,
        tuple(parameters),
    )
    return max(cursor.rowcount, 0)


def _unwrap_signalr_args(args: Any) -> Any:
    if isinstance(args, tuple) or isinstance(args, list):
        return args[0] if len(args) == 1 else list(args)
    return args


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        # The decoded feed has already passed JSON parsing. This guard keeps an
        # unexpected test object as raw-safe UNKNOWN rather than crashing live
        # ingestion and losing the already-durable source message.
        return "null"


def _validate_source(source: RaceControlSource) -> None:
    if not isinstance(source, RaceControlSource):
        raise RaceControlStoreError("source must be a RaceControlSource")
    for name in ("source_heat_id", "source_frame_id", "source_message_id"):
        if type(getattr(source, name)) is not int or getattr(source, name) <= 0:
            raise RaceControlStoreError(f"{name} must be a positive integer")
    if type(source.source_message_ordinal) is not int or source.source_message_ordinal < 0:
        raise RaceControlStoreError("source_message_ordinal must be a non-negative integer")
    if not isinstance(source.source_key, str) or not source.source_key.strip():
        raise RaceControlStoreError("source_key must be a non-empty string")
    if type(source.observed_at_us) is not int or source.observed_at_us < 0:
        raise RaceControlStoreError("observed_at_us must be a non-negative integer")
