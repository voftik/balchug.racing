"""Pure parser for Time Service race-control screen-message handles.

The provider's ``MessagesViewModel`` owns a small mutable collection of
on-screen race-control messages.  These handles are deliberately kept outside
the result-grid normalizer: they have their own identity and lifecycle, and do
not carry a provider timestamp.  A caller must therefore attach receive-time
and decoded-message provenance when it durably records a parsed update.

Observed provider semantics (from the live client):

* ``m_i`` clears the collection and applies an authoritative snapshot;
* ``m_a`` clears the collection;
* ``m_c`` creates or sparsely updates one message by ``Id``;
* ``m_d`` removes one message by ``Id``.

The parser never opens a database and never reads a clock.  Raw SignalR
messages remain the byte-for-byte evidence in ``feed_frames`` / ``feed_messages``.
``raw_payload_json`` is a canonical per-item convenience copy for a future
ledger; it is not a replacement for that raw evidence.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal


SCREEN_MESSAGE_HANDLES = frozenset({"m_i", "m_a", "m_c", "m_d"})
"""Known handles for the provider's race-control message collection."""

ScreenMessageOperation = Literal["SNAPSHOT", "RESET", "UPSERT", "DELETE", "UNKNOWN", "INVALID"]


@dataclass(frozen=True)
class ScreenMessagePatch:
    """One sparse provider message item, ready for a durable writer to merge.

    ``changed_fields`` mirrors the browser's ``value != null`` checks.  A
    missing or explicit ``null`` source field must not clear the corresponding
    materialized value.  ``source_ordinal`` keeps wire order, while
    ``application_ordinal`` represents the actual order used by ``m_i``: the
    provider client iterates snapshot arrays from the end to the beginning.
    This matters only for duplicate ids, but preserving it makes replay exact.
    """

    provider_message_id: str
    source_ordinal: int
    application_ordinal: int
    text: str | None
    line: int | None
    modality: int | None
    background_color: str | None
    font_color: str | None
    changed_fields: frozenset[str]
    raw_payload_json: str


@dataclass(frozen=True)
class ScreenMessageUpdate:
    """One parsed provider handle with no database or timestamp policy.

    ``snapshot_complete`` is only meaningful for ``SNAPSHOT``.  A writer may
    upsert valid items from an incomplete snapshot, but must *not* reconcile
    absent current messages as deleted, since malformed source input would make
    that destructive.  ``INVALID`` and ``UNKNOWN`` are intentionally no-ops.
    """

    handle: str
    operation: ScreenMessageOperation
    patches: tuple[ScreenMessagePatch, ...] = ()
    provider_message_id: str | None = None
    snapshot_complete: bool = False
    errors: tuple[str, ...] = ()

    @property
    def is_actionable(self) -> bool:
        """Whether a writer may safely apply this update's stated operation."""

        return self.operation not in {"UNKNOWN", "INVALID"}


def parse_screen_message_update(handle: str, args: Any) -> ScreenMessageUpdate:
    """Parse one decoded SignalR handle and its positional ``args``.

    Pass ``SignalRMessage.handle`` and ``SignalRMessage.args`` here.  Use
    :func:`parse_screen_message_payload` only when a caller already has the
    one-argument SignalR payload unwrapped.
    """

    return parse_screen_message_payload(handle, _message_payload(args))


def parse_screen_message_payload(handle: str, payload: Any) -> ScreenMessageUpdate:
    """Parse an already-unwrapped provider payload without side effects."""

    if handle == "m_i":
        return _parse_snapshot(handle, payload)
    if handle == "m_a":
        # The browser implementation has no parameters and clears its
        # collection even if a provider unexpectedly sends arguments.
        return ScreenMessageUpdate(handle=handle, operation="RESET")
    if handle == "m_c":
        patch, errors = _parse_patch(payload, source_ordinal=0, application_ordinal=0)
        if patch is None:
            return ScreenMessageUpdate(handle=handle, operation="INVALID", errors=errors)
        return ScreenMessageUpdate(handle=handle, operation="UPSERT", patches=(patch,), errors=errors)
    if handle == "m_d":
        message_id = _provider_message_id(payload)
        if message_id is None:
            return ScreenMessageUpdate(
                handle=handle,
                operation="INVALID",
                errors=("invalid_provider_message_id",),
            )
        return ScreenMessageUpdate(handle=handle, operation="DELETE", provider_message_id=message_id)
    return ScreenMessageUpdate(handle=handle, operation="UNKNOWN")


def _parse_snapshot(handle: str, payload: Any) -> ScreenMessageUpdate:
    # A null initial payload is a valid empty collection in the provider's
    # browser client.  It has the same visible result as an empty list.
    if payload is None:
        return ScreenMessageUpdate(handle=handle, operation="SNAPSHOT", snapshot_complete=True)
    if not _is_sequence(payload):
        return ScreenMessageUpdate(
            handle=handle,
            operation="INVALID",
            errors=("expected_snapshot_list",),
        )

    patches: list[ScreenMessagePatch] = []
    errors: list[str] = []
    # The live client uses ``while (j--)`` and therefore applies the last
    # provider array member first.  Do the same rather than silently changing
    # duplicate-id behavior during deterministic replay.
    items = list(payload)
    for application_ordinal, source_ordinal in enumerate(range(len(items) - 1, -1, -1)):
        patch, item_errors = _parse_patch(
            items[source_ordinal],
            source_ordinal=source_ordinal,
            application_ordinal=application_ordinal,
        )
        if patch is not None:
            patches.append(patch)
        for error in item_errors:
            errors.append(f"snapshot_item_{source_ordinal}:{error}")

    return ScreenMessageUpdate(
        handle=handle,
        operation="SNAPSHOT",
        patches=tuple(patches),
        snapshot_complete=not errors,
        errors=tuple(errors),
    )


def _parse_patch(
    payload: Any,
    *,
    source_ordinal: int,
    application_ordinal: int,
) -> tuple[ScreenMessagePatch | None, tuple[str, ...]]:
    if not isinstance(payload, Mapping):
        return None, ("expected_message_object",)
    message_id = _provider_message_id(payload.get("Id"))
    if message_id is None:
        return None, ("invalid_provider_message_id",)

    errors: list[str] = []
    changed_fields: set[str] = set()
    text = _string_field(payload, "t", "text", changed_fields, errors)
    line = _integer_field(payload, "l", "line", changed_fields, errors)
    modality = _integer_field(payload, "m", "modality", changed_fields, errors)
    background_color = _string_field(payload, "bc", "background_color", changed_fields, errors)
    font_color = _string_field(payload, "fc", "font_color", changed_fields, errors)

    try:
        raw_payload_json = json.dumps(
            dict(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        # SignalR-decoded input is JSON-shaped in production.  Do not allow an
        # unexpected test/dictionary object to make a pure parser throw.
        raw_payload_json = "{}"
        errors.append("raw_payload_not_json")

    return (
        ScreenMessagePatch(
            provider_message_id=message_id,
            source_ordinal=source_ordinal,
            application_ordinal=application_ordinal,
            text=text,
            line=line,
            modality=modality,
            background_color=background_color,
            font_color=font_color,
            changed_fields=frozenset(changed_fields),
            raw_payload_json=raw_payload_json,
        ),
        tuple(errors),
    )


def _provider_message_id(value: Any) -> str | None:
    # Browser equality is case- and byte-sensitive. Preserve that exact id;
    # only reject empty/whitespace-only values that cannot identify a record.
    return value if isinstance(value, str) and value.strip() else None


def _string_field(
    payload: Mapping[str, Any],
    source_name: str,
    field_name: str,
    changed_fields: set[str],
    errors: list[str],
) -> str | None:
    if source_name not in payload or payload[source_name] is None:
        return None
    value = payload[source_name]
    if not isinstance(value, str):
        errors.append(f"invalid_{field_name}")
        return None
    changed_fields.add(field_name)
    return value


def _integer_field(
    payload: Mapping[str, Any],
    source_name: str,
    field_name: str,
    changed_fields: set[str],
    errors: list[str],
) -> int | None:
    if source_name not in payload or payload[source_name] is None:
        return None
    value = payload[source_name]
    if type(value) is not int:
        errors.append(f"invalid_{field_name}")
        return None
    changed_fields.add(field_name)
    return value


def _message_payload(args: Any) -> Any:
    if _is_sequence(args):
        return args[0] if len(args) == 1 else args
    return args


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
