"""Durable, replay-safe event outbox for the live timing read stream.

The normalizer and metric runner remain the only writers of timing facts.  This
module records a compact event cursor in the same SQLite transaction as the
derived state so an SSE client can reconnect without guessing what changed.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .config import now_us


STREAM_EVENT_TYPES = frozenset({"state", "flag", "lap", "pit", "metric", "alert", "quality"})


class StreamEventError(ValueError):
    """A caller attempted to add an ambiguous event to the timing stream."""


@dataclass(frozen=True)
class StreamEventCandidate:
    """One idempotent event emitted after a normalized timing frame."""

    event_type: str
    event_key: str
    payload: Mapping[str, Any]


def append_stream_events(
    connection: sqlite3.Connection,
    *,
    analysis_session_id: str,
    source_heat_id: int,
    source_frame_id: int,
    source_message_id: int | None,
    source_key: str,
    observed_at_us: int,
    events: Sequence[StreamEventCandidate],
) -> int:
    """Append event candidates exactly once inside the caller's transaction.

    ``event_key`` belongs to a source frame and an event facet.  The unique
    index installed by migration 0005 makes replay after a process crash a
    no-op, while a fresh row id remains a monotonic SSE cursor.
    """

    if not isinstance(analysis_session_id, str) or not analysis_session_id:
        raise StreamEventError("analysis_session_id must be a non-empty string")
    if type(source_heat_id) is not int or source_heat_id <= 0:
        raise StreamEventError("source_heat_id must be a positive integer")
    if type(source_frame_id) is not int or source_frame_id <= 0:
        raise StreamEventError("source_frame_id must be a positive integer")
    if source_message_id is not None and (type(source_message_id) is not int or source_message_id <= 0):
        raise StreamEventError("source_message_id must be a positive integer or None")
    if not isinstance(source_key, str) or not source_key.strip():
        raise StreamEventError("source_key must be a non-empty string")
    if type(observed_at_us) is not int or observed_at_us < 0:
        raise StreamEventError("observed_at_us must be a non-negative integer")

    written = 0
    created_at_us = now_us()
    seen: set[str] = set()
    for event in events:
        if not isinstance(event, StreamEventCandidate):
            raise StreamEventError("events must contain StreamEventCandidate values")
        if event.event_type not in STREAM_EVENT_TYPES:
            raise StreamEventError(f"unsupported stream event type: {event.event_type!r}")
        if not isinstance(event.event_key, str) or not event.event_key or len(event.event_key) > 500:
            raise StreamEventError("event_key must be a non-empty string of at most 500 characters")
        if event.event_key in seen:
            raise StreamEventError(f"duplicate event_key in one write: {event.event_key}")
        if not isinstance(event.payload, Mapping):
            raise StreamEventError("event payload must be an object")
        seen.add(event.event_key)
        try:
            payload_json = json.dumps(
                event.payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as error:
            raise StreamEventError("event payload is not JSON serializable") from error
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO stream_events(
              analysis_session_id,source_heat_id,source_frame_id,source_message_id,source_key,event_type,
              event_key,observed_at_us,payload_json,created_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                analysis_session_id,
                source_heat_id,
                source_frame_id,
                source_message_id,
                source_key,
                event.event_type,
                event.event_key,
                observed_at_us,
                payload_json,
                created_at_us,
            ),
        )
        written += max(cursor.rowcount, 0)
    return written
