"""Transactional lifecycle rules for engineer analysis sessions.

This module deliberately has no HTTP concerns and never starts the recorder.
The API and ingest worker both use the same durable session state, while raw
provider heat creation stays with the ingest worker after its first ``h_i``.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from .config import now_us


RACE_DURATIONS_S = frozenset((14_400, 21_600, 43_200, 86_400))
RACE_REQUIRED_PITS = frozenset(range(2, 9))
MODES = frozenset(("practice", "qualifying", "race"))
ACTOR_KINDS = frozenset(("engineer_token", "engineer_session", "system"))
DEFAULT_ACTOR_KIND = "engineer_token"


class LifecycleError(RuntimeError):
    """Base error for a rejected timing session operation."""


class ValidationError(LifecycleError):
    """The caller supplied unsupported session configuration."""


class UnknownSourceError(LifecycleError):
    """The source slug is not one of the supported live timing sources."""


class SessionNotFoundError(LifecycleError):
    """The requested analysis session does not exist."""


class TransitionError(LifecycleError):
    """The requested lifecycle transition is not valid from this state."""


class ActiveSessionConflict(LifecycleError):
    """A source already has a different active analysis session."""


class IdempotencyConflict(LifecycleError):
    """An idempotency key was reused for a different operation or payload."""


@dataclass(frozen=True)
class TimingSource:
    """Static source configuration; no client provides these values."""

    slug: str
    display_name: str
    source_url: str
    timezone_name: str
    adapter_version: str = "timeservice-signalr-v1"


TIMING_SOURCE_CATALOG: dict[str, TimingSource] = {
    "igora": TimingSource(
        slug="igora",
        display_name="Igora Drive",
        source_url="https://livetiming.getraceresults.com/igora",
        timezone_name="Europe/Moscow",
    ),
    "moscow": TimingSource(
        slug="moscow",
        display_name="Moscow Raceway",
        source_url="https://livetiming.getraceresults.com/moscowraceway",
        timezone_name="Europe/Moscow",
    ),
}

# These are automatic matching hints for the normalizer. They are intentionally
# not API inputs: NR 21 is the Balchug Racing crew number, while drivers vary.
OUR_TEAM_NAME = "BALCHUG Racing"
OUR_START_NUMBER = "21"


@dataclass(frozen=True)
class AnalysisSession:
    """A public, JSON-serializable view of one durable analysis session."""

    id: str
    source_slug: str
    source_url: str
    source_name: str | None
    timezone_name: str
    mode: str
    lifecycle: str
    race_duration_s: int | None
    required_pits: int | None
    our_participant_id: str | None
    our_class: str | None
    identity_state: str
    started_at_us: int | None
    stopped_at_us: int | None
    stop_intent: str | None
    created_at_us: int
    updated_at_us: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_slug": self.source_slug,
            "source_url": self.source_url,
            "source_name": self.source_name,
            "timezone_name": self.timezone_name,
            "mode": self.mode,
            "lifecycle": self.lifecycle,
            "race_duration_s": self.race_duration_s,
            "required_pits": self.required_pits,
            "our_participant_id": self.our_participant_id,
            "our_class": self.our_class,
            "identity_state": self.identity_state,
            "started_at_us": self.started_at_us,
            "stopped_at_us": self.stopped_at_us,
            "stop_intent": self.stop_intent,
            "created_at_us": self.created_at_us,
            "updated_at_us": self.updated_at_us,
        }


@dataclass(frozen=True)
class MutationResult:
    """Result of a write, including whether a stored request was replayed."""

    session: AnalysisSession
    replayed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {"session": self.session.as_dict(), "replayed": self.replayed}


@contextmanager
def _write_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    """Serialize lifecycle writes before the active-session unique index runs."""
    if connection.in_transaction:
        raise LifecycleError("Lifecycle writes require a connection without an open transaction")
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _request_hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _as_source(source_slug: str) -> TimingSource:
    if not isinstance(source_slug, str):
        raise UnknownSourceError("Timing source must be a string slug")
    source = TIMING_SOURCE_CATALOG.get(source_slug)
    if source is None:
        raise UnknownSourceError(f"Unknown timing source: {source_slug}")
    return source


def _validate_actor_kind(actor_kind: str | None) -> str:
    value = DEFAULT_ACTOR_KIND if actor_kind is None else actor_kind
    if not isinstance(value, str) or value not in ACTOR_KINDS:
        allowed = ", ".join(sorted(ACTOR_KINDS))
        raise ValidationError(f"actor_kind must be one of: {allowed}")
    return value


def _validate_idempotency_key(idempotency_key: str | None) -> str | None:
    if idempotency_key is None:
        return None
    if not isinstance(idempotency_key, str) or not idempotency_key or len(idempotency_key) > 255:
        raise ValidationError("idempotency_key must be a non-empty string up to 255 characters")
    if idempotency_key != idempotency_key.strip() or any(ord(character) < 33 or ord(character) > 126 for character in idempotency_key):
        raise ValidationError("idempotency_key must contain visible ASCII characters without surrounding whitespace")
    return idempotency_key


def _validate_session_spec(
    *,
    mode: str,
    race_duration_s: int | None,
    required_pits: int | None,
) -> tuple[str, int | None, int | None]:
    if not isinstance(mode, str) or mode not in MODES:
        raise ValidationError("mode must be one of: practice, qualifying, race")
    if mode != "race":
        if race_duration_s is not None or required_pits is not None:
            raise ValidationError("race_duration_s and required_pits are allowed only for race mode")
        return mode, None, None
    if type(race_duration_s) is not int or race_duration_s not in RACE_DURATIONS_S:
        allowed = ", ".join(str(value) for value in sorted(RACE_DURATIONS_S))
        raise ValidationError(f"race_duration_s must be one of: {allowed}")
    if type(required_pits) is not int or required_pits not in RACE_REQUIRED_PITS:
        raise ValidationError("required_pits must be an integer from 2 through 8")
    return mode, race_duration_s, required_pits


def _seed_catalog(connection: sqlite3.Connection, timestamp_us: int) -> None:
    for source in TIMING_SOURCE_CATALOG.values():
        connection.execute(
            """
            INSERT INTO timing_sources(
              slug,source_url,adapter_version,created_at_us,display_name,timezone_name
            ) VALUES (?,?,?,?,?,?)
            ON CONFLICT(slug) DO UPDATE SET
              source_url = excluded.source_url,
              adapter_version = excluded.adapter_version,
              display_name = excluded.display_name,
              timezone_name = excluded.timezone_name
            """,
            (
                source.slug,
                source.source_url,
                source.adapter_version,
                timestamp_us,
                source.display_name,
                source.timezone_name,
            ),
        )


def ensure_source_catalog(connection: sqlite3.Connection, *, now_at_us: int | None = None) -> tuple[TimingSource, ...]:
    """Install known source rows once without accepting a client-provided URL."""
    timestamp_us = now_us() if now_at_us is None else now_at_us
    with _write_transaction(connection):
        _seed_catalog(connection, timestamp_us)
    return tuple(TIMING_SOURCE_CATALOG.values())


def _row_to_session(row: sqlite3.Row) -> AnalysisSession:
    return AnalysisSession(
        id=row["id"],
        source_slug=row["source_slug"],
        source_url=row["source_url"],
        source_name=row["source_name"],
        timezone_name=row["timezone_name"],
        mode=row["mode"],
        lifecycle=row["lifecycle"],
        race_duration_s=row["race_duration_s"],
        required_pits=row["required_pits"],
        our_participant_id=row["our_participant_id"],
        our_class=row["our_class"],
        identity_state=row["identity_state"],
        started_at_us=row["started_at_us"],
        stopped_at_us=row["stopped_at_us"],
        stop_intent=row["stop_intent"],
        created_at_us=row["created_at_us"],
        updated_at_us=row["updated_at_us"],
    )


_SESSION_SELECT = """
SELECT
  s.id,
  ts.slug AS source_slug,
  ts.source_url,
  ts.display_name AS source_name,
  s.timezone_name,
  s.mode,
  s.lifecycle,
  s.race_duration_s,
  s.required_pits,
  s.our_participant_id,
  s.our_class,
  s.identity_state,
  s.started_at_us,
  s.stopped_at_us,
  s.stop_intent,
  s.created_at_us,
  s.updated_at_us
FROM analysis_sessions s
JOIN timing_sources ts ON ts.id = s.source_id
"""


def _get_session_row(connection: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = connection.execute(_SESSION_SELECT + " WHERE s.id = ?", (session_id,)).fetchone()
    if row is None:
        raise SessionNotFoundError(f"Analysis session not found: {session_id}")
    return row


def get_session(connection: sqlite3.Connection, session_id: str) -> AnalysisSession:
    """Read an existing session without changing recorder or lifecycle state."""
    if not isinstance(session_id, str) or not session_id:
        raise SessionNotFoundError("Analysis session id is required")
    return _row_to_session(_get_session_row(connection, session_id))


def get_active_session(connection: sqlite3.Connection, source_slug: str) -> AnalysisSession | None:
    """Return the recoverable active session for one supported source, if any."""
    source = _as_source(source_slug)
    row = connection.execute(
        _SESSION_SELECT + " WHERE ts.slug = ? AND s.lifecycle = 'active'",
        (source.slug,),
    ).fetchone()
    return _row_to_session(row) if row is not None else None


def list_active_sessions(connection: sqlite3.Connection) -> tuple[AnalysisSession, ...]:
    """Recovery discovery for a restarted API or ingest worker."""
    rows = connection.execute(_SESSION_SELECT + " WHERE s.lifecycle = 'active' ORDER BY s.started_at_us, s.id").fetchall()
    return tuple(_row_to_session(row) for row in rows)


def _idempotency_replay(
    connection: sqlite3.Connection,
    *,
    idempotency_key: str | None,
    operation: str,
    request_hash: str,
) -> MutationResult | None:
    if idempotency_key is None:
        return None
    row = connection.execute(
        """
        SELECT operation,request_hash,result_json
        FROM session_idempotency_keys
        WHERE idempotency_key = ?
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        return None
    if row["operation"] != operation or row["request_hash"] != request_hash:
        raise IdempotencyConflict("Idempotency key was already used with a different request")
    try:
        payload = json.loads(row["result_json"])
        session_payload = payload["session"]
        session = AnalysisSession(
            id=session_payload["id"],
            source_slug=session_payload["source_slug"],
            source_url=session_payload["source_url"],
            source_name=session_payload["source_name"],
            timezone_name=session_payload["timezone_name"],
            mode=session_payload["mode"],
            lifecycle=session_payload["lifecycle"],
            race_duration_s=session_payload["race_duration_s"],
            required_pits=session_payload["required_pits"],
            our_participant_id=session_payload["our_participant_id"],
            our_class=session_payload["our_class"],
            identity_state=session_payload["identity_state"],
            started_at_us=session_payload["started_at_us"],
            stopped_at_us=session_payload["stopped_at_us"],
            stop_intent=session_payload["stop_intent"],
            created_at_us=session_payload["created_at_us"],
            updated_at_us=session_payload["updated_at_us"],
        )
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise LifecycleError("Stored idempotency result is invalid") from exc
    return MutationResult(session=session, replayed=True)


def _store_idempotency(
    connection: sqlite3.Connection,
    *,
    idempotency_key: str | None,
    operation: str,
    request_hash: str,
    result: MutationResult,
    timestamp_us: int,
) -> None:
    if idempotency_key is None:
        return
    connection.execute(
        """
        INSERT INTO session_idempotency_keys(
          idempotency_key,operation,request_hash,analysis_session_id,result_json,created_at_us
        ) VALUES (?,?,?,?,?,?)
        """,
        (
            idempotency_key,
            operation,
            request_hash,
            result.session.id,
            _canonical_json(result.as_dict()),
            timestamp_us,
        ),
    )


def _write_audit(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    event_type: str,
    actor_kind: str,
    parameters: dict[str, Any],
    timestamp_us: int,
) -> None:
    connection.execute(
        """
        INSERT INTO session_audit_events(
          analysis_session_id,event_type,actor_kind,parameters_json,created_at_us
        ) VALUES (?,?,?,?,?)
        """,
        (session_id, event_type, actor_kind, _canonical_json(parameters), timestamp_us),
    )


def create_session(
    connection: sqlite3.Connection,
    *,
    source_slug: str,
    mode: str,
    race_duration_s: int | None = None,
    required_pits: int | None = None,
    idempotency_key: str | None = None,
    actor_kind: str | None = None,
    now_at_us: int | None = None,
) -> MutationResult:
    """Create a draft with the only permitted engineer-selected values."""
    source = _as_source(source_slug)
    mode, race_duration_s, required_pits = _validate_session_spec(
        mode=mode,
        race_duration_s=race_duration_s,
        required_pits=required_pits,
    )
    key = _validate_idempotency_key(idempotency_key)
    actor = _validate_actor_kind(actor_kind)
    timestamp_us = now_us() if now_at_us is None else now_at_us
    request = {
        "operation": "create",
        "source_slug": source.slug,
        "mode": mode,
        "race_duration_s": race_duration_s,
        "required_pits": required_pits,
    }
    request_hash = _request_hash(request)

    with _write_transaction(connection):
        _seed_catalog(connection, timestamp_us)
        replay = _idempotency_replay(
            connection,
            idempotency_key=key,
            operation="create",
            request_hash=request_hash,
        )
        if replay is not None:
            return replay
        source_row = connection.execute(
            "SELECT id,timezone_name FROM timing_sources WHERE slug = ?",
            (source.slug,),
        ).fetchone()
        if source_row is None:  # Defensive: catalog seed and catalog lookup must agree.
            raise LifecycleError(f"Timing source catalog did not install: {source.slug}")
        session_id = str(uuid.uuid4())
        connection.execute(
            """
            INSERT INTO analysis_sessions(
              id,source_id,mode,lifecycle,race_duration_s,required_pits,
              timezone_name,created_at_us,updated_at_us
            ) VALUES (?,? ,?,'draft',?,?,?,?,?)
            """,
            (
                session_id,
                source_row["id"],
                mode,
                race_duration_s,
                required_pits,
                source_row["timezone_name"],
                timestamp_us,
                timestamp_us,
            ),
        )
        session = _row_to_session(_get_session_row(connection, session_id))
        result = MutationResult(session=session)
        _write_audit(
            connection,
            session_id=session_id,
            event_type="created",
            actor_kind=actor,
            parameters={
                "mode": mode,
                "race_duration_s": race_duration_s,
                "required_pits": required_pits,
            },
            timestamp_us=timestamp_us,
        )
        _store_idempotency(
            connection,
            idempotency_key=key,
            operation="create",
            request_hash=request_hash,
            result=result,
            timestamp_us=timestamp_us,
        )
        return result


def _transition_session(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    operation: str,
    idempotency_key: str | None,
    actor_kind: str | None,
    now_at_us: int | None,
) -> MutationResult:
    if not isinstance(session_id, str) or not session_id:
        raise SessionNotFoundError("Analysis session id is required")
    key = _validate_idempotency_key(idempotency_key)
    actor = _validate_actor_kind(actor_kind)
    timestamp_us = now_us() if now_at_us is None else now_at_us
    request_hash = _request_hash({"operation": operation, "session_id": session_id})

    try:
        with _write_transaction(connection):
            replay = _idempotency_replay(
                connection,
                idempotency_key=key,
                operation=operation,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay
            row = _get_session_row(connection, session_id)
            lifecycle = row["lifecycle"]
            if operation == "start":
                if lifecycle != "draft":
                    raise TransitionError(f"Cannot start a {lifecycle} analysis session")
                existing = connection.execute(
                    """
                    SELECT id FROM analysis_sessions
                    WHERE source_id = (SELECT source_id FROM analysis_sessions WHERE id = ?)
                      AND lifecycle = 'active'
                    """,
                    (session_id,),
                ).fetchone()
                if existing is not None:
                    raise ActiveSessionConflict(
                        f"Source already has an active analysis session: {existing['id']}"
                    )
                connection.execute(
                    """
                    UPDATE analysis_sessions
                    SET lifecycle = 'active', started_at_us = ?, updated_at_us = ?, stop_intent = NULL
                    WHERE id = ?
                    """,
                    (timestamp_us, timestamp_us, session_id),
                )
                audit_event = "started"
            elif operation == "stop":
                if lifecycle != "active":
                    raise TransitionError(f"Cannot stop a {lifecycle} analysis session")
                connection.execute(
                    """
                    UPDATE analysis_sessions
                    SET lifecycle = 'stopped', stopped_at_us = ?, stop_intent = 'operator_stop', updated_at_us = ?
                    WHERE id = ?
                    """,
                    (timestamp_us, timestamp_us, session_id),
                )
                audit_event = "stopped"
            elif operation == "abort":
                if lifecycle not in {"draft", "active"}:
                    raise TransitionError(f"Cannot abort a {lifecycle} analysis session")
                connection.execute(
                    """
                    UPDATE analysis_sessions
                    SET lifecycle = 'aborted', stopped_at_us = ?, stop_intent = 'operator_abort', updated_at_us = ?
                    WHERE id = ?
                    """,
                    (timestamp_us, timestamp_us, session_id),
                )
                audit_event = "aborted"
            else:  # Keep this internal dispatcher closed over its supported verbs.
                raise LifecycleError(f"Unsupported lifecycle operation: {operation}")
            session = _row_to_session(_get_session_row(connection, session_id))
            result = MutationResult(session=session)
            _write_audit(
                connection,
                session_id=session_id,
                event_type=audit_event,
                actor_kind=actor,
                parameters={},
                timestamp_us=timestamp_us,
            )
            _store_idempotency(
                connection,
                idempotency_key=key,
                operation=operation,
                request_hash=request_hash,
                result=result,
                timestamp_us=timestamp_us,
            )
            return result
    except sqlite3.IntegrityError as exc:
        # The explicit preflight gives a useful error in the normal case; this
        # turns the database's partial unique index into the same result if a
        # concurrent writer somehow reaches it first.
        if "analysis_sessions.source_id" in str(exc):
            raise ActiveSessionConflict("Source already has an active analysis session") from exc
        raise


def start_session(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    idempotency_key: str | None = None,
    actor_kind: str | None = None,
    now_at_us: int | None = None,
) -> MutationResult:
    """Move one draft to active after atomically checking its source."""
    return _transition_session(
        connection,
        session_id=session_id,
        operation="start",
        idempotency_key=idempotency_key,
        actor_kind=actor_kind,
        now_at_us=now_at_us,
    )


def stop_session(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    idempotency_key: str | None = None,
    actor_kind: str | None = None,
    now_at_us: int | None = None,
) -> MutationResult:
    """Finish an active session by engineer stop intent."""
    return _transition_session(
        connection,
        session_id=session_id,
        operation="stop",
        idempotency_key=idempotency_key,
        actor_kind=actor_kind,
        now_at_us=now_at_us,
    )


def abort_session(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    idempotency_key: str | None = None,
    actor_kind: str | None = None,
    now_at_us: int | None = None,
) -> MutationResult:
    """Abort a draft or active session without deleting durable observations."""
    return _transition_session(
        connection,
        session_id=session_id,
        operation="abort",
        idempotency_key=idempotency_key,
        actor_kind=actor_kind,
        now_at_us=now_at_us,
    )
