"""Engineer-controlled timing session lifecycle API.

This process deliberately owns durable session intent only. The future ingest
supervisor observes active rows in timing.db; an HTTP request never opens a
provider WebSocket or runs a recorder in the API worker.
"""

from __future__ import annotations

import hmac
import os
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, model_validator

from .db import connect
from .lifecycle import (
    ActiveSessionConflict,
    IdempotencyConflict,
    LifecycleError,
    MutationResult,
    SessionNotFoundError,
    TransitionError,
    UnknownSourceError,
    ValidationError,
    abort_session,
    create_session,
    get_active_session,
    get_session,
    list_active_sessions,
    start_session,
    stop_session,
)


RACE_DURATIONS = frozenset({14_400, 21_600, 43_200, 86_400})

app = FastAPI(title="Balchug Racing Timing Lifecycle", docs_url=None, redoc_url=None)


class SessionCreateBody(BaseModel):
    """The complete and intentionally small engineer write surface."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["practice", "qualifying", "race"]
    race_duration_s: int | None = None
    required_pits: int | None = None

    @model_validator(mode="after")
    def validate_mode_parameters(self) -> "SessionCreateBody":
        if self.mode == "race":
            if self.race_duration_s not in RACE_DURATIONS or self.required_pits not in range(2, 9):
                raise ValueError("Race requires an allowed duration and 2-8 required pits")
        elif self.race_duration_s is not None or self.required_pits is not None:
            raise ValueError("Only Race accepts duration and required pits")
        return self


def _engineer_token() -> str:
    return os.environ.get("ENGINEER_TOKEN", "")


def require_engineer(authorization: Annotated[str | None, Header()] = None) -> str:
    """Fail closed; archive's public Boris token is never accepted here."""
    expected = _engineer_token()
    if not expected:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "engineer access is not configured")
    prefix = "Bearer "
    actual = authorization[len(prefix) :] if authorization and authorization.startswith(prefix) else ""
    if not actual or not hmac.compare_digest(actual, expected):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "engineer authorization required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return "engineer_token"


def require_idempotency_key(
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> str:
    if not idempotency_key or len(idempotency_key) > 200:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Idempotency-Key is required")
    return idempotency_key


async def require_empty_body(request: Request) -> None:
    if (await request.body()).strip():
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "This transition does not accept a body")


def _session_payload(session: object) -> dict[str, object]:
    if hasattr(session, "as_dict"):
        return session.as_dict()  # type: ignore[no-any-return]
    if isinstance(session, dict):
        return session
    raise RuntimeError("Lifecycle service returned an unsupported session shape")


def _mutation_payload(result: MutationResult) -> dict[str, object]:
    return {"session": _session_payload(result.session), "replayed": result.replayed}


def _raise_lifecycle_error(error: LifecycleError) -> None:
    if isinstance(error, (UnknownSourceError, SessionNotFoundError)):
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(error)) from error
    if isinstance(error, ValidationError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(error)) from error
    if isinstance(error, (ActiveSessionConflict, TransitionError, IdempotencyConflict)):
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
    raise HTTPException(status.HTTP_400_BAD_REQUEST, str(error)) from error


@app.get("/health")
def health() -> dict[str, object]:
    connection = connect()
    try:
        return {
            "ok": True,
            "active_sessions": len(list_active_sessions(connection)),
            "engineer_access_configured": bool(_engineer_token()),
        }
    finally:
        connection.close()


@app.get("/sources/{source_slug}/sessions/active")
def active_session(source_slug: str) -> dict[str, object]:
    connection = connect()
    try:
        session = get_active_session(connection, source_slug)
        return {"session": _session_payload(session) if session else None}
    except LifecycleError as error:
        _raise_lifecycle_error(error)
    finally:
        connection.close()


@app.get("/sessions/{session_id}")
def session_detail(session_id: str) -> dict[str, object]:
    connection = connect()
    try:
        return {"session": _session_payload(get_session(connection, session_id))}
    except LifecycleError as error:
        _raise_lifecycle_error(error)
    finally:
        connection.close()


@app.post("/sources/{source_slug}/sessions", status_code=status.HTTP_201_CREATED)
def create_draft(
    source_slug: str,
    body: SessionCreateBody,
    response: Response,
    actor_kind: Annotated[str, Depends(require_engineer)],
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
) -> dict[str, object]:
    connection = connect()
    try:
        result = create_session(
            connection,
            source_slug=source_slug,
            mode=body.mode,
            race_duration_s=body.race_duration_s,
            required_pits=body.required_pits,
            idempotency_key=idempotency_key,
            actor_kind=actor_kind,
        )
        if result.replayed:
            response.status_code = status.HTTP_200_OK
        return _mutation_payload(result)
    except LifecycleError as error:
        _raise_lifecycle_error(error)
    finally:
        connection.close()


def _transition(
    operation: Literal["start", "stop", "abort"],
    session_id: str,
    actor_kind: str,
    idempotency_key: str,
) -> dict[str, object]:
    connection = connect()
    try:
        functions = {"start": start_session, "stop": stop_session, "abort": abort_session}
        result = functions[operation](
            connection,
            session_id=session_id,
            idempotency_key=idempotency_key,
            actor_kind=actor_kind,
        )
        return _mutation_payload(result)
    except LifecycleError as error:
        _raise_lifecycle_error(error)
    finally:
        connection.close()


@app.post("/sessions/{session_id}/start")
async def start(
    session_id: str,
    actor_kind: Annotated[str, Depends(require_engineer)],
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
    _: Annotated[None, Depends(require_empty_body)],
) -> dict[str, object]:
    return _transition("start", session_id, actor_kind, idempotency_key)


@app.post("/sessions/{session_id}/stop")
async def stop(
    session_id: str,
    actor_kind: Annotated[str, Depends(require_engineer)],
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
    _: Annotated[None, Depends(require_empty_body)],
) -> dict[str, object]:
    return _transition("stop", session_id, actor_kind, idempotency_key)


@app.post("/sessions/{session_id}/abort")
async def abort(
    session_id: str,
    actor_kind: Annotated[str, Depends(require_engineer)],
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
    _: Annotated[None, Depends(require_empty_body)],
) -> dict[str, object]:
    return _transition("abort", session_id, actor_kind, idempotency_key)
