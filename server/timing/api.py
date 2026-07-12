"""Engineer lifecycle writes and read-only live timing delivery.

The ingest supervisor observes durable session intent in ``timing.db``. HTTP
requests never open a provider WebSocket or run a recorder: writes only change
lifecycle intent, while reads serve committed normalized facts and metrics.
"""

from __future__ import annotations

import asyncio
import hmac
import os
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
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
    start_session,
    stop_session,
)
from .operations import collect_operational_health, read_operational_incidents
from .read_api import (
    DEFAULT_FACT_LIMIT,
    MAX_CHART_POINTS,
    ArchiveProjectionMissingError,
    MetricScopeRequest,
    ReadValidationError,
    ScopeNotFoundError as ReadScopeNotFoundError,
    SessionNotFoundError as ReadSessionNotFoundError,
    TimingReadError,
    TimingReadModel,
)
from .sse import (
    DEFAULT_BATCH_SIZE,
    ResetRequired,
    StreamCursorError,
    StreamEvent,
    TimingStreamBroker,
    format_sse_comment,
    format_sse_event,
    parse_last_event_id,
    read_cursor_window,
    read_stream_events,
)


RACE_DURATIONS = frozenset({14_400, 21_600, 43_200, 86_400})

@asynccontextmanager
async def _timing_lifespan(application: FastAPI):
    """Run one read-only outbox fanout task for the single timing API worker."""

    broker = TimingStreamBroker()
    application.state.timing_stream_broker = broker
    await broker.start()
    try:
        yield
    finally:
        await broker.stop()


app = FastAPI(title="Balchug Racing Timing API", docs_url=None, redoc_url=None, lifespan=_timing_lifespan)


class StreamCursor(BaseModel):
    """Opaque monotonic cursor returned with every live dashboard snapshot."""

    stream_event_id: int


class TimingReadResponse(BaseModel):
    """Versioned public read envelope; detailed keys remain forward-compatible."""

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "schema_version": "timing-live.v1",
                    "cursor": {"stream_event_id": 1842},
                    "barrier": {"stream_event_id": 1842},
                    "freshness": {"status": "LIVE", "age_ms": 430},
                }
            ]
        },
    )

    schema_version: Literal["timing-live.v1"]
    cursor: StreamCursor | None = None
    barrier: StreamCursor | None = None


class TimingFactsResponse(BaseModel):
    """Bounded measured-fact envelope returned by timing fact endpoints."""

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "schema_version": "timing-live.v1",
                    "session_id": "session-id",
                    "items": [],
                }
            ]
        },
    )

    schema_version: Literal["timing-live.v1"]


class TimingArchiveResponse(BaseModel):
    """Versioned public archive envelope for immutable stopped-session reads."""

    model_config = ConfigDict(extra="allow")

    schema_version: Literal["timing-archive.v1"]


def _read_model() -> TimingReadModel:
    """Create a stateless reader so tests and deployments honor TIMING_DB now."""

    return TimingReadModel()


def _live_payload(payload: dict[str, Any]) -> JSONResponse:
    """Live data must not be cached or buffered as a historical HTTP response."""

    payload.setdefault("schema_version", "timing-live.v1")
    return JSONResponse(
        payload,
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


def _archive_payload(payload: dict[str, Any]) -> JSONResponse:
    """Stopped-session reads may be buffered and compressed by the proxy."""

    payload.setdefault("schema_version", "timing-archive.v1")
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


def _raise_read_error(error: TimingReadError) -> None:
    if isinstance(error, (ReadSessionNotFoundError, ReadScopeNotFoundError)):
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(error)) from error
    if isinstance(error, ArchiveProjectionMissingError):
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
    if isinstance(error, ReadValidationError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(error)) from error
    raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "timing read model is unavailable") from error


def _metric_scope(scope_kind: str | None, scope_key: str | None) -> MetricScopeRequest | None:
    """Keep scope selection source-derived and reject half-specified filters."""

    if (scope_kind is None) != (scope_key is None):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "scope_kind and scope_key must be supplied together")
    return MetricScopeRequest(scope_kind, scope_key) if scope_kind is not None and scope_key is not None else None


async def _stream_broker() -> TimingStreamBroker:
    """Use lifespan ownership in production and lazy ownership in ASGI tests."""

    broker = getattr(app.state, "timing_stream_broker", None)
    if broker is None or getattr(broker, "_closed", False):
        broker = TimingStreamBroker()
        app.state.timing_stream_broker = broker
    await broker.start()
    return broker


def _snapshot_generation(snapshot: dict[str, Any]) -> tuple[int | None, int | None]:
    heat = snapshot.get("heat")
    if not isinstance(heat, dict):
        return None, None
    source_heat_id = heat.get("source_heat_id")
    generation = heat.get("generation")
    return (
        source_heat_id if type(source_heat_id) is int else None,
        generation if type(generation) is int else None,
    )


def _sse_event_payload(event: StreamEvent) -> tuple[str, dict[str, Any]]:
    """Wrap one immutable outbox row in the public live-stream contract."""

    event_type = event.event_type if event.event_type in {"state", "metric", "lap", "flag", "pit", "alert", "quality"} else "alert"
    source_payload = dict(event.payload)
    data = source_payload.get("data", source_payload)
    return event_type, {
        "schema_version": "timing-live.v1",
        "sequence": event.id,
        "session_id": event.analysis_session_id,
        "source_heat_id": event.source_heat_id,
        "source_frame_id": event.source_frame_id,
        "source_message_id": event.source_message_id,
        "source_key": event.source_key,
        "observed_at_us": event.observed_at_us,
        "generation": event.generation,
        "type": event_type,
        "data": data,
    }


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


def _operational_payload(payload: dict[str, Any], *, status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        payload,
        status_code=status_code,
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


@app.get("/health")
def health() -> JSONResponse:
    """Return a bounded diagnostic snapshot; degraded health remains readable."""

    payload = collect_operational_health()
    payload["engineer_access_configured"] = bool(_engineer_token())
    return _operational_payload(payload)


@app.get("/ready")
def ready() -> JSONResponse:
    """Fail readiness only for critical conditions, never for warnings."""

    payload = collect_operational_health()
    return _operational_payload(
        payload,
        status_code=200 if payload["ready"] else status.HTTP_503_SERVICE_UNAVAILABLE,
    )


@app.get("/operations/incidents")
def operational_incidents(
    open_only: bool = False,
    limit: int = Query(100, ge=1, le=500),
) -> JSONResponse:
    """Expose only allowlisted operational incident metadata."""

    return _operational_payload(
        read_operational_incidents(open_only=open_only, limit=limit)
    )


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


@app.get("/sessions/archive", response_model=TimingArchiveResponse)
def archived_timing_sessions(limit: int = 50) -> JSONResponse:
    """List stopped sessions that have a durable archive playback projection."""

    try:
        return _archive_payload(_read_model().archived_sessions(limit=limit))
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/archive", response_model=TimingArchiveResponse)
def archive_manifest(
    session_id: str,
    generation: int | None = None,
    max_points: int = MAX_CHART_POINTS,
) -> JSONResponse:
    """Return bounded archive keyframes and markers for one stopped heat."""

    try:
        return _archive_payload(
            _read_model().archive_manifest(
                session_id,
                generation=generation,
                max_points=max_points,
            )
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/archive/snapshot", response_model=TimingArchiveResponse)
def archive_snapshot(
    session_id: str,
    at_us: int | None = None,
    generation: int | None = None,
) -> JSONResponse:
    """Return the confirmed historical state at or before a selected moment."""

    try:
        return _archive_payload(
            _read_model().archive_snapshot(
                session_id,
                at_us=at_us,
                generation=generation,
            )
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/archive/comparison", response_model=TimingArchiveResponse)
def archive_comparison(
    session_id: str,
    generation: int | None = None,
    mode: Literal["all", "participant"] = "all",
    participant_id: str | None = None,
    max_points: int = MAX_CHART_POINTS,
) -> JSONResponse:
    """Return one bounded own-versus-class benchmark for an archived heat."""

    try:
        return _archive_payload(
            _read_model().archive_comparison(
                session_id,
                generation=generation,
                mode=mode,
                participant_id=participant_id,
                max_points=max_points,
            )
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}")
def session_detail(session_id: str) -> dict[str, object]:
    connection = connect()
    try:
        return {"session": _session_payload(get_session(connection, session_id))}
    except LifecycleError as error:
        _raise_lifecycle_error(error)
    finally:
        connection.close()


@app.get("/sessions/{session_id}/state", response_model=TimingReadResponse)
def timing_state(session_id: str) -> JSONResponse:
    """Return the coherent current grid, flag, statistics, and tactical state."""

    try:
        return _live_payload(_read_model().snapshot(session_id).as_dict())
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/ingest-health", response_model=TimingReadResponse)
def ingest_health(session_id: str) -> JSONResponse:
    """Return the recorder/reducer recovery surface for one engineering session."""

    try:
        return _live_payload(_read_model().ingest_health(session_id))
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/metrics/history", response_model=TimingReadResponse)
def metric_history(
    session_id: str,
    scope_kind: Literal["session", "class", "participant"],
    scope_key: str,
    from_at_us: int | None = None,
    to_at_us: int | None = None,
    max_points: int = MAX_CHART_POINTS,
) -> JSONResponse:
    """Return an allowlisted chart series, capped to one 24-hour dashboard view."""

    try:
        return _live_payload(
            _read_model().metric_history(
                session_id,
                scope=MetricScopeRequest(scope_kind, scope_key),
                from_at_us=from_at_us,
                to_at_us=to_at_us,
                max_points=max_points,
            )
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/dashboard/history", response_model=TimingReadResponse)
def dashboard_history(
    session_id: str,
    participant_id: Annotated[list[str] | None, Query()] = None,
    from_at_us: int | None = None,
    to_at_us: int | None = None,
    max_points: int = MAX_CHART_POINTS,
) -> JSONResponse:
    """Return one coherent live chart payload for BALCHUG and selected cars."""

    try:
        return _live_payload(
            _read_model().dashboard_history(
                session_id,
                participant_ids=participant_id or (),
                from_at_us=from_at_us,
                to_at_us=to_at_us,
                max_points=max_points,
            )
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/metrics", response_model=TimingReadResponse)
def current_metrics(
    session_id: str,
    scope_kind: Literal["session", "class", "participant"] | None = None,
    scope_key: str | None = None,
) -> JSONResponse:
    """Return the newest computed metric scopes for the current source heat."""

    try:
        return _live_payload(
            _read_model().current_metrics(session_id, scope=_metric_scope(scope_kind, scope_key))
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/laps", response_model=TimingFactsResponse)
def laps(
    session_id: str,
    participant_id: str | None = None,
    from_at_us: int | None = None,
    to_at_us: int | None = None,
    limit: int = DEFAULT_FACT_LIMIT,
) -> JSONResponse:
    """Return a bounded source-derived lap feed for one current source heat."""

    try:
        return _live_payload(
            _read_model().laps(
                session_id,
                participant_id=participant_id,
                from_at_us=from_at_us,
                to_at_us=to_at_us,
                limit=limit,
            )
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/pit-stops", response_model=TimingFactsResponse)
def pit_stops(
    session_id: str,
    participant_id: str | None = None,
    from_at_us: int | None = None,
    to_at_us: int | None = None,
    limit: int = DEFAULT_FACT_LIMIT,
) -> JSONResponse:
    """Return a bounded source-derived pit-stop feed for one current heat."""

    try:
        return _live_payload(
            _read_model().pit_stops(
                session_id,
                participant_id=participant_id,
                from_at_us=from_at_us,
                to_at_us=to_at_us,
                limit=limit,
            )
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/race-control-messages", response_model=TimingFactsResponse)
def race_control_messages(
    session_id: str,
    active_only: bool = False,
    limit: int = DEFAULT_FACT_LIMIT,
    observation_limit: int = DEFAULT_FACT_LIMIT,
) -> JSONResponse:
    """Read the current Race Control board and its bounded immutable ledger.

    The route remains available after a session stops.  ``observed_at_us`` is
    the recorder receive instant; a provider occurrence time is returned only
    where the source actually supplied one.
    """

    try:
        return _live_payload(
            _read_model().race_control_messages(
                session_id,
                active_only=active_only,
                limit=limit,
                observation_limit=observation_limit,
            )
        )
    except TimingReadError as error:
        _raise_read_error(error)


@app.get("/sessions/{session_id}/stream")
async def timing_stream(
    session_id: str,
    request: Request,
    generation: int | None = None,
    cursor: str | None = None,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> StreamingResponse:
    """Serve a snapshot plus replayable, bounded SSE deltas for one session."""

    if generation is not None and generation < 1:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "generation must be a positive integer")
    queue = None
    broker = None
    try:
        requested_cursor = parse_last_event_id(last_event_id if last_event_id is not None else cursor)
        broker = await _stream_broker()
        queue = await broker.subscribe(session_id)
        snapshot = (await asyncio.to_thread(_read_model().snapshot, session_id)).as_dict()
        current_heat_id, current_generation = _snapshot_generation(snapshot)
        if generation is not None and generation != current_generation:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "requested source heat generation is not current")
        barrier = int(snapshot["barrier"]["stream_event_id"])
        # ``0`` is the explicit no-history cursor used by clients that cannot
        # retain EventSource state. It always receives a complete snapshot.
        reset_required = requested_cursor == 0
        if requested_cursor is not None and not reset_required:
            window = await asyncio.to_thread(read_cursor_window, session_id, cursor=requested_cursor)
            reset_required = window.requires_reset(requested_cursor) or (
                window.deleted_through_id > 0 and requested_cursor == window.deleted_through_id
            )
            # A cursor from an earlier source-heat generation must start from
            # a full snapshot, even if no event has arrived in the new heat.
            if not reset_required and requested_cursor > 0:
                prior = await asyncio.to_thread(
                    read_stream_events,
                    session_id,
                    after_id=requested_cursor - 1,
                    limit=1,
                )
                if not prior or prior[0].id != requested_cursor:
                    reset_required = True
                elif current_heat_id is not None and prior[0].source_heat_id not in {None, current_heat_id}:
                    reset_required = True
    except StreamCursorError as error:
        if queue is not None and broker is not None:
            broker.unsubscribe(session_id, queue)
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(error)) from error
    except TimingReadError as error:
        if queue is not None and broker is not None:
            broker.unsubscribe(session_id, queue)
        _raise_read_error(error)
    except HTTPException:
        if queue is not None and broker is not None:
            broker.unsubscribe(session_id, queue)
        raise

    async def event_stream():
        nonlocal snapshot, barrier
        delivered = barrier if requested_cursor is None or reset_required else requested_cursor
        active_heat_id = current_heat_id
        active_generation = current_generation
        try:
            if requested_cursor is None:
                yield format_sse_event("snapshot", snapshot, event_id=barrier, retry_ms=3_000)
            elif reset_required:
                yield format_sse_event("reset", snapshot, event_id=barrier, retry_ms=3_000)
            else:
                replayed = 0
                while replayed < 1_024:
                    events = await asyncio.to_thread(
                        read_stream_events,
                        session_id,
                        after_id=delivered,
                        limit=DEFAULT_BATCH_SIZE,
                    )
                    if not events:
                        break
                    restarted = False
                    for event in events:
                        if event.id <= delivered:
                            continue
                        replayed += 1
                        if active_heat_id is not None and event.source_heat_id not in {None, active_heat_id}:
                            snapshot = (await asyncio.to_thread(_read_model().snapshot, session_id)).as_dict()
                            barrier = int(snapshot["barrier"]["stream_event_id"])
                            active_heat_id, active_generation = _snapshot_generation(snapshot)
                            delivered = barrier
                            yield format_sse_event("reset", snapshot, event_id=barrier, retry_ms=3_000)
                            restarted = True
                            break
                        event_type, payload = _sse_event_payload(event)
                        delivered = event.id
                        yield format_sse_event(event_type, payload, event_id=event.id)
                    if restarted or len(events) < DEFAULT_BATCH_SIZE:
                        break
                if replayed >= 1_024:
                    snapshot = (await asyncio.to_thread(_read_model().snapshot, session_id)).as_dict()
                    barrier = int(snapshot["barrier"]["stream_event_id"])
                    active_heat_id, active_generation = _snapshot_generation(snapshot)
                    delivered = barrier
                    yield format_sse_event("reset", snapshot, event_id=barrier, retry_ms=3_000)

            while not await request.is_disconnected():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=2.0)
                except TimeoutError:
                    yield format_sse_comment("keepalive")
                    continue
                if isinstance(item, ResetRequired):
                    snapshot = (await asyncio.to_thread(_read_model().snapshot, session_id)).as_dict()
                    barrier = int(snapshot["barrier"]["stream_event_id"])
                    active_heat_id, active_generation = _snapshot_generation(snapshot)
                    delivered = barrier
                    yield format_sse_event("reset", snapshot, event_id=barrier, retry_ms=3_000)
                    continue
                if item.id and item.id <= delivered:
                    continue
                if (
                    item.id
                    and active_heat_id is not None
                    and item.source_heat_id not in {None, active_heat_id}
                ):
                    snapshot = (await asyncio.to_thread(_read_model().snapshot, session_id)).as_dict()
                    barrier = int(snapshot["barrier"]["stream_event_id"])
                    active_heat_id, active_generation = _snapshot_generation(snapshot)
                    delivered = barrier
                    yield format_sse_event("reset", snapshot, event_id=barrier, retry_ms=3_000)
                    continue
                event_type, payload = _sse_event_payload(item)
                if item.id:
                    delivered = item.id
                yield format_sse_event(event_type, payload, event_id=item.id or None)
        finally:
            broker.unsubscribe(session_id, queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


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
