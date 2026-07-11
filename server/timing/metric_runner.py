"""Wire deterministic tactical metrics to normalized timing frames.

The normalizer commits source-derived facts first.  This runner then reads that
committed snapshot, evaluates metrics, and materializes the current dashboard
state plus sparse chart history.  It deliberately has no provider protocol or
wall-clock dependency, so replay uses the same path as live ingest.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from .metric_engine import (
    METRIC_ENGINE_VERSION,
    MetricBoundaryState,
    MetricEngineResult,
    deserialize_metric_boundary_state,
    evaluate_heat_metrics,
    serialize_metric_boundary_state,
)
from .metric_store import (
    HeatMetricInput,
    METRIC_SAMPLE_INTERVAL_US,
    MetricMaterializationResult,
    MetricSampleCandidate,
    load_heat_metric_input,
    load_metric_history,
    load_metric_runner_state,
    materialize_metric_samples,
    MetricRunnerStateCandidate,
    PlaybackSnapshotCandidate,
)
from .playback import build_playback_payload
from .stream_events import StreamEventCandidate


STREAM_SCHEMA_VERSION = "timing-live.v1"
_INTERVAL_FACT_BOUNDARY_PREFIX = "interval_fact:"
METRIC_HISTORY_LOOKBACK_US = 30 * 60 * 1_000_000
"""Bounded evidence for five-lap battle and ten-minute track trends."""


class MetricRunnerError(RuntimeError):
    """A normalized frame could not be turned into a durable metric snapshot."""


def _interval_fact_boundary_parts(boundary: str) -> tuple[str, str] | None:
    """Decode one field-level interval event without assuming ID formatting."""

    if not boundary.startswith(_INTERVAL_FACT_BOUNDARY_PREFIX):
        return None
    participant_id, separator, field_kind = boundary[len(_INTERVAL_FACT_BOUNDARY_PREFIX) :].rpartition(":")
    if not separator or not participant_id or field_kind not in {"GAP", "DIFF"}:
        return None
    return participant_id, field_kind


@dataclass(frozen=True)
class MetricRunResult:
    """One replay-safe metric evaluation tied to a normalized source frame."""

    evaluation: MetricEngineResult
    materialization: MetricMaterializationResult


class TimingMetricRunner:
    """Keep only the last derived boundary state for each source heat."""

    def __init__(self) -> None:
        self._previous: dict[int, MetricBoundaryState] = {}

    def process_frame(
        self,
        connection: sqlite3.Connection,
        *,
        source_heat_id: int,
        source_frame_id: int,
        observed_at_us: int,
        source_message_id: int | None,
        source_key: str,
        replay_active: bool = False,
    ) -> MetricRunResult:
        """Evaluate one committed normalized frame and persist its metric state.

        A stopped session can be rebuilt only after its capture is over.  Its
        historical frames nevertheless occurred while the tactical channel was
        active, so recovery explicitly opts into that historical evaluation
        context without mutating the durable session lifecycle.
        """
        if connection.in_transaction:
            raise MetricRunnerError("Metric runner requires normalized facts to be committed first")
        if type(replay_active) is not bool:
            raise MetricRunnerError("replay_active must be a boolean")
        # A server-time-only frame can advance the session clock without
        # changing a participant row, so the metric tick follows this durable
        # frame timestamp rather than the last changed row in the read model.
        heat = replace(
            load_heat_metric_input(
                connection,
                source_heat_id,
                metric_checkpoint_version=METRIC_ENGINE_VERSION,
            ),
            observed_at_us=observed_at_us,
        )
        if replay_active:
            heat = replace(heat, session=replace(heat.session, lifecycle="active"))
        # Five completed laps can span much longer than the old 180-second
        # closure window. Keep a bounded indexed tail that also admits the
        # ten-minute track-evolution baseline without scanning a full race.
        history = load_metric_history(
            connection,
            source_heat_id=source_heat_id,
            scope_kind="session",
            scope_key=heat.session.id,
            since_at_us=max(0, observed_at_us - METRIC_HISTORY_LOOKBACK_US - METRIC_SAMPLE_INTERVAL_US),
            metric_version=METRIC_ENGINE_VERSION,
        )
        previous = self._previous.get(source_heat_id)
        if previous is None:
            persisted = load_metric_runner_state(
                connection,
                source_heat_id=source_heat_id,
                metric_version=METRIC_ENGINE_VERSION,
            )
            if persisted is not None:
                try:
                    previous = deserialize_metric_boundary_state(persisted.boundary_state_json)
                except ValueError as error:
                    raise MetricRunnerError("Stored metric runner state is invalid") from error
                if previous.source_heat_id != source_heat_id:
                    raise MetricRunnerError("Stored metric runner state belongs to another heat")
        evaluation = evaluate_heat_metrics(
            heat,
            previous=previous,
            history=history,
        )
        materialization = materialize_metric_samples(
            connection,
            source_heat_id=source_heat_id,
            observed_at_us=observed_at_us,
            metric_version=METRIC_ENGINE_VERSION,
            source_key=source_key,
            source_message_id=source_message_id,
            # Event boundaries are scoped by the evaluator. Passing a global
            # boundary here would duplicate every unrelated participant's
            # chart point whenever one car completes a lap or pit stop.
            event_boundary=False,
            samples=evaluation.candidates,
            runner_state=MetricRunnerStateCandidate(
                source_frame_id=source_frame_id,
                state_hash=self._state_hash(evaluation.candidates),
                boundary_state_json=serialize_metric_boundary_state(evaluation.boundary_state),
            ),
            stream_events=self._stream_events(
                heat,
                evaluation,
                source_frame_id=source_frame_id,
                source_message_id=source_message_id,
                source_key=source_key,
                observed_at_us=observed_at_us,
            ),
            playback_snapshot=PlaybackSnapshotCandidate(
                payload=build_playback_payload(heat, evaluation),
                event_boundary=self._is_playback_boundary(heat, evaluation),
            ),
        )
        if materialization.runner_state_written:
            self._previous[source_heat_id] = evaluation.boundary_state
        elif previous is not None:
            self._previous[source_heat_id] = previous
        return MetricRunResult(evaluation=evaluation, materialization=materialization)

    @staticmethod
    def _is_playback_boundary(heat: HeatMetricInput, evaluation: MetricEngineResult) -> bool:
        """Keep archive anchors for track and Balchug events, not every car tick."""

        ours = heat.our_participant
        ours_id = ours.id if ours is not None else None
        ours_class_key = ours.class_key if ours is not None else None
        participants = {participant.id: participant for participant in heat.participants}
        for event_key in evaluation.event_keys:
            if event_key in {"track_flag", "source_gap"}:
                return True
            if ours_id is not None and event_key in {f"lap:{ours_id}", f"pit_or_stint:{ours_id}"}:
                return True
            interval = _interval_fact_boundary_parts(event_key)
            if interval is not None:
                participant_id, _ = interval
                participant = participants.get(participant_id)
                if participant_id == ours_id or (
                    participant is not None
                    and ours_class_key is not None
                    and participant.class_key == ours_class_key
                ):
                    return True
        return False

    @staticmethod
    def _stream_events(
        heat: HeatMetricInput,
        evaluation: MetricEngineResult,
        *,
        source_frame_id: int,
        source_message_id: int | None,
        source_key: str,
        observed_at_us: int,
    ) -> tuple[StreamEventCandidate, ...]:
        """Turn one durable metric boundary into compact SSE replay records.

        Event payloads deliberately identify changed facts instead of copying a
        sixty-car dashboard for every frame. Clients fetch the coherent
        read-only snapshot at the supplied cursor when they need full values.
        """

        metadata = {
            "schema_version": STREAM_SCHEMA_VERSION,
            "session_id": heat.session.id,
            "source_heat_id": heat.source_heat_id,
            "generation": heat.generation,
            "source_frame_id": source_frame_id,
            "source_message_id": source_message_id,
            "source_key": source_key,
            "observed_at_us": observed_at_us,
        }
        prefix = f"metric:{heat.source_heat_id}:{source_frame_id}"
        event_scopes = [
            {"scope_kind": candidate.scope_kind, "scope_key": candidate.scope_key}
            for candidate in evaluation.candidates
            if candidate.event_boundary
        ]
        interval_fact_updates = [
            {"participant_id": participant_id, "field_kind": field_kind}
            for boundary in evaluation.event_keys
            if (parts := _interval_fact_boundary_parts(boundary)) is not None
            for participant_id, field_kind in (parts,)
        ]
        events: list[StreamEventCandidate] = [
            StreamEventCandidate(
                "state",
                f"{prefix}:state",
                {
                    **metadata,
                    "data": {
                        "event_keys": list(evaluation.event_keys),
                        "event_scopes": event_scopes,
                        "interval_fact_updates": interval_fact_updates,
                    },
                },
            ),
            StreamEventCandidate(
                "metric",
                f"{prefix}:metric",
                {
                    **metadata,
                    "data": {
                        "metric_version": METRIC_ENGINE_VERSION,
                        "scopes": [
                            {"scope_kind": candidate.scope_kind, "scope_key": candidate.scope_key}
                            for candidate in evaluation.candidates
                        ],
                        "event_scopes": event_scopes,
                        "interval_fact_updates": interval_fact_updates,
                    },
                },
            ),
        ]

        seen: set[tuple[str, str | None]] = set()
        for boundary in evaluation.event_keys:
            event_type: str | None = None
            participant_id: str | None = None
            if boundary == "track_flag":
                event_type = "flag"
            elif boundary == "source_gap":
                event_type = "quality"
            elif boundary.startswith("lap:"):
                event_type, participant_id = "lap", boundary.split(":", 1)[1]
            elif boundary.startswith("pit_or_stint:"):
                event_type, participant_id = "pit", boundary.split(":", 1)[1]
            if event_type is None or (event_type, participant_id) in seen:
                continue
            seen.add((event_type, participant_id))
            data: dict[str, Any] = {"boundary": boundary}
            if participant_id is not None:
                data["participant_id"] = participant_id
            if event_type == "flag" and heat.current_flag is not None:
                data["flag"] = {
                    "value": heat.current_flag.flag,
                    "started_at_us": heat.current_flag.started_at_us,
                    "provider_code": heat.current_flag.provider_code,
                    "provider_label": heat.current_flag.provider_label,
                }
            if event_type == "quality":
                data["open_ingest_gap"] = (
                    {
                        "started_at_us": heat.open_ingest_gap.started_at_us,
                        "reason": heat.open_ingest_gap.reason,
                    }
                    if heat.open_ingest_gap is not None
                    else None
                )
            suffix = participant_id or "track"
            events.append(StreamEventCandidate(event_type, f"{prefix}:{event_type}:{suffix}", {**metadata, "data": data}))

        session_values = next(
            (
                candidate.values
                for candidate in evaluation.candidates
                if candidate.scope_kind == "session" and candidate.scope_key == heat.session.id
            ),
            {},
        )
        alerts = session_values.get("alerts") if isinstance(session_values, Mapping) else None
        if isinstance(alerts, tuple) and alerts and evaluation.event_keys:
            events.append(
                StreamEventCandidate(
                    "alert",
                    f"{prefix}:alert",
                    {**metadata, "data": {"alerts": list(alerts)}},
                )
            )
        return tuple(events)

    @staticmethod
    def _state_hash(candidates: tuple[MetricSampleCandidate, ...]) -> str:
        """Hash a derived tick before one atomic store transaction writes it."""

        payload: list[dict[str, Any]] = []
        for candidate in candidates:
            payload.append(
                {
                    "scope_kind": candidate.scope_kind,
                    "scope_key": candidate.scope_key,
                    "values": candidate.values,
                }
            )
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        ).hexdigest()
