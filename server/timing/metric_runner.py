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
    METRIC_SAMPLE_INTERVAL_US,
    MetricMaterializationResult,
    MetricSampleCandidate,
    load_heat_metric_input,
    load_metric_history,
    load_metric_runner_state,
    materialize_metric_samples,
    MetricRunnerStateCandidate,
)


class MetricRunnerError(RuntimeError):
    """A normalized frame could not be turned into a durable metric snapshot."""


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
    ) -> MetricRunResult:
        """Evaluate one committed normalized frame and persist its metric state."""
        if connection.in_transaction:
            raise MetricRunnerError("Metric runner requires normalized facts to be committed first")
        # A server-time-only frame can advance the session clock without
        # changing a participant row, so the metric tick follows this durable
        # frame timestamp rather than the last changed row in the read model.
        heat = replace(load_heat_metric_input(connection, source_heat_id), observed_at_us=observed_at_us)
        # 180 s is the longest tactical closure window. Keep a small extra
        # cadence margin so a sample immediately before the cutoff is present.
        history = load_metric_history(
            connection,
            source_heat_id=source_heat_id,
            scope_kind="session",
            scope_key=heat.session.id,
            since_at_us=max(0, observed_at_us - 185_000_000 - METRIC_SAMPLE_INTERVAL_US),
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
        )
        if materialization.runner_state_written:
            self._previous[source_heat_id] = evaluation.boundary_state
        elif previous is not None:
            self._previous[source_heat_id] = previous
        return MetricRunResult(evaluation=evaluation, materialization=materialization)

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
