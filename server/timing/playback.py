"""Public, durable archive-playback projection built from one metric tick."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .metric_engine import MetricEngineResult
from .metric_store import HeatMetricInput, ParticipantMetricInput


PLAYBACK_SCHEMA_VERSION = "timing-archive.v1"

_SESSION_KEYS = (
    "channel_status",
    "track_flag",
    "flag_phase_elapsed_s",
    "heat_name",
    "session_elapsed_s",
    "session_remaining_s",
    "ours_identity",
    "ours_participant_id",
    "ours_class_key",
    "position_overall",
    "position_class",
    "completed_laps",
    "current_state",
    "last_lap_ms",
    "best_lap_ms",
    "class_leader_id",
    "class_ahead_id",
    "class_behind_id",
    "gap_to_class_leader_ms",
    "gap_to_ahead_ms",
    "gap_to_behind_ms",
    "lap_delta_to_class_leader",
    "lap_delta_to_ahead",
    "lap_delta_to_behind",
    "pace_3_ms",
    "pace_5_ms",
    "pace_10_ms",
    "consistency_10_ms",
    "stint_number",
    "stint_started_at_us",
    "stint_elapsed_s",
    "tyre_age_laps",
    "stint_pace_5_ms",
    "stint_trend_ms_per_lap",
    "pits_completed",
    "total_pit_lane_time_ms",
    "median_pit_lane_time_ms",
    "alerts",
)

_PARTICIPANT_KEYS = (
    "participant_id",
    "start_number",
    "team_name",
    "car_name",
    "class_name",
    "class_key",
    "is_ours",
    "current_driver_name",
    "position_overall",
    "position_class",
    "completed_laps",
    "current_state",
    "last_lap_ms",
    "best_lap_ms",
    "source_gap_ms",
    "source_diff_ms",
    "pace_3_ms",
    "pace_5_ms",
    "pace_10_ms",
    "consistency_10_ms",
    "stint_number",
    "tyre_age_laps",
    "pits_completed",
    "total_pit_lane_time_ms",
    "median_pit_lane_time_ms",
)

_CLASS_KEYS = (
    "class_key",
    "class_name",
    "participant_count",
    "class_best_lap_ms",
    "class_best_start_number",
    "class_pace_5_ms",
    "class_leader_id",
    "class_order_basis",
    "class_order_participant_ids",
    "median_pit_lane_time_ms",
    "total_completed_pits",
)


def _candidate_values(
    evaluation: MetricEngineResult,
    *,
    scope_kind: str,
    scope_key: str | None,
) -> Mapping[str, Any] | None:
    if scope_key is None:
        return None
    return next(
        (
            candidate.values
            for candidate in evaluation.candidates
            if candidate.scope_kind == scope_kind and candidate.scope_key == scope_key
        ),
        None,
    )


def _compact(values: Mapping[str, Any] | None, keys: tuple[str, ...]) -> dict[str, Any] | None:
    return {key: values.get(key) for key in keys} if values is not None else None


def _participant_payload(participant: ParticipantMetricInput | None) -> dict[str, Any] | None:
    if participant is None:
        return None
    state = participant.state
    return {
        "participant_id": participant.id,
        "start_number": participant.start_number,
        "team_name": participant.team_name,
        "car_name": participant.car_name,
        "class_name": participant.class_name,
        "class_key": participant.class_key,
        "is_ours": participant.is_ours,
        "state": (
            {
                "position_overall": state.position_overall,
                "position_class": state.position_class,
                "laps": state.laps,
                "state": state.state,
                "state_raw": state.state_raw,
                "state_kind": state.state_kind,
                "driver_name": state.current_driver_name,
                "last_lap_ms": state.last_lap_ms,
                "best_lap_ms": state.best_lap_ms,
                "gap_ms": state.gap_ms,
                "gap_raw": state.gap_raw,
                "gap_kind": state.gap_kind,
                "provider_pit_count": state.provider_pit_count,
                "observed_at_us": state.updated_at_us,
            }
            if state is not None
            else None
        ),
    }


def _flag_payload(heat: HeatMetricInput) -> dict[str, Any] | None:
    flag = heat.current_flag
    if flag is None:
        return None
    return {
        "flag": flag.flag,
        "provider_code": flag.provider_code,
        "provider_label": flag.provider_label,
        "started_at_us": flag.started_at_us,
        "observed_started_at_us": flag.observed_started_at_us,
        "calibrated_started_at_us": flag.calibrated_started_at_us,
        "source_message_id": flag.source_message_id,
        "source_key": flag.source_key,
    }


def _statistics_payload(heat: HeatMetricInput) -> dict[str, Any] | None:
    statistics = heat.statistics
    if statistics is None:
        return None
    return {
        "heat_name": statistics.heat_name,
        "participants_started": statistics.participants_started,
        "participants_on_track": statistics.participants_on_track,
        "participants_in_pit_zone": statistics.participants_in_pit_zone,
        "total_laps": statistics.total_laps,
        "total_pitstops": statistics.total_pitstops,
        "safety_car_count": statistics.safety_car_count,
        "code_60_count": statistics.code_60_count,
        "full_course_yellow_count": statistics.full_course_yellow_count,
        "observed_at_us": statistics.observed_at_us,
    }


def build_playback_payload(heat: HeatMetricInput, evaluation: MetricEngineResult) -> dict[str, Any]:
    """Create the bounded public state required for one archive playhead.

    It intentionally contains the team decision strip and source facts needed
    to explain it, not raw provider payloads or a copied full timing grid.
    Competitor chart series remain in their normalized metric history tables.
    """

    if evaluation.source_heat_id != heat.source_heat_id or evaluation.observed_at_us != heat.observed_at_us:
        raise ValueError("Playback projection must match its heat metric evaluation")
    ours = heat.our_participant
    class_scope = heat.current_class_scope
    session_values = _candidate_values(
        evaluation,
        scope_kind="session",
        scope_key=heat.session.id,
    )
    ours_values = _candidate_values(
        evaluation,
        scope_kind="participant",
        scope_key=ours.id if ours is not None else None,
    )
    class_values = _candidate_values(
        evaluation,
        scope_kind="class",
        scope_key=class_scope.key if class_scope is not None else None,
    )
    return {
        "schema_version": PLAYBACK_SCHEMA_VERSION,
        "observed_at_us": heat.observed_at_us,
        "session": {
            "id": heat.session.id,
            "mode": heat.session.mode,
            "lifecycle": heat.session.lifecycle,
            "race_duration_s": heat.session.race_duration_s,
            "required_pits": heat.session.required_pits,
            "started_at_us": heat.session.started_at_us,
            "stopped_at_us": heat.session.stopped_at_us,
            "identity_state": heat.session.identity_state,
        },
        "heat": {
            "source_heat_id": heat.source_heat_id,
            "generation": heat.generation,
            "external_name": heat.external_name,
            "provider_started_at_us": heat.provider_started_at_us,
            "provider_finished_at_us": heat.provider_finished_at_us,
            "created_at_us": heat.created_at_us,
        },
        "measured": {
            "track_flag": _flag_payload(heat),
            "statistics": _statistics_payload(heat),
            "ours": _participant_payload(ours),
            "open_ingest_gap": (
                {
                    "started_at_us": heat.open_ingest_gap.started_at_us,
                    "reason": heat.open_ingest_gap.reason,
                }
                if heat.open_ingest_gap is not None
                else None
            ),
        },
        "computed": {
            "session": _compact(session_values, _SESSION_KEYS),
            "ours": _compact(ours_values, _PARTICIPANT_KEYS),
            "class": _compact(class_values, _CLASS_KEYS),
        },
        "class_participants": [
            {
                "measured": _participant_payload(participant),
                "computed": _compact(
                    _candidate_values(
                        evaluation,
                        scope_kind="participant",
                        scope_key=participant.id,
                    ),
                    _PARTICIPANT_KEYS,
                ),
            }
            for participant in (class_scope.participants if class_scope is not None else ())
        ],
        "event_keys": list(evaluation.event_keys),
    }
