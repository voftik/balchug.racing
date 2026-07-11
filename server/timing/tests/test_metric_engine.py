from dataclasses import replace
from time import perf_counter
import unittest

from timing import metric_engine
from timing.metric_engine import (
    CHANNEL_LIVE,
    CHANNEL_OFFLINE,
    METRIC_ENGINE_VERSION,
    deserialize_metric_boundary_state,
    evaluate_heat_metrics,
    serialize_metric_boundary_state,
)
from timing.metric_store import (
    ClassScopeInput,
    HeatMetricInput,
    HeatStatisticsInput,
    LapInput,
    MetricHistoryPoint,
    MetricSessionInput,
    ParticipantMetricInput,
    ParticipantStateInput,
    PitStopInput,
    StateTickInput,
    TireStintInput,
    TrackFlagInput,
)


def state(
    *,
    overall,
    position_class,
    laps,
    gap_ms,
    diff_ms,
    driver="Driver",
    state_kind="ON_TRACK",
    last_lap_ms=107_100,
    best_lap_ms=107_000,
):
    return ParticipantStateInput(
        position_overall=overall,
        position_class=position_class,
        marker=None,
        laps=laps,
        state=state_kind,
        state_raw=f"S{state_kind}",
        state_kind=state_kind,
        current_driver_name=driver,
        current_driver_stint_raw=None,
        last_lap_ms=last_lap_ms,
        last_lap_number=laps,
        best_lap_ms=best_lap_ms,
        best_lap_number=laps,
        last_sectors_json=None,
        best_sectors_json=None,
        last_speeds_json=None,
        gap_ms=gap_ms,
        gap_raw=str(gap_ms) if gap_ms is not None else None,
        gap_kind="TIME" if gap_ms is not None else None,
        diff_ms=diff_ms,
        diff_raw=str(diff_ms) if diff_ms is not None else None,
        diff_kind="TIME" if diff_ms is not None else None,
        sector_json=None,
        speed_kph=None,
        pit_time_raw=None,
        provider_pit_count=None,
        source_message_id=1,
        source_key="frame:1",
        updated_at_us=6_000_000,
    )


def laps(durations, *, first_lap=8, first_at_us=3_200_000):
    return tuple(
        LapInput(
            lap_number=first_lap + index,
            completed_at_us=first_at_us + index * 500_000,
            duration_ms=duration,
            sectors_json=None,
            flag="GREEN",
            is_in_lap=False,
            is_out_lap=False,
            crosses_pit=False,
            is_clean=True,
            source_message_id=index + 1,
            source_key=f"frame:{index + 1}",
        )
        for index, duration in enumerate(durations)
    )


def participant(
    participant_id,
    *,
    number,
    overall,
    position_class,
    lap_count,
    gap_ms,
    diff_ms,
    durations,
    ours=False,
    state_kind="ON_TRACK",
):
    completed_pit = (
        PitStopInput(
            stop_number=1,
            entered_at_us=3_000_000,
            exited_at_us=3_030_000,
            entered_lap=5,
            exited_lap=5,
            pit_lane_ms=30,
            pit_lane_duration_source_kind="RESULT_L_PIT",
            completed=True,
            entered_source_message_id=1,
            entered_source_key="frame:pit-in",
            exited_source_message_id=2,
            exited_source_key="frame:pit-out",
        ),
    ) if ours else ()
    stints = (
        TireStintInput(1, 1_000_000, 3_030_000, 0, 5, 5, 1, "frame:pit-in"),
        TireStintInput(2, 3_030_000, None, 5, None, 7, 2, "frame:pit-out"),
    ) if ours else (
        TireStintInput(1, 1_000_000, None, 0, None, lap_count, 1, "frame:1"),
    )
    return ParticipantMetricInput(
        id=participant_id,
        external_key=f"nr:{number}",
        transponder_id=None,
        start_number=number,
        team_name="BALCHUG Racing" if ours else f"Team {number}",
        car_name="Ligier JS53 evo2" if ours else "Norma",
        class_name="CN PRO",
        class_key="cn pro",
        is_ours=ours,
        active=True,
        first_seen_at_us=1_000_000,
        last_seen_at_us=6_000_000,
        state=state(
            overall=overall,
            position_class=position_class,
            laps=lap_count,
            gap_ms=gap_ms,
            diff_ms=diff_ms,
            driver="Mikhail Loboda" if ours else f"Driver {number}",
            state_kind=state_kind,
            last_lap_ms=durations[-1],
            best_lap_ms=min(durations),
        ),
        laps=laps(durations),
        pit_stops=completed_pit,
        tire_stints=stints,
    )


def attach_interval_facts(participants, *, source_message_id=1, observed_at_us=6_000_000, observation_kind="DELTA"):
    """Attach the additive GAP/DIFF provenance expected by the new evaluator.

    ``ParticipantStateInput`` gains these fields in the storage migration. The
    present fixture intentionally uses dynamic attributes so this engine-only
    test can exercise the contract before that migration lands.
    """

    by_position = {
        item.state.position_overall: item
        for item in participants
        if item.state is not None and isinstance(item.state.position_overall, int)
    }
    leader = by_position.get(1)
    for index, item in enumerate(participants, 1):
        state_value = item.state
        assert state_value is not None
        position = state_value.position_overall
        common = {
            "id": index,
            "source_message_id": source_message_id,
            "source_key": "result:" + str(source_message_id),
            "source_change_ordinal": index,
            "observed_at_us": observed_at_us,
            "source_handle": "r_c",
            "observation_kind": observation_kind,
            "subject_position_overall": position,
            "subject_state_kind": state_value.state_kind,
            "subject_laps": state_value.laps,
        }
        if state_value.gap_ms is not None and leader is not None and leader.state is not None:
            object.__setattr__(
                state_value,
                "gap_interval_fact",
                {
                    **common,
                    "field_kind": "GAP",
                    "raw_value": state_value.gap_raw,
                    "value_ms": state_value.gap_ms,
                    "value_kind": state_value.gap_kind,
                    "cell_observation_id": 100 + index,
                    "target_participant_id": leader.id,
                    "target_position_overall": leader.state.position_overall,
                    "target_state_kind": leader.state.state_kind,
                    "target_laps": leader.state.laps,
                    "relation_kind": "OVERALL_LEADER",
                },
            )
        ahead = by_position.get(position - 1) if isinstance(position, int) else None
        if state_value.diff_ms is not None and ahead is not None and ahead.state is not None:
            object.__setattr__(
                state_value,
                "diff_interval_fact",
                {
                    **common,
                    "field_kind": "DIFF",
                    "raw_value": state_value.diff_raw,
                    "value_ms": state_value.diff_ms,
                    "value_kind": state_value.diff_kind,
                    "cell_observation_id": 200 + index,
                    "target_participant_id": ahead.id,
                    "target_position_overall": ahead.state.position_overall,
                    "target_state_kind": ahead.state.state_kind,
                    "target_laps": ahead.state.laps,
                    "relation_kind": "OVERALL_AHEAD",
                },
            )
    return participants


def heat_input(*, flag="RED", ours_laps=12, ours_pic=2, with_tick=True, identity_state="resolved"):
    leader = participant(
        "leader",
        number="9",
        overall=1,
        position_class=1,
        lap_count=12,
        gap_ms=0,
        diff_ms=None,
        durations=(106_500, 106_600, 106_700, 106_800, 106_900),
    )
    ours = participant(
        "ours",
        number="21",
        overall=4,
        position_class=ours_pic,
        lap_count=ours_laps,
        gap_ms=1_250,
        diff_ms=1_250,
        durations=(107_500, 107_400, 107_300, 107_200, 107_100),
        ours=True,
    )
    follower = participant(
        "follower",
        number="35",
        overall=8,
        position_class=3,
        lap_count=12,
        gap_ms=3_000,
        diff_ms=1_750,
        durations=(108_000, 108_100, 108_200, 108_300, 108_400),
    )
    attach_interval_facts((leader, ours, follower))
    scope = ClassScopeInput(
        key="cn pro",
        display_name="CN PRO",
        class_best_lap_ms=106_500,
        class_best_start_number="9",
        participants=(leader, ours, follower),
    )
    statistics = HeatStatisticsInput(
        heat_name="Race - Heat 1",
        green_flag_at_us=1_000_000,
        finish_flag_at_us=None,
        participants_started=30,
        participants_classified=None,
        participants_not_classified=None,
        participants_on_track=28,
        participants_in_pit_zone=2,
        participants_in_tank_zone=0,
        total_laps=401,
        total_pitstops=66,
        leader_laps_green=12,
        leader_laps_safety_car=0,
        leader_laps_code_60=0,
        leader_laps_full_course_yellow=0,
        safety_car_count=0,
        code_60_count=0,
        full_course_yellow_count=0,
        observed_at_us=6_000_000,
        source_message_id=1,
        source_key="stats:1",
    )
    return HeatMetricInput(
        source_heat_id=7,
        generation=1,
        external_name="Race - Heat 1",
        provider_started_at_us=1_000_000,
        provider_finished_at_us=None,
        created_at_us=1_000_000,
        observed_at_us=6_000_000,
        session=MetricSessionInput(
            id="session-1",
            mode="race",
            lifecycle="active",
            race_duration_s=14_400,
            required_pits=3,
            started_at_us=1_000_000,
            stopped_at_us=None,
            our_participant_id="ours",
            our_class_name="CN PRO",
            identity_state=identity_state,
        ),
        latest_tick=StateTickInput(6_000_000, 120, "tick:6") if with_tick else None,
        current_flag=TrackFlagInput(
            flag=flag,
            provider_code="2" if flag == "RED" else "6",
            provider_label="Red flag" if flag == "RED" else "Green flag",
            started_at_us=5_000_000,
            observed_started_at_us=5_000_000,
            calibrated_started_at_us=5_000_000,
            start_provider_ts_raw="5000000",
            source_message_id=1,
            source_key="flag:1",
            updated_at_us=6_000_000,
        ),
        statistics=statistics,
        open_ingest_gap=None,
        participants=(leader, ours, follower),
        class_scopes=(scope,),
    )


def candidate_values(result, scope_kind, scope_key):
    return next(
        candidate.values
        for candidate in result.candidates
        if candidate.scope_kind == scope_kind and candidate.scope_key == scope_key
    )


def candidate_event_boundary(result, scope_kind, scope_key):
    return next(
        candidate.event_boundary
        for candidate in result.candidates
        if candidate.scope_kind == scope_kind and candidate.scope_key == scope_key
    )


def candidate(result, scope_kind, scope_key):
    return next(
        item
        for item in result.candidates
        if item.scope_kind == scope_kind and item.scope_key == scope_key
    )


def delta_relation_interval(
    *,
    target_participant_id,
    value_ms,
    observed_at_us,
    ours_laps,
    target_laps,
    source_message_id,
    fact_id=None,
):
    """One source-proven DELTA relation as persisted in metric history."""

    source_fact_id = source_message_id if fact_id is None else fact_id
    return {
        "target_participant_id": target_participant_id,
        "status": "VALID",
        "value_ms": value_ms,
        "source_observed_at_us": observed_at_us,
        "ours_state_kind": "ON_TRACK",
        "target_state_kind": "ON_TRACK",
        "ours_laps": ours_laps,
        "target_laps": target_laps,
        "source_facts": [
            {
                "id": source_fact_id,
                "field_kind": "GAP",
                "raw_value": f"{value_ms / 1_000:.3f}",
                "value_ms": value_ms,
                "value_kind": "TIME",
                "cell_observation_id": 10_000 + source_fact_id,
                "source_message_id": source_message_id,
                "source_key": f"result:{source_message_id}",
                "source_change_ordinal": 0,
                "observed_at_us": observed_at_us,
                "source_handle": "r_c",
                "observation_kind": "DELTA",
            }
        ],
    }


def with_ours_pit_stop(heat, stop):
    """Keep the immutable participant list and its class scope coherent."""

    ours = heat.our_participant
    assert ours is not None
    updated_ours = replace(ours, pit_stops=(stop,))
    participants = tuple(
        updated_ours if participant.id == updated_ours.id else participant
        for participant in heat.participants
    )
    scopes = tuple(
        replace(
            scope,
            participants=tuple(
                updated_ours if participant.id == updated_ours.id else participant
                for participant in scope.participants
            ),
        )
        if scope.key == updated_ours.class_key
        else scope
        for scope in heat.class_scopes
    )
    return replace(heat, participants=participants, class_scopes=scopes)


def with_participant(heat, updated):
    """Replace one immutable participant in both the heat and class scopes."""

    participants = tuple(
        updated if participant.id == updated.id else participant
        for participant in heat.participants
    )
    scopes = tuple(
        replace(
            scope,
            participants=tuple(
                updated if participant.id == updated.id else participant
                for participant in scope.participants
            ),
        )
        for scope in heat.class_scopes
    )
    return replace(heat, participants=participants, class_scopes=scopes)


class MetricEngineTests(unittest.TestCase):
    def test_ready_race_rejects_implausible_provider_start_and_keeps_full_duration(self):
        heat = heat_input(flag="GREEN")
        assert heat.current_flag is not None
        ready = replace(
            heat,
            provider_started_at_us=946_674_000_077_713,
            current_flag=replace(
                heat.current_flag,
                flag="READY",
                provider_code="1",
                provider_label="Ready",
            ),
        )
        session = candidate_values(evaluate_heat_metrics(ready), "session", "session-1")
        self.assertEqual(session["session_elapsed_s"], 0.0)
        self.assertEqual(session["session_remaining_s"], 14_400.0)

    def test_finish_flag_makes_the_tactical_channel_offline(self):
        heat = heat_input(flag="GREEN")
        assert heat.current_flag is not None
        finished = replace(
            heat,
            current_flag=replace(
                heat.current_flag,
                flag="FINISH",
                provider_code="5",
                provider_label="Finish flag",
            ),
        )
        session = candidate_values(evaluate_heat_metrics(finished), "session", "session-1")
        self.assertEqual(session["track_flag"], "FINISH")
        self.assertEqual(session["channel_status"], CHANNEL_OFFLINE)

    def test_evaluates_real_time_p0_p1_session_class_and_participant_scopes(self):
        result = evaluate_heat_metrics(heat_input())
        session = candidate_values(result, "session", "session-1")
        class_values = candidate_values(result, "class", "cn pro")
        ours = candidate_values(result, "participant", "ours")

        self.assertTrue(result.event_boundary)
        self.assertEqual(result.event_keys, ("initial_snapshot",))
        self.assertEqual(session["metric_version"], METRIC_ENGINE_VERSION)
        self.assertEqual(session["channel_status"], CHANNEL_LIVE)
        self.assertEqual(session["track_flag"], "RED")
        self.assertEqual(session["flag_phase_elapsed_s"], 1.0)
        self.assertEqual(session["session_elapsed_s"], 5.0)
        self.assertEqual(session["session_remaining_s"], 14_395.0)
        self.assertEqual(session["statistics"]["total_laps"], 401)

        self.assertEqual(session["ours_identity"]["start_number"], "21")
        self.assertEqual(session["ours_identity"]["driver_name"], "Mikhail Loboda")
        self.assertEqual(session["position_overall"], 4)
        self.assertEqual(session["position_class"], 2)
        self.assertEqual(session["completed_laps"], 12)
        self.assertEqual(session["class_leader_id"], "leader")
        self.assertEqual(session["class_ahead_id"], "leader")
        self.assertEqual(session["class_behind_id"], "follower")
        self.assertEqual(session["gap_to_class_leader_ms"], 1_250)
        self.assertEqual(session["gap_to_ahead_ms"], 1_250)
        self.assertEqual(session["gap_to_behind_ms"], 1_750)
        self.assertEqual(session["pace_5_ms"], 107_300.0)
        self.assertEqual(session["class_pace_5_ms"], 107_300.0)
        self.assertEqual(session["pace_rank_class"], 2)
        self.assertEqual(session["pace_delta_to_reference_ms"]["class_leader"], 600.0)
        self.assertEqual(session["tyre_age_laps"], 7)
        self.assertEqual(session["pits_completed"], 1)
        self.assertEqual(session["pit_history"][0]["pit_lane_duration_ms"], 30)
        self.assertEqual(session["pits_required"], 3)
        self.assertEqual(session["pits_remaining"], 2)
        self.assertEqual(session["next_equal_pit_target_elapsed_s"], 7_200.0)

        self.assertEqual(class_values["class_order_basis"], "PIC")
        self.assertEqual(class_values["class_order_participant_ids"], ["leader", "ours", "follower"])
        self.assertEqual(ours["current_state"], "ON_TRACK")
        self.assertEqual(ours["pace_3_ms"], 107_200.0)
        session_candidate = candidate(result, "session", "session-1")
        participant_candidate = candidate(result, "participant", "ours")
        self.assertIn("ours_identity", session_candidate.values)
        self.assertNotIn("ours_identity", session_candidate.history_values)
        self.assertIn("pit_history", participant_candidate.values)
        self.assertNotIn("pit_history", participant_candidate.history_values)

    def test_relation_intervals_expose_exact_direct_and_pair_provenance(self):
        session = candidate_values(evaluate_heat_metrics(heat_input(flag="GREEN")), "session", "session-1")

        leader = session["relation_intervals"]["class_leader"]
        self.assertEqual(leader["status"], "VALID")
        self.assertEqual(leader["value_ms"], 1_250)
        self.assertEqual(leader["target_participant_id"], "leader")
        self.assertEqual(leader["relation_kind"], "GAP_TO_OVERALL_LEADER")
        self.assertEqual(leader["source_facts"][0]["field_kind"], "GAP")
        self.assertEqual(leader["source_facts"][0]["raw_value"], "1250")
        self.assertEqual(leader["source_facts"][0]["cell_observation_id"], 102)
        self.assertEqual(leader["source_facts"][0]["observed_at_us"], 6_000_000)

        behind = session["relation_intervals"]["class_behind"]
        self.assertEqual(behind["status"], "VALID")
        self.assertEqual(behind["value_ms"], 1_750)
        self.assertEqual(behind["relation_kind"], "GAP_PAIR_COMMON_OVERALL_LEADER")
        self.assertEqual(len(behind["source_facts"]), 2)
        self.assertEqual({fact["source_message_id"] for fact in behind["source_facts"]}, {1})

    def test_gap_pair_requires_one_atomic_source_message(self):
        heat = heat_input(flag="GREEN")
        leader, ours, follower = heat.participants
        fact = dict(getattr(follower.state, "gap_interval_fact"))
        fact["source_message_id"] = 2
        fact["source_key"] = "result:2"
        object.__setattr__(follower.state, "gap_interval_fact", fact)

        session = candidate_values(evaluate_heat_metrics(heat), "session", "session-1")
        relation = session["relation_intervals"]["class_behind"]
        self.assertEqual(relation["status"], "NO_COHERENT_SOURCE_PAIR")
        self.assertIsNone(relation["value_ms"])
        self.assertIsNone(session["gap_to_behind_ms"])

    def test_interval_fact_pointer_has_its_own_boundary_and_scoped_history(self):
        initial = heat_input(flag="GREEN")
        first = evaluate_heat_metrics(initial)
        ours = initial.our_participant
        assert ours is not None and ours.state is not None
        gap_fact = dict(ours.state.gap_interval_fact)
        gap_fact.update(
            id=902,
            cell_observation_id=1202,
            source_message_id=2,
            source_key="result:2",
            source_change_ordinal=0,
            observed_at_us=7_000_000,
        )
        diff_fact = {
            **gap_fact,
            "id": 903,
            "field_kind": "DIFF",
            "cell_observation_id": 1203,
        }
        changed_ours = replace(
            ours,
            state=replace(
                ours.state,
                gap_interval_fact=gap_fact,
                diff_interval_fact=diff_fact,
            ),
        )
        changed = replace(with_participant(initial, changed_ours), observed_at_us=7_000_000)

        result = evaluate_heat_metrics(changed, previous=first)

        self.assertEqual(result.event_keys, ("interval_fact:ours:GAP", "interval_fact:ours:DIFF"))
        self.assertTrue(candidate_event_boundary(result, "session", "session-1"))
        self.assertTrue(candidate_event_boundary(result, "class", "cn pro"))
        self.assertTrue(candidate_event_boundary(result, "participant", "ours"))
        self.assertFalse(candidate_event_boundary(result, "participant", "leader"))
        self.assertFalse(candidate_event_boundary(result, "participant", "follower"))

        restored = deserialize_metric_boundary_state(serialize_metric_boundary_state(result.boundary_state))
        restored_ours = next(item for item in restored.participants if item.participant_id == "ours")
        self.assertEqual(restored_ours.gap_interval_fact_pointer, (902, 1202, 2, "result:2", 0, "GAP"))
        self.assertEqual(restored_ours.diff_interval_fact_pointer, (903, 1203, 2, "result:2", 0, "DIFF"))

        # A row-level state/timestamp refresh must retain the exact GAP cell
        # cursor and must not manufacture another interval event.
        refreshed_state = replace(
            changed_ours.state,
            state_raw="E70001000",
            source_key="frame:state-only",
            updated_at_us=8_000_000,
        )
        refreshed = replace(
            with_participant(changed, replace(changed_ours, state=refreshed_state)),
            observed_at_us=8_000_000,
        )
        no_interval = evaluate_heat_metrics(refreshed, previous=result)
        self.assertNotIn("interval_fact:ours:GAP", no_interval.event_keys)
        self.assertNotIn("interval_fact:ours:DIFF", no_interval.event_keys)
        self.assertEqual(no_interval.event_keys, ())

    def test_out_lap_accepts_a_source_proven_interval(self):
        base = heat_input(flag="GREEN")
        ours = base.our_participant
        assert ours is not None and ours.state is not None
        gap_fact = dict(ours.state.gap_interval_fact)
        gap_fact["subject_state_kind"] = "OUT_LAP"
        out_lap_ours = replace(
            ours,
            state=replace(
                ours.state,
                state="OUT_LAP",
                state_raw="SOutLap",
                state_kind="OUT_LAP",
                gap_interval_fact=gap_fact,
            ),
        )
        session = candidate_values(
            evaluate_heat_metrics(with_participant(base, out_lap_ours)),
            "session",
            "session-1",
        )

        relation = session["relation_intervals"]["class_leader"]
        self.assertEqual(relation["status"], "VALID")
        self.assertEqual(relation["value_ms"], 1_250)
        self.assertEqual(session["gap_to_class_leader_ms"], 1_250)

    def test_direct_diff_requires_the_stored_absolute_ahead_target(self):
        leader = participant(
            "leader",
            number="9",
            overall=1,
            position_class=1,
            lap_count=12,
            gap_ms=None,
            diff_ms=None,
            durations=(106_500, 106_600, 106_700, 106_800, 106_900),
        )
        ours = participant(
            "ours",
            number="21",
            overall=2,
            position_class=2,
            lap_count=12,
            gap_ms=None,
            diff_ms=840,
            durations=(107_500, 107_400, 107_300, 107_200, 107_100),
            ours=True,
        )
        follower = participant(
            "follower",
            number="35",
            overall=3,
            position_class=3,
            lap_count=12,
            gap_ms=None,
            diff_ms=920,
            durations=(108_000, 108_100, 108_200, 108_300, 108_400),
        )
        attach_interval_facts((leader, ours, follower))
        scope = ClassScopeInput(
            key="cn pro",
            display_name="CN PRO",
            class_best_lap_ms=106_500,
            class_best_start_number="9",
            participants=(leader, ours, follower),
        )
        heat = replace(heat_input(flag="GREEN"), participants=(leader, ours, follower), class_scopes=(scope,))

        session = candidate_values(evaluate_heat_metrics(heat), "session", "session-1")
        relation = session["relation_intervals"]["class_leader"]
        self.assertEqual(relation["status"], "VALID")
        self.assertEqual(relation["value_ms"], 840)
        self.assertEqual(relation["relation_kind"], "DIFF_TO_OVERALL_AHEAD")
        self.assertEqual(relation["source_facts"][0]["field_kind"], "DIFF")

    def test_relation_invalidates_when_current_or_source_state_is_not_on_track(self):
        base = heat_input(flag="GREEN")
        leader, ours, follower = base.participants
        source_fact = dict(getattr(ours.state, "gap_interval_fact"))
        source_fact["subject_state_kind"] = "IN_PIT"
        object.__setattr__(ours.state, "gap_interval_fact", source_fact)
        source_invalid = candidate_values(evaluate_heat_metrics(base), "session", "session-1")
        self.assertEqual(
            source_invalid["relation_intervals"]["class_leader"]["status"],
            "SOURCE_STATE_MISMATCH",
        )
        self.assertIsNone(source_invalid["gap_to_class_leader_ms"])

        current_state = replace(ours.state, state="IN_PIT", state_raw="SIn Pit", state_kind="IN_PIT")
        object.__setattr__(current_state, "gap_interval_fact", getattr(ours.state, "gap_interval_fact"))
        stopped_ours = replace(ours, state=current_state)
        scope = replace(base.class_scopes[0], participants=(leader, stopped_ours, follower))
        stopped = replace(base, participants=(leader, stopped_ours, follower), class_scopes=(scope,))
        current_invalid = candidate_values(evaluate_heat_metrics(stopped), "session", "session-1")
        self.assertEqual(
            current_invalid["relation_intervals"]["class_leader"]["status"],
            "NON_RACING_STATE",
        )
        self.assertIsNone(current_invalid["gap_to_class_leader_ms"])

    def test_live_frame_without_preceding_state_tick_is_live(self):
        result = evaluate_heat_metrics(heat_input(with_tick=False))
        self.assertEqual(candidate_values(result, "session", "session-1")["channel_status"], CHANNEL_LIVE)

    def test_pit_history_uses_confirmed_l_pit_not_observed_boundary_delta(self):
        heat = heat_input()
        assert heat.our_participant is not None
        # 31 seconds elapsed between observed pit boundaries, but the result
        # grid's L-PIT source fact reports the measured 30-second lane time.
        stop = replace(
            heat.our_participant.pit_stops[0],
            exited_at_us=34_000_000,
            pit_lane_ms=30_000,
            pit_lane_duration_source_kind="RESULT_L_PIT",
        )

        session = candidate_values(
            evaluate_heat_metrics(with_ours_pit_stop(heat, stop)),
            "session",
            "session-1",
        )

        self.assertEqual(session["pit_history"][0]["pit_lane_duration_ms"], 30_000)
        self.assertEqual(session["total_pit_lane_time_ms"], 30_000)
        self.assertEqual(session["median_pit_lane_time_ms"], 30_000.0)

    def test_unproven_pit_duration_does_not_produce_time_totals_or_alerts(self):
        heat = heat_input()
        assert heat.our_participant is not None
        # A numeric field without an exact L-PIT source is deliberately not a
        # pit-duration fact, even when the observed boundaries differ by 31s.
        stop = replace(
            heat.our_participant.pit_stops[0],
            exited_at_us=34_000_000,
            pit_lane_ms=30_000,
            pit_lane_duration_source_kind=None,
        )

        result = evaluate_heat_metrics(with_ours_pit_stop(heat, stop))
        session = candidate_values(result, "session", "session-1")
        class_values = candidate_values(result, "class", "cn pro")
        participant_values = candidate_values(result, "participant", "ours")

        self.assertIsNone(session["pit_history"][0]["pit_lane_duration_ms"])
        self.assertIsNone(session["total_pit_lane_time_ms"])
        self.assertIsNone(session["median_pit_lane_time_ms"])
        self.assertIsNone(participant_values["total_pit_lane_time_ms"])
        self.assertIsNone(class_values["median_pit_lane_time_ms"])
        self.assertIsNone(session["pit_lane_anomaly"])
        self.assertFalse(
            any(alert["key"] in {"mandatory_pits_infeasible", "ours_pit_too_long"} for alert in session["alerts"])
        )

    def test_non_pic_and_lapped_targets_do_not_invent_tactical_order_or_time_gap(self):
        no_pic = heat_input(ours_pic=None)
        session = candidate_values(evaluate_heat_metrics(no_pic), "session", "session-1")
        self.assertIsNone(session["class_leader_id"])
        self.assertIsNone(session["class_ahead_id"])
        self.assertIsNone(session["class_behind_id"])

        lapped = heat_input(ours_laps=11)
        lapped_session = candidate_values(evaluate_heat_metrics(lapped), "session", "session-1")
        self.assertEqual(lapped_session["lap_delta_to_class_leader"], -1)
        self.assertIsNone(lapped_session["gap_to_class_leader_ms"])

    def test_partial_capture_tracker_laps_do_not_reject_source_gap_behind(self):
        """Missing source LAPS must not turn local tracker counts into class gaps."""

        base = heat_input(flag="GREEN")
        _, ours, follower = base.participants
        partial_ours = replace(
            ours,
            state=replace(
                ours.state,
                position_overall=1,
                position_class=1,
                laps=None,
                gap_ms=None,
                gap_raw=None,
                gap_kind=None,
            ),
        )
        partial_follower = replace(
            follower,
            state=replace(
                follower.state,
                position_overall=2,
                position_class=2,
                laps=None,
                gap_ms=1_246,
                gap_raw="1.246",
                gap_kind="TIME",
            ),
            # These are tracker passings seen only after this capture began,
            # not a source LAPS column for the whole heat.
            laps=laps((108_000, 108_100), first_lap=1),
        )
        attach_interval_facts((partial_ours, partial_follower))
        partial_scope = replace(base.class_scopes[0], participants=(partial_ours, partial_follower))
        partial = replace(base, participants=(partial_ours, partial_follower), class_scopes=(partial_scope,))
        partial_session = candidate_values(evaluate_heat_metrics(partial), "session", "session-1")

        self.assertEqual(partial_session["gap_to_behind_ms"], 1_246)
        self.assertIsNone(partial_session["lap_delta_to_behind"])
        self.assertIsNone(partial_session["completed_laps"])
        self.assertIsNone(partial_session["class_behind_completed_laps"])
        self.assertEqual(partial_session["class_density"], {"5000": 1, "10000": 1, "30000": 1})

        explicit_follower = replace(partial_follower, state=replace(partial_follower.state, laps=2))
        explicit_ours = replace(partial_ours, state=replace(partial_ours.state, laps=12))
        attach_interval_facts((explicit_ours, explicit_follower))
        explicit_scope = replace(base.class_scopes[0], participants=(explicit_ours, explicit_follower))
        explicit = replace(base, participants=(explicit_ours, explicit_follower), class_scopes=(explicit_scope,))
        explicit_session = candidate_values(evaluate_heat_metrics(explicit), "session", "session-1")

        self.assertIsNone(explicit_session["gap_to_behind_ms"])
        self.assertEqual(explicit_session["lap_delta_to_behind"], 10)

    def test_missing_source_laps_keeps_time_closure_without_per_lap_forecast(self):
        base = heat_input(flag="GREEN")
        leader, ours, follower = base.participants
        no_laps_leader = replace(leader, state=replace(leader.state, laps=None))
        no_laps_ours = replace(ours, state=replace(ours.state, laps=None))
        no_laps_follower = replace(follower, state=replace(follower.state, laps=None))
        attach_interval_facts(
            (no_laps_leader, no_laps_ours, no_laps_follower),
            observed_at_us=180_000_000,
        )
        scope = replace(base.class_scopes[0], participants=(no_laps_leader, no_laps_ours, no_laps_follower))
        heat = replace(
            base,
            participants=(no_laps_leader, no_laps_ours, no_laps_follower),
            class_scopes=(scope,),
            provider_started_at_us=0,
            observed_at_us=180_000_000,
            current_flag=replace(base.current_flag, started_at_us=0, calibrated_started_at_us=0),
        )
        history = tuple(
            MetricHistoryPoint(
                observed_at_us=timestamp,
                metric_version=METRIC_ENGINE_VERSION,
                values={
                    "track_flag": "GREEN",
                    "channel_status": "LIVE",
                    "current_state": "ON_TRACK",
                    "completed_laps": None,
                    "class_ahead_id": "leader",
                    "class_ahead_state": "ON_TRACK",
                    "lap_delta_to_ahead": None,
                    "gap_to_ahead_ms": gap,
                    "relation_intervals": {
                        "class_ahead": delta_relation_interval(
                            target_participant_id="leader",
                            value_ms=gap,
                            observed_at_us=timestamp,
                            ours_laps=None,
                            target_laps=None,
                            source_message_id=source_message_id,
                        ),
                    },
                },
            )
            for source_message_id, (timestamp, gap) in enumerate(
                ((0, 5_000), (30_000_000, 4_000), (60_000_000, 3_000), (120_000_000, 2_000)),
                1,
            )
        )

        session = candidate_values(evaluate_heat_metrics(heat, history=history), "session", "session-1")

        closure = session["closure_ahead"]["60"]
        self.assertIsNotNone(closure)
        self.assertEqual(closure["closure_ms_per_min"], 750.0)
        self.assertIsNone(closure["closure_ms_per_lap"])
        self.assertIsNone(session["catch_range"]["ahead"])
        self.assertIsNone(session["projected_gap_ms"]["ahead"])

    def test_event_boundary_is_stateful_and_ignores_ordinary_repeat(self):
        initial = heat_input()
        first = evaluate_heat_metrics(initial)
        repeated = evaluate_heat_metrics(initial, previous=first)
        self.assertFalse(repeated.event_boundary)
        self.assertEqual(repeated.event_keys, ())

        old_ours = initial.our_participant
        changed_ours = replace(
            old_ours,
            state=replace(old_ours.state, laps=13, last_lap_ms=107_000),
            laps=old_ours.laps
            + (
                LapInput(13, 6_500_000, 107_000, None, "GREEN", False, False, False, True, 7, "frame:7"),
            ),
        )
        changed_scope = replace(
            initial.class_scopes[0],
            participants=(initial.participants[0], changed_ours, initial.participants[2]),
        )
        changed = replace(
            initial,
            observed_at_us=7_000_000,
            current_flag=replace(
                initial.current_flag,
                flag="GREEN",
                provider_code="6",
                provider_label="Green flag",
                started_at_us=6_500_000,
                calibrated_started_at_us=6_500_000,
            ),
            participants=(initial.participants[0], changed_ours, initial.participants[2]),
            class_scopes=(changed_scope,),
        )
        changed_result = evaluate_heat_metrics(changed, previous=first)
        self.assertTrue(changed_result.event_boundary)
        self.assertIn("track_flag", changed_result.event_keys)
        self.assertIn("lap:ours", changed_result.event_keys)
        self.assertIn(
            "flag_changed",
            {alert["key"] for alert in candidate_values(changed_result, "session", "session-1")["alerts"]},
        )

    def test_no_laps_source_event_boundary_survives_an_equal_last_duration(self):
        initial = heat_input(flag="GREEN")
        first = evaluate_heat_metrics(initial)
        old_ours = initial.our_participant
        changed_ours = replace(
            old_ours,
            # The source layout has no LAPS.  The provider can publish an
            # identical LAST duration for two distinct laps, so the raw cell
            # id—not a fabricated lap number—must advance the event cursor.
            latest_timing_event_id=712,
            laps=old_ours.laps
            + (
                LapInput(
                    lap_number=None,
                    completed_at_us=6_500_000,
                    duration_ms=107_100,
                    sectors_json=None,
                    flag="GREEN",
                    is_in_lap=False,
                    is_out_lap=False,
                    crosses_pit=False,
                    is_clean=True,
                    source_message_id=7,
                    source_key="frame:7",
                    timing_event_id=712,
                    capture_sequence=1,
                ),
            ),
        )
        changed_scope = replace(
            initial.class_scopes[0],
            participants=(initial.participants[0], changed_ours, initial.participants[2]),
        )
        changed = replace(
            initial,
            observed_at_us=7_000_000,
            participants=(initial.participants[0], changed_ours, initial.participants[2]),
            class_scopes=(changed_scope,),
        )

        result = evaluate_heat_metrics(changed, previous=first)

        self.assertIn("lap:ours", result.event_keys)
        self.assertTrue(candidate_event_boundary(result, "session", "session-1"))
        self.assertTrue(candidate_event_boundary(result, "class", "cn pro"))
        self.assertTrue(candidate_event_boundary(result, "participant", "ours"))
        self.assertEqual(candidate_values(result, "participant", "ours")["pace_5_ms"], 107_200.0)

    def test_tracker_only_laps_do_not_dilute_timing_but_explicit_grid_laps_do(self):
        initial = heat_input(flag="GREEN")
        old_ours = initial.our_participant
        changed_ours = replace(
            old_ours,
            laps=old_ours.laps
            + (
                LapInput(
                    lap_number=13,
                    completed_at_us=6_300_000,
                    duration_ms=None,
                    sectors_json=None,
                    flag="GREEN",
                    is_in_lap=False,
                    is_out_lap=False,
                    crosses_pit=False,
                    is_clean=True,
                    source_message_id=7,
                    source_key="tracker:13",
                    timing_eligible=False,
                ),
                LapInput(
                    lap_number=14,
                    completed_at_us=6_500_000,
                    duration_ms=107_100,
                    sectors_json=None,
                    flag="GREEN",
                    is_in_lap=False,
                    is_out_lap=False,
                    crosses_pit=False,
                    is_clean=True,
                    source_message_id=8,
                    source_key="grid:14",
                    timing_eligible=True,
                ),
            ),
        )
        changed_scope = replace(
            initial.class_scopes[0],
            participants=(initial.participants[0], changed_ours, initial.participants[2]),
        )
        changed = replace(
            initial,
            participants=(initial.participants[0], changed_ours, initial.participants[2]),
            class_scopes=(changed_scope,),
        )

        values = candidate_values(evaluate_heat_metrics(changed), "participant", "ours")
        self.assertEqual(values["observed_lap_count"], len(old_ours.laps) + 1)
        self.assertEqual(values["clean_lap_count"], len(old_ours.laps) + 1)

    def test_raw_last_source_order_beats_non_monotonic_receipt_time(self):
        initial = heat_input(flag="GREEN")
        old_ours = initial.our_participant
        raw_later_frame = LapInput(
            lap_number=None,
            completed_at_us=7_000_000,
            duration_ms=107_600,
            sectors_json=None,
            flag="GREEN",
            is_in_lap=False,
            is_out_lap=False,
            crosses_pit=False,
            is_clean=True,
            source_message_id=7,
            source_key="raw:later",
            timing_event_id=702,
            capture_sequence=2,
            source_frame_id=12,
            source_message_ordinal=0,
            source_change_ordinal=0,
        )
        raw_earlier_frame = LapInput(
            lap_number=None,
            completed_at_us=8_000_000,
            duration_ms=107_500,
            sectors_json=None,
            flag="GREEN",
            is_in_lap=False,
            is_out_lap=False,
            crosses_pit=False,
            is_clean=True,
            source_message_id=8,
            source_key="raw:earlier",
            timing_event_id=701,
            capture_sequence=1,
            source_frame_id=11,
            source_message_ordinal=0,
            source_change_ordinal=0,
        )
        samples = metric_engine._lap_samples(replace(old_ours, laps=(raw_later_frame, raw_earlier_frame)))
        self.assertEqual([sample.duration_ms for sample in samples], [107_500, 107_600])

    def test_red_flag_transition_emits_critical_alert_once(self):
        initial = heat_input(flag="GREEN")
        first = evaluate_heat_metrics(initial)
        red = replace(
            initial,
            current_flag=replace(
                initial.current_flag,
                flag="RED",
                provider_code="2",
                provider_label="Red flag",
                started_at_us=6_000_000,
                calibrated_started_at_us=6_000_000,
            ),
            observed_at_us=7_000_000,
        )
        changed = evaluate_heat_metrics(red, previous=first)
        alerts = candidate_values(changed, "session", "session-1")["alerts"]
        self.assertIn("red_flag_or_session_reset", {alert["key"] for alert in alerts})
        repeated = evaluate_heat_metrics(red, previous=changed)
        self.assertNotIn(
            "red_flag_or_session_reset",
            {alert["key"] for alert in candidate_values(repeated, "session", "session-1")["alerts"]},
        )

    def test_event_boundary_is_scoped_to_the_affected_class_and_participant(self):
        initial = heat_input()
        first = evaluate_heat_metrics(initial)
        old_follower = initial.participants[2]
        changed_follower = replace(
            old_follower,
            state=replace(old_follower.state, laps=13, last_lap_ms=108_300),
            laps=old_follower.laps
            + (
                LapInput(13, 6_500_000, 108_300, None, "GREEN", False, False, False, True, 8, "frame:8"),
            ),
        )
        changed_scope = replace(
            initial.class_scopes[0],
            participants=(initial.participants[0], initial.participants[1], changed_follower),
        )
        changed = replace(
            initial,
            observed_at_us=7_000_000,
            participants=(initial.participants[0], initial.participants[1], changed_follower),
            class_scopes=(changed_scope,),
        )
        result = evaluate_heat_metrics(changed, previous=first)
        self.assertTrue(candidate_event_boundary(result, "session", "session-1"))
        self.assertTrue(candidate_event_boundary(result, "class", "cn pro"))
        self.assertTrue(candidate_event_boundary(result, "participant", "follower"))
        self.assertFalse(candidate_event_boundary(result, "participant", "ours"))
        self.assertFalse(candidate_event_boundary(result, "participant", "leader"))

    def test_unresolved_ours_identity_keeps_tactical_values_null(self):
        result = evaluate_heat_metrics(heat_input(identity_state="unresolved"))
        session = candidate_values(result, "session", "session-1")
        self.assertIsNone(session["ours_identity"])
        self.assertIsNone(session["position_overall"])
        self.assertIsNone(session["pits_required"])

    def test_reusing_normalized_laps_preserves_the_full_metric_payload(self):
        durations = tuple(107_000 + lap for lap in range(12))
        ours = participant(
            "ours",
            number="21",
            overall=1,
            position_class=1,
            lap_count=12,
            gap_ms=0,
            diff_ms=None,
            durations=durations,
            ours=True,
        )
        scope = ClassScopeInput(
            key="cn pro",
            display_name="CN PRO",
            class_best_lap_ms=107_000,
            class_best_start_number="21",
            participants=(ours,),
        )
        heat = replace(heat_input(), participants=(ours,), class_scopes=(scope,))

        reused = evaluate_heat_metrics(heat)
        p10_p90_values = metric_engine._p10_p90_values

        def legacy_p10_p90(participant, pace, *, lap_samples=None):
            return p10_p90_values(participant, pace)

        metric_engine._p10_p90_values = legacy_p10_p90
        try:
            baseline = evaluate_heat_metrics(heat)
        finally:
            metric_engine._p10_p90_values = p10_p90_values

        self.assertEqual(reused, baseline)

    def test_evaluates_a_sixty_car_class_well_within_a_live_second(self):
        # 720 laps is an intentionally conservative 24-hour-class fixture;
        # five-lap synthetic rows cannot expose accidental O(laps²) work.
        durations = tuple(107_000 + (lap % 41) for lap in range(720))
        participants = []
        for index in range(60):
            is_ours = index == 0
            participants.append(
                participant(
                    "ours" if is_ours else f"car-{index + 1}",
                    number="21" if is_ours else str(index + 1),
                    overall=index + 1,
                    position_class=index + 1,
                    lap_count=720,
                    gap_ms=index * 1_200,
                    diff_ms=1_200 if index else None,
                    durations=tuple(duration + index for duration in durations),
                    ours=is_ours,
                )
            )
        scope = ClassScopeInput(
            key="cn pro",
            display_name="CN PRO",
            class_best_lap_ms=107_000,
            class_best_start_number="21",
            participants=tuple(participants),
        )
        heat = replace(heat_input(), participants=tuple(participants), class_scopes=(scope,))
        started = perf_counter()
        result = evaluate_heat_metrics(heat)
        elapsed = perf_counter() - started
        self.assertEqual(len(result.candidates), 62)
        self.assertLess(elapsed, 1.0)

    def test_green_same_lap_history_drives_closure_catch_and_projection(self):
        heat = replace(
            heat_input(flag="GREEN"),
            provider_started_at_us=0,
            observed_at_us=180_000_000,
            current_flag=replace(
                heat_input(flag="GREEN").current_flag,
                started_at_us=0,
                calibrated_started_at_us=0,
            ),
        )
        attach_interval_facts(heat.participants, observed_at_us=180_000_000)
        history = tuple(
            MetricHistoryPoint(
                observed_at_us=timestamp,
                metric_version=METRIC_ENGINE_VERSION,
                values={
                    "track_flag": "GREEN",
                    "channel_status": "LIVE",
                    "current_state": "ON_TRACK",
                    "completed_laps": laps_count,
                    "class_ahead_id": "leader",
                    "class_behind_id": "follower",
                    "class_ahead_state": "ON_TRACK",
                    "class_behind_state": "ON_TRACK",
                    "lap_delta_to_ahead": 0,
                    "lap_delta_to_behind": 0,
                    "gap_to_ahead_ms": ahead_gap,
                    "gap_to_behind_ms": behind_gap,
                    "relation_intervals": {
                        "class_ahead": delta_relation_interval(
                            target_participant_id="leader",
                            value_ms=ahead_gap,
                            observed_at_us=timestamp,
                            ours_laps=laps_count,
                            target_laps=laps_count,
                            source_message_id=source_message_id,
                            fact_id=source_message_id * 10 + 1,
                        ),
                        "class_behind": delta_relation_interval(
                            target_participant_id="follower",
                            value_ms=behind_gap,
                            observed_at_us=timestamp,
                            ours_laps=laps_count,
                            target_laps=laps_count,
                            source_message_id=source_message_id,
                            fact_id=source_message_id * 10 + 2,
                        ),
                    },
                },
            )
            for source_message_id, (timestamp, laps_count, ahead_gap, behind_gap) in enumerate(
                (
                    (0, 8, 5_000, 750),
                    (30_000_000, 9, 4_000, 1_000),
                    (60_000_000, 10, 3_000, 1_250),
                    (120_000_000, 11, 2_000, 1_500),
                ),
                1,
            )
        )
        session = candidate_values(evaluate_heat_metrics(heat, history=history), "session", "session-1")
        ahead_60 = session["closure_ahead"]["60"]
        behind_60 = session["closure_behind"]["60"]
        self.assertEqual(ahead_60["closure_ms_per_lap"], 750.0)
        self.assertEqual(ahead_60["label"], "догоняем")
        self.assertEqual(behind_60["closure_ms_per_lap"], 250.0)
        self.assertEqual(behind_60["label"], "отрываемся")
        self.assertIsNotNone(session["catch_range"]["ahead"])
        self.assertIsNotNone(session["required_pace_to_catch_ahead_ms"])
        self.assertIsNotNone(session["required_pace_to_defend_behind_ms"])
        self.assertEqual(session["projected_gap_ms"]["ahead"]["5"], -2_500.0)
        self.assertEqual(session["class_density"], {"5000": 2, "10000": 2, "30000": 2})

    def test_ahead_target_change_resets_gap_closure_and_catch(self):
        initial = heat_input(flag="GREEN")
        leader, ours, follower = initial.participants
        changed_ours = replace(ours, state=replace(ours.state, position_class=3))
        changed_follower = replace(follower, state=replace(follower.state, position_class=2))
        scope = replace(
            initial.class_scopes[0],
            participants=(leader, changed_follower, changed_ours),
        )
        heat = replace(
            initial,
            provider_started_at_us=0,
            observed_at_us=180_000_000,
            current_flag=replace(initial.current_flag, started_at_us=0, calibrated_started_at_us=0),
            participants=(leader, changed_ours, changed_follower),
            class_scopes=(scope,),
        )
        history = tuple(
            MetricHistoryPoint(
                observed_at_us=timestamp,
                metric_version=METRIC_ENGINE_VERSION,
                values={
                    "track_flag": "GREEN",
                    "channel_status": "LIVE",
                    "current_state": "ON_TRACK",
                    "completed_laps": 12,
                    "class_ahead_id": "leader",
                    "class_ahead_state": "ON_TRACK",
                    "lap_delta_to_ahead": 0,
                    "gap_to_ahead_ms": gap,
                },
            )
            for timestamp, gap in ((0, 5_000), (60_000_000, 3_000), (120_000_000, 1_000))
        )
        session = candidate_values(evaluate_heat_metrics(heat, history=history), "session", "session-1")
        self.assertEqual(session["class_ahead_id"], "follower")
        self.assertIsNone(session["closure_ahead"]["30"])
        self.assertIsNone(session["closure_ahead"]["60"])
        self.assertIsNone(session["catch_range"]["ahead"])

    def test_sector_metrics_appear_only_from_valid_dynamic_sector_cells(self):
        base = heat_input()
        leader, ours, follower = base.participants
        ours_laps = (
            replace(ours.laps[0], sectors_json='{"sector_1":"35000000","sector_2":"36000000"}'),
            replace(ours.laps[1], sectors_json='{"sector_1":"34000000","sector_2":"37000000"}'),
        ) + ours.laps[2:]
        leader_laps = (
            replace(leader.laps[0], sectors_json='{"sector_1":"33000000","sector_2":"38000000"}'),
            replace(leader.laps[1], sectors_json='{"sector_1":"34000000","sector_2":"37000000"}'),
        ) + leader.laps[2:]
        # Sparse current-grid sectors are not a completed lap and must not
        # change a benchmark before a source LAST boundary confirms them.
        changed_ours = replace(
            ours,
            state=replace(ours.state, last_sectors_json='{"sector_1":"34000000","sector_2":"36500000"}'),
            laps=ours_laps,
        )
        changed_leader = replace(leader, laps=leader_laps)
        scope = replace(base.class_scopes[0], participants=(changed_leader, changed_ours, follower))
        heat = replace(base, participants=(changed_leader, changed_ours, follower), class_scopes=(scope,))
        session = candidate_values(evaluate_heat_metrics(heat), "session", "session-1")
        self.assertEqual(session["last_sector_ms"], {"sector_1": 34_000, "sector_2": 37_000})
        self.assertEqual(session["personal_best_sector_ms"], {"sector_1": 34_000, "sector_2": 36_000})
        self.assertEqual(session["class_best_sector_ms"], {"sector_1": 33_000, "sector_2": 36_000})
        self.assertEqual(session["ideal_lap_ms"], 70_000)
        self.assertEqual(session["potential_to_best_ms"], 37_100)
        self.assertEqual(session["sector_delta_to_competitor_ms"]["leader"], {"sector_1": 1_000, "sector_2": -1_000})
        self.assertEqual(session["largest_sector_loss"]["leader"], {"sector_index": "sector_1", "delta_ms": 1_000, "competitor_id": "leader"})

    def test_sector_metrics_reject_time_service_open_value(self):
        base = heat_input()
        leader, ours, follower = base.participants
        changed_ours = replace(
            ours,
            state=replace(
                ours.state,
                last_sectors_json='{"sector_1":"9223372036854775807"}',
            ),
        )
        scope = replace(
            base.class_scopes[0],
            participants=(leader, changed_ours, follower),
        )
        heat = replace(
            base,
            participants=(leader, changed_ours, follower),
            class_scopes=(scope,),
        )

        session = candidate_values(evaluate_heat_metrics(heat), "session", "session-1")

        self.assertIsNone(session["last_sector_ms"])
        self.assertIsNone(session["personal_best_sector_ms"])
        self.assertIsNone(session["class_best_sector_ms"])


if __name__ == "__main__":
    unittest.main()
