import unittest

from timing.metrics import (
    GAP_DIRECTION_BEING_CAUGHT,
    GAP_DIRECTION_CLOSING,
    GAP_RELATION_AHEAD,
    GAP_RELATION_BEHIND,
    GREEN_FLAG,
    ON_TRACK_STATE,
    GapSample,
    LapSample,
    PitStop,
    RacePlan,
    calculate_catch_range,
    calculate_gap_trend,
    calculate_gap_lap_trend,
    calculate_gap_lap_trends,
    calculate_gap_trends,
    calculate_pace_metrics,
    calculate_pit_obligations,
    class_median_pace_ms,
    completed_pit_stops,
    derive_tire_ledger,
    is_clean_lap,
    mad_ms,
    median_ms,
    pace_delta_ms,
    pace_rank,
    select_clean_laps,
)


def clean_lap(number, duration):
    return LapSample(
        lap_number=number,
        duration_ms=duration,
        flag_kinds=(GREEN_FLAG,),
        is_in_lap=False,
        is_out_lap=False,
        crosses_pit=False,
        has_feed_gap=False,
    )


def green_gap(timestamp, gap, lap, *, target_lap=None, target_id="target", flag=GREEN_FLAG):
    return GapSample(
        target_participant_id=target_id,
        observed_at_us=timestamp,
        gap_ms=gap,
        our_lap_number=lap,
        target_lap_number=lap if target_lap is None else target_lap,
        flag_kind=flag,
        our_state_kind=ON_TRACK_STATE,
        target_state_kind=ON_TRACK_STATE,
        has_feed_gap=False,
    )


class CleanLapAndPaceTests(unittest.TestCase):
    def test_clean_lap_requires_full_green_non_pit_evidence(self):
        valid = clean_lap(1, 100_000)
        rejected = (
            LapSample(lap_number=2, duration_ms=100_000, flag_kinds=("RED",), is_in_lap=False, is_out_lap=False, crosses_pit=False, has_feed_gap=False),
            LapSample(lap_number=3, duration_ms=100_000, flag_kinds=(GREEN_FLAG,), is_in_lap=True, is_out_lap=False, crosses_pit=False, has_feed_gap=False),
            LapSample(lap_number=4, duration_ms=100_000, flag_kinds=(GREEN_FLAG,), is_in_lap=False, is_out_lap=True, crosses_pit=False, has_feed_gap=False),
            LapSample(lap_number=5, duration_ms=100_000, flag_kinds=(GREEN_FLAG,), is_in_lap=False, is_out_lap=False, crosses_pit=True, has_feed_gap=False),
            LapSample(lap_number=6, duration_ms=100_000, flag_kinds=(GREEN_FLAG,), is_in_lap=False, is_out_lap=False, crosses_pit=False, has_feed_gap=True),
            LapSample(lap_number=7, duration_ms=100_000, flag_kinds=(), is_in_lap=False, is_out_lap=False, crosses_pit=False, has_feed_gap=False),
            LapSample(lap_number=8, duration_ms=100_000, flag_kinds=(GREEN_FLAG,), is_in_lap=None, is_out_lap=False, crosses_pit=False, has_feed_gap=False),
        )
        self.assertTrue(is_clean_lap(valid))
        self.assertEqual(select_clean_laps((valid, *rejected)), (valid,))

    def test_pace_windows_mad_and_slow_lap_are_deterministic(self):
        laps = tuple(clean_lap(index, duration) for index, duration in enumerate(range(100_000, 110_000, 1_000), start=1)) + (clean_lap(11, 120_000),)
        metrics = calculate_pace_metrics(laps)

        self.assertEqual(metrics.pace3_ms, 109_000.0)
        self.assertEqual(metrics.pace5_ms, 108_000.0)
        self.assertEqual(metrics.pace10_ms, 105_500.0)
        self.assertEqual(metrics.consistency10_ms, 3_706.5)
        self.assertEqual(metrics.p10_p90_range_ms, 8_000.0)
        self.assertEqual(metrics.clean_lap_count, 11)
        self.assertEqual(metrics.clean_lap_ratio, 1.0)
        self.assertEqual(metrics.slow_lap_numbers, (11,))
        self.assertEqual(median_ms((100_000, 102_000, None)), 101_000.0)
        self.assertEqual(mad_ms((100_000, 102_000, 104_000)), 2_000.0)

    def test_insufficient_clean_laps_stay_null_instead_of_zero(self):
        metrics = calculate_pace_metrics((clean_lap(1, 100_000), clean_lap(2, 101_000)))
        self.assertIsNone(metrics.pace3_ms)
        self.assertIsNone(metrics.pace5_ms)
        self.assertIsNone(metrics.pace10_ms)
        self.assertIsNone(metrics.consistency10_ms)
        self.assertIsNone(metrics.slow_lap_numbers)

    def test_pace_delta_and_rank_keep_the_documented_sign(self):
        paces = {"ours": 100_000, "equal": 100_000, "slower": 102_000, "unknown": None}
        self.assertEqual(pace_delta_ms(100_000, 102_000), -2_000.0)
        self.assertEqual(pace_delta_ms(None, 102_000), None)
        self.assertEqual(class_median_pace_ms(paces), 100_000.0)
        self.assertEqual(pace_rank("ours", paces), 1)
        self.assertEqual(pace_rank("slower", paces), 3)
        self.assertIsNone(pace_rank("unknown", paces))


class GapMetricsTests(unittest.TestCase):
    def test_lap_window_prefers_exact_five_and_falls_back_to_three(self):
        samples = tuple(
            green_gap(index * 100_000_000, 10_000 - index * 500, 10 + index)
            for index in range(6)
        )
        trends = calculate_gap_lap_trends(samples, relation=GAP_RELATION_AHEAD)
        self.assertEqual(trends[5].window_laps, 5)
        self.assertEqual(trends[5].closure_ms_per_lap, 500.0)
        self.assertEqual(trends[3].window_laps, 3)
        self.assertEqual(trends[3].closure_ms_per_lap, 500.0)

    def test_lap_window_has_relation_specific_signs(self):
        shrinking = tuple(
            green_gap(index * 100_000_000, 10_000 - index * 1_000, 20 + index)
            for index in range(4)
        )
        ahead = calculate_gap_lap_trend(shrinking, relation=GAP_RELATION_AHEAD, window_laps=3)
        behind = calculate_gap_lap_trend(shrinking, relation=GAP_RELATION_BEHIND, window_laps=3)
        self.assertEqual((ahead.direction, ahead.closure_ms_per_lap), (GAP_DIRECTION_CLOSING, 1_000.0))
        self.assertEqual((behind.direction, behind.closure_ms_per_lap), (GAP_DIRECTION_BEING_CAUGHT, -1_000.0))

    def test_lap_window_resets_on_every_safety_gate(self):
        base = [green_gap(index * 100_000_000, 10_000 - index * 500, 30 + index) for index in range(4)]
        interruptions = (
            green_gap(150_000_000, 9_250, 31, flag="RED"),
            green_gap(150_000_000, 9_250, 31, target_id="new-target"),
            green_gap(150_000_000, 9_250, 31, target_lap=30),
            GapSample(**{**base[1].__dict__, "observed_at_us": 150_000_000, "has_feed_gap": True}),
            GapSample(**{**base[1].__dict__, "observed_at_us": 150_000_000, "our_state_kind": "IN_PIT"}),
        )
        for interruption in interruptions:
            with self.subTest(interruption=interruption):
                samples = tuple(base[:2]) + (interruption,) + tuple(base[2:])
                self.assertIsNone(
                    calculate_gap_lap_trend(samples, relation=GAP_RELATION_AHEAD, window_laps=3)
                )

    def test_target_change_breaks_the_gap_window(self):
        samples = (
            green_gap(0, 10_000, 10, target_id="leader"),
            green_gap(30_000_000, 8_500, 11, target_id="leader"),
            green_gap(60_000_000, 6_000, 12, target_id="new-ahead"),
        )
        self.assertIsNone(calculate_gap_trend(samples, relation=GAP_RELATION_AHEAD, window_s=60))
        resumed = samples + (green_gap(120_000_000, 4_000, 13, target_id="new-ahead"),)
        trend = calculate_gap_trend(resumed, relation=GAP_RELATION_AHEAD, window_s=60)
        self.assertIsNotNone(trend)
        self.assertEqual(trend.started_gap_ms, 6_000)
        self.assertEqual(trend.ended_gap_ms, 4_000)

    def test_green_same_lap_gap_trend_has_fixed_closure_sign_and_label(self):
        samples = (
            green_gap(0, 10_000, 10),
            green_gap(30_000_000, 8_500, 11),
            green_gap(60_000_000, 6_000, 12),
        )
        ahead = calculate_gap_trend(samples, relation=GAP_RELATION_AHEAD, window_s=60)
        behind = calculate_gap_trend(samples, relation=GAP_RELATION_BEHIND, window_s=60)

        self.assertIsNotNone(ahead)
        self.assertEqual(ahead.gap_change_ms, -4_000)
        self.assertEqual(ahead.closure_ms_per_min, 4_000.0)
        self.assertEqual(ahead.closure_ms_per_lap, 2_000.0)
        self.assertEqual(ahead.direction, GAP_DIRECTION_CLOSING)
        self.assertIsNotNone(behind)
        self.assertEqual(behind.closure_ms_per_lap, -2_000.0)
        self.assertEqual(behind.direction, GAP_DIRECTION_BEING_CAUGHT)

    def test_non_green_or_lap_down_breaks_the_trend_and_catch_gate(self):
        red_interruption = (
            green_gap(0, 10_000, 10),
            green_gap(30_000_000, 8_000, 11, flag="RED"),
            green_gap(60_000_000, 6_000, 12),
        )
        self.assertIsNone(calculate_gap_trend(red_interruption, relation=GAP_RELATION_AHEAD, window_s=60))

        lapped = green_gap(60_000_000, 6_000, 12, target_lap=11)
        trend = calculate_gap_trend(
            (green_gap(0, 10_000, 10), green_gap(60_000_000, 6_000, 12)),
            relation=GAP_RELATION_AHEAD,
            window_s=60,
        )
        self.assertIsNone(calculate_catch_range(lapped, (trend,), relation=GAP_RELATION_AHEAD, reference_pace_ms=100_000))
        self.assertIsNone(calculate_catch_range(green_gap(60_000_000, 6_000, 12, flag="RED"), (trend,), relation=GAP_RELATION_AHEAD, reference_pace_ms=100_000))

    def test_catch_range_uses_multiple_green_closure_windows(self):
        samples = (
            green_gap(0, 10_000, 10),
            green_gap(30_000_000, 8_500, 11),
            green_gap(60_000_000, 6_000, 12),
        )
        trends = calculate_gap_trends(samples, relation=GAP_RELATION_AHEAD, windows_s=(30, 60))
        forecast = calculate_catch_range(
            samples[-1],
            tuple(trends.values()),
            relation=GAP_RELATION_AHEAD,
            reference_pace_ms=100_000,
        )
        self.assertIsNotNone(forecast)
        self.assertEqual(forecast.minimum_laps, 2.4)
        self.assertEqual(forecast.maximum_laps, 3.0)
        self.assertEqual(forecast.minimum_time_ms, 240_000.0)
        self.assertEqual(forecast.maximum_time_ms, 300_000.0)
        self.assertEqual(forecast.source_windows_s, (30, 60))

    def test_source_time_gap_without_laps_keeps_time_trend_but_not_lap_forecast(self):
        samples = tuple(
            GapSample(
                target_participant_id="target",
                observed_at_us=timestamp,
                gap_ms=gap,
                flag_kind=GREEN_FLAG,
                our_state_kind=ON_TRACK_STATE,
                target_state_kind=ON_TRACK_STATE,
                has_feed_gap=False,
                source_time_interval=True,
            )
            for timestamp, gap in ((0, 10_000), (30_000_000, 8_500), (60_000_000, 6_000))
        )

        trend = calculate_gap_trend(samples, relation=GAP_RELATION_AHEAD, window_s=60)

        self.assertIsNotNone(trend)
        self.assertEqual(trend.closure_ms_per_min, 4_000.0)
        self.assertIsNone(trend.closure_ms_per_lap)
        trends = calculate_gap_trends(samples, relation=GAP_RELATION_AHEAD, windows_s=(30, 60))
        self.assertIsNone(
            calculate_catch_range(
                samples[-1],
                tuple(trends.values()),
                relation=GAP_RELATION_AHEAD,
                reference_pace_ms=100_000,
            )
        )


class TireAndPitTests(unittest.TestCase):
    def test_completed_pit_out_resets_automatic_tire_age(self):
        laps = (
            LapSample(lap_number=5, completed_at_us=10),
            LapSample(lap_number=6, completed_at_us=20),
            LapSample(lap_number=7, completed_at_us=50),
            LapSample(lap_number=8, completed_at_us=60),
        )
        incomplete = PitStop(stop_number=1, entered_at_us=25, exited_at_us=None, entered_lap=6, completed=False)
        completed = PitStop(stop_number=2, entered_at_us=30, exited_at_us=40, entered_lap=6, exited_lap=6, pit_lane_ms=10, completed=True)
        duplicate_update = PitStop(stop_number=2, entered_at_us=30, exited_at_us=40, entered_lap=6, exited_lap=6, pit_lane_ms=10, completed=True)

        self.assertEqual(completed_pit_stops((incomplete, completed, duplicate_update)), (completed,))
        ledger = derive_tire_ledger(laps, (incomplete, completed, duplicate_update))
        self.assertEqual(ledger.completed_pit_count, 1)
        self.assertEqual(len(ledger.stints), 2)
        self.assertTrue(ledger.stints[0].is_partial)
        self.assertEqual(ledger.current_stint.started_lap, 6)
        self.assertEqual(ledger.current_tire_age_laps, 2)

        reset_ledger = derive_tire_ledger(laps[:2], (completed,))
        self.assertEqual(reset_ledger.current_tire_age_laps, 0)

    def test_pit_obligations_count_only_finished_pit_in_to_out_events(self):
        complete = PitStop(stop_number=1, entered_at_us=100, exited_at_us=130, entered_lap=5, exited_lap=5, pit_lane_ms=30, completed=True)
        incomplete = PitStop(stop_number=2, entered_at_us=200, exited_at_us=230, entered_lap=10, exited_lap=10, pit_lane_ms=30, completed=False)
        obligations = calculate_pit_obligations(
            RacePlan(duration_s=14_400, required_pits=3),
            (complete, incomplete),
            elapsed_s=3_600,
        )
        self.assertIsNotNone(obligations)
        self.assertEqual(obligations.completed_pits, 1)
        self.assertEqual(obligations.remaining_pits, 2)
        self.assertEqual(obligations.initial_equal_stint_target_s, 3_600.0)
        self.assertEqual(obligations.remaining_equal_stint_target_s, 3_600.0)
        self.assertAlmostEqual(obligations.stop_load_per_hour, 2 / 3)
        self.assertEqual(obligations.next_equal_pit_elapsed_s, 7_200.0)
        self.assertEqual(obligations.next_equal_pit_in_s, 3_600.0)
        self.assertEqual(obligations.schedule_deviation_s, -3_600.0)
        self.assertIsNone(calculate_pit_obligations(RacePlan(duration_s=None, required_pits=3), (complete,), elapsed_s=3_600))


if __name__ == "__main__":
    unittest.main()
