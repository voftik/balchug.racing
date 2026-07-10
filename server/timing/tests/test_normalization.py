import unittest

from timing.normalization import (
    OPEN_ENDED_TS_TIME,
    TIME_SERVICE_EPOCH_UNIX_US,
    ConnectionClockCalibrator,
    canonical_flag,
    is_open_ended_ts_time,
    normalize_statistics_update,
    parse_result_state,
    parse_tracker_passing,
    parse_ts_time,
    received_at_to_unix_us,
    result_columns,
    time_service_to_unix_us,
)


class TimeServiceTimeTests(unittest.TestCase):
    def test_parses_raw_ts_time_without_treating_it_as_utc(self):
        raw = "837026446926000"
        self.assertEqual(parse_ts_time(raw), 837026446926000)
        self.assertEqual(time_service_to_unix_us(raw), TIME_SERVICE_EPOCH_UNIX_US + 837026446926000)
        self.assertIsNone(parse_ts_time(True))
        self.assertIsNone(parse_ts_time(1.5))
        self.assertIsNone(parse_ts_time("-1"))
        self.assertTrue(is_open_ended_ts_time(str(OPEN_ENDED_TS_TIME)))

    def test_clock_calibration_is_per_connection_and_uses_only_server_handles(self):
        provider = 1_000_000
        received = "2000-01-01T03:00:01.500000Z"
        expected_offset = 10_800_500_000
        calibrator = ConnectionClockCalibrator()

        self.assertIsNone(calibrator.observe_server_time("r_c", [provider], received))
        self.assertEqual(calibrator.sample_count, 0)
        self.assertEqual(calibrator.observe_server_time("s_i", [provider], received), expected_offset)
        self.assertEqual(calibrator.observe_server_time("s_t", [provider], received), expected_offset)
        self.assertEqual(calibrator.to_utc_us(provider), received_at_to_unix_us(received))
        self.assertIsNone(ConnectionClockCalibrator().to_utc_us(provider))


class ResultTableNormalizationTests(unittest.TestCase):
    def test_dynamic_headers_use_aliases_without_guessing_unknown_columns(self):
        columns = result_columns(
            {
                "l": {
                    "h": [
                        {"n": "POS"},
                        {"n": "NR"},
                        {"n": "STATE"},
                        {"n": "TEAM"},
                        {"n": "DRIVER IN CAR"},
                        {"n": "CLS"},
                        {"n": "PIC"},
                        {"n": "L-PIT"},
                        {"n": "SECT 1"},
                        {"n": "CAR"},
                        {"n": "S"},
                        {"n": "new provider field", "p": "x"},
                    ]
                }
            }
        )
        self.assertEqual(columns[0].key, "position_overall")
        self.assertEqual(columns[1].key, "start_number")
        self.assertEqual(columns[2].key, "state")
        self.assertEqual(columns[3].key, "team_name")
        self.assertEqual(columns[4].key, "current_driver")
        self.assertEqual(columns[5].key, "class_name")
        self.assertEqual(columns[6].key, "position_class")
        self.assertEqual(columns[7].key, "pit_time")
        self.assertEqual(columns[8].key, "sector_1")
        self.assertEqual(columns[9].key, "car_name")
        self.assertEqual(columns[10].key, "section_marker")
        self.assertIsNone(columns[11].key)
        self.assertEqual(columns[11].source_parameter, "x")

    def test_state_keeps_timer_and_literals_distinct(self):
        on_track = parse_result_state("E837026446926000")
        self.assertEqual(on_track.kind, "ON_TRACK")
        self.assertEqual(on_track.timer_target_ts_time, 837026446926000)
        self.assertEqual(on_track.timer_target_raw, "837026446926000")

        self.assertEqual(parse_result_state("SIn Pit").kind, "IN_PIT")
        self.assertEqual(parse_result_state("OutLap").kind, "OUT_LAP")
        unknown = parse_result_state("SStopped")
        self.assertEqual(unknown.kind, "UNKNOWN")
        self.assertEqual(unknown.literal, "Stopped")
        self.assertEqual(parse_result_state("Enot-a-time").kind, "UNKNOWN")


class FlagAndTrackerNormalizationTests(unittest.TestCase):
    def test_all_known_current_flag_codes_have_canonical_meanings(self):
        expected = {
            -1: "NOT_STARTED",
            0: "NOT_STARTED",
            1: "READY",
            2: "RED",
            3: "SAFETY_CAR",
            4: "CODE_60",
            5: "FINISH",
            6: "GREEN",
            7: "FULL_COURSE_YELLOW",
        }
        self.assertEqual({code: canonical_flag(code).kind for code in expected}, expected)
        self.assertEqual(canonical_flag("RedFlag").provider_code, 2)
        self.assertEqual(canonical_flag("FullCourseYellow").kind, "FULL_COURSE_YELLOW")
        self.assertEqual(canonical_flag("BlueFlag").kind, "UNKNOWN")

    def test_tracker_tuple_converts_speed_from_mm_per_second_to_kph(self):
        passing = parse_tracker_passing([42, "21", 1200, 1400, 1, 47000, False, "837021005000000", "main"])
        self.assertEqual(passing.transponder_id, "42")
        self.assertEqual(passing.start_number, "21")
        self.assertEqual(passing.distance_mm, 1200)
        self.assertEqual(passing.sector_id, 1)
        self.assertEqual(passing.speed_mm_s, 47000)
        self.assertAlmostEqual(passing.speed_kph, 169.2)
        self.assertFalse(passing.is_in_pit)
        self.assertEqual(passing.passed_at_ts_time, 837021005000000)
        self.assertEqual(passing.path_id, "main")
        self.assertEqual(passing.errors, ())

    def test_bad_tracker_tuple_stays_explicitly_invalid(self):
        passing = parse_tracker_passing("not a tuple")
        self.assertEqual(passing.errors, ("expected_tuple",))
        self.assertIsNone(passing.speed_kph)


class StatisticsNormalizationTests(unittest.TestCase):
    def test_normalizes_statistics_history_collections_and_preserves_raw_flag_boundaries(self):
        payload = {
            "h": "Practice - Open-Pit",
            "p": "30",
            "o": "401",
            "x": "66",
            "g": "837021600660000",
            "f": "837028800660000",
            "u": "0",
            "b": {
                "2": {
                    "r": "9",
                    "i": "107491000",
                    "t": "837024027000000",
                    "a": "173.58",
                    "d": "Киракозов Кирилл",
                    "n": "BALCHUG Racing",
                    "c": "Ligier JS53 evo2",
                    "s": "21",
                }
            },
            "q": {
                "1": {
                    "r": "9",
                    "i": "107491000",
                    "t": "837024027000000",
                    "a": "173.58",
                    "d": "Киракозов Кирилл",
                    "n": "BALCHUG Racing",
                    "m": "CN PRO",
                    "p": "0",
                    "c": "Ligier JS53 evo2",
                    "s": "21",
                }
            },
            "i": {
                "2": {
                    "k": "RedFlag",
                    "f": "837026446926000",
                    "t": str(OPEN_ENDED_TS_TIME),
                    "s": "0",
                    "r": "",
                }
            },
            "l": {
                "1": {
                    "f": "837024027000000",
                    "l": "9",
                    "s": "21",
                    "n": "BALCHUG Racing",
                    "d": "Киракозов Кирилл",
                    "c": "Ligier JS53 evo2",
                }
            },
            "d": {"1": {"e": "7", "n": "BALCHUG Racing", "c": "Ligier JS53 evo2", "s": "21"}},
            "bC": "12",
            "newCompactKey": "kept raw by caller",
        }

        update = normalize_statistics_update(payload)
        self.assertEqual(update.summary["heat_name"], "Practice - Open-Pit")
        self.assertEqual(update.summary["participants_started"], 30)
        self.assertEqual(update.summary["total_laps"], 401)
        self.assertEqual(update.summary["total_pitstops"], 66)
        self.assertEqual(update.summary["green_flag_ts_time"], 837021600660000)
        self.assertEqual(update.summary["safety_car_total_time_raw"], "0")

        best = update.best_lap_history[0]
        self.assertEqual(best.lap_time_us, 107491000)
        self.assertEqual(best.team_name, "BALCHUG Racing")
        self.assertEqual(best.vehicle_name, "Ligier JS53 evo2")
        self.assertEqual(best.start_number, "21")
        by_class = update.best_lap_per_class[0]
        self.assertEqual(by_class.class_name, "CN PRO")
        self.assertEqual(by_class.class_order, 0)

        caution = update.caution_periods[0]
        self.assertEqual(caution.flag.kind, "RED")
        self.assertEqual(caution.started_at_raw, "837026446926000")
        self.assertEqual(caution.started_at_ts_time, 837026446926000)
        self.assertEqual(caution.ended_at_raw, str(OPEN_ENDED_TS_TIME))
        self.assertTrue(caution.is_open)
        self.assertIsNone(caution.ended_at_ts_time)
        self.assertFalse(caution.clock_stopped)

        self.assertEqual(update.leader_history[0].lap_number, 9)
        self.assertEqual(update.leader_history[0].start_number, "21")
        self.assertEqual(update.leader_lap_aggregates[0].leader_laps, 7)
        self.assertEqual(update.truncations, {"b": 12})
        self.assertEqual(update.unknown_keys, ("newCompactKey",))

    def test_rejects_non_object_statistics_payload(self):
        self.assertEqual(normalize_statistics_update([]).errors, ("expected_object",))


if __name__ == "__main__":
    unittest.main()
