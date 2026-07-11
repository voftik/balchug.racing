import json
import tempfile
import time
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.ingest_store import RawIngestStore
from timing.lifecycle import create_session, start_session
from timing.normalization import OPEN_ENDED_TS_TIME, TIME_SERVICE_EPOCH_UNIX_US
from timing.normalizer_writer import TimingNormalizer
from timing.protocol import Bootstrap


class TimingNormalizerWriterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        self.connection = connect(self.path)
        draft = create_session(
            self.connection,
            source_slug="igora",
            mode="practice",
            idempotency_key="create-normalizer",
        ).session
        self.session = start_session(
            self.connection,
            session_id=draft.id,
            idempotency_key="start-normalizer",
        ).session
        self.store = RawIngestStore(self.connection, analysis_session_id=self.session.id)
        self.store.start_run()
        self.upstream = self.store.open_connection(Bootstrap("https://example.test/igora", "tid", None))
        self.normalizer = TimingNormalizer(self.session.id)
        self.sequence = 0

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def apply(self, messages, *, received_at_us):
        self.sequence += 1
        raw = json.dumps({"M": messages}, ensure_ascii=False, separators=(",", ":"))
        frame = self.store.persist_raw_frame(
            self.upstream,
            sequence=self.sequence,
            raw_text=raw,
            received_at_us=received_at_us,
            monotonic_ns=time.monotonic_ns(),
        )
        decoded = self.store.decode_frame(frame)
        self.normalizer(self.connection, frame, decoded)
        self.store.mark_processed(frame)
        return frame

    def test_red_flag_is_provisional_then_reconciled_to_precise_provider_boundaries(self):
        provider_start = 1_000_000
        provider_red_start = 1_002_000
        provider_red_end = 1_004_000
        offset = 10_800_000_000
        first_receive = TIME_SERVICE_EPOCH_UNIX_US + provider_start + offset
        self.apply(
            [
                [
                    "h_i",
                    {"n": "Practice - Open-Pit", "f": 6, "s": provider_start},
                ],
                ["s_i", provider_start],
                [
                    "r_i",
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
                                {"n": "PIT"},
                                {"n": "LAST"},
                            ]
                        },
                        "r": [
                            [0, 0, "1"],
                            [0, 1, "21"],
                            [0, 2, "E1003000"],
                            [0, 3, "BALCHUG Racing"],
                            [0, 4, "Киракозов Кирилл"],
                            [0, 5, "CN PRO"],
                            [0, 6, "1"],
                            [0, 7, "0"],
                            [0, 8, "107491000"],
                        ],
                    },
                ],
            ],
            received_at_us=first_receive,
        )
        self.assertEqual(
            self.connection.execute("SELECT provider_started_at_us FROM source_heats").fetchone()[0],
            first_receive,
        )
        red_observed = first_receive + 3_000_000
        self.apply([["h_h", {"f": 2}]], received_at_us=red_observed)
        provisional = self.connection.execute(
            "SELECT started_at_us,observed_started_at_us,calibrated_started_at_us FROM track_flag_periods WHERE flag='RED'"
        ).fetchone()
        self.assertEqual(tuple(provisional), (red_observed, red_observed, None))

        self.apply(
            [
                [
                    "a_u",
                    {
                        "h": "Practice - Open-Pit",
                        "o": "401",
                        "x": "66",
                        "i": {
                            "2": {
                                "k": "RedFlag",
                                "f": str(provider_red_start),
                                "t": str(provider_red_end),
                                "s": "0",
                                "r": "",
                            }
                        },
                        "q": {
                            "1": {
                                "r": "9",
                                "i": "107491000",
                                "t": "1003000",
                                "a": "173.58",
                                "d": "Киракозов Кирилл",
                                "n": "BALCHUG Racing",
                                "m": "CN PRO",
                                "c": "Ligier JS53 evo2",
                                "s": "21",
                            }
                        },
                    },
                ]
            ],
            received_at_us=first_receive + 4_000_000,
        )
        self.apply([["h_h", {"f": 6}]], received_at_us=first_receive + 5_000_000)

        red = self.connection.execute(
            """
            SELECT start_provider_ts_raw,end_provider_ts_raw,started_at_us,ended_at_us,
                   calibrated_started_at_us,calibrated_ended_at_us,reconciliation_key
            FROM track_flag_periods WHERE flag='RED'
            """
        ).fetchone()
        self.assertEqual(red["start_provider_ts_raw"], str(provider_red_start))
        self.assertEqual(red["end_provider_ts_raw"], str(provider_red_end))
        self.assertEqual(red["started_at_us"], TIME_SERVICE_EPOCH_UNIX_US + provider_red_start + offset)
        self.assertEqual(red["ended_at_us"], TIME_SERVICE_EPOCH_UNIX_US + provider_red_end + offset)
        self.assertEqual(red["calibrated_started_at_us"], red["started_at_us"])
        self.assertEqual(red["calibrated_ended_at_us"], red["ended_at_us"])
        self.assertEqual(red["reconciliation_key"], f"RED:{provider_red_start}")
        self.assertEqual(
            self.connection.execute("SELECT flag FROM track_flag_current").fetchone()[0], "GREEN"
        )
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM track_flag_periods WHERE flag='RED'").fetchone()[0],
            1,
        )

        participant = self.connection.execute(
            """
            SELECT p.start_number,p.team_name,p.class_name,p.car_name,s.position_overall,s.position_class,
                   s.state_kind,s.last_lap_ms
            FROM participants p JOIN participant_state_current s ON s.participant_id=p.id
            WHERE p.is_ours=1
            """
        ).fetchone()
        self.assertEqual(tuple(participant), ("21", "BALCHUG Racing", "CN PRO", "Ligier JS53 evo2", 1, 1, "ON_TRACK", 107491))
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM participant_result_cell_observations").fetchone()[0],
            9,
        )
        self.assertEqual(
            tuple(self.connection.execute("SELECT total_laps,total_pitstops FROM heat_statistics_current").fetchone()),
            (401, 66),
        )

    def test_delayed_open_red_history_cannot_reopen_a_period_after_finish(self):
        provider_start = 10_000_000
        provider_red_start = 10_002_000
        provider_red_end = 10_004_000
        offset = 10_800_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_start + offset
        self.apply(
            [["h_i", {"n": "Practice - Open-Pit", "f": 6, "s": provider_start}], ["s_i", provider_start]],
            received_at_us=received,
        )
        self.apply([["h_h", {"f": 2}]], received_at_us=received + 3_000_000)
        open_red = {
            "i": {
                "1": {
                    "k": "RedFlag",
                    "f": str(provider_red_start),
                    "t": "9223372036854775807",
                    "s": "0",
                    "r": "",
                }
            }
        }
        self.apply([["a_u", open_red]], received_at_us=received + 4_000_000)
        finish_received = received + 5_000_000
        self.apply([["h_h", {"f": 5}]], received_at_us=finish_received)

        # The next aggregate snapshot may still call the Red period open. It
        # must not undo the observed transition to Finish.
        self.apply([["a_u", open_red]], received_at_us=received + 6_000_000)
        red = self.connection.execute(
            """
            SELECT started_at_us,ended_at_us,observed_ended_at_us,end_provider_ts_raw,
                   calibrated_ended_at_us
            FROM track_flag_periods WHERE flag='RED'
            """
        ).fetchone()
        self.assertEqual(red["started_at_us"], TIME_SERVICE_EPOCH_UNIX_US + provider_red_start + offset)
        self.assertEqual(red["ended_at_us"], finish_received)
        self.assertEqual(red["observed_ended_at_us"], finish_received)
        self.assertEqual(red["end_provider_ts_raw"], "9223372036854775807")
        self.assertIsNone(red["calibrated_ended_at_us"])

        # When the provider later supplies the real boundary, it replaces the
        # receive-time fallback without changing the recorded observation.
        closed_red = {
            "i": {
                "1": {
                    "k": "RedFlag",
                    "f": str(provider_red_start),
                    "t": str(provider_red_end),
                    "s": "0",
                    "r": "",
                }
            }
        }
        self.apply([["a_u", closed_red]], received_at_us=received + 7_000_000)
        red = self.connection.execute(
            """
            SELECT ended_at_us,observed_ended_at_us,end_provider_ts_raw,calibrated_ended_at_us
            FROM track_flag_periods WHERE flag='RED'
            """
        ).fetchone()
        expected_end = TIME_SERVICE_EPOCH_UNIX_US + provider_red_end + offset
        self.assertEqual(red["ended_at_us"], expected_end)
        self.assertEqual(red["calibrated_ended_at_us"], expected_end)
        self.assertEqual(red["observed_ended_at_us"], finish_received)
        self.assertEqual(red["end_provider_ts_raw"], str(provider_red_end))
        self.assertEqual(self.connection.execute("SELECT flag FROM track_flag_current").fetchone()[0], "FINISH")
        timeline = self.connection.execute(
            "SELECT flag,started_at_us,ended_at_us FROM track_flag_periods ORDER BY started_at_us,id"
        ).fetchall()
        self.assertEqual([row["flag"] for row in timeline], ["GREEN", "RED", "FINISH"])
        self.assertEqual(timeline[0]["ended_at_us"], timeline[1]["started_at_us"])
        self.assertEqual(timeline[1]["ended_at_us"], expected_end)
        self.assertEqual(timeline[2]["started_at_us"], expected_end)

    def test_statistics_finish_reconciles_an_open_red_to_the_exact_finish_boundary(self):
        provider_start = 30_000_000
        provider_red_start = 32_000_000
        provider_finish = 35_000_000
        offset = 10_800_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_start + offset
        self.apply(
            [["h_i", {"n": "Practice", "f": 6, "s": provider_start}], ["s_i", provider_start]],
            received_at_us=received,
        )
        self.apply([["h_h", {"f": 2}]], received_at_us=received + 3_000_000)
        open_red = {
            "f": "0",
            "i": {
                "1": {
                    "k": "RedFlag",
                    "f": str(provider_red_start),
                    "t": "9223372036854775807",
                    "s": "0",
                    "r": "",
                }
            },
        }
        self.apply([["a_u", open_red]], received_at_us=received + 4_000_000)
        self.assertIsNone(
            self.connection.execute("SELECT finish_flag_at_us FROM heat_statistics_current").fetchone()[0]
        )
        self.apply([["h_h", {"f": 5}]], received_at_us=received + 6_000_000)
        self.apply(
            [["a_u", {"f": str(provider_finish), "i": open_red["i"]}]],
            received_at_us=received + 7_000_000,
        )

        expected_red_start = TIME_SERVICE_EPOCH_UNIX_US + provider_red_start + offset
        expected_finish = TIME_SERVICE_EPOCH_UNIX_US + provider_finish + offset
        timeline = self.connection.execute(
            """
            SELECT flag,started_at_us,ended_at_us,observed_started_at_us,observed_ended_at_us,
                   calibrated_started_at_us,calibrated_ended_at_us,start_provider_ts_raw,end_provider_ts_raw
            FROM track_flag_periods ORDER BY started_at_us,id
            """
        ).fetchall()
        self.assertEqual([row["flag"] for row in timeline], ["GREEN", "RED", "FINISH"])
        self.assertEqual(timeline[0]["ended_at_us"], expected_red_start)
        self.assertEqual(timeline[1]["started_at_us"], expected_red_start)
        self.assertEqual(timeline[1]["ended_at_us"], expected_finish)
        self.assertEqual(timeline[2]["started_at_us"], expected_finish)
        self.assertEqual(timeline[1]["end_provider_ts_raw"], str(provider_finish))
        self.assertEqual(timeline[2]["start_provider_ts_raw"], str(provider_finish))
        self.assertEqual(timeline[2]["observed_started_at_us"], received + 6_000_000)
        self.assertEqual(timeline[2]["calibrated_started_at_us"], expected_finish)
        self.assertEqual(
            self.connection.execute("SELECT finish_flag_at_us FROM heat_statistics_current").fetchone()[0],
            expected_finish,
        )

    def test_history_only_unknown_caution_splits_the_surrounding_green_status(self):
        provider_start = 40_000_000
        provider_blue_start = 42_000_000
        provider_blue_end = 43_000_000
        offset = 10_800_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_start + offset
        self.apply(
            [["h_i", {"n": "Practice", "f": 6, "s": provider_start}], ["s_i", provider_start]],
            received_at_us=received,
        )
        # This is deliberately not accompanied by h_h: it models history
        # arriving after a reconnect while the direct current state is Green.
        self.apply(
            [
                [
                    "a_u",
                    {
                        "i": {
                            "1": {
                                "k": "BlueFlag",
                                "f": str(provider_blue_start),
                                "t": str(provider_blue_end),
                                "s": "0",
                                "r": "",
                            }
                        }
                    },
                ]
            ],
            received_at_us=received + 5_000_000,
        )

        expected_start = TIME_SERVICE_EPOCH_UNIX_US + provider_blue_start + offset
        expected_end = TIME_SERVICE_EPOCH_UNIX_US + provider_blue_end + offset
        timeline = self.connection.execute(
            """
            SELECT flag,provider_label,started_at_us,ended_at_us,calibrated_started_at_us,
                   calibrated_ended_at_us,start_provider_ts_raw,end_provider_ts_raw
            FROM track_flag_periods ORDER BY started_at_us,id
            """
        ).fetchall()
        self.assertEqual([row["flag"] for row in timeline], ["GREEN", "UNKNOWN", "GREEN"])
        self.assertEqual(timeline[1]["provider_label"], "BlueFlag")
        self.assertEqual(timeline[0]["ended_at_us"], expected_start)
        self.assertEqual(timeline[1]["started_at_us"], expected_start)
        self.assertEqual(timeline[1]["ended_at_us"], expected_end)
        self.assertEqual(timeline[2]["started_at_us"], expected_end)
        current = self.connection.execute(
            "SELECT flag,started_at_us,calibrated_started_at_us FROM track_flag_current"
        ).fetchone()
        self.assertEqual(tuple(current), ("GREEN", expected_end, expected_end))

    def test_unknown_live_flag_values_remain_distinct_track_statuses(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 15_000_000
        self.apply([["h_i", {"n": "Practice", "f": 6, "s": 15_000_000}]], received_at_us=received)
        self.apply([["h_h", {"f": 8}]], received_at_us=received + 1_000_000)
        self.apply([["h_h", {"f": "BlueFlag"}]], received_at_us=received + 2_000_000)
        self.apply([["h_h", {"f": "BlackFlag"}]], received_at_us=received + 3_000_000)

        periods = self.connection.execute(
            """
            SELECT flag,provider_code,provider_label,source_flag_kind_raw,started_at_us,ended_at_us
            FROM track_flag_periods ORDER BY id
            """
        ).fetchall()
        self.assertEqual(
            [tuple(period[:4]) for period in periods],
            [
                ("GREEN", "6", "Green flag", "6"),
                ("UNKNOWN", "8", None, "8"),
                ("UNKNOWN", None, "BlueFlag", "BlueFlag"),
                ("UNKNOWN", None, "BlackFlag", "BlackFlag"),
            ],
        )
        self.assertEqual([period["ended_at_us"] for period in periods[:-1]], [received + 1_000_000, received + 2_000_000, received + 3_000_000])
        current = self.connection.execute(
            "SELECT flag,provider_label,source_flag_kind_raw FROM track_flag_current"
        ).fetchone()
        self.assertEqual(tuple(current), ("UNKNOWN", "BlackFlag", "BlackFlag"))

    def test_late_red_history_invalidates_a_previously_clean_green_lap(self):
        provider_start = 25_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_start
        self.apply(
            [
                ["h_i", {"n": "Practice", "f": 6, "s": provider_start}],
                ["s_i", provider_start],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "LAPS"}]},
                        "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E25000000"], [0, 4, "5"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply([["r_c", [[0, 4, "6"]]]], received_at_us=received + 1_000_000)
        self.apply([["r_c", [[0, 4, "7"]]]], received_at_us=received + 2_000_000)
        self.assertEqual(
            self.connection.execute("SELECT is_clean FROM laps WHERE lap_number = 7").fetchone()[0], 1
        )

        # This provider history arrives after the grid update but proves that
        # the candidate lap crossed a Red interval from 1.5 to 2.5 seconds.
        self.apply(
            [
                [
                    "a_u",
                    {
                        "i": {
                            "1": {
                                "k": "RedFlag",
                                "f": str(provider_start + 1_500_000),
                                "t": str(provider_start + 2_500_000),
                                "s": "0",
                                "r": "",
                            }
                        }
                    },
                ]
            ],
            received_at_us=received + 3_000_000,
        )
        self.assertEqual(
            self.connection.execute("SELECT is_clean FROM laps WHERE lap_number = 7").fetchone()[0], 0
        )

    def test_state_transitions_complete_pit_and_automatically_open_a_new_tire_stint(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 20_000_000
        self.apply(
            [
                ["s_i", 20_000_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "LAPS"}, {"n": "PIT"}, {"n": "L-PIT"}]},
                        "r": [[0, 0, "21"], [0, 1, "E20000000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "5"], [0, 5, "0"], [0, 6, "L0"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply(
            [["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"], [0, 6, "S20001000"]]]],
            received_at_us=received + 1_000_000,
        )
        self.apply(
            [["r_c", [[0, 1, "SOutLap"], [0, 4, "6"], [0, 6, "L30000000"]]]],
            received_at_us=received + 31_000_000,
        )
        pit = self.connection.execute(
            """
            SELECT stop_number,completed,pit_lane_ms,entered_lap,exited_lap,
                   entered_state_cell_observation_id,entered_pit_count_cell_observation_id,
                   exited_state_cell_observation_id,pit_lane_duration_source_cell_observation_id,
                   pit_lane_duration_source_kind
            FROM pit_stops
            """
        ).fetchone()
        self.assertEqual(tuple(pit[:5]), (1, 1, 30000, 5, 6))
        self.assertIsNotNone(pit["entered_state_cell_observation_id"])
        self.assertIsNotNone(pit["entered_pit_count_cell_observation_id"])
        self.assertIsNotNone(pit["exited_state_cell_observation_id"])
        self.assertIsNotNone(pit["pit_lane_duration_source_cell_observation_id"])
        self.assertEqual(pit["pit_lane_duration_source_kind"], "RESULT_L_PIT")
        stints = self.connection.execute(
            "SELECT stint_number,started_lap,ended_lap,completed_laps FROM tire_stints ORDER BY stint_number"
        ).fetchall()
        self.assertEqual([tuple(stint) for stint in stints], [(1, 5, 6, 1), (2, 6, None, 0)])

    def test_stale_l_pit_cannot_supply_duration_to_a_later_pit_count_or_state_event(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 21_000_000
        stale_duration = "L1590000000"
        self.apply(
            [
                ["s_i", 21_000_000],
                [
                    "r_i",
                    {
                        "l": {
                            "h": [
                                {"n": "NR"},
                                {"n": "STATE"},
                                {"n": "TEAM"},
                                {"n": "CLS"},
                                {"n": "LAPS"},
                                {"n": "PIT"},
                                {"n": "L-PIT"},
                            ]
                        },
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "E21000000"],
                            [0, 2, "BALCHUG Racing"],
                            [0, 3, "CN PRO"],
                            [0, 4, "10"],
                            [0, 5, "0"],
                            [0, 6, stale_duration],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        # STATE and PIT create a verified entry, but the old L-PIT cell was
        # not resent with that event.
        self.apply(
            [["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"]]]],
            received_at_us=received + 1_000_000,
        )
        # STATE closes the observed pit in a later source message; no L-PIT
        # accompanies it, so duration remains unknown rather than 26.5 min.
        self.apply([["r_c", [[0, 1, "SOutLap"]]]], received_at_us=received + 2_000_000)

        pit = self.connection.execute(
            """
            SELECT completed,pit_lane_ms,entered_pit_count_cell_observation_id,
                   exited_state_cell_observation_id,pit_lane_duration_source_cell_observation_id
            FROM pit_stops
            """
        ).fetchone()
        self.assertEqual(pit["completed"], 1)
        self.assertIsNone(pit["pit_lane_ms"])
        self.assertIsNotNone(pit["entered_pit_count_cell_observation_id"])
        self.assertIsNotNone(pit["exited_state_cell_observation_id"])
        self.assertIsNone(pit["pit_lane_duration_source_cell_observation_id"])

    def test_same_frame_l_pit_start_timestamp_binds_to_in_pit_state_across_r_c_messages(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 21_250_000
        entered_provider_time = 21_251_000
        self.apply(
            [
                ["s_i", 21_250_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "LAPS"}, {"n": "PIT"}, {"n": "L-PIT"}]},
                        "r": [[0, 0, "21"], [0, 1, "E21250000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "10"], [0, 5, "0"], [0, 6, "L0"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply(
            [
                ["r_c", [[0, 6, f"S{entered_provider_time}"]]],
                ["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"]]],
            ],
            received_at_us=received + 1_000_000,
        )

        pit = self.connection.execute(
            """
            SELECT entered_at_us,entered_at_source_cell_observation_id,
                   entered_at_source_message_id,entered_at_source_kind
            FROM pit_stops
            """
        ).fetchone()
        self.assertEqual(pit["entered_at_us"], TIME_SERVICE_EPOCH_UNIX_US + entered_provider_time)
        self.assertIsNotNone(pit["entered_at_source_cell_observation_id"])
        self.assertIsNotNone(pit["entered_at_source_message_id"])
        self.assertEqual(pit["entered_at_source_kind"], "RESULT_L_PIT_S")

    def test_l_pit_entry_sentinels_use_observed_boundary_without_source_provenance(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 21_375_000
        self.apply(
            [
                ["s_i", 21_375_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "L-PIT"}]},
                        "r": [
                            [0, 0, "21"], [0, 1, "E21375000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "L0"],
                            [1, 0, "22"], [1, 1, "E21375000"], [1, 2, "Benchmark Racing"], [1, 3, "CN PRO"], [1, 4, "L0"],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        observed_entry_at_us = received + 1_000_000
        self.apply(
            [
                [
                    "r_c",
                    [
                        [0, 4, "S0"], [0, 1, "SIn Pit"],
                        [1, 4, f"S{OPEN_ENDED_TS_TIME}"], [1, 1, "SIn Pit"],
                    ],
                ]
            ],
            received_at_us=observed_entry_at_us,
        )

        pits = self.connection.execute(
            """
            SELECT p.start_number,f.entered_at_us,f.entered_at_source_cell_observation_id,f.entered_at_source_kind
            FROM pit_stops AS f JOIN participants AS p ON p.id = f.participant_id
            ORDER BY p.start_number
            """
        ).fetchall()
        self.assertEqual(
            [tuple(pit) for pit in pits],
            [("21", observed_entry_at_us, None, None), ("22", observed_entry_at_us, None, None)],
        )

    def test_reconnect_snapshot_cannot_reuse_unchanged_l_pit_as_a_pit_duration(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 21_500_000
        headers = [
            {"n": "NR"},
            {"n": "STATE"},
            {"n": "TEAM"},
            {"n": "CLS"},
            {"n": "LAPS"},
            {"n": "PIT"},
            {"n": "L-PIT"},
        ]
        stale_duration = "L1590000000"
        self.apply(
            [
                ["s_i", 21_500_000],
                [
                    "r_i",
                    {
                        "l": {"h": headers},
                        "r": [
                            [0, 0, "21"], [0, 1, "E21500000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"],
                            [0, 4, "10"], [0, 5, "0"], [0, 6, stale_duration],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply(
            [["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"]]]],
            received_at_us=received + 1_000_000,
        )
        # A reconnect r_i repeats the stale L-PIT display while exposing the
        # state change. It closes the pit but cannot provide its duration.
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": headers},
                        "r": [
                            [0, 0, "21"], [0, 1, "SOutLap"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"],
                            [0, 4, "10"], [0, 5, "1"], [0, 6, stale_duration],
                        ],
                    },
                ]
            ],
            received_at_us=received + 2_000_000,
        )

        pit = self.connection.execute(
            """
            SELECT completed,pit_lane_ms,pit_lane_duration_source_cell_observation_id
            FROM pit_stops
            """
        ).fetchone()
        self.assertEqual(tuple(pit), (1, None, None))
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM pit_stops").fetchone()[0], 1)

    def test_unknown_state_cannot_close_an_open_pit_or_roll_a_tire_stint(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 21_750_000
        self.apply(
            [
                ["s_i", 21_750_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "LAPS"}, {"n": "PIT"}]},
                        "r": [[0, 0, "21"], [0, 1, "E21750000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "10"], [0, 5, "0"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply(
            [["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"]]]],
            received_at_us=received + 1_000_000,
        )
        self.apply([["r_c", [[0, 1, "SGarage Hold"]]]], received_at_us=received + 2_000_000)

        pit = self.connection.execute("SELECT completed,exited_at_us FROM pit_stops").fetchone()
        self.assertEqual(tuple(pit), (0, None))
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM tire_stints").fetchone()[0], 1)

    def test_pit_counter_only_update_cannot_create_a_completed_or_mandatory_stop(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 21_900_000
        self.apply(
            [
                ["s_i", 21_900_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "LAPS"}, {"n": "PIT"}]},
                        "r": [[0, 0, "21"], [0, 1, "E21900000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "10"], [0, 5, "0"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply([["r_c", [[0, 5, "1"]]]], received_at_us=received + 1_000_000)
        self.apply([["r_c", [[0, 1, "E21902000"]]]], received_at_us=received + 2_000_000)

        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM pit_stops").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM tire_stints").fetchone()[0], 1)
        pit_count = self.connection.execute(
            "SELECT provider_pit_count,provider_pit_count_source_cell_observation_id FROM participant_state_current"
        ).fetchone()
        self.assertEqual(pit_count["provider_pit_count"], 1)
        self.assertIsNotNone(pit_count["provider_pit_count_source_cell_observation_id"])

    def test_split_message_l_pit_never_backfills_a_pit_duration(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 22_050_000
        self.apply(
            [
                ["s_i", 22_050_000],
                [
                    "r_i",
                    {
                        "l": {
                            "h": [
                                {"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"},
                                {"n": "LAPS"}, {"n": "PIT"}, {"n": "L-PIT"},
                            ]
                        },
                        "r": [
                            [0, 0, "21"], [0, 1, "E22050000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"],
                            [0, 4, "10"], [0, 5, "0"], [0, 6, "L0"],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply([["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"]]]], received_at_us=received + 1_000_000)
        # L-PIT before the outbound state is source data, but it is not
        # causally bound to the exit event and therefore cannot fill duration.
        self.apply([["r_c", [[0, 6, "L30000000"]]]], received_at_us=received + 2_000_000)
        self.apply([["r_c", [[0, 1, "SOutLap"]]]], received_at_us=received + 3_000_000)
        self.apply([["r_c", [[0, 1, "SIn Pit"], [0, 5, "2"]]]], received_at_us=received + 4_000_000)
        self.apply([["r_c", [[0, 1, "SOutLap"]]]], received_at_us=received + 5_000_000)
        # Nor can an L-PIT sent after a completed outbound boundary backfill it.
        self.apply([["r_c", [[0, 6, "L31000000"]]]], received_at_us=received + 6_000_000)

        pits = self.connection.execute(
            "SELECT stop_number,pit_lane_ms,pit_lane_duration_source_cell_observation_id FROM pit_stops ORDER BY stop_number"
        ).fetchall()
        self.assertEqual([tuple(pit) for pit in pits], [(1, None, None), (2, None, None)])

    def test_same_frame_tracker_and_outbound_state_order_keeps_identical_pit_and_stint_ledgers(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 22_200_000
        self.apply(
            [
                ["s_i", 22_200_000],
                ["t_i", {"l": [[0, False, 0], [500, False, 1]], "d": []}],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "PIT"}]},
                        "r": [
                            [0, 0, "21"], [0, 1, "E22200000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "0"],
                            [1, 0, "22"], [1, 1, "E22200000"], [1, 2, "Benchmark Racing"], [1, 3, "CN PRO"], [1, 4, "0"],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply(
            [["r_c", [[0, 1, "SIn Pit"], [0, 4, "1"], [1, 1, "SIn Pit"], [1, 4, "1"]]]],
            received_at_us=received + 1_000_000,
        )
        # #21 receives t_p before its outbound r_c; #22 receives it after.
        # The final facts must depend on source evidence, not handle order.
        self.apply(
            [
                ["t_p", [[42, "21", 0, 500, 0, 47000, False, 22_202_000]]],
                ["r_c", [[0, 1, "SOutLap"]]],
                ["r_c", [[1, 1, "SOutLap"]]],
                ["t_p", [[43, "22", 0, 500, 0, 47000, False, 22_202_000]]],
            ],
            received_at_us=received + 2_000_000,
        )

        pits = self.connection.execute(
            """
            SELECT p.start_number,f.completed,f.exited_lap,f.pit_lane_ms
            FROM pit_stops AS f JOIN participants AS p ON p.id = f.participant_id
            ORDER BY p.start_number
            """
        ).fetchall()
        self.assertEqual([tuple(pit) for pit in pits], [("21", 1, 1, None), ("22", 1, 1, None)])
        stints = self.connection.execute(
            """
            SELECT p.start_number,t.stint_number,t.started_lap,t.ended_lap,t.completed_laps
            FROM tire_stints AS t JOIN participants AS p ON p.id = t.participant_id
            ORDER BY p.start_number,t.stint_number
            """
        ).fetchall()
        self.assertEqual(
            [tuple(stint) for stint in stints],
            [("21", 1, None, 1, 1), ("21", 2, 1, None, 0), ("22", 1, None, 1, 1), ("22", 2, 1, None, 0)],
        )
        laps = self.connection.execute(
            """
            SELECT p.start_number,l.is_in_lap,l.is_out_lap,l.is_clean
            FROM laps AS l JOIN participants AS p ON p.id = l.participant_id
            ORDER BY p.start_number
            """
        ).fetchall()
        self.assertEqual([tuple(lap) for lap in laps], [("21", 0, 1, 0), ("22", 0, 1, 0)])

    def test_equal_l_pit_durations_in_distinct_r_c_exit_events_remain_source_facts(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 22_350_000
        self.apply(
            [
                ["s_i", 22_350_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "LAPS"}, {"n": "PIT"}, {"n": "L-PIT"}]},
                        "r": [[0, 0, "21"], [0, 1, "E22350000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "10"], [0, 5, "0"], [0, 6, "L0"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply([["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"]]]], received_at_us=received + 1_000_000)
        self.apply(
            [["r_c", [[0, 1, "SOutLap"], [0, 6, "L30000000"]]]],
            received_at_us=received + 2_000_000,
        )
        self.apply([["r_c", [[0, 1, "SIn Pit"], [0, 5, "2"]]]], received_at_us=received + 3_000_000)
        self.apply(
            [["r_c", [[0, 1, "SOutLap"], [0, 6, "L30000000"]]]],
            received_at_us=received + 4_000_000,
        )

        pits = self.connection.execute(
            """
            SELECT stop_number,pit_lane_ms,pit_lane_duration_source_cell_observation_id
            FROM pit_stops ORDER BY stop_number
            """
        ).fetchall()
        self.assertEqual([(pit["stop_number"], pit["pit_lane_ms"]) for pit in pits], [(1, 30000), (2, 30000)])
        self.assertNotEqual(
            pits[0]["pit_lane_duration_source_cell_observation_id"],
            pits[1]["pit_lane_duration_source_cell_observation_id"],
        )

    def test_driver_stint_s_p_l_and_unknown_forms_are_typed_with_source_provenance_only(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 22_500_000
        self.apply(
            [
                ["s_i", 22_500_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STINT"}]},
                        "r": [[0, 0, "21"], [0, 1, "E22500000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "S22500100"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply([["r_c", [[0, 4, "P22500200"]]]], received_at_us=received + 100_000)
        self.apply([["r_c", [[0, 4, "L30000000"]]]], received_at_us=received + 200_000)
        self.apply([["r_c", [[0, 4, "S0"]]]], received_at_us=received + 250_000)
        self.apply([["r_c", [[0, 4, "Xopaque"]]]], received_at_us=received + 300_000)

        observations = self.connection.execute(
            """
            SELECT driver_stint_raw,driver_stint_kind,driver_stint_provider_ts_time,
                   driver_stint_duration_ms,driver_stint_cell_observation_id
            FROM participant_state_observations
            WHERE driver_stint_raw IS NOT NULL
            ORDER BY id
            """
        ).fetchall()
        self.assertEqual(
            [
                (
                    row["driver_stint_raw"],
                    row["driver_stint_kind"],
                    row["driver_stint_provider_ts_time"],
                    row["driver_stint_duration_ms"],
                )
                for row in observations
            ],
            [
                ("S22500100", "START_TS", 22_500_100, None),
                ("P22500200", "POINT_TS", 22_500_200, None),
                ("L30000000", "DURATION", None, 30_000),
                ("S0", "UNKNOWN", None, None),
                ("Xopaque", "UNKNOWN", None, None),
            ],
        )
        self.assertTrue(all(row["driver_stint_cell_observation_id"] is not None for row in observations))
        current = self.connection.execute(
            """
            SELECT current_driver_stint_raw,driver_stint_kind,driver_stint_duration_ms,
                   driver_stint_source_cell_observation_id
            FROM participant_state_current
            """
        ).fetchone()
        self.assertEqual(
            (current["current_driver_stint_raw"], current["driver_stint_kind"], current["driver_stint_duration_ms"]),
            ("Xopaque", "UNKNOWN", None),
        )
        self.assertIsNotNone(current["driver_stint_source_cell_observation_id"])

    def test_completed_pit_interval_rejects_the_next_lap_as_clean(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 22_000_000
        self.apply(
            [
                ["s_i", 22_000_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "LAPS"}, {"n": "PIT"}, {"n": "LAST"}]},
                        "r": [[0, 0, "21"], [0, 1, "E22000000"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "5"], [0, 5, "0"], [0, 6, "110000000"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply([["r_c", [[0, 4, "6"], [0, 6, "109000000"]]]], received_at_us=received + 1_000_000)
        self.apply([["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"]]]], received_at_us=received + 2_000_000)
        self.apply([["r_c", [[0, 1, "SOutLap"]]]], received_at_us=received + 3_000_000)
        self.apply(
            [["r_c", [[0, 1, "E22004000"], [0, 4, "7"], [0, 6, "109500000"]]]],
            received_at_us=received + 4_000_000,
        )

        lap = self.connection.execute(
            "SELECT is_clean FROM laps WHERE lap_number = 7"
        ).fetchone()
        self.assertEqual(lap["is_clean"], 0)

    def test_initial_in_pit_row_is_a_baseline_not_a_historical_pit_stop(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 20_000_000
        self.apply(
            [
                ["s_i", 20_000_000],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "LAPS"}, {"n": "PIT"}]},
                        "r": [[0, 0, "21"], [0, 1, "SIn Pit"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"], [0, 4, "5"], [0, 5, "3"]],
                    },
                ],
            ],
            received_at_us=received,
        )

        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM pit_stops").fetchone()[0], 0)
        stint = self.connection.execute(
            "SELECT stint_number,started_lap,completed_laps FROM tire_stints"
        ).fetchone()
        self.assertEqual(tuple(stint), (1, 5, 0))

    def test_new_heat_timestamp_creates_a_new_source_heat_but_reconnect_does_not(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 30_000_000
        self.apply([["h_i", {"n": "Heat 1", "s": 30_000_000, "f": 6}], ["s_i", 30_000_000]], received_at_us=received)
        # A fresh h_i with the same provider start is the normal reconnect snapshot.
        self.apply([["h_i", {"n": "Heat 1", "s": 30_000_000, "f": 6}]], received_at_us=received + 1_000_000)
        self.apply([["h_i", {"n": "Heat 2", "s": 40_000_000, "f": 6}]], received_at_us=received + 2_000_000)
        heats = self.connection.execute(
            "SELECT generation,external_name FROM source_heats ORDER BY generation"
        ).fetchall()
        self.assertEqual([tuple(heat) for heat in heats], [(1, "Heat 1"), (2, "Heat 2")])

    def test_finish_loop_passings_age_tires_when_the_layout_has_no_laps_column(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 50_000_000
        self.apply(
            [
                ["s_i", 50_000_000],
                ["t_i", {"l": [[0, False, 0], [500, False, 1], [50, True, -1]], "d": []}],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "PIT"}]},
                        "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E50000000"], [0, 4, "0"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply([["t_p", [[42, "21", 0, 500, 0, 47000, False, 50_010_000]]]], received_at_us=received + 10_000)
        self.apply([["r_c", [[0, 3, "SIn Pit"], [0, 4, "1"]]]], received_at_us=received + 20_000)
        self.apply([["r_c", [[0, 3, "SOutLap"]]]], received_at_us=received + 30_000)
        self.apply([["t_p", [[42, "21", 0, 500, 0, 47000, False, 50_120_000]]]], received_at_us=received + 120_000)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM laps").fetchone()[0], 2)
        stints = self.connection.execute(
            "SELECT stint_number,started_lap,ended_lap,completed_laps FROM tire_stints ORDER BY stint_number"
        ).fetchall()
        self.assertEqual([tuple(stint) for stint in stints], [(1, None, 1, 1), (2, 1, None, 1)])

    def test_no_laps_tracker_boundary_uses_same_frame_last_and_sector_cells(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 55_000_000
        self.apply(
            [
                ["s_i", 55_000_000],
                ["t_i", {"l": [[0, False, 0], [500, False, 1]], "d": []}],
                [
                    "r_i",
                    {
                        "l": {
                            "h": [
                                {"n": "NR"},
                                {"n": "TEAM"},
                                {"n": "CLS"},
                                {"n": "STATE"},
                                {"n": "LAST"},
                                {"n": "SectorTimes", "p": "1"},
                                {"n": "SectorTimes", "p": "2"},
                                {"n": "SectorTimes", "p": "3"},
                            ]
                        },
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "BALCHUG Racing"],
                            [0, 2, "CN PRO"],
                            [0, 3, "E55000000"],
                            [0, 4, "108000000"],
                            [0, 5, "35000000"],
                            [0, 6, "36000000"],
                            [0, 7, "37000000"],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        # The production feed emits S1/S2 in earlier result messages, then
        # sends LAST followed by S3 in the same r_c as the finish passing.
        self.apply(
            [["r_c", [[0, 5, "34500000"], [0, 6, "35500000"]]]],
            received_at_us=received + 50_000,
        )
        self.apply(
            [
                ["r_c", [[0, 4, "107491000"], [0, 7, "34552000"]]],
                ["t_p", [[42, "21", 0, 500, 0, 47000, False, 55_110_000]]],
            ],
            received_at_us=received + 110_000,
        )
        # A following tracker passing without a new LAST cell must not become
        # a synthetic 110 ms "lap".
        self.apply(
            [["t_p", [[42, "21", 0, 500, 0, 47000, False, 55_220_000]]]],
            received_at_us=received + 220_000,
        )

        laps = self.connection.execute(
            """
            SELECT lap_number,completed_at_us,duration_ms,sectors_json,
                   completion_passing_observation_id,duration_source_cell_observation_id,
                   duration_source_message_id,source_message_id,duration_source_kind,
                   sectors_source_cell_observation_ids_json
            FROM laps ORDER BY lap_number
            """
        ).fetchall()
        first, second = laps
        self.assertEqual(first["duration_ms"], 107491)
        self.assertEqual(
            json.loads(first["sectors_json"]),
            {"sector_1": "34500000", "sector_2": "35500000", "sector_3": "34552000"},
        )
        self.assertIsNotNone(first["completion_passing_observation_id"])
        self.assertIsNotNone(first["duration_source_cell_observation_id"])
        self.assertNotEqual(first["source_message_id"], first["duration_source_message_id"])
        self.assertEqual(first["duration_source_kind"], "RESULT_GRID_LAST")
        sector_sources = json.loads(first["sectors_source_cell_observation_ids_json"])
        self.assertEqual(set(sector_sources), {"sector_1", "sector_2", "sector_3"})
        source_sectors = self.connection.execute(
            "SELECT id,value_text FROM participant_result_cell_observations WHERE id IN (?, ?, ?) ORDER BY id",
            (sector_sources["sector_1"], sector_sources["sector_2"], sector_sources["sector_3"]),
        ).fetchall()
        self.assertEqual({row["value_text"] for row in source_sectors}, {"34500000", "35500000", "34552000"})
        source_cell = self.connection.execute(
            "SELECT value_text FROM participant_result_cell_observations WHERE id = ?",
            (first["duration_source_cell_observation_id"],),
        ).fetchone()
        self.assertEqual(source_cell["value_text"], "107491000")
        self.assertIsNone(second["duration_ms"])
        self.assertIsNone(second["duration_source_cell_observation_id"])

    def test_no_laps_tracker_boundary_requires_last_in_the_same_frame(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 56_000_000
        self.apply(
            [
                ["s_i", 56_000_000],
                ["t_i", {"l": [[0, False, 0], [500, False, 1]], "d": []}],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "LAST"}]},
                        "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E56000000"], [0, 4, "108000000"]],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply([["r_c", [[0, 4, "107491000"]]]], received_at_us=received + 10_000)
        self.apply(
            [["t_p", [[42, "21", 0, 500, 0, 47000, False, 56_110_000]]]],
            received_at_us=received + 110_000,
        )
        lap = self.connection.execute(
            "SELECT duration_ms,duration_source_cell_observation_id FROM laps"
        ).fetchone()
        self.assertEqual(tuple(lap), (None, None))

    def test_same_frame_last_marks_pit_entry_lap_non_clean_when_tracker_arrives_first(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 57_000_000
        self.apply(
            [
                ["s_i", 57_000_000],
                ["t_i", {"l": [[0, False, 0], [500, False, 1]], "d": []}],
                [
                    "r_i",
                    {
                        "l": {
                            "h": [
                                {"n": "NR"},
                                {"n": "TEAM"},
                                {"n": "CLS"},
                                {"n": "STATE"},
                                {"n": "PIT"},
                                {"n": "LAST"},
                            ]
                        },
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "BALCHUG Racing"],
                            [0, 2, "CN PRO"],
                            [0, 3, "E57000000"],
                            [0, 4, "0"],
                            [0, 5, "108000000"],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        self.apply(
            [
                ["t_p", [[42, "21", 0, 500, 0, 47000, True, 57_110_000]]],
                ["r_c", [[0, 3, "SIn Pit"], [0, 4, "1"], [0, 5, "255104000"]]],
            ],
            received_at_us=received + 110_000,
        )
        lap = self.connection.execute(
            "SELECT duration_ms,is_in_lap,is_out_lap,crosses_pit,is_clean FROM laps"
        ).fetchone()
        self.assertEqual(tuple(lap), (255104, 1, 0, 1, 0))

    def test_explicit_laps_create_known_and_skipped_lap_rows_without_guessing_missing_times(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 60_000_000
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "LAPS"}, {"n": "LAST"}]},
                        "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E60000000"], [0, 4, "5"], [0, 5, "110000000"]],
                    },
                ]
            ],
            received_at_us=received,
        )
        self.apply([["r_c", [[0, 4, "7"], [0, 5, "108000000"]]]], received_at_us=received + 1_000_000)
        laps = self.connection.execute(
            "SELECT lap_number,completed_at_us,duration_ms FROM laps ORDER BY lap_number"
        ).fetchall()
        self.assertEqual([tuple(lap) for lap in laps], [(6, None, None), (7, received + 1_000_000, 108000)])

    def test_completed_grid_lap_keeps_dynamic_sector_cells(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 65_000_000
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {
                            "h": [
                                {"n": "NR"},
                                {"n": "TEAM"},
                                {"n": "CLS"},
                                {"n": "STATE"},
                                {"n": "LAPS"},
                                {"n": "LAST"},
                                # Time Service sends the ordinal in `p`; the
                                # display caption is not the canonical key.
                                {"n": "SectorTimes", "p": "1", "c": "SECT 1"},
                                {"n": "SectorTimes", "p": "2", "c": "SECT 2"},
                            ]
                        },
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "BALCHUG Racing"],
                            [0, 2, "CN PRO"],
                            [0, 3, "E65000000"],
                            [0, 4, "5"],
                            [0, 5, "110000000"],
                            [0, 6, "35000000"],
                            [0, 7, "36000000"],
                        ],
                    },
                ]
            ],
            received_at_us=received,
        )
        self.apply(
            [
                [
                    "r_c",
                    [[0, 4, "6"], [0, 6, "34000000"], [0, 7, "9223372036854775807"], [0, 5, "108000000"]],
                ]
            ],
            received_at_us=received + 1_000_000,
        )
        lap = self.connection.execute(
            "SELECT sectors_json,sectors_source_cell_observation_ids_json FROM laps WHERE lap_number = 6"
        ).fetchone()
        sectors = json.loads(lap["sectors_json"])
        self.assertEqual(sectors, {"sector_1": "34000000", "sector_2": None})
        self.assertEqual(set(json.loads(lap["sectors_source_cell_observation_ids_json"])), {"sector_1", "sector_2"})

    def test_missing_nr_reuses_team_identity_and_conflicting_nr_21_does_not_replace_ours(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 70_000_000
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}]},
                        "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E70000000"]],
                    },
                ]
            ],
            received_at_us=received,
        )
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}]},
                        "r": [[0, 0, "BALCHUG Racing"], [0, 1, "CN PRO"], [0, 2, "E70001000"]],
                    },
                ]
            ],
            received_at_us=received + 1_000_000,
        )
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM participants").fetchone()[0], 1)
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}]},
                        "r": [[0, 0, "21"], [0, 1, "Another Team"], [0, 2, "CN PRO"], [0, 3, "E70002000"]],
                    },
                ]
            ],
            received_at_us=received + 2_000_000,
        )
        participants = self.connection.execute(
            "SELECT team_name,is_ours FROM participants ORDER BY is_ours DESC,team_name"
        ).fetchall()
        self.assertEqual([tuple(row) for row in participants], [("BALCHUG Racing", 1), ("Another Team", 0)])
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM stream_events WHERE event_type='identity_conflict'").fetchone()[0],
            1,
        )

    def test_statistics_reset_clears_only_current_materialization_not_history(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 80_000_000
        self.apply(
            [
                ["s_i", 80_000_000],
                [
                    "a_i",
                    {
                        "o": "10",
                        "x": "2",
                        "b": {
                            "1": {
                                "r": "3",
                                "i": "107491000",
                                "t": "80001000",
                                "a": "173.58",
                                "d": "Киракозов Кирилл",
                                "n": "BALCHUG Racing",
                                "c": "Ligier JS53 evo2",
                                "s": "21",
                            }
                        },
                    },
                ],
            ],
            received_at_us=received,
        )
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM heat_statistics_current").fetchone()[0], 1)
        self.apply([["a_r", {}]], received_at_us=received + 1_000_000)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM heat_statistics_current").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM source_statistics_current").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM statistics_best_lap_history").fetchone()[0], 1)

    def test_lapped_gap_stays_raw_instead_of_becoming_zero_milliseconds(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 90_000_000
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "GAP"}]},
                        "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E90000000"], [0, 4, "1 lap"]],
                    },
                ]
            ],
            received_at_us=received,
        )
        gap = self.connection.execute("SELECT gap_raw,gap_ms,gap_kind FROM participant_state_current").fetchone()
        self.assertEqual(tuple(gap), ("1 lap", None, None))


if __name__ == "__main__":
    unittest.main()
