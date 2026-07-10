import json
import tempfile
import time
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.ingest_store import RawIngestStore
from timing.lifecycle import create_session, start_session
from timing.normalization import TIME_SERVICE_EPOCH_UNIX_US
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
        pit = self.connection.execute("SELECT stop_number,completed,pit_lane_ms,entered_lap,exited_lap FROM pit_stops").fetchone()
        self.assertEqual(tuple(pit), (1, 1, 30000, 5, 6))
        stints = self.connection.execute(
            "SELECT stint_number,started_lap,ended_lap,completed_laps FROM tire_stints ORDER BY stint_number"
        ).fetchall()
        self.assertEqual([tuple(stint) for stint in stints], [(1, 5, 6, 1), (2, 6, None, 0)])

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
