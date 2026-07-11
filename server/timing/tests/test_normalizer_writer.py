import json
import tempfile
import time
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.gap_coordinates import parse_gap_display
from timing.ingest_store import RawIngestStore
from timing.lifecycle import create_session, start_session, stop_session
from timing.metric_store import load_heat_metric_input
from timing.normalization import OPEN_ENDED_TS_TIME, TIME_SERVICE_EPOCH_UNIX_US
from timing.normalizer_writer import (
    RUNTIME_CHECKPOINT_REDUCER_VERSION,
    NormalizerError,
    TimingNormalizer,
)
from timing.protocol import Bootstrap
from timing.read_api import TimingReadModel
from timing.retention import apply_retention, plan_retention


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

    def apply(self, messages, *, received_at_us, upstream=None, checkpoint=False):
        self.sequence += 1
        raw = json.dumps({"M": messages}, ensure_ascii=False, separators=(",", ":"))
        frame = self.store.persist_raw_frame(
            upstream or self.upstream,
            sequence=self.sequence,
            raw_text=raw,
            received_at_us=received_at_us,
            monotonic_ns=time.monotonic_ns(),
        )
        decoded = self.store.decode_frame(frame)
        self.normalizer(self.connection, frame, decoded)
        self.store.mark_processed(
            frame,
            checkpoint=self.normalizer.checkpoint_for_processed_frame(self.connection, frame) if checkpoint else None,
        )
        return frame

    def test_prestart_zero_and_duration_are_not_heat_clock_boundaries(self):
        provider_now = 837_104_000_000_000
        duration = 14_400_000_000
        offset = 946_674_000_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_now + offset
        self.apply(
            [
                ["h_i", {"n": "Race - REC", "s": 0, "e": duration, "f": 1}],
                ["s_i", provider_now],
            ],
            received_at_us=received,
        )
        prestart = self.connection.execute(
            "SELECT provider_started_at_us,provider_finished_at_us FROM source_heats"
        ).fetchone()
        self.assertEqual(tuple(prestart), (None, None))

        provider_start = provider_now + 3_000_000
        provider_finish = provider_start + duration
        self.apply(
            [["h_h", {"s": provider_start, "e": provider_finish, "f": 6}]],
            received_at_us=received + 3_000_000,
        )
        started = self.connection.execute(
            "SELECT provider_started_at_us,provider_finished_at_us FROM source_heats"
        ).fetchone()
        self.assertEqual(
            tuple(started),
            (received + 3_000_000, received + 3_000_000 + duration),
        )

    def test_runtime_checkpoint_restores_sparse_grid_and_replays_only_unanchored_tail(self):
        base = TIME_SERVICE_EPOCH_UNIX_US + 1_000_000
        checkpoint_frame = self.apply(
            [
                ["s_i", 1_000_000],
                ["h_i", {"n": "Practice", "s": 1_000_000, "f": 1}],
                ["t_i", {"l": [[0, False, 0]], "d": []}],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "POS"}, {"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "LAST"}]},
                        "r": [
                            [0, 0, "1"],
                            [0, 1, "21"],
                            [0, 2, "BALCHUG Racing"],
                            [0, 3, "CN PRO"],
                            [0, 4, "E1000000"],
                            [0, 5, "110000000"],
                        ],
                    },
                ],
            ],
            received_at_us=base,
            checkpoint=True,
        )
        tail_frame = self.apply([["r_c", [[0, 4, "In Pit"]]]], received_at_us=base + 10_000_000)

        checkpoint = self.connection.execute(
            "SELECT source_frame_id,checkpoint_format,reducer_version FROM state_checkpoints"
        ).fetchone()
        self.assertEqual(
            tuple(checkpoint),
            (checkpoint_frame.id, "timing-normalizer", RUNTIME_CHECKPOINT_REDUCER_VERSION),
        )

        self.normalizer = TimingNormalizer(self.session.id)
        self.apply([["r_c", [[0, 5, "107000000"]]]], received_at_us=base + 11_000_000)

        self.assertEqual(self.normalizer.grid.row_values(0)["last_lap"], "107000000")
        self.assertEqual(self.normalizer.grid.row_values(0)["state"], "In Pit")
        restore = self.connection.execute(
            "SELECT outcome,replayed_tail_frames FROM normalizer_restore_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(tuple(restore), ("RESTORED", 1))
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM participant_result_cell_observations WHERE source_key = ?",
                (f"{tail_frame.source_key}:0",),
            ).fetchone()[0],
            1,
        )

    def test_corrupt_runtime_checkpoint_falls_back_to_full_processed_raw_replay(self):
        base = TIME_SERVICE_EPOCH_UNIX_US + 2_000_000
        self.apply(
            [
                ["s_i", 2_000_000],
                ["h_i", {"n": "Practice", "s": 2_000_000, "f": 1}],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}]},
                        "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E2000000"]],
                    },
                ],
            ],
            received_at_us=base,
            checkpoint=True,
        )
        self.connection.execute("UPDATE state_checkpoints SET state_hash = 'corrupt'")
        self.connection.commit()

        self.normalizer = TimingNormalizer(self.session.id)
        self.apply([["r_c", [[0, 3, "In Pit"]]]], received_at_us=base + 1_000_000)

        self.assertEqual(self.normalizer.grid.row_values(0)["state"], "In Pit")
        restore = self.connection.execute(
            "SELECT outcome,replayed_tail_frames FROM normalizer_restore_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(tuple(restore), ("FALLBACK", 1))

    def test_retention_floor_rejects_an_older_checkpoint_when_receive_times_regress(self):
        """A retained old anchor cannot bridge RAW deleted later in frame order."""

        base = TIME_SERVICE_EPOCH_UNIX_US + 1_000_000
        early_anchor = self.apply(
            [
                ["s_i", 1_000_000],
                ["h_i", {"n": "Practice", "s": 1_000_000, "f": 1}],
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}]},
                        "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E1000000"]],
                    },
                ],
            ],
            # The provider/reconnect can produce source frames whose receipt
            # clock is not ordered with the durable frame id.
            received_at_us=base + 1_000_000_000,
            checkpoint=True,
        )
        deleted_tail = self.apply([["r_c", [[0, 3, "In Pit"]]]], received_at_us=base)
        later_anchor = self.apply(
            [["h_h", {"f": 2}]],
            received_at_us=base + 1_100_000_000,
            checkpoint=True,
        )
        self.assertGreater(later_anchor.id, deleted_tail.id)
        self.assertLess(early_anchor.id, deleted_tail.id)

        stop_session(
            self.connection,
            session_id=self.session.id,
            idempotency_key="stop-retention-floor-checkpoint",
        )
        plan = plan_retention(
            self.connection,
            now_at_us=base + 500_000_000,
            raw_days=0,
            stream_days=999_999,
        )
        self.assertEqual(plan.feed_frame_ids, (deleted_tail.id,))
        self.assertEqual(apply_retention(self.connection, plan), 1)
        self.connection.execute(
            "UPDATE state_checkpoints SET state_hash = 'corrupt' WHERE source_frame_id = ?",
            (later_anchor.id,),
        )
        self.connection.commit()

        self.normalizer = TimingNormalizer(self.session.id)
        with self.assertRaisesRegex(NormalizerError, "Cannot cold-replay"):
            self.normalizer._prime(self.connection)

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
                [
                    "t_i",
                    {
                        "l": [[100, True, -1], [500, False, 0], [750, False, 1], [0, False, 0]],
                        "d": [
                            [42, "21", 0, 1000, 0, 47000, False, 0],
                            [43, "22", 0, 1000, 0, 47000, False, 0],
                        ],
                    },
                ],
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
        self.assertEqual([tuple(pit) for pit in pits], [("21", 1, None, None), ("22", 1, None, None)])
        stints = self.connection.execute(
            """
            SELECT p.start_number,t.stint_number,t.started_lap,t.ended_lap,t.completed_laps
            FROM tire_stints AS t JOIN participants AS p ON p.id = t.participant_id
            ORDER BY p.start_number,t.stint_number
            """
        ).fetchall()
        self.assertEqual(
            [tuple(stint) for stint in stints],
            [
                ("21", 1, None, None, 0),
                ("21", 2, None, None, 0),
                ("22", 1, None, None, 0),
                ("22", 2, None, None, 0),
            ],
        )
        laps = self.connection.execute(
            """
            SELECT p.start_number,l.is_in_lap,l.is_out_lap,l.is_clean
            FROM laps AS l JOIN participants AS p ON p.id = l.participant_id
            ORDER BY p.start_number
            """
        ).fetchall()
        self.assertEqual([tuple(lap) for lap in laps], [])

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

    def test_anomalous_result_event_timestamps_keep_raw_and_use_observed_pit_boundary(self):
        """A numerically valid low TsTime must never become a 2000-era fact."""

        provider_now = 837_026_446_926_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_now
        headers = [
            {"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"},
            {"n": "LAPS"}, {"n": "PIT"}, {"n": "L-PIT"}, {"n": "STINT"},
        ]
        entries = (
            ("21", "BALCHUG Racing"),
            ("22", "Benchmark Racing"),
            ("77", "Anomaly Racing"),
            ("29", "Control Racing"),
            ("31", "Point Racing"),
        )
        initial_cells = []
        for row_index, (number, team) in enumerate(entries):
            values = (number, f"E{provider_now}", team, "CN PRO", "5", "0", "L0", f"S{provider_now}")
            initial_cells.extend([row_index, column_index, value] for column_index, value in enumerate(values))
        self.apply(
            [
                ["s_i", provider_now],
                ["r_i", {"l": {"h": headers}, "r": initial_cells}],
            ],
            received_at_us=received,
        )

        state_received = received + 1_000_000
        self.apply(
            [
                [
                    "r_c",
                    [
                        [0, 1, "E0"],
                        [1, 1, f"E{OPEN_ENDED_TS_TIME}"],
                        [2, 1, "E120000000"],
                        [3, 1, f"E{provider_now + 2_000_000}"],
                    ],
                ]
            ],
            received_at_us=state_received,
        )
        states = {
            row["start_number"]: row
            for row in self.connection.execute(
                """
                SELECT participant.start_number,current.state_raw,current.state_kind,
                       current.state_timer_target_raw,current.state_timer_target_provider_us,
                       current.state_timer_target_at_us,current.state_timer_calibration_id
                FROM participant_state_current AS current
                JOIN participants AS participant ON participant.id = current.participant_id
                """
            )
        }
        self.assertEqual(
            tuple(states["21"]),
            ("21", "E0", "ON_TRACK", "0", 0, None, None),
        )
        self.assertEqual(
            tuple(states["22"]),
            ("22", f"E{OPEN_ENDED_TS_TIME}", "UNKNOWN", str(OPEN_ENDED_TS_TIME), None, None, None),
        )
        self.assertEqual(
            tuple(states["77"]),
            ("77", "E120000000", "ON_TRACK", "120000000", 120_000_000, None, None),
        )
        self.assertEqual(states["29"]["state_timer_target_at_us"], received + 2_000_000)
        self.assertIsNotNone(states["29"]["state_timer_calibration_id"])
        anomaly_observation = self.connection.execute(
            """
            SELECT observation.state_raw,observation.state_kind,
                   observation.state_timer_target_provider_us,observation.state_timer_target_at_us,
                   observation.state_timer_calibration_id
            FROM participant_state_observations AS observation
            JOIN participants AS participant ON participant.id = observation.participant_id
            WHERE participant.start_number = '77' AND observation.state_raw = 'E120000000'
            """
        ).fetchone()
        self.assertEqual(tuple(anomaly_observation), ("E120000000", "ON_TRACK", 120_000_000, None, None))

        stint_received = received + 2_000_000
        self.apply(
            [
                [
                    "r_c",
                    [
                        [0, 7, "S0"],
                        [1, 7, f"P{OPEN_ENDED_TS_TIME}"],
                        [2, 7, "S120000000"],
                        [3, 7, f"P{provider_now + 3_000_000}"],
                        [4, 7, "P120000000"],
                    ],
                ]
            ],
            received_at_us=stint_received,
        )
        stints = {
            row["start_number"]: row
            for row in self.connection.execute(
                """
                SELECT participant.start_number,current.current_driver_stint_raw,current.driver_stint_kind,
                       current.driver_stint_provider_ts_time,current.driver_stint_at_us,
                       current.driver_stint_calibration_id
                FROM participant_state_current AS current
                JOIN participants AS participant ON participant.id = current.participant_id
                """
            )
        }
        self.assertEqual(tuple(stints["21"]), ("21", "S0", "UNKNOWN", None, None, None))
        self.assertEqual(
            tuple(stints["22"]),
            ("22", f"P{OPEN_ENDED_TS_TIME}", "UNKNOWN", None, None, None),
        )
        self.assertEqual(tuple(stints["77"]), ("77", "S120000000", "START_TS", 120_000_000, None, None))
        self.assertEqual(tuple(stints["31"]), ("31", "P120000000", "POINT_TS", 120_000_000, None, None))
        self.assertEqual(stints["29"]["driver_stint_at_us"], received + 3_000_000)
        self.assertIsNotNone(stints["29"]["driver_stint_calibration_id"])
        self.assertEqual(
            tuple(
                self.connection.execute(
                    """
                    SELECT driver_stint_raw,driver_stint_kind,driver_stint_provider_ts_time,
                           driver_stint_at_us,driver_stint_calibration_id
                    FROM participant_state_observations AS observation
                    JOIN participants AS participant ON participant.id = observation.participant_id
                    WHERE participant.start_number = '77' AND driver_stint_raw = 'S120000000'
                    """
                ).fetchone()
            ),
            ("S120000000", "START_TS", 120_000_000, None, None),
        )

        pit_received = received + 3_000_000
        self.apply(
            [
                [
                    "r_c",
                    [
                        [0, 1, "SIn Pit"], [0, 5, "1"], [0, 6, "S0"],
                        [1, 1, "SIn Pit"], [1, 5, "1"], [1, 6, f"S{OPEN_ENDED_TS_TIME}"],
                        [2, 1, "SIn Pit"], [2, 5, "1"], [2, 6, "S120000000"],
                        [3, 1, "SIn Pit"], [3, 5, "1"], [3, 6, f"S{provider_now + 4_000_000}"],
                    ],
                ]
            ],
            received_at_us=pit_received,
        )
        pits = {
            row["start_number"]: row
            for row in self.connection.execute(
                """
                SELECT participant.start_number,pit.entered_at_us,pit.entered_at_source_cell_observation_id,
                       pit.entered_at_source_message_id,pit.entered_at_source_key,pit.entered_at_source_kind,
                       current.pit_time_raw
                FROM pit_stops AS pit
                JOIN participants AS participant ON participant.id = pit.participant_id
                JOIN participant_state_current AS current
                  ON current.participant_id = participant.id AND current.source_heat_id = pit.source_heat_id
                """
            )
        }
        for number, raw in (("21", "S0"), ("22", f"S{OPEN_ENDED_TS_TIME}"), ("77", "S120000000")):
            self.assertEqual(pits[number]["entered_at_us"], pit_received)
            self.assertIsNone(pits[number]["entered_at_source_cell_observation_id"])
            self.assertIsNone(pits[number]["entered_at_source_message_id"])
            self.assertIsNone(pits[number]["entered_at_source_key"])
            self.assertIsNone(pits[number]["entered_at_source_kind"])
            self.assertEqual(pits[number]["pit_time_raw"], raw)
        self.assertEqual(pits["29"]["entered_at_us"], received + 4_000_000)
        self.assertIsNotNone(pits["29"]["entered_at_source_cell_observation_id"])
        self.assertIsNotNone(pits["29"]["entered_at_source_message_id"])
        self.assertEqual(pits["29"]["entered_at_source_kind"], "RESULT_L_PIT_S")

    def test_current_provider_layout_persists_a_current_contract_and_alias_drift(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 22_600_000
        headers = [
            {"n": "position"}, {"n": "marker"}, {"n": "startnumber"}, {"n": "State"},
            {"n": "Team name"}, {"n": "CurrentDriver"}, {"n": "class"},
            {"n": "position_in_class"}, {"n": "hole"}, {"n": "fastestRoundTime"},
            {"n": "lastRoundTime"}, {"n": "CurrentDriverStintTime"}, {"n": "PitTime"},
            {"n": "pitstops"}, {"n": "SectorTimes", "p": "1"},
            {"n": "SectorTimes", "p": "2"}, {"n": "SectorTimes", "p": "3"},
            {"n": "sectionMarker"},
        ]
        values = [
            "1", "", "21", "E22600000", "BALCHUG Racing", "Киракозов Кирилл", "CN PRO", "1",
            "0.000", "107491000", "108000000", "S22600000", "L0", "0",
            "35000000", "36000000", "36500000", "",
        ]
        self.apply(
            [["r_i", {"l": {"h": headers}, "r": [[0, index, value] for index, value in enumerate(values)]}]],
            received_at_us=received,
        )
        current = self.connection.execute(
            """
            SELECT status,missing_required_keys_json,optional_present_keys_json
            FROM result_schema_contract_observations
            """
        ).fetchone()
        self.assertEqual(current["status"], "CURRENT")
        self.assertEqual(json.loads(current["missing_required_keys_json"]), [])
        self.assertEqual(json.loads(current["optional_present_keys_json"]), ["section_marker"])

        # A pre-contract database can already hold this raw layout with stale
        # NULL sector keys. Replaying any later result update repairs only the
        # semantic metadata; its raw layout/cells are unchanged.
        self.connection.execute(
            """
            UPDATE result_column_definitions SET canonical_key = NULL
            WHERE column_index IN (14,15,16)
            """
        )
        self.connection.commit()
        self.apply([['r_c', [[0, 10, '107400000']]]], received_at_us=received + 500_000)
        repaired_sectors = self.connection.execute(
            """
            SELECT canonical_key FROM result_column_definitions
            WHERE column_index IN (14,15,16) ORDER BY column_index
            """
        ).fetchall()
        self.assertEqual([row[0] for row in repaired_sectors], ["sector_1", "sector_2", "sector_3"])

        alias_headers = list(headers)
        alias_headers[0] = {"n": "POS"}
        self.apply(
            [["r_l", {"h": alias_headers}], ["r_i", {"r": [[0, 2, "21"], [0, 4, "BALCHUG Racing"], [0, 6, "CN PRO"]]}]],
            received_at_us=received + 1_000_000,
        )
        drift = self.connection.execute(
            """
            SELECT status,missing_required_keys_json,binding_mismatches_json
            FROM result_schema_contract_observations
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
        self.assertEqual(drift["status"], "DEGRADED")
        self.assertIn("position_overall", json.loads(drift["missing_required_keys_json"]))
        self.assertEqual(json.loads(drift["binding_mismatches_json"])[0]["observed"][0]["source_name"], "POS")
        raw_alias = self.connection.execute(
            """
            SELECT source_name_raw,canonical_key FROM result_column_definitions
            WHERE layout_version_id = (
              SELECT layout_version_id FROM result_schema_contract_observations ORDER BY id DESC LIMIT 1
            ) AND column_index = 0
            """
        ).fetchone()
        self.assertEqual(tuple(raw_alias), ("POS", "position_overall"))

    def test_sparse_non_state_rows_preserve_state_provenance_and_laps(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 22_700_000
        headers = [
            {"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "LAPS"},
            {"n": "LAST"}, {"n": "PIT"}, {"n": "L-PIT"}, {"n": "STINT"},
        ]
        self.apply(
            [
                ["s_i", 22_700_000],
                [
                    "r_i",
                    {
                        "l": {"h": headers},
                        "r": [
                            [0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"],
                            [0, 3, "E22700000"], [0, 4, "5"], [0, 5, "108000000"],
                            [0, 6, "0"], [0, 7, "L0"], [0, 8, "S22700000"],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        before = self.connection.execute(
            """
            SELECT state_source_cell_observation_id,state_source_message_id,state_source_key
            FROM participant_state_current
            """
        ).fetchone()
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM participant_state_observations").fetchone()[0], 1)

        # LAST and LAPS are real source cells, but neither is a source STATE,
        # PIT, L-PIT or STINT event. They must not manufacture state history.
        self.apply([['r_c', [[0, 5, '107500000']]]], received_at_us=received + 1_000_000)
        self.apply([['r_c', [[0, 4, '6']]]], received_at_us=received + 2_000_000)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM participant_state_observations").fetchone()[0], 1)
        lap = self.connection.execute("SELECT lap_number,duration_ms FROM laps ORDER BY lap_number").fetchone()
        self.assertEqual(tuple(lap), (6, None))
        after_sparse = self.connection.execute(
            """
            SELECT source_message_id,state_source_cell_observation_id,state_source_message_id,state_source_key
            FROM participant_state_current
            """
        ).fetchone()
        self.assertNotEqual(after_sparse["source_message_id"], before["state_source_message_id"])
        self.assertEqual(after_sparse["state_source_cell_observation_id"], before["state_source_cell_observation_id"])
        self.assertEqual(after_sparse["state_source_message_id"], before["state_source_message_id"])
        self.assertEqual(after_sparse["state_source_key"], before["state_source_key"])

        # PIT-only and STINT-only deltas are preserved as their own source
        # events, without falsely claiming a new STATE cell.
        self.apply([['r_c', [[0, 6, '1']]]], received_at_us=received + 3_000_000)
        self.apply([['r_c', [[0, 8, 'P22704000']]]], received_at_us=received + 4_000_000)
        observations = self.connection.execute(
            """
            SELECT state_cell_observation_id,provider_pit_count_cell_observation_id,
                   driver_stint_cell_observation_id,state_kind
            FROM participant_state_observations ORDER BY id
            """
        ).fetchall()
        self.assertEqual(len(observations), 3)
        self.assertIsNone(observations[1]["state_cell_observation_id"])
        self.assertIsNotNone(observations[1]["provider_pit_count_cell_observation_id"])
        self.assertIsNone(observations[2]["state_cell_observation_id"])
        self.assertIsNotNone(observations[2]["driver_stint_cell_observation_id"])
        self.assertTrue(all(row["state_kind"] == "ON_TRACK" for row in observations))

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
                [
                    "t_i",
                    {
                        "l": [[50, True, -1], [500, False, 0], [750, False, 1], [0, False, 0]],
                        "d": [[42, "21", 0, 1000, 0, 47000, False, 0]],
                    },
                ],
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
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM laps").fetchone()[0], 1)
        stints = self.connection.execute(
            "SELECT stint_number,started_lap,ended_lap,completed_laps FROM tire_stints ORDER BY stint_number"
        ).fetchall()
        self.assertEqual([tuple(stint) for stint in stints], [(1, None, None, 0), (2, None, None, 1)])

    def test_no_laps_tracker_boundary_uses_same_frame_last_and_sector_cells(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 55_000_000
        self.apply(
            [
                ["s_i", 55_000_000],
                ["t_i", {"l": [[100, True, -1], [500, False, 0], [750, False, 1], [0, False, 0]], "d": []}],
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
                ["a_i", {"g": "55000000", "f": "0"}],
                ["t_p", [[42, "21", 0, 1000, 0, 47000, False, 55_000_000]]],
            ],
            received_at_us=received,
        )
        # The production feed emits S1/S2 in earlier result messages, then
        # sends LAST followed by S3 in the same r_c as the finish passing.
        self.apply(
            [["r_c", [[0, 5, "34500000"], [0, 6, "35500000"]]]],
            received_at_us=received + 50_000_000,
        )
        self.apply(
            [
                ["r_c", [[0, 4, "107491000"], [0, 7, "34552000"]]],
                ["t_p", [[42, "21", 0, 500, 0, 47000, False, 162_491_000]]],
            ],
            received_at_us=received + 107_491_000,
        )
        # A following tracker passing without a new LAST cell must not become
        # a synthetic duration from tracker chronology.
        self.apply(
            [["t_p", [[42, "21", 0, 500, 0, 47000, False, 272_491_000]]]],
            received_at_us=received + 217_491_000,
        )

        laps = self.connection.execute(
            """
            SELECT id,lap_number,completed_at_us,duration_ms,sectors_json,
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
        ledger = self.connection.execute(
            """
            SELECT classification,classification_reason,linked_lap_id,sectors_json,
                   sectors_source_cell_observation_ids_json
            FROM result_last_cell_ledger
            WHERE source_cell_observation_id = ?
            """,
            (first["duration_source_cell_observation_id"],),
        ).fetchone()
        self.assertEqual(
            tuple(ledger[:3]),
            ("CONFIRMED_LAP", "CANONICAL_TRACKER_DURATION_MATCH", first["id"]),
        )
        self.assertEqual(json.loads(ledger["sectors_json"]), json.loads(first["sectors_json"]))
        self.assertEqual(
            json.loads(ledger["sectors_source_cell_observation_ids_json"]),
            json.loads(first["sectors_source_cell_observation_ids_json"]),
        )
        self.assertIsNone(second["duration_ms"])
        self.assertIsNone(second["duration_source_cell_observation_id"])

    def test_last_ledger_distinguishes_late_sparse_cells_refreshes_and_invalid_values(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 58_000_000
        headers = [
            {"n": "NR"},
            {"n": "TEAM"},
            {"n": "CLS"},
            {"n": "STATE"},
            {"n": "LAST"},
        ]
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": headers},
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "BALCHUG Racing"],
                            [0, 2, "CN PRO"],
                            [0, 3, "E58000000"],
                            [0, 4, "108000000"],
                            [2, 0, "77"],
                            [2, 1, "MAZDA HIGH POWER 77"],
                            [2, 2, "GT L"],
                            [2, 3, "SIn Pit"],
                            [2, 4, "140000000"],
                        ],
                    },
                ]
            ],
            received_at_us=received,
        )
        baseline = self.connection.execute(
            "SELECT id FROM result_schema_baselines ORDER BY id"
        ).fetchone()
        self.assertIsNotNone(baseline)

        # #9 appears after the connection-wide schema snapshot. It has no
        # personal r_i LAST baseline, but its direct r_c cell is valid timing
        # evidence and must not be discarded.
        self.apply(
            [
                [
                    "r_c",
                    [
                        [1, 0, "9"],
                        [1, 1, "Про Моторспорт"],
                        [1, 2, "CN PRO"],
                        [1, 3, "E58001000"],
                        [1, 4, "110000000"],
                    ],
                ]
            ],
            received_at_us=received + 1_000,
        )
        self.apply([["r_c", [[0, 4, "107500000"]]]], received_at_us=received + 2_000)
        self.apply([["r_c", [[0, 4, "107500000"]]]], received_at_us=received + 3_000)
        self.apply(
            [["r_c", [[0, 4, str(OPEN_ENDED_TS_TIME)]]]],
            received_at_us=received + 4_000,
        )
        self.apply([["r_c", [[0, 4, "107400000"]]]], received_at_us=received + 5_000)

        # Three rows are retained in the sparse grid, but this r_c contains a
        # complete two-row block (ten cells). Refresh density is measured from
        # its transmitted rows, so the unchanged #21 LAST is a repaint and the
        # changed #9 value is deliberately not promoted into a timing event.
        self.apply(
            [
                [
                    "r_c",
                    [
                        [0, 0, "21"],
                        [0, 1, "BALCHUG Racing"],
                        [0, 2, "CN PRO"],
                        [0, 3, "E58006000"],
                        [0, 4, "107400000"],
                        [1, 0, "9"],
                        [1, 1, "Про Моторспорт"],
                        [1, 2, "CN PRO"],
                        [1, 3, "E58006000"],
                        [1, 4, "109000000"],
                    ],
                ]
            ],
            received_at_us=received + 6_000,
        )
        rows = self.connection.execute(
            """
            SELECT participant.start_number,ledger.duration_ms,ledger.classification,
                   ledger.classification_reason,ledger.schema_baseline_id,
                   ledger.predecessor_source_cell_observation_id
            FROM result_last_cell_ledger AS ledger
            LEFT JOIN participants AS participant ON participant.id = ledger.participant_id
            WHERE ledger.source_handle = 'r_c'
            ORDER BY ledger.source_frame_id,ledger.source_message_ordinal,
                     ledger.source_change_ordinal,ledger.source_cell_observation_id
            """
        ).fetchall()
        self.assertEqual(
            [
                (row["start_number"], row["duration_ms"], row["classification"], row["classification_reason"])
                for row in rows
            ],
            [
                ("9", 110_000, "CONFIRMED_LAP", "DIRECT_FIRST_OBSERVATION"),
                ("21", 107_500, "CONFIRMED_LAP", "DIRECT_VALUE_CHANGED"),
                ("21", 107_500, "UNCONFIRMED", "AMBIGUOUS_EQUAL_DURATION"),
                ("21", None, "INVALID", "INVALID_DURATION"),
                ("21", 107_400, "CONFIRMED_LAP", "DIRECT_VALUE_CHANGED"),
                ("21", 107_400, "REFRESH_REPEAT", "FULL_GRID_REFRESH_REPEAT"),
                ("9", 109_000, "UNCONFIRMED", "FULL_GRID_NON_REPEAT"),
            ],
        )
        self.assertTrue(all(row["schema_baseline_id"] == baseline["id"] for row in rows))
        self.assertTrue(all(row["predecessor_source_cell_observation_id"] is not None for row in rows[1:]))

    def test_last_ledger_requires_a_new_r_i_after_reconnect(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 59_000_000
        payload = {
            "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "LAST"}]},
            "r": [
                [0, 0, "21"],
                [0, 1, "BALCHUG Racing"],
                [0, 2, "CN PRO"],
                [0, 3, "E59000000"],
                [0, 4, "108000000"],
            ],
        }
        self.apply([["r_i", payload]], received_at_us=received)
        reconnect = self.store.open_connection(Bootstrap("https://example.test/igora", "tid-reconnect", None))
        self.apply(
            [["r_c", [[0, 4, "107800000"]]]],
            received_at_us=received + 1_000,
            upstream=reconnect,
        )
        self.apply([["r_i", payload]], received_at_us=received + 2_000, upstream=reconnect)
        self.apply(
            [["r_c", [[0, 4, "107700000"]]]],
            received_at_us=received + 3_000,
            upstream=reconnect,
        )
        rows = self.connection.execute(
            """
            SELECT duration_ms,classification,classification_reason,schema_baseline_id
            FROM result_last_cell_ledger
            WHERE source_handle = 'r_c'
            ORDER BY source_frame_id,source_message_ordinal,source_change_ordinal
            """
        ).fetchall()
        self.assertEqual(
            [(row["duration_ms"], row["classification"], row["classification_reason"]) for row in rows],
            [
                (107_800, "UNCONFIRMED", "SCHEMA_BASELINE_MISSING"),
                (107_700, "CONFIRMED_LAP", "DIRECT_VALUE_CHANGED"),
            ],
        )
        self.assertIsNone(rows[0]["schema_baseline_id"])
        self.assertIsNotNone(rows[1]["schema_baseline_id"])

    def test_initial_r_i_before_h_i_persists_connection_schema_baseline(self):
        """The production compressed bootstrap orders r_i before h_i/s_i."""

        received = TIME_SERVICE_EPOCH_UNIX_US + 59_500_000
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "LAST"}]},
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "BALCHUG Racing"],
                            [0, 2, "CN PRO"],
                            [0, 3, "E59500000"],
                            [0, 4, "108000000"],
                        ],
                    },
                ],
                ["h_i", {"n": "Practice - Open-Pit", "s": 59_500_000, "f": 6}],
                ["s_i", 59_500_000],
            ],
            received_at_us=received,
        )
        baseline = self.connection.execute(
            "SELECT source_message_ordinal,layout_generation FROM result_schema_baselines"
        ).fetchone()
        self.assertEqual(tuple(baseline), (0, 1))
        self.apply([["r_c", [[0, 4, "107600000"]]]], received_at_us=received + 1_000)
        fact = self.connection.execute(
            """
            SELECT classification,classification_reason,schema_baseline_id
            FROM result_last_cell_ledger
            WHERE source_handle = 'r_c'
            """
        ).fetchone()
        self.assertEqual(tuple(fact[:2]), ("CONFIRMED_LAP", "DIRECT_VALUE_CHANGED"))
        self.assertIsNotNone(fact["schema_baseline_id"])

    def test_no_laps_tracker_boundary_accepts_a_delayed_exact_last(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 56_000_000
        self.apply(
            [
                ["s_i", 56_000_000],
                [
                    "t_i",
                    {
                        "l": [[100, True, -1], [500, False, 0], [750, False, 1], [0, False, 0]],
                        "d": [[42, "21", 0, 1000, 0, 47000, False, 0]],
                    },
                ],
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
        self.apply(
            [["t_p", [[42, "21", 0, 500, 0, 47000, False, 56_000_000]]]],
            received_at_us=received,
        )
        self.apply(
            [["t_p", [[42, "21", 0, 500, 0, 47000, False, 163_491_000]]]],
            received_at_us=received + 107_491_000,
        )
        self.apply(
            [["r_c", [[0, 4, "107491000"]]]],
            received_at_us=received + 108_291_000,
        )
        lap = self.connection.execute(
            """
            SELECT duration_reconciliation,tracker_duration_ms,source_duration_ms,
                   source_last_cell_observation_id,coverage_complete,lap_number
            FROM canonical_laps
            """
        ).fetchone()
        self.assertEqual(tuple(lap), ("EXACT", 107491, 107491, lap["source_last_cell_observation_id"], 0, None))
        self.assertIsNotNone(lap["source_last_cell_observation_id"])
        ledger = self.connection.execute(
            "SELECT classification_reason,linked_canonical_lap_id FROM result_last_cell_ledger WHERE source_handle='r_c'"
        ).fetchone()
        self.assertEqual(ledger["classification_reason"], "CANONICAL_TRACKER_DURATION_MATCH")
        self.assertIsNotNone(ledger["linked_canonical_lap_id"])

    def test_live_layout_adds_laps_and_continues_sparse_updates_without_new_snapshot(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 65_000_000
        initial_headers = [
            {"n": "NR"},
            {"n": "TEAM"},
            {"n": "CLS"},
            {"n": "STATE"},
            {"n": "GAP"},
            {"n": "LAST"},
        ]
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {"h": initial_headers},
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "BALCHUG Racing"],
                            [0, 2, "CN PRO"],
                            [0, 3, "E65000000"],
                            [0, 4, "-- 11 laps --"],
                            [0, 5, "120000000"],
                        ],
                    },
                ]
            ],
            received_at_us=received,
        )

        # Production sent this r_l during the race and immediately resumed r_c;
        # it did not send a replacement r_i snapshot.
        next_headers = [
            {"n": "NR"},
            {"n": "TEAM"},
            {"n": "CLS"},
            {"n": "STATE"},
            {"n": "LAPS"},
            {"n": "GAP"},
            {"n": "LAST"},
        ]
        self.apply([["r_l", {"h": next_headers}]], received_at_us=received + 1_000_000)
        self.apply(
            [["r_c", [[0, 4, "12"], [0, 5, "-- 12 laps --"], [0, 6, "110000000"]]]],
            received_at_us=received + 2_000_000,
        )

        state = self.connection.execute(
            """
            SELECT state.laps,state.gap_raw,state.last_lap_ms,participant.start_number,
                   participant.team_name,participant.class_name
            FROM participant_state_current AS state
            JOIN participants AS participant ON participant.id = state.participant_id
            WHERE participant.is_ours = 1
            """
        ).fetchone()
        self.assertEqual(tuple(state), (12, "-- 12 laps --", 110000, "21", "BALCHUG Racing", "CN PRO"))
        anchors = self.connection.execute(
            """
            SELECT baseline.id,message.handle,baseline.layout_generation
            FROM result_schema_baselines AS baseline
            JOIN feed_messages AS message ON message.id = baseline.source_message_id
            ORDER BY baseline.id
            """
        ).fetchall()
        self.assertEqual(
            [(anchor["handle"], anchor["layout_generation"]) for anchor in anchors],
            [("r_i", 1), ("r_l", 2)],
        )
        cells = self.connection.execute(
            """
            SELECT definition.canonical_key,observation.participant_id
            FROM participant_result_cell_observations AS observation
            JOIN result_column_definitions AS definition
              ON definition.layout_version_id = observation.layout_version_id
             AND definition.column_index = observation.column_index
            JOIN feed_messages AS message ON message.id = observation.source_message_id
            WHERE message.handle = 'r_c'
            ORDER BY observation.source_change_ordinal
            """
        ).fetchall()
        self.assertEqual([cell["canonical_key"] for cell in cells], ["laps", "gap", "last_lap"])
        self.assertTrue(all(cell["participant_id"] is not None for cell in cells))
        ledger = self.connection.execute(
            """
            SELECT classification,classification_reason,schema_baseline_id
            FROM result_last_cell_ledger WHERE source_handle = 'r_c'
            """
        ).fetchone()
        self.assertEqual(tuple(ledger[:2]), ("CONFIRMED_LAP", "DIRECT_VALUE_CHANGED"))
        self.assertEqual(ledger["schema_baseline_id"], anchors[-1]["id"])

    def test_layout_removal_clears_removed_laps_and_gap_without_moving_last(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 68_000_000
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
                                {"n": "completedLapCounterV2", "c": "LAPS"},
                                {"n": "GAP"},
                                {"n": "LAST"},
                            ]
                        },
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "BALCHUG Racing"],
                            [0, 2, "CN PRO"],
                            [0, 3, "E68000000"],
                            [0, 4, "12"],
                            [0, 5, "2.567"],
                            [0, 6, "110000000"],
                        ],
                    },
                ]
            ],
            received_at_us=received,
        )

        before = self.connection.execute(
            """
            SELECT state.laps,state.gap_ms,state.gap_interval_fact_id,state.last_lap_ms
            FROM participant_state_current AS state
            JOIN participants AS participant ON participant.id = state.participant_id
            WHERE participant.is_ours = 1
            """
        ).fetchone()
        self.assertEqual(tuple(before), (12, 2_567, before["gap_interval_fact_id"], 110_000))
        self.assertIsNotNone(before["gap_interval_fact_id"])

        # Both fields disappear and LAST moves to index 4. The next sparse
        # update must not reinterpret index 4 as the removed LAPS value or
        # continue exposing a GAP source fact from the previous layout.
        self.apply(
            [["r_l", {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}, {"n": "LAST"}]}]],
            received_at_us=received + 1_000_000,
        )
        self.apply(
            [["r_c", [[0, 4, "109000000"]]]],
            received_at_us=received + 2_000_000,
        )

        after = self.connection.execute(
            """
            SELECT state.laps,state.gap_ms,state.gap_interval_fact_id,state.last_lap_ms
            FROM participant_state_current AS state
            JOIN participants AS participant ON participant.id = state.participant_id
            WHERE participant.is_ours = 1
            """
        ).fetchone()
        self.assertEqual(tuple(after), (None, None, None, 109_000))
        definition = self.connection.execute(
            """
            SELECT definition.canonical_key
            FROM participant_result_cell_observations AS observation
            JOIN result_column_definitions AS definition
              ON definition.layout_version_id = observation.layout_version_id
             AND definition.column_index = observation.column_index
            JOIN feed_messages AS message ON message.id = observation.source_message_id
            WHERE message.handle = 'r_c'
            """
        ).fetchone()
        self.assertEqual(definition["canonical_key"], "last_lap")

    def test_canonical_tracker_chronology_preserves_equal_laps_sectors_and_pit_boundary(self):
        green = 70_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + green
        headers = [
            {"n": "NR"},
            {"n": "TEAM"},
            {"n": "CLS"},
            {"n": "STATE"},
            {"n": "PIT"},
            {"n": "LAST"},
            {"n": "SectorTimes", "p": "1"},
            {"n": "SectorTimes", "p": "2"},
            {"n": "SectorTimes", "p": "3"},
        ]
        self.apply(
            [
                ["s_i", green],
                ["t_i", {"l": [[100, True, -1], [500, False, 0], [750, False, 1], [0, False, 0]], "d": []}],
                [
                    "r_i",
                    {
                        "l": {"h": headers},
                        "r": [
                            [0, 0, "21"],
                            [0, 1, "BALCHUG Racing"],
                            [0, 2, "CN PRO"],
                            [0, 3, f"E{green}"],
                            [0, 4, "0"],
                            [0, 5, str(OPEN_ENDED_TS_TIME)],
                            [0, 6, str(OPEN_ENDED_TS_TIME)],
                            [0, 7, str(OPEN_ENDED_TS_TIME)],
                            [0, 8, str(OPEN_ENDED_TS_TIME)],
                        ],
                    },
                ],
                ["a_i", {"g": str(green), "f": "0"}],
                ["h_h", {"f": 6}],
                ["t_p", [[42, "21", 0, 1000, 0, 47000, False, green]]],
            ],
            received_at_us=received,
        )

        def sector(column, raw, start_distance, stop_distance, sector_id, provider_time):
            self.apply(
                [
                    ["r_c", [[0, column, raw]]],
                    [
                        "t_p",
                        [[42, "21", start_distance, stop_distance, sector_id, 47000, False, provider_time]],
                    ],
                ],
                received_at_us=received + provider_time - green,
            )

        sector(6, "40000000", 500, 750, 1, 110_000_000)
        sector(7, "34000000", 750, 1000, 2, 144_000_000)
        self.apply(
            [["t_p", [[42, "21", 0, 500, 0, 47000, False, 180_000_000]]]],
            received_at_us=received + 110_000_000,
        )
        self.apply(
            [["r_c", [[0, 5, "110000000"], [0, 8, "36000000"]]]],
            received_at_us=received + 110_800_000,
        )

        sector(6, "41000000", 500, 750, 1, 221_000_000)
        sector(7, "34000000", 750, 1000, 2, 255_000_000)
        self.apply(
            [["t_p", [[42, "21", 0, 500, 0, 47000, False, 290_000_000]]]],
            received_at_us=received + 220_000_000,
        )
        # Equal consecutive LAST values are still two distinct exact laps.
        self.apply(
            [["r_c", [[0, 5, "110000000"], [0, 8, "35000000"]]]],
            received_at_us=received + 220_800_000,
        )

        sector(6, "40000000", 500, 750, 1, 330_000_000)
        sector(7, "35000000", 750, 1000, 2, 365_000_000)
        self.apply(
            [
                ["t_p", [[42, "21", 0, 100, -1, 47000, True, 410_000_000]]],
                [
                    "r_c",
                    [[0, 3, "SIn Pit"], [0, 4, "1"], [0, 5, "120000000"], [0, 8, "45000000"]],
                ],
            ],
            received_at_us=received + 340_000_000,
        )
        # Pit exit is a path passing, never a completed lap.
        self.apply(
            [["t_p", [[42, "21", 100, 500, 0, 47000, True, 415_000_000]]]],
            received_at_us=received + 345_000_000,
        )
        # A reconnect replay of the same physical pit boundary is deduplicated.
        self.apply(
            [["t_p", [[42, "21", 0, 100, -1, 47000, True, 410_000_000]]]],
            received_at_us=received + 346_000_000,
        )

        laps = self.connection.execute(
            """
            SELECT lap_number,started_at_provider_us,finished_at_provider_us,
                   tracker_duration_ms,source_duration_ms,duration_reconciliation,is_pit_lap
            FROM canonical_laps ORDER BY lap_number
            """
        ).fetchall()
        self.assertEqual(
            [tuple(lap) for lap in laps],
            [
                (1, 70_000_000, 180_000_000, 110000, 110000, "EXACT", 0),
                (2, 180_000_000, 290_000_000, 110000, 110000, "EXACT", 0),
                (3, 290_000_000, 410_000_000, 120000, 120000, "EXACT", 1),
            ],
        )
        boundaries = self.connection.execute(
            "SELECT boundary_kind FROM canonical_lap_boundaries ORDER BY boundary_ordinal"
        ).fetchall()
        self.assertEqual(
            [boundary["boundary_kind"] for boundary in boundaries],
            ["HEAT_START", "MAIN_FINISH", "MAIN_FINISH", "PIT_FINISH"],
        )
        sectors = self.connection.execute(
            """
            SELECT lap.lap_number,sector.sector_number,sector.tracker_duration_ms,
                   sector.source_duration_ms,sector.duration_reconciliation,
                   sector.source_cell_observation_id
            FROM canonical_lap_sectors AS sector
            JOIN canonical_laps AS lap ON lap.id = sector.canonical_lap_id
            ORDER BY lap.lap_number,sector.sector_number
            """
        ).fetchall()
        self.assertEqual(len(sectors), 9)
        self.assertTrue(all(row["duration_reconciliation"] == "EXACT" for row in sectors))
        self.assertTrue(all(row["tracker_duration_ms"] == row["source_duration_ms"] for row in sectors))
        self.assertTrue(all(row["source_cell_observation_id"] is not None for row in sectors))
        equal_last = self.connection.execute(
            """
            SELECT classification,classification_reason,linked_canonical_lap_id
            FROM result_last_cell_ledger
            WHERE duration_ms = 110000 AND source_handle = 'r_c'
            ORDER BY source_frame_id
            """
        ).fetchall()
        self.assertEqual(len(equal_last), 2)
        self.assertTrue(all(row["classification"] == "CONFIRMED_LAP" for row in equal_last))
        self.assertTrue(
            all(row["classification_reason"] == "CANONICAL_TRACKER_DURATION_MATCH" for row in equal_last)
        )
        self.assertEqual(len({row["linked_canonical_lap_id"] for row in equal_last}), 2)
        participant_id = self.connection.execute(
            "SELECT id FROM participants WHERE source_heat_id = ? AND start_number_key = '21'",
            (self.normalizer.heat_id,),
        ).fetchone()[0]
        payload = TimingReadModel(self.path).laps(
            self.session.id,
            participant_id=participant_id,
            limit=10,
        )
        self.assertEqual(payload["lap_counts"][0]["completed_laps"], 3)
        self.assertTrue(payload["lap_counts"][0]["coverage_complete"])
        self.assertEqual(payload["lap_counts"][0]["exact_last_laps"], 3)
        self.assertEqual(len(payload["items"]), 3)
        first = payload["items"][0]
        self.assertEqual(
            (first["started_at_provider_us"], first["finished_at_provider_us"], first["duration_ms"]),
            (70_000_000, 180_000_000, 110_000),
        )
        self.assertEqual(len(first["sector_facts"]), 3)
        self.assertEqual(len(first["tracker_passings"]), 3)
        self.assertEqual(first["source"]["last_raw_value"], ["110000000"])
        self.assertEqual(first["finish_boundary"]["kind"], "MAIN_FINISH")
        metric_input = load_heat_metric_input(self.connection, self.normalizer.heat_id)
        self.assertEqual(
            [lap.is_clean for lap in metric_input.our_participant.laps if lap.timing_eligible],
            [True, True, False],
        )
        snapshot = TimingReadModel(self.path).snapshot(self.session.id).as_dict()
        ours = next(item for item in snapshot["measured"]["participants"] if item["participant_id"] == participant_id)
        self.assertEqual(
            ours["lap_count"],
            {
                "completed_laps": 3,
                "observed_complete_laps": 3,
                "coverage_complete": True,
                "exact_last_laps": 3,
                "latest_finished_at_provider_us": 410_000_000,
            },
        )

    def test_gap_lap_groups_are_snapshotted_across_classes_without_summing_rows(self):
        self.assertEqual(parse_gap_display("2.567").time_ms, 2_567)
        self.assertEqual(parse_gap_display("1:02.973").time_ms, 62_973)
        self.assertEqual(parse_gap_display("6:20.111").time_ms, 380_111)
        self.assertEqual(parse_gap_display("-- 28 laps --").completed_laps, 28)
        self.assertEqual(parse_gap_display("not timing").kind, "UNKNOWN")

        received = TIME_SERVICE_EPOCH_UNIX_US + 80_000_000
        headers = [{"n": "POS"}, {"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "GAP"}]
        rows = []
        values = [
            (1, "9", "Про Моторспорт", "CN PRO", "-- 28 laps --"),
            (2, "29", "TEAMGARIS", "CN PRO", "2.567"),
            (3, "13", "CapitalRT", "GT PRO", "8.216"),
            (4, "93", "ИСКРА Моторспорт", "GT PRO", "-- 27 laps --"),
            (5, "37", "ИСКРА Моторспорт", "GT PRO", "41.740"),
            (6, "90", "Vracing", "GT PRO", "-- 14 laps --"),
            (7, "21", "BALCHUG Racing", "CN PRO", "6:20.111"),
        ]
        for row_index, value in enumerate(values):
            for column_index, cell in enumerate(value):
                rows.append([row_index, column_index, str(cell)])
        self.apply(
            [["r_i", {"l": {"h": headers}, "r": rows}]],
            received_at_us=received,
        )

        snapshot = self.connection.execute(
            "SELECT id,leader_completed_laps,participant_count,resolved_coordinate_count,lap_group_count FROM gap_coordinate_snapshots"
        ).fetchone()
        self.assertEqual(tuple(snapshot[1:]), (28, 7, 7, 3))
        coordinates = self.connection.execute(
            """
            SELECT participant.start_number,coordinate.lap_group_completed_laps,
                   coordinate.time_from_lap_group_leader_ms,
                   coordinate.gap_to_overall_leader_laps,
                   coordinate.gap_to_overall_leader_residual_ms,coordinate.coordinate_status
            FROM participant_gap_coordinates AS coordinate
            JOIN participants AS participant ON participant.id = coordinate.participant_id
            WHERE coordinate.snapshot_id = ? ORDER BY coordinate.source_position_overall
            """,
            (snapshot["id"],),
        ).fetchall()
        by_number = {row["start_number"]: row for row in coordinates}
        self.assertEqual(tuple(by_number["9"][1:]), (28, 0, 0, 0, "EXACT"))
        self.assertEqual(tuple(by_number["29"][1:]), (28, 2_567, 0, 2_567, "EXACT"))
        # 8.216 is cumulative from the 28-lap group leader, not 2.567 + 8.216.
        self.assertEqual(by_number["13"]["gap_to_overall_leader_residual_ms"], 8_216)
        self.assertEqual(tuple(by_number["21"][1:]), (14, 380_111, 14, 380_111, "EXACT"))
        state = self.connection.execute(
            """
            SELECT current.gap_ms,current.gap_kind
            FROM participant_state_current AS current
            JOIN participants AS participant ON participant.id = current.participant_id
            WHERE participant.start_number = '21'
            """
        ).fetchone()
        self.assertEqual(tuple(state), (380_111, "TIME"))

        # The reducer stores at most one full-table projection per second.
        self.apply([["r_c", [[6, 4, "6:19.500"]]]], received_at_us=received + 500_000)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM gap_coordinate_snapshots").fetchone()[0], 1)
        self.apply([["r_c", [[6, 4, "6:19.000"]]]], received_at_us=received + 1_000_000)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM gap_coordinate_snapshots").fetchone()[0], 2)
        latest = self.connection.execute(
            """
            SELECT coordinate.gap_to_overall_leader_laps,coordinate.gap_to_overall_leader_residual_ms
            FROM participant_gap_coordinates AS coordinate
            JOIN participants AS participant ON participant.id = coordinate.participant_id
            JOIN gap_coordinate_snapshots AS snapshot ON snapshot.id = coordinate.snapshot_id
            WHERE participant.start_number = '21'
            ORDER BY snapshot.observed_second DESC LIMIT 1
            """
        ).fetchone()
        self.assertEqual(tuple(latest), (14, 379_000))
        public = TimingReadModel(self.path).snapshot(self.session.id).as_dict()
        car = next(item for item in public["measured"]["participants"] if item["start_number"] == "21")
        self.assertEqual(car["gap_coordinate"]["gap_to_overall_leader_laps"], 14)
        self.assertEqual(car["gap_coordinate"]["gap_to_overall_leader_residual_ms"], 379_000)

    def test_same_frame_last_marks_pit_entry_lap_non_clean_when_tracker_arrives_first(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 57_000_000
        self.apply(
            [
                ["s_i", 57_000_000],
                [
                    "t_i",
                    {
                        "l": [[100, True, -1], [500, False, 0], [750, False, 1], [0, False, 0]],
                        "d": [[42, "21", 0, 1000, 0, 47000, False, 0]],
                    },
                ],
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
            [["t_p", [[42, "21", 0, 500, 0, 47000, False, 57_000_000]]]],
            received_at_us=received,
        )
        self.apply(
            [
                ["t_p", [[42, "21", 0, 100, -1, 47000, True, 312_104_000]]],
                ["r_c", [[0, 3, "SIn Pit"], [0, 4, "1"], [0, 5, "255104000"]]],
            ],
            received_at_us=received + 255_104_000,
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
        fact = self.connection.execute(
            """
            SELECT id,interval_kind,raw_value,interval_ms,value_kind,source_cell_observation_id
            FROM participant_interval_source_facts
            """
        ).fetchone()
        self.assertEqual(tuple(fact)[1:5], ("GAP", "1 lap", None, None))
        self.assertIsNotNone(fact["source_cell_observation_id"])
        self.assertEqual(
            self.connection.execute("SELECT gap_interval_fact_id FROM participant_state_current").fetchone()[0],
            fact["id"],
        )

    def test_interval_facts_follow_exact_gap_and_diff_cells_not_cached_grid_values(self):
        received = TIME_SERVICE_EPOCH_UNIX_US + 91_000_000
        self.apply(
            [
                [
                    "r_i",
                    {
                        "l": {
                            "h": [
                                {"n": "POS"}, {"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"},
                                {"n": "STATE"}, {"n": "LAPS"}, {"n": "GAP"}, {"n": "DIFF"}, {"n": "PIC"},
                            ]
                        },
                        "r": [
                            [0, 0, "2"], [0, 1, "21"], [0, 2, "BALCHUG Racing"], [0, 3, "CN PRO"],
                            [0, 4, "E91000000"], [0, 5, "11"], [0, 6, "1.246"], [0, 7, "0.120"], [0, 8, "1"],
                            [1, 0, "1"], [1, 1, "9"], [1, 2, "Про Моторспорт"], [1, 3, "CN PRO"],
                            [1, 4, "E91000000"], [1, 5, "11"], [1, 8, "1"],
                        ],
                    },
                ]
            ],
            received_at_us=received,
        )
        current = self.connection.execute(
            """
            SELECT gap_interval_fact_id,diff_interval_fact_id,gap_raw,gap_ms,gap_kind,diff_raw,diff_ms,diff_kind
            FROM participant_state_current
            """
        ).fetchone()
        first_gap_id = current["gap_interval_fact_id"]
        first_diff_id = current["diff_interval_fact_id"]
        self.assertIsNotNone(first_gap_id)
        self.assertIsNotNone(first_diff_id)
        self.assertEqual(tuple(current)[2:], ("1.246", 1246, "TIME", "0.120", 120, "TIME"))
        facts = self.connection.execute(
            """
            SELECT interval_kind,raw_value,interval_ms,value_kind,source_position_overall,
                   source_position_class,source_laps,source_state_kind,relation_kind,
                   target.start_number AS target_start_number,target_position_overall,
                   target_state_kind,target_laps
            FROM participant_interval_source_facts AS fact
            LEFT JOIN participants AS target ON target.id = fact.target_participant_id
            ORDER BY fact.id
            """
        ).fetchall()
        self.assertEqual(
            [tuple(fact) for fact in facts],
            [
                ("GAP", "1.246", 1246, "TIME", 2, 1, 11, "ON_TRACK", "OVERALL_LEADER", "9", 1, "ON_TRACK", 11),
                ("DIFF", "0.120", 120, "TIME", 2, 1, 11, "ON_TRACK", "OVERALL_AHEAD", "9", 1, "ON_TRACK", 11),
            ],
        )

        # STATE updates materialize the same visible GAP/DIFF text, but no
        # interval source cell changed.  Current pointers and facts must stay
        # untouched instead of treating the cached grid values as new facts.
        self.apply([['r_c', [[0, 4, 'E91001000']]]], received_at_us=received + 1_000_000)
        cached = self.connection.execute(
            """
            SELECT gap_interval_fact_id,diff_interval_fact_id,gap_raw,gap_ms,diff_raw,diff_ms
            FROM participant_state_current
            """
        ).fetchone()
        self.assertEqual(tuple(cached), (first_gap_id, first_diff_id, "1.246", 1246, "0.120", 120))
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM participant_interval_source_facts").fetchone()[0], 2)

        # A new GAP cell is a new source fact even if its displayed text is
        # unchanged.  DIFF still points to its original exact source cell.
        self.apply([['r_c', [[0, 6, '1.246']]]], received_at_us=received + 2_000_000)
        refreshed = self.connection.execute(
            """
            SELECT gap_interval_fact_id,diff_interval_fact_id,gap_raw,gap_ms,diff_raw,diff_ms
            FROM participant_state_current
            """
        ).fetchone()
        self.assertNotEqual(refreshed["gap_interval_fact_id"], first_gap_id)
        self.assertEqual(refreshed["diff_interval_fact_id"], first_diff_id)
        self.assertEqual(tuple(refreshed)[2:], ("1.246", 1246, "0.120", 120))
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM participant_interval_source_facts").fetchone()[0], 3)

    def test_race_control_messages_keep_an_immutable_ledger_and_safe_current_board(self):
        """m_* lifecycle is independent from grid data and replay-safe."""

        received = TIME_SERVICE_EPOCH_UNIX_US + 120_000_000
        first = {
            "Id": "race-control-21",
            "t": "№1 - Нарушение границы гоночной дорожки в Т12 - Аннулирование результата круга 4",
            "l": 2,
            "m": 0,
            "bc": "255,102,0",
            "fc": "0,0,0",
        }
        second = {
            "Id": "race-control-34",
            "t": "№34 - Нарушение границы гоночной дорожки в Т10 - Аннулирование результата круга 9",
            "l": 2,
            "m": 0,
            "bc": "255,102,0",
            "fc": "0,0,0",
        }
        self.apply(
            [
                ["h_i", {"n": "Qualifying - Group A", "f": 6}],
                ["m_i", [first, second]],
            ],
            received_at_us=received,
        )
        observations = self.connection.execute(
            """
            SELECT source_handle,operation,message_id_raw,source_message_ordinal,
                   source_change_ordinal,observed_at_us,provider_occurred_at_us
            FROM race_control_message_observations ORDER BY id
            """
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in observations],
            [
                ("m_i", "INITIAL_SNAPSHOT", second["Id"], 1, 1, received, None),
                ("m_i", "INITIAL_SNAPSHOT", first["Id"], 1, 0, received, None),
            ],
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM race_control_messages_current WHERE is_active = 1"
            ).fetchone()[0],
            2,
        )

        upsert_frame = self.apply(
            [["m_c", {"Id": first["Id"], "t": "№1 - результат круга 4 восстановлен", "m": 1}]],
            received_at_us=received + 1_000_000,
        )
        current = self.connection.execute(
            """
            SELECT text_raw,line,modality,background_color_raw,font_color_raw,is_active,
                   first_observation_kind,last_action,first_observed_at_us,last_observed_at_us,
                   provider_occurred_at_us
            FROM race_control_messages_current WHERE message_id_raw = ?
            """,
            (first["Id"],),
        ).fetchone()
        self.assertEqual(
            tuple(current),
            (
                "№1 - результат круга 4 восстановлен",
                2,
                1,
                "255,102,0",
                "0,0,0",
                1,
                "INITIAL_SNAPSHOT",
                "UPSERT",
                received,
                received + 1_000_000,
                None,
            ),
        )
        # A crash retry can invoke the normalizer with the same decoded RAW
        # frame. The immutable key must make it a no-op for the newer board.
        self.normalizer(self.connection, upsert_frame, self.store.decode_frame(upsert_frame))
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM race_control_message_observations").fetchone()[0],
            3,
        )

        # A valid snapshot is authoritative: a previously shown message not
        # present in it becomes inactive with snapshot provenance.
        self.apply(
            [["m_i", [{"Id": first["Id"], "t": "№1 - результат круга 4 восстановлен"}]]],
            received_at_us=received + 2_000_000,
        )
        removed_by_snapshot = self.connection.execute(
            """
            SELECT is_active,last_action,removal_action,removed_at_us,removed_source_key
            FROM race_control_messages_current WHERE message_id_raw = ?
            """,
            (second["Id"],),
        ).fetchone()
        self.assertEqual(
            tuple(removed_by_snapshot)[:4],
            (0, "SNAPSHOT_RECONCILIATION", "SNAPSHOT_RECONCILIATION", received + 2_000_000),
        )
        self.assertTrue(removed_by_snapshot["removed_source_key"])

        self.apply(
            [["m_d", first["Id"]]],
            received_at_us=received + 3_000_000,
        )
        self.assertEqual(
            tuple(
                self.connection.execute(
                    "SELECT is_active,last_action,removal_action,removed_at_us FROM race_control_messages_current WHERE message_id_raw = ?",
                    (first["Id"],),
                ).fetchone()
            ),
            (0, "DELETE", "DELETE", received + 3_000_000),
        )

        # An incomplete initial snapshot may add its valid record, but cannot
        # falsely remove a message omitted by a malformed provider payload.
        hold = {"Id": "race-control-hold", "t": "Hold position", "l": 1, "m": 0}
        self.apply([["m_c", hold]], received_at_us=received + 4_000_000)
        self.apply(
            [["m_i", [hold, {"t": "missing provider id"}]]],
            received_at_us=received + 5_000_000,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT is_active FROM race_control_messages_current WHERE message_id_raw = ?", (hold["Id"],)
            ).fetchone()[0],
            1,
        )

        self.apply([["m_a"]], received_at_us=received + 6_000_000)
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM race_control_messages_current WHERE is_active = 1"
            ).fetchone()[0],
            0,
        )
        self.apply([["m_x", {"unexpected": True}]], received_at_us=received + 7_000_000)
        self.assertEqual(
            self.connection.execute(
                "SELECT operation FROM race_control_message_observations ORDER BY id DESC LIMIT 1"
            ).fetchone()[0],
            "UNKNOWN",
        )


if __name__ == "__main__":
    unittest.main()
