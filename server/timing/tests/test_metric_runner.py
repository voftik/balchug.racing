import gzip
import json
import tempfile
import time
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.ingest_store import RawIngestStore
from timing.lifecycle import create_session, start_session
from timing.metric_engine import METRIC_ENGINE_VERSION
from timing.normalization import TIME_SERVICE_EPOCH_UNIX_US
from timing.normalizer_writer import TimingNormalizer
from timing.protocol import Bootstrap


class MetricRunnerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        self.connection = connect(self.path)
        draft = create_session(
            self.connection,
            source_slug="igora",
            mode="practice",
            idempotency_key="create-metric-runner",
        ).session
        self.session = start_session(
            self.connection,
            session_id=draft.id,
            idempotency_key="start-metric-runner",
        ).session
        self.store = RawIngestStore(self.connection, analysis_session_id=self.session.id)
        self.store.start_run()
        self.upstream = self.store.open_connection(Bootstrap("https://example.test/igora", "metric-runner", None))
        self.normalizer = TimingNormalizer(self.session.id)
        self.sequence = 0

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def apply(self, messages, *, received_at_us):
        self.sequence += 1
        frame = self.store.persist_raw_frame(
            self.upstream,
            sequence=self.sequence,
            raw_text=json.dumps({"M": messages}, ensure_ascii=False, separators=(",", ":")),
            received_at_us=received_at_us,
            monotonic_ns=time.monotonic_ns(),
        )
        decoded = self.store.decode_frame(frame)
        self.normalizer(self.connection, frame, decoded)
        self.store.mark_processed(frame)
        return frame

    @staticmethod
    def playback_payload(row):
        return json.loads(gzip.decompress(bytes(row["payload"])))

    def test_normalizer_materializes_current_each_tick_and_sparse_history(self):
        provider_start = 40_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_start
        self.apply(
            [
                ["h_i", {"n": "Practice - Open-Pit", "s": provider_start, "f": 6}],
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
                                {"n": "LAPS"},
                            ]
                        },
                        "r": [
                            [0, 0, "1"],
                            [0, 1, "21"],
                            [0, 2, "E40000000"],
                            [0, 3, "BALCHUG Racing"],
                            [0, 4, "Киракозов Кирилл"],
                            [0, 5, "CN PRO"],
                            [0, 6, "1"],
                            [0, 7, "5"],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        scopes = self.connection.execute(
            "SELECT scope_kind,scope_key,values_json FROM metric_current ORDER BY scope_kind,scope_key"
        ).fetchall()
        self.assertEqual([(row["scope_kind"], row["scope_key"]) for row in scopes], [("class", "cn pro"), ("participant", self.session.our_participant_id or next(row["scope_key"] for row in scopes if row["scope_kind"] == "participant")), ("session", self.session.id)])
        session_current = next(json.loads(row["values_json"]) for row in scopes if row["scope_kind"] == "session")
        self.assertEqual(session_current["channel_status"], "LIVE")
        self.assertEqual(session_current["track_flag"], "GREEN")
        self.assertEqual(session_current["ours_identity"]["start_number"], "21")
        self.assertEqual(session_current["ours_class_key"], "cn pro")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM state_ticks").fetchone()[0], 1)
        first_history_count = self.connection.execute("SELECT COUNT(*) FROM metric_samples").fetchone()[0]
        self.assertEqual(first_history_count, 3)
        playback = self.connection.execute(
            "SELECT observed_at_us,is_event_boundary,payload FROM playback_snapshots"
        ).fetchone()
        self.assertEqual(playback["observed_at_us"], received)
        self.assertFalse(playback["is_event_boundary"])
        playback_payload = self.playback_payload(playback)
        self.assertEqual(playback_payload["schema_version"], "timing-archive.v1")
        self.assertEqual(playback_payload["measured"]["ours"]["start_number"], "21")
        self.assertEqual(playback_payload["computed"]["session"]["track_flag"], "GREEN")
        first_events = self.connection.execute(
            "SELECT id,event_type,event_key,source_frame_id FROM stream_events ORDER BY id"
        ).fetchall()
        self.assertEqual([event["event_type"] for event in first_events], ["state", "metric"])
        self.assertTrue(all(event["event_key"] for event in first_events))
        self.assertTrue(all(event["source_frame_id"] is not None for event in first_events))

        self.apply([["s_t", provider_start + 1_000_000]], received_at_us=received + 1_000_000)
        session_row = self.connection.execute(
            """
            SELECT observed_at_us,values_json FROM metric_current
            WHERE scope_kind = 'session' AND scope_key = ?
            """,
            (self.session.id,),
        ).fetchone()
        self.assertEqual(session_row["observed_at_us"], received + 1_000_000)
        self.assertEqual(json.loads(session_row["values_json"])["session_elapsed_s"], 1.0)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM state_ticks").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM metric_samples").fetchone()[0], first_history_count)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM stream_events").fetchone()[0], 4)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM playback_snapshots").fetchone()[0], 1)

    def test_interval_fact_boundary_scopes_sse_and_archive_without_state_noise(self):
        provider_start = 45_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_start
        self.apply(
            [
                ["h_i", {"n": "Practice - Open-Pit", "s": provider_start, "f": 6}],
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
                                {"n": "LAPS"},
                                {"n": "GAP"},
                                {"n": "DIFF"},
                            ]
                        },
                        "r": [
                            [0, 0, "2"],
                            [0, 1, "21"],
                            [0, 2, "E45000000"],
                            [0, 3, "BALCHUG Racing"],
                            [0, 4, "Киракозов Кирилл"],
                            [0, 5, "CN PRO"],
                            [0, 6, "1"],
                            [0, 7, "5"],
                            [0, 8, "1.246"],
                            [0, 9, "0.120"],
                        ],
                    },
                ],
            ],
            received_at_us=received,
        )
        current = self.connection.execute(
            "SELECT participant_id,gap_interval_fact_id FROM participant_state_current"
        ).fetchone()
        participant_id = current["participant_id"]
        first_gap_fact_id = current["gap_interval_fact_id"]

        state_only = self.apply(
            [["r_c", [[0, 2, "E45001000"]]]],
            received_at_us=received + 1_000_000,
        )
        self.assertEqual(
            self.connection.execute("SELECT gap_interval_fact_id FROM participant_state_current").fetchone()[0],
            first_gap_fact_id,
        )
        state_payload = json.loads(
            self.connection.execute(
                "SELECT payload_json FROM stream_events WHERE source_frame_id = ? AND event_type = 'state'",
                (state_only.id,),
            ).fetchone()[0]
        )
        self.assertEqual(state_payload["data"]["event_keys"], [])
        self.assertEqual(state_payload["data"]["event_scopes"], [])
        self.assertEqual(state_payload["data"]["interval_fact_updates"], [])

        gap_update = self.apply(
            [["r_c", [[0, 8, "1.246"]]]],
            received_at_us=received + 2_000_000,
        )
        self.assertNotEqual(
            self.connection.execute("SELECT gap_interval_fact_id FROM participant_state_current").fetchone()[0],
            first_gap_fact_id,
        )
        state_payload = json.loads(
            self.connection.execute(
                "SELECT payload_json FROM stream_events WHERE source_frame_id = ? AND event_type = 'state'",
                (gap_update.id,),
            ).fetchone()[0]
        )
        self.assertEqual(state_payload["data"]["event_keys"], [f"interval_fact:{participant_id}:GAP"])
        self.assertEqual(
            state_payload["data"]["event_scopes"],
            [
                {"scope_kind": "session", "scope_key": self.session.id},
                {"scope_kind": "class", "scope_key": "cn pro"},
                {"scope_kind": "participant", "scope_key": participant_id},
            ],
        )
        self.assertEqual(
            state_payload["data"]["interval_fact_updates"],
            [{"participant_id": participant_id, "field_kind": "GAP"}],
        )
        metric_payload = json.loads(
            self.connection.execute(
                "SELECT payload_json FROM stream_events WHERE source_frame_id = ? AND event_type = 'metric'",
                (gap_update.id,),
            ).fetchone()[0]
        )
        self.assertEqual(metric_payload["data"]["event_scopes"], state_payload["data"]["event_scopes"])
        playback = self.connection.execute(
            "SELECT is_event_boundary FROM playback_snapshots WHERE source_frame_id = ?",
            (gap_update.id,),
        ).fetchone()
        self.assertIsNotNone(playback)
        self.assertTrue(playback["is_event_boundary"])

    def test_restart_restores_the_prior_boundary_before_a_red_transition(self):
        provider_start = 50_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_start
        self.apply(
            [["h_i", {"n": "Practice", "s": provider_start, "f": 6}], ["s_i", provider_start]],
            received_at_us=received,
        )
        self.normalizer = TimingNormalizer(self.session.id)
        self.apply([["h_h", {"f": 2}]], received_at_us=received + 1_000_000)

        session = self.connection.execute(
            "SELECT values_json FROM metric_current WHERE scope_kind='session' AND scope_key=?",
            (self.session.id,),
        ).fetchone()
        alerts = json.loads(session["values_json"])["alerts"]
        self.assertIn("flag_changed", {alert["key"] for alert in alerts})
        self.assertIn("red_flag_or_session_reset", {alert["key"] for alert in alerts})
        state = self.connection.execute(
            "SELECT source_frame_id,metric_version,boundary_state_json FROM metric_runner_state"
        ).fetchone()
        self.assertIsNotNone(state)
        self.assertEqual(state["metric_version"], METRIC_ENGINE_VERSION)
        self.assertIn('"flag"', state["boundary_state_json"])
        event_types = {
            row["event_type"]
            for row in self.connection.execute("SELECT event_type FROM stream_events")
        }
        self.assertTrue({"state", "metric", "flag", "alert"}.issubset(event_types))
        playback = self.connection.execute(
            "SELECT is_event_boundary,payload FROM playback_snapshots ORDER BY observed_at_us"
        ).fetchall()
        self.assertEqual([row["is_event_boundary"] for row in playback], [0, 1])
        self.assertEqual(self.playback_payload(playback[-1])["measured"]["track_flag"]["flag"], "RED")

    def test_retry_after_metric_runner_failure_preserves_the_pending_red_transition(self):
        provider_start = 60_000_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_start
        self.apply(
            [["h_i", {"n": "Practice", "s": provider_start, "f": 6}], ["s_i", provider_start]],
            received_at_us=received,
        )
        self.sequence += 1
        frame = self.store.persist_raw_frame(
            self.upstream,
            sequence=self.sequence,
            raw_text=json.dumps({"M": [["h_h", {"f": 2}]]}, separators=(",", ":")),
            received_at_us=received + 1_000_000,
            monotonic_ns=time.monotonic_ns(),
        )
        decoded = self.store.decode_frame(frame)
        original = self.normalizer._metric_runner.process_frame
        self.normalizer._metric_runner.process_frame = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("injected"))
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.normalizer(self.connection, frame, decoded)
        self.normalizer._metric_runner.process_frame = original
        self.assertIsNone(self.connection.execute("SELECT processed_at_us FROM feed_frames WHERE id=?", (frame.id,)).fetchone()[0])

        self.normalizer = TimingNormalizer(self.session.id)
        self.normalizer(self.connection, frame, decoded)
        self.store.mark_processed(frame)
        session = self.connection.execute(
            "SELECT values_json FROM metric_current WHERE scope_kind='session' AND scope_key=?",
            (self.session.id,),
        ).fetchone()
        alerts = json.loads(session["values_json"])["alerts"]
        self.assertIn("red_flag_or_session_reset", {alert["key"] for alert in alerts})
        event_count = self.connection.execute("SELECT COUNT(*) FROM stream_events").fetchone()[0]
        self.normalizer(self.connection, frame, decoded)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM stream_events").fetchone()[0], event_count)


if __name__ == "__main__":
    unittest.main()
