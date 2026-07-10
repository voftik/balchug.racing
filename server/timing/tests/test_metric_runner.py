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


if __name__ == "__main__":
    unittest.main()
