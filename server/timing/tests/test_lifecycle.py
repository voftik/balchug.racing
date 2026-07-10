import json
import tempfile
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.lifecycle import (
    ActiveSessionConflict,
    IdempotencyConflict,
    OUR_START_NUMBER,
    OUR_TEAM_NAME,
    TIMING_SOURCE_CATALOG,
    TransitionError,
    ValidationError,
    abort_session,
    create_session,
    ensure_source_catalog,
    get_active_session,
    get_session,
    list_active_sessions,
    start_session,
    stop_session,
)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        self.connection = connect(self.path)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def test_catalog_is_server_owned_and_records_source_timezone(self):
        sources = ensure_source_catalog(self.connection, now_at_us=1_000)
        self.assertEqual({source.slug for source in sources}, {"igora", "moscow"})
        self.assertEqual(TIMING_SOURCE_CATALOG["igora"].source_url, "https://livetiming.getraceresults.com/igora")
        self.assertEqual(OUR_TEAM_NAME, "BALCHUG Racing")
        self.assertEqual(OUR_START_NUMBER, "21")
        rows = self.connection.execute(
            "SELECT slug,display_name,timezone_name FROM timing_sources ORDER BY slug"
        ).fetchall()
        self.assertEqual([(row["slug"], row["timezone_name"]) for row in rows], [("igora", "Europe/Moscow"), ("moscow", "Europe/Moscow")])

    def test_only_race_accepts_the_two_race_parameters(self):
        for mode in ("practice", "qualifying"):
            result = create_session(
                self.connection,
                source_slug="igora",
                mode=mode,
                now_at_us=1_000,
            )
            self.assertEqual(result.session.mode, mode)
            self.assertIsNone(result.session.race_duration_s)
            self.assertIsNone(result.session.required_pits)
        race = create_session(
            self.connection,
            source_slug="moscow",
            mode="race",
            race_duration_s=21_600,
            required_pits=6,
            now_at_us=2_000,
        )
        self.assertEqual((race.session.race_duration_s, race.session.required_pits), (21_600, 6))
        invalid = (
            {"mode": "race", "race_duration_s": None, "required_pits": 2},
            {"mode": "race", "race_duration_s": 1_800, "required_pits": 2},
            {"mode": "race", "race_duration_s": 14_400, "required_pits": 1},
            {"mode": "practice", "race_duration_s": 14_400, "required_pits": 2},
            {"mode": "qualifying", "race_duration_s": None, "required_pits": 2},
        )
        for payload in invalid:
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                create_session(self.connection, source_slug="igora", now_at_us=3_000, **payload)

    def test_transitions_are_strict_and_recovery_reads_active_session(self):
        created = create_session(self.connection, source_slug="igora", mode="practice", now_at_us=1_000)
        with self.assertRaises(TransitionError):
            stop_session(self.connection, session_id=created.session.id, now_at_us=1_100)
        active = start_session(self.connection, session_id=created.session.id, now_at_us=1_200)
        self.assertEqual(active.session.lifecycle, "active")
        self.assertEqual(active.session.started_at_us, 1_200)
        self.assertEqual(get_active_session(self.connection, "igora").id, created.session.id)
        self.assertEqual([session.id for session in list_active_sessions(self.connection)], [created.session.id])
        stopped = stop_session(self.connection, session_id=created.session.id, now_at_us=1_300)
        self.assertEqual((stopped.session.lifecycle, stopped.session.stop_intent), ("stopped", "operator_stop"))
        self.assertIsNone(get_active_session(self.connection, "igora"))
        with self.assertRaises(TransitionError):
            start_session(self.connection, session_id=created.session.id, now_at_us=1_400)
        draft = create_session(self.connection, source_slug="igora", mode="qualifying", now_at_us=1_500)
        aborted = abort_session(self.connection, session_id=draft.session.id, now_at_us=1_600)
        self.assertEqual((aborted.session.lifecycle, aborted.session.stop_intent), ("aborted", "operator_abort"))
        events = self.connection.execute(
            "SELECT event_type,parameters_json FROM session_audit_events WHERE analysis_session_id = ? ORDER BY id",
            (created.session.id,),
        ).fetchall()
        self.assertEqual([event["event_type"] for event in events], ["created", "started", "stopped"])
        self.assertEqual(json.loads(events[0]["parameters_json"]), {"mode": "practice", "race_duration_s": None, "required_pits": None})

    def test_idempotency_replays_exact_result_and_rejects_mismatched_request(self):
        first = create_session(
            self.connection,
            source_slug="igora",
            mode="race",
            race_duration_s=14_400,
            required_pits=2,
            idempotency_key="create-race-1",
            now_at_us=1_000,
        )
        replay = create_session(
            self.connection,
            source_slug="igora",
            mode="race",
            race_duration_s=14_400,
            required_pits=2,
            idempotency_key="create-race-1",
            now_at_us=9_999,
        )
        self.assertTrue(replay.replayed)
        self.assertEqual(replay.session.as_dict(), first.session.as_dict())
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM analysis_sessions").fetchone()[0],
            1,
        )
        with self.assertRaises(IdempotencyConflict):
            create_session(
                self.connection,
                source_slug="igora",
                mode="race",
                race_duration_s=21_600,
                required_pits=2,
                idempotency_key="create-race-1",
                now_at_us=10_000,
            )
        start = start_session(
            self.connection,
            session_id=first.session.id,
            idempotency_key="start-race-1",
            now_at_us=2_000,
        )
        stopped = stop_session(self.connection, session_id=first.session.id, now_at_us=3_000)
        replay_start = start_session(
            self.connection,
            session_id=first.session.id,
            idempotency_key="start-race-1",
            now_at_us=4_000,
        )
        self.assertEqual(stopped.session.lifecycle, "stopped")
        self.assertTrue(replay_start.replayed)
        self.assertEqual(replay_start.session.as_dict(), start.session.as_dict())

    def test_only_one_active_session_can_exist_per_source(self):
        first = create_session(self.connection, source_slug="igora", mode="practice", now_at_us=1_000)
        second = create_session(self.connection, source_slug="igora", mode="qualifying", now_at_us=1_100)
        start_session(self.connection, session_id=first.session.id, now_at_us=1_200)
        with self.assertRaises(ActiveSessionConflict):
            start_session(self.connection, session_id=second.session.id, now_at_us=1_300)
        self.assertEqual(get_session(self.connection, second.session.id).lifecycle, "draft")


if __name__ == "__main__":
    unittest.main()
