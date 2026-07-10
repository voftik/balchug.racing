import asyncio
import tempfile
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.sse import (
    ResetRequired,
    TimingStreamBroker,
    format_sse_comment,
    format_sse_event,
    parse_last_event_id,
    read_cursor_window,
    read_stream_events,
)


class TimingStreamReadTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        self.connection = connect(self.path)
        self._seed()

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def _seed(self):
        self.connection.execute(
            """
            INSERT INTO timing_sources(slug,source_url,adapter_version,created_at_us)
            VALUES ('igora','https://example.test/igora','test',1)
            """
        )
        source_id = self.connection.execute("SELECT id FROM timing_sources WHERE slug = 'igora'").fetchone()[0]
        self.connection.executemany(
            """
            INSERT INTO analysis_sessions(id,source_id,mode,lifecycle,identity_state,created_at_us,updated_at_us)
            VALUES (?,?,'practice',?,'pending',1,1)
            """,
            (("session-a", source_id, "active"), ("session-b", source_id, "stopped")),
        )
        self.connection.executemany(
            """
            INSERT INTO source_heats(analysis_session_id,generation,created_at_us)
            VALUES (?,1,1)
            """,
            (("session-a",), ("session-b",)),
        )
        self.connection.commit()

    def event(self, session_id, *, event_type="state", payload='{"generation":1,"data":{}}'):
        heat_id = self.connection.execute(
            "SELECT id FROM source_heats WHERE analysis_session_id = ?", (session_id,)
        ).fetchone()[0]
        self.connection.execute(
            """
            INSERT INTO stream_events(
              analysis_session_id,source_heat_id,event_type,event_key,observed_at_us,payload_json,created_at_us
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (session_id, heat_id, event_type, f"{session_id}:{event_type}:{self.connection.total_changes}", 1, payload, 1),
        )
        self.connection.commit()
        return self.connection.execute("SELECT last_insert_rowid()").fetchone()[0]

    def test_cursor_window_replay_and_invalid_payload_are_session_isolated(self):
        first = self.event("session-a")
        second = self.event("session-b", payload="not-json")
        third = self.event("session-a", event_type="lap")

        rows = read_stream_events("session-a", after_id=0, database=self.path)
        self.assertEqual([row.id for row in rows], [first, third])
        self.assertEqual(rows[-1].event_type, "lap")
        invalid = read_stream_events("session-b", after_id=0, database=self.path)[0]
        self.assertEqual(invalid.payload["data"]["stream_payload_error"], "invalid_json")

        window = read_cursor_window("session-a", cursor=first, database=self.path)
        self.assertFalse(window.requires_reset(first))
        self.assertTrue(read_cursor_window("session-a", cursor=second, database=self.path).requires_reset(second))
        self.connection.execute(
            """
            INSERT INTO stream_event_cursor_floors(analysis_session_id,deleted_through_id,updated_at_us)
            VALUES ('session-a',?,2)
            """,
            (first,),
        )
        self.connection.commit()
        self.assertTrue(read_cursor_window("session-a", cursor=0, database=self.path).requires_reset(0))
        self.assertTrue(read_cursor_window("session-a", cursor=first, database=self.path).requires_reset(first))

    def test_sse_format_and_cursor_parser_are_non_ambiguous(self):
        self.assertEqual(parse_last_event_id("00012"), 12)
        self.assertIsNone(parse_last_event_id(None))
        with self.assertRaises(ValueError):
            parse_last_event_id("12.0")
        body = format_sse_event("state", {"answer": 42}, event_id=7, retry_ms=3000)
        self.assertEqual(body, b'retry: 3000\nid: 7\nevent: state\ndata: {"answer":42}\n\n')
        self.assertEqual(format_sse_comment("a\nb"), b": a b\n\n")


class TimingStreamBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        self.connection = connect(self.path)
        self.connection.execute(
            "INSERT INTO timing_sources(slug,source_url,adapter_version,created_at_us) VALUES ('igora','https://example.test','test',1)"
        )
        source_id = self.connection.execute("SELECT id FROM timing_sources").fetchone()[0]
        for session_id, lifecycle in (("session-a", "active"), ("session-b", "stopped")):
            self.connection.execute(
                """
                INSERT INTO analysis_sessions(id,source_id,mode,lifecycle,identity_state,created_at_us,updated_at_us)
                VALUES (?,?,'practice',?,'pending',1,1)
                """,
                (session_id, source_id, lifecycle),
            )
            self.connection.execute(
                "INSERT INTO source_heats(analysis_session_id,generation,created_at_us) VALUES (?,1,1)",
                (session_id,),
            )
        self.connection.commit()
        self.broker = TimingStreamBroker(self.path, poll_interval_s=0.01, quality_interval_s=60, queue_size=1)
        await self.broker.start()

    async def asyncTearDown(self):
        await self.broker.stop()
        self.connection.close()
        self.temporary.cleanup()

    def emit(self, session_id, suffix):
        heat_id = self.connection.execute(
            "SELECT id FROM source_heats WHERE analysis_session_id = ?", (session_id,)
        ).fetchone()[0]
        self.connection.execute(
            """
            INSERT INTO stream_events(
              analysis_session_id,source_heat_id,event_type,event_key,observed_at_us,payload_json,created_at_us
            ) VALUES (?,?,'state',?,?,?,?)
            """,
            (session_id, heat_id, f"{session_id}:{suffix}", 1, '{"generation":1,"data":{}}', 1),
        )
        self.connection.commit()

    async def test_broker_dispatches_only_session_events_and_bounds_a_slow_client(self):
        queue_a = await self.broker.subscribe("session-a")
        queue_b = await self.broker.subscribe("session-b")
        await asyncio.sleep(0.05)
        while not queue_a.empty():
            queue_a.get_nowait()
        while not queue_b.empty():
            queue_b.get_nowait()
        self.emit("session-a", "one")
        delivered = await asyncio.wait_for(queue_a.get(), timeout=1)
        self.assertEqual(delivered.analysis_session_id, "session-a")
        await asyncio.sleep(0.05)
        self.assertTrue(queue_b.empty())

        self.emit("session-a", "two")
        self.emit("session-a", "three")
        reset = await asyncio.wait_for(queue_a.get(), timeout=1)
        self.assertIsInstance(reset, ResetRequired)
        self.assertEqual(reset.reason, "subscriber_backpressure")
        self.broker.unsubscribe("session-a", queue_a)
        self.broker.unsubscribe("session-b", queue_b)
        self.assertEqual(self.broker.subscriber_count, 0)


if __name__ == "__main__":
    unittest.main()
