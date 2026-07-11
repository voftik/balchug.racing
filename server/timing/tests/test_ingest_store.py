import sqlite3
import tempfile
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.ingest_store import IngestStoreError, RawIngestStore
from timing.lifecycle import create_session, start_session
from timing.protocol import Bootstrap


class RawIngestStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        self.connection = connect(self.path)
        draft = create_session(
            self.connection,
            source_slug="igora",
            mode="practice",
            idempotency_key="create-raw-store",
            now_at_us=1_000_000,
        ).session
        self.session = start_session(
            self.connection,
            session_id=draft.id,
            idempotency_key="start-raw-store",
            now_at_us=1_000_001,
        ).session

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def test_raw_frame_commits_before_decode_and_keeps_decode_failure(self):
        store = RawIngestStore(self.connection, analysis_session_id=self.session.id)
        store.start_run(started_at_us=2_000_000)
        upstream = store.open_connection(
            Bootstrap("https://example.test/igora", "timekeeper", "marker"),
            connected_at_us=2_000_001,
        )
        frame = store.persist_raw_frame(
            upstream,
            sequence=1,
            raw_text="not-json",
            received_at_us=2_000_002,
            monotonic_ns=123,
        )
        before = self.connection.execute(
            "SELECT decode_state,raw_payload,processed_at_us FROM feed_frames WHERE id = ?", (frame.id,)
        ).fetchone()
        self.assertEqual(before["decode_state"], "pending")
        self.assertEqual(bytes(before["raw_payload"]), b"not-json")
        self.assertIsNone(before["processed_at_us"])

        self.assertEqual(store.decode_frame(frame), ())
        after = self.connection.execute(
            "SELECT decode_state,decode_error,processed_at_us FROM feed_frames WHERE id = ?", (frame.id,)
        ).fetchone()
        self.assertEqual(after["decode_state"], "failed")
        self.assertIn("ProtocolError", after["decode_error"])
        self.assertIsNotNone(after["processed_at_us"])

    def test_decoded_messages_are_idempotent_pending_until_normalizer_marks_frame(self):
        store = RawIngestStore(self.connection, analysis_session_id=self.session.id)
        store.start_run(started_at_us=3_000_000)
        upstream = store.open_connection(
            Bootstrap("https://example.test/igora", "timekeeper", None), connected_at_us=3_000_001
        )
        frame = store.persist_raw_frame(
            upstream,
            sequence=1,
            raw_text='{"C":"cursor-1","G":"groups-1","M":[["h_h",{"f":2}],["r_c",[[0,3,"SIn Pit"]]]]}',
            received_at_us=3_000_002,
            monotonic_ns=456,
        )
        messages = store.decode_frame(frame)
        self.assertEqual([message.handle for message in messages], ["h_h", "r_c"])
        self.assertEqual(len(store.pending_decoded_frames()), 1)
        self.assertEqual(store.decode_frame(frame), messages)
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM feed_messages WHERE frame_id = ?", (frame.id,)).fetchone()[0],
            2,
        )
        persisted = self.connection.execute(
            "SELECT upstream_cursor,groups_token,decode_state,processed_at_us FROM feed_frames WHERE id = ?", (frame.id,)
        ).fetchone()
        self.assertEqual(tuple(persisted), ("cursor-1", "groups-1", "decoded", None))
        store.mark_processed(frame, processed_at_us=3_000_003)
        self.assertEqual(store.pending_decoded_frames(), ())

    def test_replay_uses_persisted_messages_when_raw_payload_cannot_be_redecoded(self):
        """A stored SignalR compression context is not available after restart."""

        store = RawIngestStore(self.connection, analysis_session_id=self.session.id)
        store.start_run(started_at_us=3_100_000)
        upstream = store.open_connection(
            Bootstrap("https://example.test/igora", "timekeeper", None), connected_at_us=3_100_001
        )
        frame = store.persist_raw_frame(
            upstream,
            sequence=1,
            raw_text='{"M":[["r_i",{"l":{"h":[]},"r":[]}]]}',
            received_at_us=3_100_002,
            monotonic_ns=789,
        )
        self.assertEqual([message.handle for message in store.decode_frame(frame)], ["r_i"])
        # Simulate an initial compressed invocation which re-decodes to no
        # usable messages without its connection-local dictionary, while its
        # decoded r_i was committed durably during live ingest.
        self.connection.execute("UPDATE feed_frames SET raw_payload = ? WHERE id = ?", (b'{"M":[]}', frame.id))
        self.connection.commit()
        replayed = store.decode_frame(frame)
        self.assertEqual([message.handle for message in replayed], ["r_i"])
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM feed_messages WHERE frame_id = ?", (frame.id,)).fetchone()[0],
            1,
        )

    def test_inactive_session_cannot_start_a_live_writer(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = ?", (self.session.id,))
        self.connection.commit()
        with self.assertRaises(IngestStoreError):
            RawIngestStore(self.connection, analysis_session_id=self.session.id).start_run()

    def test_duplicate_frame_sequence_is_rejected_without_overwriting_evidence(self):
        store = RawIngestStore(self.connection, analysis_session_id=self.session.id)
        store.start_run()
        upstream = store.open_connection(Bootstrap("https://example.test", "timekeeper", None))
        store.persist_raw_frame(upstream, sequence=1, raw_text='{"M":[]}', received_at_us=4_000_000, monotonic_ns=1)
        with self.assertRaises(sqlite3.IntegrityError):
            store.persist_raw_frame(upstream, sequence=1, raw_text='{"M":[["h_h",{}]]}', received_at_us=4_000_001, monotonic_ns=2)
        self.assertEqual(
            self.connection.execute("SELECT raw_payload FROM feed_frames WHERE ingest_connection_id = ?", (upstream.id,)).fetchone()[0],
            b'{"M":[]}',
        )


if __name__ == "__main__":
    unittest.main()
