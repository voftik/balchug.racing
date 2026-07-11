import json
import tempfile
import time
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.ingest_store import RawIngestStore
from timing.lifecycle import create_session, start_session
from timing.protocol import Bootstrap
from timing.race_control_backfill import RaceControlBackfillError, rebuild_race_control_messages


class RaceControlBackfillTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "timing.db"
        migrate(self.database)
        self.connection = connect(self.database)
        draft = create_session(
            self.connection,
            source_slug="igora",
            mode="qualifying",
            idempotency_key="race-control-backfill-create",
        ).session
        self.session = start_session(
            self.connection,
            session_id=draft.id,
            idempotency_key="race-control-backfill-start",
        ).session
        self.base_at_us = 1_000_000
        self.connection.execute(
            """
            INSERT INTO source_heats(analysis_session_id,generation,external_name,created_at_us)
            VALUES (?,1,'Qualifying - Group A',?)
            """,
            (self.session.id, self.base_at_us),
        )
        self.connection.commit()
        self.store = RawIngestStore(self.connection, analysis_session_id=self.session.id)
        self.store.start_run(started_at_us=self.base_at_us)
        self.upstream = self.store.open_connection(
            Bootstrap("https://example.test/igora", "tid", None),
            connected_at_us=self.base_at_us,
        )

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def _raw(self, sequence, messages, *, received_at_us):
        frame = self.store.persist_raw_frame(
            self.upstream,
            sequence=sequence,
            raw_text=json.dumps({"M": messages}, ensure_ascii=False, separators=(",", ":")),
            received_at_us=received_at_us,
            monotonic_ns=time.monotonic_ns(),
        )
        self.store.decode_frame(frame)
        # Model the emergency collector: all evidence is already durable but
        # had no m_* projector at its original capture time.
        self.store.mark_processed(frame, processed_at_us=received_at_us)
        return frame

    def _stop(self):
        self.store.close_connection(self.upstream, reason="session_inactive", disconnected_at_us=self.base_at_us + 9)
        self.store.finish_run(reason="session_inactive", stopped_at_us=self.base_at_us + 9)
        self.connection.execute(
            """
            UPDATE analysis_sessions
            SET lifecycle = 'stopped', stopped_at_us = ?, updated_at_us = ?
            WHERE id = ?
            """,
            (self.base_at_us + 9, self.base_at_us + 9, self.session.id),
        )
        self.connection.commit()

    def test_rebuilds_current_board_from_processed_raw_frames_in_source_order(self):
        first = {
            "Id": "message-21",
            "t": "№1 - Нарушение границы гоночной дорожки в Т12 - Аннулирование результата круга 4",
            "l": 2,
            "m": 0,
            "bc": "255,102,0",
            "fc": "0,0,0",
        }
        second = {"Id": "message-34", "t": "№34 - Нарушение границы в Т10", "l": 2, "m": 0}
        self._raw(1, [["m_i", [first, second]]], received_at_us=self.base_at_us + 1)
        self._raw(
            2,
            [["m_c", {"Id": first["Id"], "t": "№1 - результат круга 4 восстановлен"}]],
            received_at_us=self.base_at_us + 2,
        )
        self._raw(3, [["m_d", second["Id"]]], received_at_us=self.base_at_us + 3)

        with self.assertRaises(RaceControlBackfillError):
            rebuild_race_control_messages(self.database, self.session.id)

        self._stop()
        result = rebuild_race_control_messages(self.database, self.session.id)
        self.assertEqual(result.source_heats, 1)
        self.assertEqual(result.frames_seen, 3)
        self.assertEqual(result.messages_seen, 3)
        self.assertEqual(result.observations_written, 4)
        self.assertEqual((result.current_messages, result.active_messages), (2, 1))

        current = self.connection.execute(
            """
            SELECT message_id_raw,text_raw,is_active,last_action,removed_at_us
            FROM race_control_messages_current ORDER BY message_id_raw
            """
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in current],
            [
                ("message-21", "№1 - результат круга 4 восстановлен", 1, "UPSERT", None),
                ("message-34", "№34 - Нарушение границы в Т10", 0, "DELETE", self.base_at_us + 3),
            ],
        )
        observations = self.connection.execute(
            """
            SELECT operation,message_id_raw,observed_at_us,source_key,source_change_ordinal
            FROM race_control_message_observations ORDER BY id
            """
        ).fetchall()
        self.assertEqual(
            [tuple(row)[:3] for row in observations],
            [
                ("INITIAL_SNAPSHOT", second["Id"], self.base_at_us + 1),
                ("INITIAL_SNAPSHOT", first["Id"], self.base_at_us + 1),
                ("UPSERT", first["Id"], self.base_at_us + 2),
                ("DELETE", second["Id"], self.base_at_us + 3),
            ],
        )
        self.assertTrue(all(str(row["source_key"]).count(":") == 2 for row in observations))

        # A second rebuild must replace exactly the same derived facts rather
        # than duplicate a reconnect snapshot or alter the final materialized board.
        rerun = rebuild_race_control_messages(self.database, self.session.id)
        self.assertEqual(rerun, result)


if __name__ == "__main__":
    unittest.main()
