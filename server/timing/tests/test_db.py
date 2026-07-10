import sqlite3
import shutil
import tempfile
import unittest
from pathlib import Path

from timing.config import now_us
from timing.db import CheckpointError, MigrationError, backup_database, connect, encode_checkpoint, load_latest_checkpoint, migrate, save_checkpoint
from timing.retention import apply_retention, plan_retention


class TimingDatabaseTests(unittest.TestCase):
    def insert_source(self, connection):
        timestamp = now_us()
        connection.execute(
            "INSERT INTO timing_sources(slug,source_url,adapter_version,created_at_us) VALUES ('igora','https://example.test/igora','test',?)",
            (timestamp,),
        )
        return connection.execute("SELECT id FROM timing_sources WHERE slug='igora'").fetchone()[0]

    def insert_session(self, connection, session_id, source_id, lifecycle="stopped"):
        timestamp = now_us()
        connection.execute(
            """
            INSERT INTO analysis_sessions(id,source_id,mode,lifecycle,created_at_us,updated_at_us)
            VALUES (?,?,'practice',?,?,?)
            """,
            (session_id, source_id, lifecycle, timestamp, timestamp),
        )
        connection.execute(
            "INSERT INTO source_heats(analysis_session_id,generation,external_name,created_at_us) VALUES (?,0,'Heat 1',?)",
            (session_id, timestamp),
        )
        return connection.execute("SELECT id FROM source_heats WHERE analysis_session_id=?", (session_id,)).fetchone()[0]

    def insert_frame(self, connection, session_id, *, received_at_us, processed=True):
        timestamp = now_us()
        run_id = f"run-{session_id}"
        connection.execute(
            "INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us) VALUES (?,?,'test',?)",
            (run_id, session_id, timestamp),
        )
        connection_id = f"connection-{session_id}"
        connection.execute(
            "INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us) VALUES (?,?,1,?)",
            (connection_id, run_id, timestamp),
        )
        connection.execute(
            """
            INSERT INTO feed_frames(
              analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,monotonic_ns,
              raw_payload,raw_sha256,decode_state,processed_at_us,created_at_us
            ) VALUES (?,?,?,?,?,'{}','hash','decoded',?,?)
            """,
            (session_id, connection_id, 1, received_at_us, received_at_us * 1000, received_at_us if processed else None, timestamp),
        )
        frame_id = connection.execute("SELECT id FROM feed_frames WHERE ingest_connection_id=?", (connection_id,)).fetchone()[0]
        connection.execute(
            "INSERT INTO feed_messages(frame_id,ordinal,handle,args_json,compressed,created_at_us) VALUES (?,0,'r_c','[]',0,?)",
            (frame_id, timestamp),
        )
        message_id = connection.execute("SELECT id FROM feed_messages WHERE frame_id=?", (frame_id,)).fetchone()[0]
        return frame_id, message_id

    def test_migration_is_repeatable_and_enables_wal(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timing.db"
            self.assertEqual(migrate(path), ["0001"])
            self.assertEqual(migrate(path), [])
            connection = connect(path)
            try:
                self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")
                self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertTrue({"feed_frames", "feed_messages", "laps", "participant_identity_segments", "metric_samples", "stream_events"}.issubset(tables))
            finally:
                connection.close()

    def test_current_live_grid_and_track_state_have_query_ready_columns(self):
        """Keep the production result-grid contract out of opaque JSON blobs."""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timing.db"
            migrate(path)
            connection = connect(path)
            try:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(participant_state_current)")
                }
                self.assertTrue(
                    {
                        "position_overall",
                        "position_class",
                        "marker",
                        "laps",
                        "state_raw",
                        "current_driver_name",
                        "last_lap_ms",
                        "best_lap_ms",
                        "gap_raw",
                        "gap_kind",
                        "diff_raw",
                        "diff_kind",
                        "pit_time_raw",
                        "source_message_id",
                        "source_key",
                    }.issubset(columns)
                )
                identity_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(participant_identity_segments)")
                }
                self.assertTrue(
                    {"team_name", "car_name", "class_name", "driver_name_raw"}.issubset(identity_columns)
                )
                flag_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(track_flag_current)")
                }
                self.assertTrue(
                    {"flag", "provider_code", "provider_label", "started_at_us", "source_key"}.issubset(
                        flag_columns
                    )
                )
            finally:
                connection.close()

    def test_changed_migration_checksum_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "timing.db"
            migrations = root / "migrations"
            migrations.mkdir()
            original = Path(__file__).parent.parent / "migrations" / "0001_initial.sql"
            copied = migrations / original.name
            shutil.copyfile(original, copied)
            self.assertEqual(migrate(path, directory=migrations), ["0001"])
            copied.write_text(copied.read_text(encoding="utf-8") + "\n-- changed\n", encoding="utf-8")
            with self.assertRaises(MigrationError):
                migrate(path, directory=migrations)

    def test_active_session_constraint_and_backup(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timing.db"
            backup = Path(temporary) / "backup.db"
            migrate(path)
            connection = connect(path)
            try:
                source_id = self.insert_source(connection)
                timestamp = now_us()
                connection.execute("INSERT INTO analysis_sessions(id,source_id,mode,lifecycle,created_at_us,updated_at_us) VALUES ('active-1',?,'practice','active',?,?)", (source_id, timestamp, timestamp))
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute("INSERT INTO analysis_sessions(id,source_id,mode,lifecycle,created_at_us,updated_at_us) VALUES ('active-2',?,'practice','active',?,?)", (source_id, timestamp, timestamp))
                connection.commit()
            finally:
                connection.close()
            backup_database(path, backup)
            restored = sqlite3.connect(backup)
            try:
                self.assertEqual(restored.execute("SELECT COUNT(*) FROM analysis_sessions").fetchone()[0], 1)
                self.assertEqual(restored.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            finally:
                restored.close()

    def test_checkpoint_round_trip_is_idempotent_and_detects_conflicts(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timing.db"
            migrate(path)
            connection = connect(path)
            try:
                source_id = self.insert_source(connection)
                heat_id = self.insert_session(connection, "checkpoint", source_id)
                state = {"heat": {"f": 6}, "rows": {"8": {"startnumber": "21", "class": "CN PRO"}}}
                self.assertTrue(
                    save_checkpoint(
                        connection,
                        source_heat_id=heat_id,
                        source_frame_id=None,
                        source_key="connection-1:60",
                        observed_at_us=1_000_000,
                        state=state,
                    )
                )
                self.assertFalse(
                    save_checkpoint(
                        connection,
                        source_heat_id=heat_id,
                        source_frame_id=None,
                        source_key="connection-1:60",
                        observed_at_us=1_000_000,
                        state=state,
                    )
                )
                checkpoint = load_latest_checkpoint(connection, heat_id)
                self.assertIsNotNone(checkpoint)
                row, restored_state = checkpoint
                self.assertEqual(row["source_key"], "connection-1:60")
                self.assertEqual(restored_state, state)
                with self.assertRaises(CheckpointError):
                    save_checkpoint(
                        connection,
                        source_heat_id=heat_id,
                        source_frame_id=None,
                        source_key="connection-1:60",
                        observed_at_us=1_000_000,
                        state={"heat": {"f": 2}},
                    )
                _, corrupted_payload, _ = encode_checkpoint({"heat": {"f": 2}})
                connection.execute(
                    "UPDATE state_checkpoints SET payload = ? WHERE source_heat_id = ?",
                    (corrupted_payload, heat_id),
                )
                with self.assertRaises(CheckpointError):
                    load_latest_checkpoint(connection, heat_id)
            finally:
                connection.close()

    def test_wal_reader_does_not_block_a_writer(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timing.db"
            migrate(path)
            writer = connect(path)
            reader = connect(path, readonly=True)
            try:
                reader.execute("BEGIN")
                self.assertEqual(reader.execute("SELECT COUNT(*) FROM timing_sources").fetchone()[0], 0)
                self.insert_source(writer)
                writer.commit()
                self.assertEqual(reader.execute("SELECT COUNT(*) FROM timing_sources").fetchone()[0], 0)
                reader.execute("COMMIT")
                self.assertEqual(reader.execute("SELECT COUNT(*) FROM timing_sources").fetchone()[0], 1)
            finally:
                reader.close()
                writer.close()

    def test_retention_preserves_normalized_provenance_and_active_frames(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timing.db"
            migrate(path)
            connection = connect(path)
            try:
                source_id = self.insert_source(connection)
                active_heat = self.insert_session(connection, "active", source_id, lifecycle="active")
                finished_heat = self.insert_session(connection, "finished", source_id, lifecycle="stopped")
                timestamp = now_us()
                old = timestamp - 10 * 86_400_000_000
                _, active_message = self.insert_frame(connection, "active", received_at_us=old)
                _finished_frame, finished_message = self.insert_frame(connection, "finished", received_at_us=old)
                connection.execute(
                    """
                    INSERT INTO feed_frames(
                      analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,monotonic_ns,
                      raw_payload,raw_sha256,decode_state,processed_at_us,created_at_us
                    ) VALUES ('finished','connection-finished',2,? ,?,'{}','anchor','decoded',?,?)
                    """,
                    (old, old * 1000, old, timestamp),
                )
                anchor_frame = connection.execute(
                    "SELECT id FROM feed_frames WHERE ingest_connection_id = 'connection-finished' AND frame_sequence = 2"
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO participants(id,source_heat_id,external_key,team_name,class_name,first_seen_at_us,last_seen_at_us) VALUES ('car-1',?,'42','BALCHUG Racing','CN PRO',?,?)",
                    (finished_heat, timestamp, timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO laps(id,source_heat_id,participant_id,lap_number,source_message_id,source_key,created_at_us)
                    VALUES ('lap-1',?,'car-1',1,?,'connection-finished:1:0',?)
                    """,
                    (finished_heat, finished_message, timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO state_checkpoints(
                      source_heat_id,source_frame_id,source_key,observed_at_us,state_hash,codec,payload,created_at_us
                    ) VALUES (?,?,'connection-finished:1',?,'hash','identity','{}',?)
                    """,
                    (finished_heat, anchor_frame, timestamp, timestamp),
                )
                connection.execute("INSERT INTO stream_events(analysis_session_id,source_heat_id,event_type,payload_json,created_at_us) VALUES ('active',?,'state','{}',?)", (active_heat, old))
                connection.execute("INSERT INTO stream_events(analysis_session_id,source_heat_id,event_type,payload_json,created_at_us) VALUES ('finished',?,'state','{}',?)", (finished_heat, old))
                connection.commit()
                plan = plan_retention(connection, now_at_us=timestamp)
                self.assertEqual(plan.total, 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM feed_frames").fetchone()[0], 3)
                connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'active'")
                connection.execute("UPDATE analysis_sessions SET lifecycle = 'active' WHERE id = 'finished'")
                connection.commit()
                self.assertEqual(apply_retention(connection, plan), 0)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM feed_frames").fetchone()[0], 3)
                connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'finished'")
                connection.execute("UPDATE analysis_sessions SET lifecycle = 'active' WHERE id = 'active'")
                connection.commit()
                plan = plan_retention(connection, now_at_us=timestamp)
                self.assertEqual(apply_retention(connection, plan), 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM feed_frames").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM stream_events").fetchone()[0], 1)
                self.assertIsNone(connection.execute("SELECT source_message_id FROM laps WHERE id='lap-1'").fetchone()[0])
                self.assertEqual(
                    connection.execute("SELECT source_frame_id FROM state_checkpoints WHERE source_heat_id = ?", (finished_heat,)).fetchone()[0],
                    anchor_frame,
                )
                self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
                self.assertIsNotNone(active_message)
                with self.assertRaises(ValueError):
                    plan_retention(connection, now_at_us=timestamp, raw_days=-1)
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
