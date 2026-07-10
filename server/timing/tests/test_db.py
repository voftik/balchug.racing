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
            self.assertEqual(migrate(path), ["0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008"])
            self.assertEqual(migrate(path), [])
            connection = connect(path)
            try:
                self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")
                self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertTrue(
                    {
                        "feed_frames",
                        "feed_messages",
                        "laps",
                        "participant_identity_segments",
                        "metric_samples",
                        "metric_current",
                        "metric_runner_state",
                        "stream_events",
                        "stream_event_cursor_floors",
                        "playback_snapshots",
                    }.issubset(tables)
                )
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

    def test_normalizer_schema_keeps_raw_time_provenance_and_deduplicates_source_events(self):
        """The #15 writer can replay a connection without duplicating facts."""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "timing.db"
            migrate(path)
            connection = connect(path)
            try:
                tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertTrue(
                    {
                        "result_layout_versions",
                        "result_column_definitions",
                        "participant_result_cell_observations",
                        "connection_clock_samples",
                        "connection_clock_calibrations",
                        "participant_identity_observations",
                        "participant_state_observations",
                        "tracker_passing_observations",
                        "heat_statistics_current",
                        "heat_statistics_samples",
                        "statistics_best_lap_history",
                        "statistics_class_best_laps",
                        "statistics_caution_history",
                    }.issubset(tables)
                )

                state_columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(participant_state_current)")
                }
                self.assertTrue(
                    {
                        "state_timer_target_raw",
                        "state_timer_target_provider_us",
                        "state_timer_target_at_us",
                        "state_timer_calibration_id",
                        "state_timer_source_message_id",
                        "state_timer_source_key",
                        "state_timer_observed_at_us",
                        "provider_pit_count",
                        "provider_pit_count_raw",
                    }.issubset(state_columns)
                )
                passing_columns = {row[1] for row in connection.execute("PRAGMA table_info(tracker_passings)")}
                self.assertTrue(
                    {
                        "raw_speed_mm_s",
                        "provider_passed_at_provider_us",
                        "provider_passed_at_kind",
                        "clock_calibration_id",
                        "event_fingerprint",
                        "observed_at_us",
                    }.issubset(passing_columns)
                )
                flag_columns = {row[1] for row in connection.execute("PRAGMA table_info(track_flag_periods)")}
                self.assertTrue(
                    {
                        "start_provider_ts_raw",
                        "end_provider_ts_raw",
                        "observed_started_at_us",
                        "observed_ended_at_us",
                        "calibrated_started_at_us",
                        "calibrated_ended_at_us",
                        "reconciliation_key",
                        "reconciliation_source_message_id",
                    }.issubset(flag_columns)
                )

                source_id = self.insert_source(connection)
                heat_id = self.insert_session(connection, "normalizer", source_id)
                _frame_id, message_id = self.insert_frame(connection, "normalizer", received_at_us=1_000_000)
                timestamp = now_us()
                connection.execute(
                    """
                    INSERT INTO result_layout_versions(
                      source_heat_id,version_ordinal,layout_fingerprint,raw_layout_json,
                      source_message_id,source_key,observed_at_us,created_at_us
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (heat_id, 0, "layout-a", '{"h":[]}', message_id, "connection-normalizer:1:0", timestamp, timestamp),
                )
                layout_id = connection.execute(
                    "SELECT id FROM result_layout_versions WHERE source_heat_id = ?", (heat_id,)
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO result_column_definitions(
                      layout_version_id,column_index,source_name_raw,canonical_key,raw_definition_json
                    ) VALUES (?,?,?,?,?)
                    """,
                    (layout_id, 3, "State", "state", '{"n":"State"}'),
                )
                connection.execute(
                    """
                    INSERT INTO connection_clock_calibrations(
                      ingest_connection_id,source_heat_id,calibration_key,provider_timestamp_kind,
                      offset_us,sample_count,valid_from_observed_at_us,source_message_id,source_key,created_at_us
                    ) VALUES ('connection-normalizer',?,'median-1','ts_time',1,1,?,?,?,?)
                    """,
                    (heat_id, timestamp, message_id, "connection-normalizer:1:clock", timestamp),
                )
                calibration_id = connection.execute("SELECT id FROM connection_clock_calibrations").fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO participants(
                      id,source_heat_id,external_key,start_number,team_name,class_name,first_seen_at_us,last_seen_at_us
                    ) VALUES ('car-21',?,'21','21','BALCHUG Racing','CN PRO',?,?)
                    """,
                    (heat_id, timestamp, timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO participant_state_observations(
                      source_heat_id,participant_id,layout_version_id,provider_row_index,state_raw,state_kind,
                      state_timer_target_raw,state_timer_target_provider_us,state_timer_target_at_us,
                      state_timer_calibration_id,provider_pit_count,source_message_id,source_key,
                      source_event_key,observed_at_us,created_at_us
                    ) VALUES (?, 'car-21', ?, 1, 'E837026446926000', 'ON_TRACK',
                              '837026446926000', 837026446926000, ?, ?, 2, ?, ?, 'state:1:3', ?, ?)
                    """,
                    (heat_id, layout_id, timestamp, calibration_id, message_id, "connection-normalizer:1:0", timestamp, timestamp),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """
                        INSERT INTO participant_state_observations(
                          source_heat_id,provider_row_index,state_kind,source_key,source_event_key,observed_at_us,created_at_us
                        ) VALUES (?,1,'UNKNOWN','connection-normalizer:1:0','state:1:3',?,?)
                        """,
                        (heat_id, timestamp, timestamp),
                    )
                connection.execute(
                    """
                    INSERT INTO tracker_passing_observations(
                      source_heat_id,participant_id,transponder_id_raw,raw_speed_mm_s,is_in_pit,
                      provider_passed_at_raw,provider_passed_at_provider_us,provider_passed_at_kind,
                      passed_at_us,clock_calibration_id,event_fingerprint,raw_passing_json,
                      source_message_id,source_key,source_event_key,observed_at_us,created_at_us
                    ) VALUES (?, 'car-21', '42', 47000, 0, '837026446926000', 837026446926000,
                              'ts_time', ?, ?, '42:837026446926000:1', '[]', ?,
                              'connection-normalizer:1:0', 'passing:1', ?, ?)
                    """,
                    (heat_id, timestamp, calibration_id, message_id, timestamp, timestamp),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """
                        INSERT INTO tracker_passing_observations(
                          source_heat_id,event_fingerprint,raw_passing_json,source_key,source_event_key,observed_at_us,created_at_us
                        ) VALUES (?, '42:837026446926000:1', '[]', 'connection-normalizer:2:0', 'passing:replay', ?, ?)
                        """,
                        (heat_id, timestamp, timestamp),
                    )
                connection.execute(
                    """
                    INSERT INTO track_flag_periods(
                      source_heat_id,flag,started_at_us,source_key,created_at_us,reconciliation_key,
                      start_provider_ts_raw,observed_started_at_us,calibrated_started_at_us
                    ) VALUES (?, 'RED', ?, 'connection-normalizer:1:flag', ?, 'red:837026446926000',
                              '837026446926000', ?, ?)
                    """,
                    (heat_id, timestamp, timestamp, timestamp, timestamp),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """
                        INSERT INTO track_flag_periods(
                          source_heat_id,flag,started_at_us,source_key,created_at_us,reconciliation_key
                        ) VALUES (?, 'RED', ?, 'connection-normalizer:2:flag', ?, 'red:837026446926000')
                        """,
                        (heat_id, timestamp, timestamp),
                    )
                self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            finally:
                connection.close()

    def test_normalizer_migration_upgrades_a_populated_v2_database(self):
        """0003 must be deployable over the already deployed 0001/0002 data plane."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "timing.db"
            legacy_migrations = root / "legacy-migrations"
            legacy_migrations.mkdir()
            for filename in ("0001_initial.sql", "0002_session_lifecycle.sql"):
                shutil.copyfile(Path(__file__).parent.parent / "migrations" / filename, legacy_migrations / filename)
            self.assertEqual(migrate(path, directory=legacy_migrations), ["0001", "0002"])
            connection = connect(path)
            try:
                source_id = self.insert_source(connection)
                heat_id = self.insert_session(connection, "legacy-normalizer", source_id)
                timestamp = now_us()
                connection.execute(
                    """
                    INSERT INTO participants(id,source_heat_id,external_key,first_seen_at_us,last_seen_at_us)
                    VALUES ('legacy-car',?,'21',?,?)
                    """,
                    (heat_id, timestamp, timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO participant_identity_segments(
                      id,source_heat_id,participant_id,started_at_us,source_key,created_at_us,updated_at_us
                    ) VALUES ('legacy-segment',?,'legacy-car',?,'legacy:1',?,?)
                    """,
                    (heat_id, timestamp, timestamp, timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO participant_state_current(source_heat_id,participant_id,source_key,updated_at_us)
                    VALUES (?,'legacy-car','legacy:1',?)
                    """,
                    (heat_id, timestamp),
                )
                connection.commit()
            finally:
                connection.close()

            self.assertEqual(migrate(path), ["0003", "0004", "0005", "0006", "0007", "0008"])
            connection = connect(path)
            try:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM participants").fetchone()[0], 1)
                self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
                self.assertIn(
                    "state_timer_target_at_us",
                    {row[1] for row in connection.execute("PRAGMA table_info(participant_state_current)")},
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
                finished_frame, finished_message = self.insert_frame(connection, "finished", received_at_us=old)
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
                connection.execute(
                    """
                    INSERT INTO playback_snapshots(
                      source_heat_id,observed_second,observed_at_us,source_frame_id,source_message_id,
                      source_key,projection_version,metric_version,is_event_boundary,payload_codec,payload,
                      payload_sha256,created_at_us,updated_at_us
                    ) VALUES (?,?,?,?,?,'playback:finished',1,1,0,'gzip-json-v1',X'7B7D',?, ?,?)
                    """,
                    (finished_heat, old // 1_000_000, old, finished_frame, finished_message, "0" * 64, timestamp, timestamp),
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
                floor = connection.execute(
                    "SELECT deleted_through_id FROM stream_event_cursor_floors WHERE analysis_session_id = 'finished'"
                ).fetchone()
                self.assertIsNotNone(floor)
                self.assertGreater(floor["deleted_through_id"], 0)
                self.assertIsNone(connection.execute("SELECT source_message_id FROM laps WHERE id='lap-1'").fetchone()[0])
                self.assertEqual(
                    connection.execute("SELECT source_frame_id FROM state_checkpoints WHERE source_heat_id = ?", (finished_heat,)).fetchone()[0],
                    anchor_frame,
                )
                self.assertIsNone(
                    connection.execute(
                        "SELECT source_frame_id FROM playback_snapshots WHERE source_heat_id = ?", (finished_heat,)
                    ).fetchone()[0]
                )
                self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
                self.assertIsNotNone(active_message)
                with self.assertRaises(ValueError):
                    plan_retention(connection, now_at_us=timestamp, raw_days=-1)
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
