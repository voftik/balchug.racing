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
            self.assertEqual(
                migrate(path),
                ["0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0009", "0010", "0011", "0012", "0013", "0014", "0015"],
            )
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
                        "participant_interval_source_facts",
                        "result_schema_baselines",
                        "result_last_cell_ledger",
                        "result_schema_contract_observations",
                        "race_control_message_observations",
                        "race_control_messages_current",
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
                        "gap_interval_fact_id",
                        "diff_interval_fact_id",
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
                        "participant_interval_source_facts",
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
                        "state_source_cell_observation_id",
                        "state_source_message_id",
                        "state_source_key",
                        "state_observed_at_us",
                        "provider_pit_count_source_cell_observation_id",
                        "pit_time_source_cell_observation_id",
                        "pit_time_source_message_id",
                        "driver_stint_kind",
                        "driver_stint_provider_ts_time",
                        "driver_stint_duration_ms",
                        "driver_stint_source_cell_observation_id",
                        "gap_interval_fact_id",
                        "diff_interval_fact_id",
                    }.issubset(state_columns)
                )
                interval_fact_columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(participant_interval_source_facts)")
                }
                self.assertTrue(
                    {
                        "interval_kind",
                        "raw_value",
                        "interval_ms",
                        "value_kind",
                        "source_cell_observation_id",
                        "source_message_id",
                        "source_key",
                        "source_change_ordinal",
                        "source_handle",
                        "observation_kind",
                        "observed_at_us",
                        "source_position_overall",
                        "source_position_class",
                        "source_laps",
                        "source_state_kind",
                        "relation_kind",
                        "target_participant_id",
                        "target_position_overall",
                        "target_state_kind",
                        "target_laps",
                    }.issubset(interval_fact_columns)
                )
                baseline_columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(result_schema_baselines)")
                }
                self.assertTrue(
                    {
                        "ingest_connection_id",
                        "layout_version_id",
                        "layout_generation",
                        "source_frame_id",
                        "source_message_id",
                        "source_message_ordinal",
                        "source_key",
                    }.issubset(baseline_columns)
                )
                last_ledger_columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(result_last_cell_ledger)")
                }
                self.assertTrue(
                    {
                        "source_cell_observation_id",
                        "duration_ms",
                        "classification",
                        "classification_reason",
                        "predecessor_source_cell_observation_id",
                        "schema_baseline_id",
                        "linked_lap_id",
                        "sectors_json",
                        "sectors_source_cell_observation_ids_json",
                    }.issubset(last_ledger_columns)
                )
                contract_columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(result_schema_contract_observations)")
                }
                self.assertTrue(
                    {
                        "layout_version_id",
                        "contract_name",
                        "status",
                        "required_keys_json",
                        "missing_required_keys_json",
                        "binding_mismatches_json",
                        "optional_present_keys_json",
                    }.issubset(contract_columns)
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
                lap_columns = {row[1] for row in connection.execute("PRAGMA table_info(laps)")}
                self.assertTrue(
                    {
                        "completion_passing_observation_id",
                        "duration_source_cell_observation_id",
                        "duration_source_message_id",
                        "duration_source_key",
                        "duration_source_kind",
                        "sectors_source_cell_observation_ids_json",
                    }.issubset(lap_columns)
                )
                pit_columns = {row[1] for row in connection.execute("PRAGMA table_info(pit_stops)")}
                self.assertTrue(
                    {
                        "entered_state_cell_observation_id",
                        "entered_pit_count_cell_observation_id",
                        "entered_at_source_cell_observation_id",
                        "exited_state_cell_observation_id",
                        "pit_lane_duration_source_cell_observation_id",
                        "pit_lane_duration_source_kind",
                    }.issubset(pit_columns)
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

            self.assertEqual(
                migrate(path),
                ["0003", "0004", "0005", "0006", "0007", "0008", "0009", "0010", "0011", "0012", "0013", "0014", "0015"],
            )
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

    def test_schema_contract_migration_backfills_state_source_and_removes_only_synthetic_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "timing.db"
            legacy_migrations = root / "legacy-migrations"
            legacy_migrations.mkdir()
            for migration in sorted((Path(__file__).parent.parent / "migrations").glob("00[0-1][0-9]_*.sql")):
                if migration.name.startswith(("0013_", "0014_", "0015_")):
                    continue
                shutil.copyfile(migration, legacy_migrations / migration.name)
            self.assertEqual(
                migrate(path, directory=legacy_migrations),
                ["0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0009", "0010", "0011", "0012"],
            )
            connection = connect(path)
            try:
                source_id = self.insert_source(connection)
                heat_id = self.insert_session(connection, "pre-0013", source_id)
                observed_at_us = 4_000_000
                _frame_id, message_id = self.insert_frame(
                    connection, "pre-0013", received_at_us=observed_at_us
                )
                source_key = "connection-pre-0013:1:0"
                connection.execute(
                    """
                    INSERT INTO result_layout_versions(
                      source_heat_id,version_ordinal,layout_fingerprint,raw_layout_json,
                      source_message_id,source_key,observed_at_us,created_at_us
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (heat_id, 0, "pre-0013-layout", '{"h":[{"n":"State"}]}', message_id, source_key, observed_at_us, observed_at_us),
                )
                layout_id = connection.execute("SELECT id FROM result_layout_versions").fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO result_column_definitions(
                      layout_version_id,column_index,source_name_raw,canonical_key,raw_definition_json
                    ) VALUES (?,0,'State','state','{"n":"State"}')
                    """,
                    (layout_id,),
                )
                connection.execute(
                    """
                    INSERT INTO result_column_definitions(
                      layout_version_id,column_index,source_name_raw,source_parameter_raw,canonical_key,raw_definition_json
                    ) VALUES (?,1,'SectorTimes','1',NULL,'{"n":"SectorTimes","p":"1"}')
                    """,
                    (layout_id,),
                )
                connection.execute(
                    """
                    INSERT INTO participants(
                      id,source_heat_id,external_key,start_number,team_name,class_name,first_seen_at_us,last_seen_at_us
                    ) VALUES ('car-21',?,'21','21','BALCHUG Racing','CN PRO',?,?)
                    """,
                    (heat_id, observed_at_us, observed_at_us),
                )
                connection.execute(
                    """
                    INSERT INTO participant_result_cell_observations(
                      source_heat_id,participant_id,layout_version_id,provider_row_index,column_index,
                      raw_value_json,value_text,source_message_id,source_key,source_change_ordinal,
                      observed_at_us,created_at_us
                    ) VALUES (?, 'car-21', ?, 0, 0, '["E4000000"]', 'E4000000', ?, ?, 0, ?, ?)
                    """,
                    (heat_id, layout_id, message_id, source_key, observed_at_us, observed_at_us),
                )
                state_cell_id = connection.execute("SELECT id FROM participant_result_cell_observations").fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO participant_state_current(
                      source_heat_id,participant_id,state,state_raw,state_kind,source_message_id,
                      source_key,updated_at_us,state_source_cell_observation_id
                    ) VALUES (?, 'car-21', 'ON_TRACK', 'E4000000', 'ON_TRACK', ?, 'generic:last', ?, ?)
                    """,
                    (heat_id, message_id, observed_at_us + 1, state_cell_id),
                )
                # This is the old erroneous sparse-row artifact: no source
                # cells and no source value. It must disappear in 0013.
                connection.execute(
                    """
                    INSERT INTO participant_state_observations(
                      source_heat_id,participant_id,layout_version_id,provider_row_index,state_kind,
                      source_key,source_event_key,observed_at_us,created_at_us
                    ) VALUES (?, 'car-21', ?, 0, 'UNKNOWN', 'generic:last', 'synthetic:1', ?, ?)
                    """,
                    (heat_id, layout_id, observed_at_us + 1, observed_at_us + 1),
                )
                # A legitimate historic source observation without the newer
                # cell-id columns must survive the conservative cleanup.
                connection.execute(
                    """
                    INSERT INTO participant_state_observations(
                      source_heat_id,participant_id,layout_version_id,provider_row_index,state_raw,state_kind,
                      source_key,source_event_key,observed_at_us,created_at_us
                    ) VALUES (?, 'car-21', ?, 0, 'E4000000', 'ON_TRACK', ?, 'historic:1', ?, ?)
                    """,
                    (heat_id, layout_id, source_key, observed_at_us, observed_at_us),
                )
                connection.commit()
            finally:
                connection.close()

            self.assertEqual(migrate(path), ["0013", "0014", "0015"])
            connection = connect(path)
            try:
                current = connection.execute(
                    """
                    SELECT state_source_message_id,state_source_key,state_observed_at_us
                    FROM participant_state_current
                    """
                ).fetchone()
                self.assertEqual(tuple(current), (message_id, source_key, observed_at_us))
                observations = connection.execute(
                    "SELECT source_event_key FROM participant_state_observations ORDER BY source_event_key"
                ).fetchall()
                self.assertEqual([row[0] for row in observations], ["historic:1"])
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT canonical_key FROM result_column_definitions
                        WHERE layout_version_id = ? AND column_index = 1
                        """,
                        (layout_id,),
                    ).fetchone()[0],
                    "sector_1",
                )
                self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            finally:
                connection.close()

    def test_event_timestamp_guard_migration_clears_only_unsafe_derived_times(self):
        """0014 must retain raw source facts while removing prior false UTC."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "timing.db"
            legacy_migrations = root / "legacy-migrations"
            legacy_migrations.mkdir()
            for migration in sorted((Path(__file__).parent.parent / "migrations").glob("00[0-1][0-9]_*.sql")):
                if migration.name.startswith(("0014_", "0015_")):
                    continue
                shutil.copyfile(migration, legacy_migrations / migration.name)
            self.assertEqual(
                migrate(path, directory=legacy_migrations),
                ["0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0009", "0010", "0011", "0012", "0013"],
            )

            received_at_us = 1_783_697_986_978_444
            false_at_us = received_at_us - 100_000_000_000
            connection = connect(path)
            try:
                source_id = self.insert_source(connection)
                heat_id = self.insert_session(connection, "pre-0014", source_id)
                _frame_id, message_id = self.insert_frame(
                    connection,
                    "pre-0014",
                    received_at_us=received_at_us,
                )
                connection.execute(
                    """
                    INSERT INTO participants(
                      id,source_heat_id,external_key,start_number,team_name,class_name,first_seen_at_us,last_seen_at_us
                    ) VALUES ('car-77',?,'77','77','Anomaly Racing','CN PRO',?,?)
                    """,
                    (heat_id, received_at_us, received_at_us),
                )
                connection.execute(
                    """
                    INSERT INTO participant_state_observations(
                      source_heat_id,participant_id,provider_row_index,state_raw,state_kind,
                      state_timer_target_raw,state_timer_target_provider_us,state_timer_target_at_us,
                      driver_stint_raw,driver_stint_kind,driver_stint_provider_ts_time,driver_stint_at_us,
                      source_message_id,source_key,source_event_key,observed_at_us,created_at_us
                    ) VALUES (?, 'car-77', 0, 'E120000000', 'ON_TRACK',
                              '120000000', 120000000, ?, 'S120000000', 'START_TS', 120000000, ?,
                              ?, 'pre-0014:1:0', 'pre-0014:state', ?, ?)
                    """,
                    (heat_id, false_at_us, false_at_us, message_id, received_at_us, received_at_us),
                )
                connection.execute(
                    """
                    INSERT INTO participant_state_current(
                      source_heat_id,participant_id,state,state_raw,state_kind,
                      state_timer_target_raw,state_timer_target_provider_us,state_timer_target_at_us,
                      state_timer_observed_at_us,driver_stint_kind,driver_stint_provider_ts_time,
                      driver_stint_at_us,driver_stint_observed_at_us,source_key,updated_at_us
                    ) VALUES (?, 'car-77', 'ON_TRACK', 'E120000000', 'ON_TRACK',
                              '120000000', 120000000, ?, ?, 'START_TS', 120000000, ?, ?,
                              'pre-0014:1:0', ?)
                    """,
                    (
                        heat_id,
                        false_at_us,
                        received_at_us,
                        false_at_us,
                        received_at_us,
                        received_at_us,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO pit_stops(
                      id,source_heat_id,participant_id,stop_number,entered_at_us,completed,
                      entered_source_message_id,entered_source_key,entered_at_source_message_id,
                      entered_at_source_key,entered_at_source_kind,created_at_us,updated_at_us
                    ) VALUES ('pit-77',?,'car-77',1,?,0,?,'pre-0014:1:0',?,
                              'pre-0014:1:0','RESULT_L_PIT_S',?,?)
                    """,
                    (heat_id, false_at_us, message_id, message_id, received_at_us, received_at_us),
                )
                connection.commit()
            finally:
                connection.close()

            self.assertEqual(migrate(path), ["0014", "0015"])
            connection = connect(path)
            try:
                observation = connection.execute(
                    """
                    SELECT state_raw,state_timer_target_provider_us,state_timer_target_at_us,
                           driver_stint_raw,driver_stint_provider_ts_time,driver_stint_at_us
                    FROM participant_state_observations
                    """
                ).fetchone()
                self.assertEqual(
                    tuple(observation),
                    ("E120000000", 120_000_000, None, "S120000000", 120_000_000, None),
                )
                current = connection.execute(
                    """
                    SELECT state_raw,state_timer_target_provider_us,state_timer_target_at_us,
                           driver_stint_provider_ts_time,driver_stint_at_us
                    FROM participant_state_current
                    """
                ).fetchone()
                self.assertEqual(tuple(current), ("E120000000", 120_000_000, None, 120_000_000, None))
                pit = connection.execute(
                    """
                    SELECT entered_at_us,entered_source_message_id,entered_at_source_cell_observation_id,
                           entered_at_source_message_id,entered_at_source_key,entered_at_source_kind
                    FROM pit_stops
                    """
                ).fetchone()
                self.assertEqual(
                    tuple(pit),
                    (received_at_us, message_id, None, None, None, None),
                )
                self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
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
