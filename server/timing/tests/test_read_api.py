import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from timing.db import RUNTIME_CHECKPOINT_FORMAT, RUNTIME_CHECKPOINT_FORMAT_VERSION, connect, migrate, save_checkpoint
from timing.normalization import TIME_SERVICE_EPOCH_UNIX_US, parse_ts_time
from timing.normalizer_writer import (
    RUNTIME_CHECKPOINT_PAYLOAD_FORMAT,
    RUNTIME_CHECKPOINT_PAYLOAD_VERSION,
    RUNTIME_CHECKPOINT_REDUCER_VERSION,
)
from timing.read_api import (
    ArchiveProjectionMissingError,
    MetricScopeRequest,
    ReadValidationError,
    ScopeNotFoundError,
    TimingReadModel,
    _archive_comparison_lap_rows,
    _archive_result_last_rows,
    _archive_raw_last_or_lap_rows,
    _bounded_archive_lap_rows,
)


class TimingReadModelTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        self.connection = connect(self.path)
        self._seed()
        self.connection.commit()
        self.model = TimingReadModel(self.path, clock=lambda: 13_000_000)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def _seed(self):
        timestamp = 1_000_000
        self.connection.execute(
            """
            INSERT INTO timing_sources(slug,source_url,adapter_version,created_at_us)
            VALUES ('igora','https://example.test/igora','test',?)
            """,
            (timestamp,),
        )
        source_id = self.connection.execute("SELECT id FROM timing_sources WHERE slug = 'igora'").fetchone()[0]
        self.connection.execute(
            """
            INSERT INTO analysis_sessions(
              id,source_id,mode,lifecycle,identity_state,started_at_us,created_at_us,updated_at_us
            ) VALUES ('session-1',?,'practice','active','resolved',?,?,?)
            """,
            (source_id, timestamp, timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO analysis_sessions(
              id,source_id,mode,lifecycle,identity_state,created_at_us,updated_at_us
            ) VALUES ('pending-1',?,'practice','draft','pending',?,?)
            """,
            (source_id, timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO source_heats(
              analysis_session_id,generation,external_name,provider_started_at_us,created_at_us
            ) VALUES ('session-1',1,'Practice - Open-Pit',?,?)
            """,
            (timestamp, timestamp),
        )
        self.heat_id = self.connection.execute("SELECT id FROM source_heats").fetchone()[0]
        self.connection.execute(
            """
            INSERT INTO participants(
              id,source_heat_id,external_key,start_number,team_name,car_name,class_name,
              class_name_key,is_ours,active,first_seen_at_us,last_seen_at_us
            ) VALUES ('ours',?,'nr:21','21','BALCHUG Racing','Ligier JS53 evo2','CN PRO',
                      'cn pro',1,1,?,?)
            """,
            (self.heat_id, timestamp, 12_000_000),
        )
        self.connection.execute(
            """
            INSERT INTO participant_state_current(
              source_heat_id,participant_id,position_overall,position_class,laps,state,state_raw,
              state_kind,current_driver_name,last_lap_ms,best_lap_ms,last_sectors_json,
              source_key,updated_at_us
            ) VALUES (?,'ours',4,1,12,'ON_TRACK','E123','ON_TRACK','Лобода Михаил',107200,
                      107100,'[35000,36000,36200]','frame:12',12000000)
            """,
            (self.heat_id,),
        )
        self.connection.execute(
            """
            INSERT INTO state_ticks(
              source_heat_id,observed_second,observed_at_us,source_key,state_hash,freshness_ms,created_at_us
            ) VALUES (?,10,10000000,'tick:10','state-10',0,?)
            """,
            (self.heat_id, timestamp),
        )

        self.connection.execute(
            """
            INSERT INTO track_flag_current(
              source_heat_id,flag,provider_code,provider_label,started_at_us,source_key,updated_at_us,
              observed_started_at_us,calibrated_started_at_us
            ) VALUES (?,'GREEN','6','Green flag',9000000,'flag:9',9000000,9000000,9000000)
            """,
            (self.heat_id,),
        )
        self.connection.execute(
            """
            INSERT INTO heat_statistics_current(
              source_heat_id,heat_name_raw,participants_started,total_laps,total_pitstops,
              raw_payload_json,source_key,source_event_key,observed_at_us,updated_at_us
            ) VALUES (?,'Practice - Open-Pit',30,401,66,'{}','stats:12','stats:12',12000000,12000000)
            """,
            (self.heat_id,),
        )
        self.connection.executemany(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,
              flag,is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us
            ) VALUES (?,?,?,?,?,?,?,'GREEN',0,0,0,1,?,?)
            """,
            (
                ("lap-11", self.heat_id, "ours", 11, 11_000_000, 107_500, "[35000,36000,36500]", "lap:11", timestamp),
                ("lap-12", self.heat_id, "ours", 12, 12_000_000, 107_200, "[34800,35900,36500]", "lap:12", timestamp),
            ),
        )
        self.connection.execute(
            """
            INSERT INTO pit_stops(
              id,source_heat_id,participant_id,stop_number,entered_at_us,exited_at_us,entered_lap,
              exited_lap,pit_lane_ms,pit_lane_duration_source_kind,completed,entered_source_key,exited_source_key,created_at_us,updated_at_us
            ) VALUES ('pit-1',?,'ours',1,5000000,5030000,5,5,30000,'RESULT_L_PIT',1,'pit:in','pit:out',?,?)
            """,
            (self.heat_id, timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO metric_current(
              source_heat_id,scope_kind,scope_key,observed_at_us,metric_version,values_json,
              source_key,created_at_us,updated_at_us
            ) VALUES (?,'participant','ours',12000000,1,'{"pace_5_ms":107200}',
                      'metric:12',?,?)
            """,
            (self.heat_id, timestamp, timestamp),
        )
        self.connection.executemany(
            """
            INSERT INTO metric_samples(
              source_heat_id,scope_kind,scope_key,observed_second,observed_at_us,metric_version,
              values_json,source_key,created_at_us
            ) VALUES (?,'participant','ours',?,?,1,?,?,?)
            """,
            (
                (
                    self.heat_id,
                    point + 1,
                    (point + 1) * 1_000_000,
                    json.dumps({"pace_5_ms": 107_000 + point}),
                    f"metric:{point}",
                    timestamp,
                )
                for point in range(800)
            ),
        )
        self.connection.execute(
            """
            INSERT INTO stream_events(
              analysis_session_id,source_heat_id,event_type,payload_json,created_at_us
            ) VALUES ('session-1',?,'state','{}',?)
            """,
            (self.heat_id, timestamp),
        )

    def _add_playback_snapshot(self, observed_at_us, payload, *, event_boundary=False):
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.connection.execute(
            """
            INSERT INTO playback_snapshots(
              source_heat_id,observed_second,observed_at_us,source_key,projection_version,metric_version,
              is_event_boundary,payload_codec,payload,payload_sha256,created_at_us,updated_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                self.heat_id,
                observed_at_us // 1_000_000,
                observed_at_us,
                f"playback:{observed_at_us}",
                1,
                1,
                int(event_boundary),
                "gzip-json-v1",
                gzip.compress(raw, mtime=0),
                hashlib.sha256(raw).hexdigest(),
                observed_at_us,
                observed_at_us,
            ),
        )

    def _add_result_last_cells(self, cells):
        """Seed immutable result-grid LAST evidence for archive read tests."""

        self.connection.execute(
            """
            INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us)
            VALUES ('raw-run','session-1','test',1)
            """
        )
        self.connection.execute(
            """
            INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us)
            VALUES ('raw-connection','raw-run',1,1)
            """
        )
        layout_message_id = None
        cell_ids = []
        for sequence, (handle, value_text, observed_at_us) in enumerate(cells, start=1):
            raw_payload = b"{}"
            self.connection.execute(
                """
                INSERT INTO feed_frames(
                  analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,monotonic_ns,
                  raw_payload,raw_sha256,decode_state,processed_at_us,created_at_us
                ) VALUES ('session-1','raw-connection',?,?,?,?,'hash','decoded',?,?)
                """,
                (sequence, observed_at_us, sequence, raw_payload, observed_at_us, observed_at_us),
            )
            frame_id = self.connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            self.connection.execute(
                """
                INSERT INTO feed_messages(frame_id,ordinal,handle,args_json,compressed,created_at_us)
                VALUES (?,?,?, '[]',0,?)
                """,
                (frame_id, 0, handle, observed_at_us),
            )
            message_id = self.connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            if layout_message_id is None:
                layout_message_id = message_id
                self.connection.execute(
                    """
                    INSERT INTO result_layout_versions(
                      source_heat_id,version_ordinal,layout_fingerprint,raw_layout_json,
                      source_message_id,source_key,observed_at_us,created_at_us
                    ) VALUES (?,1,'layout:last','{}',?,'raw:layout',?,?)
                    """,
                    (self.heat_id, message_id, observed_at_us, observed_at_us),
                )
                layout_id = self.connection.execute("SELECT last_insert_rowid()").fetchone()[0]
                self.connection.execute(
                    """
                    INSERT INTO result_column_definitions(
                      layout_version_id,column_index,source_name_raw,canonical_key,raw_definition_json
                    ) VALUES (?,0,'LAST','last_lap','{}')
                    """,
                    (layout_id,),
                )
            source_key = f"raw:{sequence}"
            self.connection.execute(
                """
                INSERT INTO participant_result_cell_observations(
                  source_heat_id,participant_id,layout_version_id,provider_row_index,column_index,
                  raw_value_json,value_text,source_message_id,source_key,source_change_ordinal,
                  observed_at_us,created_at_us
                ) VALUES (?,'ours',?,0,0,?,?,?,?,0,?,?)
                """,
                (self.heat_id, layout_id, json.dumps([value_text]), value_text, message_id, source_key, observed_at_us, observed_at_us),
            )
            cell_ids.append(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        return cell_ids

    def _add_last_cell_ledger(self, cell_id, *, classification, linked_lap_id=None):
        """Classify one seeded immutable LAST cell as the writer would."""

        row = self.connection.execute(
            """
            SELECT observation.source_heat_id,observation.participant_id,observation.layout_version_id,
                   observation.source_message_id,observation.source_key,
                   observation.source_change_ordinal,observation.observed_at_us,
                   observation.value_text,message.handle,message.ordinal AS message_ordinal,
                   frame.id AS frame_id
            FROM participant_result_cell_observations AS observation
            JOIN feed_messages AS message ON message.id = observation.source_message_id
            JOIN feed_frames AS frame ON frame.id = message.frame_id
            WHERE observation.id = ?
            """,
            (cell_id,),
        ).fetchone()
        assert row is not None
        duration_us = parse_ts_time(row["value_text"])
        duration_ms = duration_us // 1_000 if duration_us is not None else None
        self.connection.execute(
            """
            INSERT INTO result_last_cell_ledger(
              source_cell_observation_id,source_heat_id,participant_id,layout_version_id,
              source_frame_id,source_message_id,source_message_ordinal,source_key,
              source_change_ordinal,source_handle,observed_at_us,duration_ms,
              classification,classification_reason,predecessor_source_cell_observation_id,
              schema_baseline_id,linked_lap_id,sectors_json,
              sectors_source_cell_observation_ids_json,created_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,?,NULL,NULL,?)
            """,
            (
                cell_id,
                row["source_heat_id"],
                row["participant_id"],
                row["layout_version_id"],
                row["frame_id"],
                row["source_message_id"],
                row["message_ordinal"],
                row["source_key"],
                row["source_change_ordinal"],
                row["handle"],
                row["observed_at_us"],
                duration_ms,
                classification,
                f"test:{classification.lower()}",
                linked_lap_id,
                row["observed_at_us"],
            ),
        )

    def _add_interval_source_fact(self, *, raw_value, interval_ms, observed_at_us):
        """Seed one exact current GAP cell independently of cached state."""

        cell_id = self._add_result_last_cells([("r_c", raw_value, observed_at_us)])[0]
        cell = self.connection.execute(
            """
            SELECT layout_version_id,provider_row_index,source_message_id,source_key,source_change_ordinal
            FROM participant_result_cell_observations
            WHERE id = ?
            """,
            (cell_id,),
        ).fetchone()
        self.connection.execute(
            """
            INSERT INTO participant_interval_source_facts(
              source_heat_id,participant_id,interval_kind,raw_value,interval_ms,value_kind,
              source_cell_observation_id,source_message_id,source_key,source_change_ordinal,
              source_handle,observation_kind,observed_at_us,source_layout_version_id,
              source_provider_row_index,source_position_overall,source_position_class,
              source_laps,source_state_kind,created_at_us
            ) VALUES (?,'ours','GAP',?,?,'TIME',?,?,?,?,'r_c','DELTA',?,?,?,4,1,12,'ON_TRACK',?)
            """,
            (
                self.heat_id,
                raw_value,
                interval_ms,
                cell_id,
                cell["source_message_id"],
                cell["source_key"],
                cell["source_change_ordinal"],
                observed_at_us,
                cell["layout_version_id"],
                cell["provider_row_index"],
                observed_at_us,
            ),
        )
        return self.connection.execute("SELECT last_insert_rowid()").fetchone()[0], cell_id

    def test_snapshot_exposes_source_facts_metrics_freshness_and_stream_barrier(self):
        snapshot = self.model.snapshot("session-1").as_dict()

        self.assertEqual(snapshot["schema_version"], "timing-live.v1")
        self.assertEqual(snapshot["heat"]["generation"], 1)
        self.assertEqual(snapshot["freshness"]["status"], "LIVE")
        self.assertEqual(snapshot["freshness"]["age_ms"], 3_000)
        self.assertEqual(snapshot["measured"]["track_flag"]["flag"], "GREEN")
        self.assertEqual(snapshot["measured"]["statistics"]["total_pitstops"], 66)
        participant = snapshot["measured"]["participants"][0]
        self.assertEqual((participant["team_name"], participant["driver_name"], participant["class_name"]), (
            "BALCHUG Racing",
            "Лобода Михаил",
            "CN PRO",
        ))
        self.assertEqual(snapshot["computed"]["metrics"][0]["values"]["pace_5_ms"], 107200)
        self.assertEqual(snapshot["cursor"], snapshot["barrier"])
        self.assertEqual(snapshot["cursor"]["stream_event_id"], 1)
        self.assertTrue(snapshot["system_assumption"]["tyre_change_on_confirmed_pit_out"])

    def test_snapshot_separates_exact_state_source_from_latest_materialized_row(self):
        self.connection.execute(
            """
            UPDATE participant_state_current
            SET state_source_key = 'state:9', state_observed_at_us = 9_000_000
            WHERE source_heat_id = ? AND participant_id = 'ours'
            """,
            (self.heat_id,),
        )
        self.connection.commit()

        state = self.model.snapshot("session-1").as_dict()["measured"]["participants"][0]["state"]
        self.assertEqual(state["source"], {"message_id": None, "key": "frame:12", "observed_at_us": 12_000_000})
        self.assertEqual(
            state["state_source"],
            {"cell_observation_id": None, "message_id": None, "key": "state:9", "observed_at_us": 9_000_000},
        )

    def test_snapshot_interval_scalars_come_only_from_their_exact_source_facts(self):
        self.connection.execute(
            """
            UPDATE participant_state_current
            SET gap_ms = 99_999,gap_raw = '99.999',gap_kind = 'TIME',
                diff_ms = 88_888,diff_raw = '88.888',diff_kind = 'TIME'
            WHERE source_heat_id = ? AND participant_id = 'ours'
            """,
            (self.heat_id,),
        )
        self.connection.commit()

        no_fact_state = self.model.snapshot("session-1").as_dict()["measured"]["participants"][0]["state"]
        self.assertIsNone(no_fact_state["gap_ms"])
        self.assertIsNone(no_fact_state["gap_raw"])
        self.assertIsNone(no_fact_state["diff_ms"])
        self.assertIsNone(no_fact_state["diff_raw"])
        self.assertIsNone(no_fact_state["gap_source_fact"])

        fact_id, cell_id = self._add_interval_source_fact(
            raw_value="1.246",
            interval_ms=1_246,
            observed_at_us=12_500_000,
        )
        self.connection.execute(
            """
            UPDATE participant_state_current
            SET gap_interval_fact_id = ?
            WHERE source_heat_id = ? AND participant_id = 'ours'
            """,
            (fact_id, self.heat_id),
        )
        self.connection.commit()

        state = self.model.snapshot("session-1").as_dict()["measured"]["participants"][0]["state"]
        self.assertEqual((state["gap_ms"], state["gap_raw"], state["gap_kind"]), (1_246, "1.246", "TIME"))
        self.assertEqual(state["gap_source_fact"]["id"], fact_id)
        self.assertEqual(state["gap_source_fact"]["cell_observation_id"], cell_id)
        self.assertEqual(state["gap_source_fact"]["source_handle"], "r_c")
        self.assertEqual(state["gap_source_fact"]["observed_at_us"], 12_500_000)
        self.assertIsNone(state["diff_ms"])
        self.assertIsNone(state["diff_source_fact"])

    def test_terminal_and_open_gap_overrides_are_explicit(self):
        self.connection.execute(
            """
            INSERT INTO ingest_gaps(analysis_session_id,source_heat_id,started_at_us,reason,created_at_us)
            VALUES ('session-1',?,12000000,'socket_closed',12000000)
            """,
            (self.heat_id,),
        )
        self.connection.commit()
        self.assertEqual(self.model.snapshot("session-1").freshness.reason, "source_gap")

        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        self.connection.commit()
        self.assertEqual(self.model.snapshot("session-1").freshness.reason, "session_stopped")

        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'active' WHERE id = 'session-1'")
        self.connection.execute("DELETE FROM ingest_gaps WHERE source_heat_id = ?", (self.heat_id,))
        self.connection.execute("UPDATE track_flag_current SET flag = 'FINISH' WHERE source_heat_id = ?", (self.heat_id,))
        self.connection.commit()
        self.assertEqual(self.model.snapshot("session-1").freshness.reason, "track_finished")

    def test_session_level_open_gap_is_visible_to_read_model(self):
        self.connection.execute(
            """
            INSERT INTO ingest_gaps(analysis_session_id,source_heat_id,started_at_us,reason,created_at_us)
            VALUES ('session-1',NULL,12000000,'connection_reset',12000000)
            """
        )
        self.connection.commit()

        snapshot = self.model.snapshot("session-1")
        self.assertEqual(snapshot.freshness.reason, "source_gap")
        self.assertEqual(snapshot.freshness.open_gap["reason"], "connection_reset")

    def test_pending_session_is_a_valid_empty_snapshot_and_read_surface(self):
        snapshot = self.model.snapshot("pending-1").as_dict()
        self.assertIsNone(snapshot["heat"])
        self.assertEqual(snapshot["freshness"]["reason"], "no_source_heat")
        self.assertEqual(snapshot["computed"]["metrics"], [])
        self.assertEqual(self.model.current_metrics("pending-1")["metrics"], [])
        self.assertEqual(
            self.model.metric_history(
                "pending-1", scope=MetricScopeRequest("session", "pending-1")
            )["points"],
            [],
        )

    def test_history_is_allowlisted_bounded_and_preserves_both_endpoints(self):
        history = self.model.metric_history(
            "session-1",
            scope=MetricScopeRequest("participant", "ours"),
            from_at_us=1_000_000,
            to_at_us=800_000_000,
            max_points=100,
        )

        self.assertEqual(history["source_point_count"], 800)
        self.assertTrue(history["downsampled"])
        self.assertLessEqual(len(history["points"]), 100)
        self.assertEqual(history["points"][0]["values"]["pace_5_ms"], 107000)
        self.assertEqual(history["points"][-1]["values"]["pace_5_ms"], 107799)
        with self.assertRaises(ScopeNotFoundError):
            self.model.metric_history(
                "session-1",
                scope=MetricScopeRequest("participant", "not-a-crew"),
            )

    def test_lap_and_pit_reads_are_bounded_and_filter_to_a_known_participant(self):
        laps = self.model.laps("session-1", participant_id="ours", limit=1)
        pits = self.model.pit_stops("session-1", participant_id="ours", limit=1)

        self.assertEqual([item["lap_number"] for item in laps["items"]], [12])
        self.assertEqual(pits["items"][0]["pit_lane_ms"], 30_000)
        self.assertEqual(pits["items"][0]["provenance"], "measured")
        with self.assertRaises(ScopeNotFoundError):
            self.model.laps("session-1", participant_id="not-a-crew")

    def test_ingest_health_exposes_checkpoint_tail_and_recovery_evidence(self):
        """Health must distinguish work waiting after a checkpoint from failed RAW."""

        created_at_us = 20_000_000
        self.connection.execute(
            """
            INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us)
            VALUES ('health-run','session-1','test',?)
            """,
            (created_at_us,),
        )
        self.connection.execute(
            """
            INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us)
            VALUES ('health-connection','health-run',1,?)
            """,
            (created_at_us,),
        )
        frame_ids: list[int] = []
        for sequence, decode_state, processed_at_us in (
            (1, "decoded", 21_000_000),
            (2, "decoded", 22_000_000),
            (3, "pending", None),
            (4, "failed", None),
        ):
            self.connection.execute(
                """
                INSERT INTO feed_frames(
                  analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,monotonic_ns,
                  raw_payload,raw_sha256,decode_state,processed_at_us,created_at_us
                ) VALUES ('session-1','health-connection',?,?,?,?,'health-hash',?,?,?)
                """,
                (
                    sequence,
                    sequence * 1_000_000,
                    sequence,
                    b"{}",
                    decode_state,
                    processed_at_us,
                    created_at_us,
                ),
            )
            frame_ids.append(int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0]))

        save_checkpoint(
            self.connection,
            source_heat_id=self.heat_id,
            source_frame_id=frame_ids[0],
            source_key="health-connection:1",
            observed_at_us=1_000_000,
            state={
                "format": RUNTIME_CHECKPOINT_PAYLOAD_FORMAT,
                "format_version": RUNTIME_CHECKPOINT_PAYLOAD_VERSION,
                "reducer_version": RUNTIME_CHECKPOINT_REDUCER_VERSION,
                "analysis_session_id": "session-1",
                "source_heat": {
                    "id": self.heat_id,
                    "generation": 1,
                    "provider_heat_start_ts": None,
                },
                "anchor": {
                    "source_frame_id": frame_ids[0],
                    "source_key": "health-connection:1",
                    "observed_at_us": 1_000_000,
                },
                "reducer": {
                    "heat": {},
                    "statistics": {},
                    "grid": {
                        "layout": None,
                        "rows": {},
                        "metadata_changes": [],
                        "layout_generation": 0,
                        "schema_pending": True,
                        "schema_conflicts": {},
                    },
                    "layout_version": {"id": None, "fingerprint": None},
                    "calibrators": {},
                    "finish_sector_ids": [],
                    "schema_baselines": {},
                },
            },
            checkpoint_format=RUNTIME_CHECKPOINT_FORMAT,
            checkpoint_format_version=RUNTIME_CHECKPOINT_FORMAT_VERSION,
            reducer_version=RUNTIME_CHECKPOINT_REDUCER_VERSION,
        )
        checkpoint_id = int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        self.connection.execute(
            """
            INSERT INTO ingest_gaps(
              analysis_session_id,source_heat_id,ingest_connection_id,started_at_us,reason,created_at_us
            ) VALUES ('session-1',?,'health-connection',?,'connection_reset',?)
            """,
            (self.heat_id, 23_000_000, created_at_us),
        )
        self.connection.execute(
            """
            INSERT INTO normalizer_restore_events(
              analysis_session_id,source_heat_id,checkpoint_id,anchor_frame_id,outcome,reason,
              replayed_tail_frames,created_at_us
            ) VALUES ('session-1',?,?,?,'RESTORED','checkpoint_restored',1,?)
            """,
            (self.heat_id, checkpoint_id, frame_ids[0], 24_000_000),
        )
        self.connection.commit()

        health = self.model.ingest_health("session-1")

        self.assertEqual(health["schema_version"], "timing-live.v1")
        self.assertEqual(health["raw"]["retained_frame_count"], 4)
        self.assertEqual(health["raw"]["decoded_frame_count"], 2)
        self.assertEqual(health["raw"]["latest_frame"]["frame_id"], frame_ids[3])
        self.assertEqual(health["processing"], {
            "processed_frame_count": 2,
            "pending_frame_count": 1,
            "failed_frame_count": 1,
            "latest_processed_frame": {
                "frame_id": frame_ids[1],
                "ingest_connection_id": "health-connection",
                "frame_sequence": 2,
                "received_at_us": 2_000_000,
                "decode_state": "decoded",
                "processed_at_us": 22_000_000,
            },
        })
        self.assertEqual(health["runtime_checkpoints"]["runtime_checkpoint_count"], 1)
        self.assertEqual(health["runtime_checkpoints"]["eligible_runtime_checkpoint_count"], 1)
        self.assertEqual(
            health["runtime_checkpoints"]["latest_validation"],
            {
                "status": "RESTORABLE",
                "rejected_newer_or_incompatible_checkpoint_count": 0,
                "scan_limit": 8,
                "truncated": False,
            },
        )
        self.assertEqual(health["runtime_checkpoints"]["latest"]["checkpoint_id"], checkpoint_id)
        self.assertEqual(health["runtime_checkpoints"]["latest"]["source_frame_id"], frame_ids[0])
        self.assertEqual(health["tail"]["anchor_frame_id"], frame_ids[0])
        self.assertEqual(health["tail"]["scope"], "after_latest_runtime_checkpoint")
        self.assertEqual(health["tail"]["retained_frame_count"], 3)
        self.assertEqual(health["tail"]["processed_frame_count"], 1)
        self.assertEqual(health["tail"]["pending_frame_count"], 1)
        self.assertEqual(health["tail"]["failed_frame_count"], 1)
        self.assertEqual(health["tail"]["received_span_us"], 2_000_000)
        self.assertEqual(health["tail"]["first_frame"]["frame_id"], frame_ids[1])
        self.assertEqual(health["tail"]["latest_frame"]["frame_id"], frame_ids[3])
        self.assertEqual(health["open_gap"], {
            "gap_id": health["open_gap"]["gap_id"],
            "source_heat_id": self.heat_id,
            "connection_id": "health-connection",
            "started_at_us": 23_000_000,
            "reason": "connection_reset",
        })
        self.assertEqual(health["last_restore"], {
            "restore_event_id": health["last_restore"]["restore_event_id"],
            "source_heat_id": self.heat_id,
            "checkpoint_id": checkpoint_id,
            "anchor_frame_id": frame_ids[0],
            "outcome": "RESTORED",
            "reason": "checkpoint_restored",
            "replayed_tail_frames": 1,
            "created_at_us": 24_000_000,
        })

        self.connection.execute("UPDATE state_checkpoints SET state_hash = 'corrupt' WHERE id = ?", (checkpoint_id,))
        self.connection.commit()
        corrupt = self.model.ingest_health("session-1")
        self.assertIsNone(corrupt["runtime_checkpoints"]["latest"])
        self.assertEqual(
            corrupt["runtime_checkpoints"]["latest_validation"],
            {
                "status": "NO_RESTORABLE",
                "rejected_newer_or_incompatible_checkpoint_count": 1,
                "scan_limit": 8,
                "truncated": False,
            },
        )
        self.assertIsNone(corrupt["tail"]["anchor_frame_id"])
        self.assertEqual(corrupt["tail"]["scope"], "all_retained_raw_no_checkpoint")

    def test_race_control_read_keeps_current_board_and_immutable_observations_distinct(self):
        """Receipt time and exact SignalR evidence survive an ended session."""

        observed_at_us = 20_000_000
        self.connection.execute(
            """
            INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us)
            VALUES ('race-control-run','session-1','test',?)
            """,
            (observed_at_us,),
        )
        self.connection.execute(
            """
            INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us)
            VALUES ('race-control-connection','race-control-run',1,?)
            """,
            (observed_at_us,),
        )
        self.connection.execute(
            """
            INSERT INTO feed_frames(
              analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,monotonic_ns,
              raw_payload,raw_sha256,decode_state,processed_at_us,created_at_us
            ) VALUES ('session-1','race-control-connection',1,?,?,?,'race-control-hash','decoded',?,?)
            """,
            (observed_at_us, observed_at_us, b"{}", observed_at_us, observed_at_us),
        )
        frame_id = self.connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.connection.execute(
            """
            INSERT INTO feed_messages(frame_id,ordinal,handle,args_json,compressed,created_at_us)
            VALUES (?,0,'m_i','[]',0,?)
            """,
            (frame_id, observed_at_us),
        )
        source_message_id = self.connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        live_text = "№1 - Нарушение границы гоночной дорожки в Т12 - Аннулирование результата круга 4"
        live_raw = json.dumps(
            {"Id": "race-control-live", "t": live_text, "l": 2, "m": 0, "bc": "255,102,0", "fc": "0,0,0"},
            ensure_ascii=False,
        )
        removed_raw = json.dumps({"Id": "race-control-removed", "t": "Black flag"})
        self.connection.executemany(
            """
            INSERT INTO race_control_messages_current(
              source_heat_id,message_id_raw,text_raw,line,modality,background_color_raw,font_color_raw,
              raw_record_json,is_active,first_observed_at_us,last_observed_at_us,removed_at_us,
              provider_occurred_at_us,first_observation_kind,last_action,
              first_source_message_id,first_source_key,first_source_change_ordinal,
              last_source_message_id,last_source_key,last_source_change_ordinal,
              removal_action,removed_source_frame_id,removed_source_message_id,removed_source_key,
              removed_source_change_ordinal,removed_observation_id,created_at_us,updated_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                (
                    self.heat_id,
                    "race-control-live",
                    live_text,
                    2,
                    0,
                    "255,102,0",
                    "0,0,0",
                    live_raw,
                    1,
                    observed_at_us,
                    22_000_000,
                    None,
                    None,
                    "INITIAL_SNAPSHOT",
                    "UPSERT",
                    source_message_id,
                    "race-control:1:0",
                    0,
                    source_message_id,
                    "race-control:1:2",
                    2,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    observed_at_us,
                    22_000_000,
                ),
                (
                    self.heat_id,
                    "race-control-removed",
                    "Black flag",
                    1,
                    0,
                    None,
                    None,
                    removed_raw,
                    0,
                    observed_at_us,
                    21_000_000,
                    21_000_000,
                    None,
                    "INITIAL_SNAPSHOT",
                    "DELETE",
                    source_message_id,
                    "race-control:1:1",
                    1,
                    source_message_id,
                    "race-control:1:3",
                    3,
                    "DELETE",
                    frame_id,
                    source_message_id,
                    "race-control:1:3",
                    3,
                    None,
                    observed_at_us,
                    21_000_000,
                ),
            ),
        )
        self.connection.executemany(
            """
            INSERT INTO race_control_message_observations(
              source_heat_id,source_handle,operation,message_id_raw,text_raw,line,modality,
              background_color_raw,font_color_raw,provider_occurred_at_us,raw_record_json,raw_payload_json,
              source_frame_id,source_message_id,source_message_ordinal,source_key,source_change_ordinal,
              observed_at_us,created_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                (
                    self.heat_id,
                    "m_i",
                    "INITIAL_SNAPSHOT",
                    "race-control-live",
                    live_text,
                    2,
                    0,
                    "255,102,0",
                    "0,0,0",
                    None,
                    live_raw,
                    live_raw,
                    frame_id,
                    source_message_id,
                    0,
                    "race-control:1:0",
                    0,
                    observed_at_us,
                    observed_at_us,
                ),
                (
                    self.heat_id,
                    "m_i",
                    "INITIAL_SNAPSHOT",
                    "race-control-removed",
                    "Black flag",
                    1,
                    0,
                    None,
                    None,
                    None,
                    removed_raw,
                    removed_raw,
                    frame_id,
                    source_message_id,
                    0,
                    "race-control:1:1",
                    1,
                    observed_at_us,
                    observed_at_us,
                ),
                (
                    self.heat_id,
                    "m_d",
                    "DELETE",
                    "race-control-removed",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    json.dumps({"Id": "race-control-removed"}),
                    json.dumps({"Id": "race-control-removed"}),
                    frame_id,
                    source_message_id,
                    0,
                    "race-control:1:3",
                    3,
                    21_000_000,
                    21_000_000,
                ),
                (
                    self.heat_id,
                    "m_c",
                    "UPSERT",
                    "race-control-live",
                    live_text,
                    2,
                    0,
                    "255,102,0",
                    "0,0,0",
                    None,
                    live_raw,
                    live_raw,
                    frame_id,
                    source_message_id,
                    0,
                    "race-control:1:2",
                    2,
                    22_000_000,
                    22_000_000,
                ),
            ),
        )
        self.connection.commit()

        payload = self.model.race_control_messages("session-1")
        self.assertEqual(payload["current_source_count"], 2)
        self.assertEqual(payload["observation_source_count"], 4)
        self.assertEqual([item["message_id"] for item in payload["items"]], ["race-control-live", "race-control-removed"])
        current = payload["items"][0]
        self.assertTrue(current["is_active"])
        self.assertEqual(current["text_raw"], live_text)
        self.assertIsNone(current["provider_occurred_at_us"])
        self.assertEqual(current["first_observation"]["observed_at_us"], observed_at_us)
        self.assertEqual(current["last_observation"]["source"], {
            "message_id": source_message_id,
            "key": "race-control:1:2",
            "message_ordinal": 0,
            "source_change_ordinal": 2,
        })
        self.assertEqual([item["action"] for item in payload["observations"]], ["INITIAL_SNAPSHOT", "INITIAL_SNAPSHOT", "DELETE", "UPSERT"])
        self.assertEqual(payload["observations"][0]["raw_payload"]["t"], live_text)
        self.assertIsNone(payload["observations"][0]["provider_occurred_at_us"])

        active = self.model.race_control_messages("session-1", active_only=True, limit=1, observation_limit=2)
        self.assertEqual(active["current_source_count"], 1)
        self.assertEqual(active["observation_source_count"], 4)
        self.assertEqual([item["message_id"] for item in active["items"]], ["race-control-live"])
        self.assertEqual([item["action"] for item in active["observations"]], ["DELETE", "UPSERT"])
        with self.assertRaises(ReadValidationError):
            self.model.race_control_messages("session-1", active_only=1)  # type: ignore[arg-type]

        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        self.connection.commit()
        self.assertEqual(self.model.race_control_messages("session-1")["items"][0]["message_id"], "race-control-live")

    def test_unproven_pit_duration_is_null_in_every_public_archive_surface(self):
        self.connection.execute(
            "UPDATE pit_stops SET pit_lane_duration_source_kind = NULL WHERE id = 'pit-1'"
        )
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")

        def payload(observed_at_us):
            return {
                "schema_version": "timing-archive.v1",
                "observed_at_us": observed_at_us,
                "computed": {"session": {"ours_participant_id": "ours"}},
                "class_participants": [
                    {
                        "measured": {
                            "participant_id": "ours",
                            "start_number": "21",
                            "team_name": "BALCHUG Racing",
                            "car_name": "Ligier JS53 evo2",
                            "class_name": "CN PRO",
                            "is_ours": True,
                            "state": {"state_kind": "ON_TRACK"},
                        },
                        "computed": {
                            "participant_id": "ours",
                            "is_ours": True,
                            "current_state": "ON_TRACK",
                            "pace_5_ms": 107_200,
                        },
                    }
                ],
            }

        self._add_playback_snapshot(2_000_000, payload(2_000_000))
        self._add_playback_snapshot(7_000_000, payload(7_000_000))
        self.connection.commit()

        fact = self.model.pit_stops("session-1", participant_id="ours", limit=1)
        manifest = self.model.archive_manifest("session-1")
        comparison = self.model.archive_comparison("session-1")

        self.assertTrue(comparison["comparison"]["available"])
        self.assertIsNone(fact["items"][0]["pit_lane_ms"])
        self.assertEqual(fact["items"][0]["provenance"], "observed_boundaries")
        self.assertIsNone(manifest["markers"]["pits"][0]["pit_lane_ms"])
        self.assertIsNone(comparison["pit_stops"][0]["pit_lane_ms"])

    def test_archive_manifest_and_seek_use_only_durable_playback_keyframes(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        for observed_at_us, position, event_boundary in (
            (2_000_000, 4, False),
            (5_000_000, 3, True),
            (7_000_000, 2, False),
        ):
            self._add_playback_snapshot(
                observed_at_us,
                {
                    "schema_version": "timing-archive.v1",
                    "observed_at_us": observed_at_us,
                    "measured": {"track_flag": {"flag": "GREEN"}},
                    "computed": {"session": {"position_overall": position, "pace_5_ms": 107_000 + position}},
                    "class_participants": [{"measured": {"participant_id": "competitor"}}],
                },
                event_boundary=event_boundary,
            )
        self.connection.execute(
            """
            INSERT INTO track_flag_periods(
              source_heat_id,flag,started_at_us,ended_at_us,source_key,created_at_us
            ) VALUES (?,'RED',4000000,6000000,'flag:4',4000000)
            """,
            (self.heat_id,),
        )
        self.connection.commit()

        sessions = self.model.archived_sessions()
        self.assertEqual([item["session"]["id"] for item in sessions["items"]], ["session-1"])
        manifest = self.model.archive_manifest("session-1")
        self.assertEqual(manifest["schema_version"], "timing-archive.v1")
        self.assertEqual(manifest["range"]["source_point_count"], 3)
        self.assertEqual([point["observed_at_us"] for point in manifest["keyframes"]], [2_000_000, 5_000_000, 7_000_000])
        self.assertTrue(manifest["keyframes"][1]["is_event_boundary"])
        self.assertNotIn("class_participants", manifest["keyframes"][1]["snapshot"])
        self.assertEqual(manifest["markers"]["flags"][0]["flag"], "RED")

        snapshot = self.model.archive_snapshot("session-1", at_us=6_000_000)
        self.assertEqual(snapshot["playback"]["requested_at_us"], 6_000_000)
        self.assertEqual(snapshot["playback"]["effective_at_us"], 5_000_000)
        self.assertEqual(snapshot["playback"]["next_at_us"], 7_000_000)
        self.assertEqual(snapshot["snapshot"]["computed"]["session"]["position_overall"], 3)
        self.assertEqual(snapshot["snapshot"]["class_participants"][0]["measured"]["participant_id"], "competitor")
        with self.assertRaises(ReadValidationError):
            self.model.archive_snapshot("session-1", at_us=1_999_999)

    def test_archive_intervals_do_not_synthesize_a_legacy_source_gap(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")

        def participant(participant_id, position, source_laps, gap_ms, *, ours=False):
            return {
                "measured": {
                    "participant_id": participant_id,
                    "is_ours": ours,
                    "state": {
                        "position_overall": position,
                        "position_class": position,
                        "laps": source_laps,
                        "state_kind": "ON_TRACK",
                        "gap_ms": gap_ms,
                        "gap_kind": "TIME" if gap_ms is not None else None,
                    },
                },
                "computed": {
                    "participant_id": participant_id,
                    "is_ours": ours,
                    "position_overall": position,
                    "position_class": position,
                    "source_gap_ms": gap_ms,
                    "source_diff_ms": None,
                },
            }

        def payload(observed_at_us, source_laps):
            return {
                "schema_version": "timing-archive.v1",
                "observed_at_us": observed_at_us,
                "measured": {"track_flag": {"flag": "GREEN"}},
                "computed": {
                    "session": {
                        "ours_participant_id": "ours",
                        "class_leader_id": "ours",
                        "class_ahead_id": None,
                        "class_behind_id": "behind",
                        # This legacy projection retains cached grid values,
                        # but not source-bound interval provenance.
                        "gap_to_behind_ms": None,
                        "lap_delta_to_behind": 10,
                    },
                },
                "class_participants": [
                    participant("ours", 1, source_laps[0], None, ours=True),
                    participant("behind", 2, source_laps[1], 1_246),
                ],
            }

        self._add_playback_snapshot(2_000_000, payload(2_000_000, (None, None)))
        self._add_playback_snapshot(4_000_000, payload(4_000_000, (12, 2)))
        self.connection.commit()

        manifest = self.model.archive_manifest("session-1")
        partial = manifest["keyframes"][0]["snapshot"]["archive_intervals"]
        explicit = manifest["keyframes"][1]["snapshot"]["archive_intervals"]

        self.assertEqual(partial["lap_count_scope"], "capture_tracker")
        self.assertEqual(partial["relations"]["class_behind"]["status"], "UNAVAILABLE_PROVENANCE")
        self.assertEqual(partial["relations"]["class_behind"]["source_facts"], [])
        self.assertIsNone(partial["gap_to_behind_ms"])
        self.assertIsNone(partial["lap_delta_to_behind"])
        self.assertEqual(explicit["lap_count_scope"], "source_grid")
        self.assertEqual(explicit["relations"]["class_behind"]["status"], "UNAVAILABLE_PROVENANCE")
        self.assertIsNone(explicit["gap_to_behind_ms"])
        self.assertEqual(explicit["lap_delta_to_behind"], 10)

    def test_archive_intervals_preserve_engine_relation_provenance(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        source_fact = {
            "id": 17,
            "field_kind": "GAP",
            "raw_value": "1.246",
            "value_ms": 1_246,
            "value_kind": "TIME",
            "cell_observation_id": 71,
            "source_message_id": 23,
            "source_key": "r_c:23",
            "source_change_ordinal": 4,
            "observed_at_us": 2_000_000,
            "source_handle": "r_c",
            "observation_kind": "DELTA",
            "subject_position_overall": 2,
            "subject_state_kind": "ON_TRACK",
            "subject_laps": 12,
            "target_participant_id": "ours",
            "target_position_overall": 1,
            "target_state_kind": "ON_TRACK",
            "target_laps": 12,
            "relation_kind": "OVERALL_LEADER",
        }
        payload = {
            "schema_version": "timing-archive.v1",
            "observed_at_us": 2_000_000,
            "measured": {"track_flag": {"flag": "GREEN"}},
            "computed": {
                "session": {
                    "ours_participant_id": "ours",
                    "class_leader_id": "ours",
                    "class_ahead_id": None,
                    "class_behind_id": "behind",
                    # A stale compatibility scalar must not override the
                    # structured relation below.
                    "gap_to_behind_ms": 99_999,
                    "relation_intervals": {
                        "class_behind": {
                            "target_participant_id": "behind",
                            "status": "VALID",
                            "value_ms": 1_246,
                            "relation_kind": "GAP_PAIR_COMMON_OVERALL_LEADER",
                            "source_facts": [source_fact],
                            "source_observed_at_us": 2_000_000,
                            "source_age_ms": 0,
                            "ours_state_kind": "ON_TRACK",
                            "target_state_kind": "ON_TRACK",
                            "ours_laps": 12,
                            "target_laps": 12,
                        },
                    },
                },
            },
            "class_participants": [
                {
                    "measured": {
                        "participant_id": "ours",
                        "is_ours": True,
                        "state": {"position_overall": 1, "laps": 12, "state_kind": "ON_TRACK"},
                    },
                    "computed": {"participant_id": "ours", "position_overall": 1},
                },
                {
                    "measured": {
                        "participant_id": "behind",
                        "state": {"position_overall": 2, "laps": 12, "state_kind": "ON_TRACK"},
                    },
                    "computed": {"participant_id": "behind", "position_overall": 2},
                },
            ],
        }
        self._add_playback_snapshot(2_000_000, payload)
        self.connection.commit()

        intervals = self.model.archive_snapshot("session-1", at_us=2_000_000)["snapshot"]["archive_intervals"]
        relation = intervals["relations"]["class_behind"]
        self.assertEqual((relation["status"], relation["value_ms"]), ("VALID", 1_246))
        self.assertEqual(relation["relation_kind"], "GAP_PAIR_COMMON_OVERALL_LEADER")
        self.assertEqual(relation["source_facts"], [source_fact])
        self.assertEqual(intervals["gap_to_behind_ms"], 1_246)

    def test_archive_intervals_preserve_non_racing_engine_status_without_raw_fallback(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        payload = {
            "schema_version": "timing-archive.v1",
            "observed_at_us": 2_000_000,
            "measured": {"track_flag": {"flag": "GREEN"}},
            "computed": {
                "session": {
                    "ours_participant_id": "ours",
                    "class_leader_id": "ours",
                    "class_ahead_id": None,
                    "class_behind_id": "behind",
                    "relation_intervals": {
                        "class_behind": {
                            "target_participant_id": "behind",
                            "status": "NON_RACING_STATE",
                            "value_ms": None,
                            "relation_kind": None,
                            "source_facts": [],
                            "source_observed_at_us": None,
                            "source_age_ms": None,
                            "ours_state_kind": "ON_TRACK",
                            "target_state_kind": "IN_PIT",
                            "ours_laps": 12,
                            "target_laps": 12,
                        },
                    },
                },
            },
            "class_participants": [
                {
                    "measured": {
                        "participant_id": "ours",
                        "is_ours": True,
                        "state": {"position_overall": 1, "state_kind": "ON_TRACK", "gap_ms": None},
                    },
                    "computed": {"participant_id": "ours", "position_overall": 1},
                },
                {
                    "measured": {
                        "participant_id": "behind",
                        "state": {"position_overall": 2, "state_kind": "IN_PIT", "gap_ms": 1_246, "gap_kind": "TIME"},
                    },
                    "computed": {"participant_id": "behind", "position_overall": 2, "source_gap_ms": 1_246},
                },
            ],
        }
        self._add_playback_snapshot(2_000_000, payload)
        self.connection.commit()

        snapshot = self.model.archive_snapshot("session-1", at_us=2_000_000)
        relation = snapshot["snapshot"]["archive_intervals"]["relations"]["class_behind"]
        self.assertEqual(relation["status"], "NON_RACING_STATE")
        self.assertIsNone(relation["value_ms"])
        self.assertIsNone(snapshot["snapshot"]["archive_intervals"]["gap_to_behind_ms"])

    def test_archive_comparison_returns_one_bounded_competitor_benchmark(self):
        def participant(participant_id, number, team, pace, state, *, ours=False):
            return {
                "measured": {
                    "participant_id": participant_id,
                    "start_number": number,
                    "team_name": team,
                    "car_name": "Ligier",
                    "class_name": "CN PRO",
                    "is_ours": ours,
                    "state": {"state_kind": state, "driver_name": team + " Driver"},
                },
                "computed": {
                    "participant_id": participant_id,
                    "start_number": number,
                    "team_name": team,
                    "car_name": "Ligier",
                    "class_name": "CN PRO",
                    "is_ours": ours,
                    "current_state": state,
                    "pace_5_ms": pace,
                },
            }

        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        self.connection.executemany(
            """
            INSERT INTO participants(
              id,source_heat_id,external_key,start_number,team_name,car_name,class_name,class_name_key,
              is_ours,active,first_seen_at_us,last_seen_at_us
            ) VALUES (?,?,?,?,?,?,?,?,0,1,?,?)
            """,
            (
                ("nr-9", self.heat_id, "nr:9", "9", "Pro Motorsport", "Norma", "CN PRO", "cn pro", 1_000_000, 7_000_000),
                ("nr-29", self.heat_id, "nr:29", "29", "TEAMGARIS 29", "Ligier", "CN PRO", "cn pro", 1_000_000, 7_000_000),
                ("nr-77", self.heat_id, "nr:77", "77", "Retired CN PRO", "Ligier", "CN PRO", "cn pro", 1_000_000, 3_000_000),
            ),
        )
        self.connection.executemany(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,
              flag,is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us
            ) VALUES (?,?,?,?,?,?,?,'GREEN',0,0,0,1,?,?)
            """,
            (
                ("lap-9-1", self.heat_id, "nr-9", 1, 2_000_000, 109_000, "[]", "lap:9:1", 2_000_000),
                ("lap-9-2", self.heat_id, "nr-9", 2, 3_000_000, 109_200, "[]", "lap:9:2", 3_000_000),
                ("lap-29-1", self.heat_id, "nr-29", 1, 5_000_000, 110_600, "[]", "lap:29:1", 5_000_000),
            ),
        )
        self.connection.executemany(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,
              flag,is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                # The source can advance LAPS without publishing a duration.
                # It remains a raw engineer-facing lap, even though it must
                # not influence the legacy clean-lap benchmark.
                (
                    "lap-9-null-duration", self.heat_id, "nr-9", 3, 4_000_000, None, "[]",
                    "YELLOW", 0, 1, 0, 0, "lap:9:3", 4_000_000,
                ),
                # A skipped grid number has neither a trusted completion time
                # nor a duration. It is still a durable raw source fact.
                (
                    "lap-9-unplaced", self.heat_id, "nr-9", 4, None, None, None,
                    None, 0, 0, 0, 0, "lap:9:4", 4_000_000,
                ),
                # Facts can be written after the last retained playback
                # keyframe. The raw series is a heat fact surface, not a
                # keyframe-derived chart, so it must retain this as well.
                (
                    "lap-9-after-playback", self.heat_id, "nr-9", 5, 8_000_000, 112_000, "[]",
                    "GREEN", 0, 0, 0, 1, "lap:9:5", 8_000_000,
                ),
            ),
        )
        self.connection.execute(
            """
            INSERT INTO pit_stops(
              id,source_heat_id,participant_id,stop_number,entered_at_us,exited_at_us,entered_lap,
              exited_lap,pit_lane_ms,completed,entered_source_key,exited_source_key,created_at_us,updated_at_us
            ) VALUES ('pit-9',?,'nr-9',1,3000000,3040000,1,1,40000,1,'pit:9:in','pit:9:out',3000000,3040000)
            """,
            (self.heat_id,),
        )
        for observed_at_us, ours_pace, first_pace, second_pace, second_state in (
            (2_000_000, 108_000, 109_000, 110_000, "IN_PIT"),
            (5_000_000, 107_600, 108_600, 110_600, "ON_TRACK"),
            (7_000_000, 107_400, 108_200, 109_200, "ON_TRACK"),
        ):
            self._add_playback_snapshot(
                observed_at_us,
                {
                    "schema_version": "timing-archive.v1",
                    "observed_at_us": observed_at_us,
                    "computed": {"session": {"ours_participant_id": "ours"}},
                    "class_participants": [
                        participant("ours", "21", "BALCHUG Racing", ours_pace, "ON_TRACK", ours=True),
                        participant("nr-9", "9", "Pro Motorsport", first_pace, "ON_TRACK"),
                        participant("nr-29", "29", "TEAMGARIS 29", second_pace, second_state),
                    ],
                },
            )
        self.connection.commit()

        aggregate = self.model.archive_comparison("session-1", mode="all")
        self.assertTrue(aggregate["comparison"]["available"])
        self.assertEqual(aggregate["comparison"]["ours_participant_id"], "ours")
        self.assertEqual([item["participant_id"] for item in aggregate["participants"]], ["ours", "nr-9", "nr-29", "nr-77"])
        self.assertEqual(aggregate["points"][0]["benchmark_pace_5_ms"], 109_000.0)
        self.assertEqual(aggregate["points"][0]["benchmark_participant_count"], 1)
        self.assertEqual(aggregate["points"][1]["benchmark_pace_5_ms"], 109_600.0)
        self.assertEqual(aggregate["points"][1]["benchmark_p25_pace_5_ms"], 109_100.0)
        self.assertEqual(aggregate["points"][1]["benchmark_p75_pace_5_ms"], 110_100.0)
        self.assertEqual(aggregate["pit_stops"][0]["participant_id"], "nr-9")
        self.assertEqual(aggregate["lap_series"]["benchmark_kind"], "minute_median")
        self.assertEqual(aggregate["lap_series"]["benchmark"][0]["median_duration_ms"], 109_900.0)
        self.assertEqual(aggregate["lap_series"]["benchmark"][0]["participant_count"], 2)
        self.assertEqual(
            [lap["lap_number"] for lap in aggregate["lap_series"]["ours_raw"]],
            [11, 12],
        )
        self.assertIn("without averaging or decimation", aggregate["semantics"]["lap_series_ours_raw"])
        raw_competitors = aggregate["lap_series"]["competitors"]
        self.assertEqual(
            [competitor["participant_id"] for competitor in raw_competitors],
            ["nr-9", "nr-29", "nr-77"],
        )
        self.assertEqual(
            [lap["lap_number"] for lap in raw_competitors[0]["laps"]],
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(raw_competitors[0]["laps"][2]["duration_ms"], None)
        self.assertFalse(raw_competitors[0]["laps"][2]["is_clean"])
        self.assertTrue(raw_competitors[0]["laps"][2]["is_out_lap"])
        self.assertEqual(raw_competitors[0]["laps"][3]["completed_at_us"], None)
        self.assertEqual(raw_competitors[2]["laps"], [])
        self.assertIn("without averaging or decimation", aggregate["semantics"]["lap_series_competitors"])
        # The legacy bounded player field remains bounded, but this raw
        # contract must retain every competitor lap regardless of that limit.
        with patch("timing.read_api.MAX_ARCHIVE_COMPARISON_LAPS_PER_PARTICIPANT", 1):
            unbounded_raw = self.model.archive_comparison("session-1", mode="all")
        self.assertEqual(
            [lap["lap_number"] for lap in unbounded_raw["lap_series"]["competitors"][0]["laps"]],
            [1, 2, 3, 4, 5],
        )

        selected = self.model.archive_comparison("session-1", mode="participant", participant_id="nr-29")
        self.assertEqual(selected["comparison"]["participant_id"], "nr-29")
        self.assertEqual(selected["points"][0]["benchmark_pace_5_ms"], None)
        self.assertEqual(selected["points"][2]["benchmark_pace_5_ms"], 109_200)
        self.assertEqual([lap["participant_id"] for lap in selected["lap_series"]["benchmark"]], ["nr-29"])
        self.assertEqual(
            [competitor["participant_id"] for competitor in selected["lap_series"]["competitors"]],
            ["nr-29"],
        )
        self.assertEqual([lap["lap_number"] for lap in selected["lap_series"]["competitors"][0]["laps"]], [1])
        # A legacy source row can carry is_clean=1 even though its complete
        # interval spans a persisted pit. The archive reader must correct that
        # fact before it can influence an engineer-facing chart.
        self.connection.execute(
            """
            INSERT INTO pit_stops(
              id,source_heat_id,participant_id,stop_number,entered_at_us,exited_at_us,entered_lap,
              exited_lap,pit_lane_ms,completed,entered_source_key,exited_source_key,created_at_us,updated_at_us
            ) VALUES ('pit-29-cross',?,'nr-29',1,5500000,5700000,1,1,200000,1,'pit:29:in','pit:29:out',5500000,5700000)
            """,
            (self.heat_id,),
        )
        self.connection.execute(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,
              flag,is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us
            ) VALUES ('lap-29-cross',?,'nr-29',2,7000000,110000,'[]','GREEN',0,0,0,1,'lap:29:cross',7000000)
            """,
            (self.heat_id,),
        )
        self.connection.commit()
        selected_after_pit = self.model.archive_comparison("session-1", mode="participant", participant_id="nr-29")
        crossing = next(lap for lap in selected_after_pit["lap_series"]["benchmark"] if lap["lap_number"] == 2)
        self.assertTrue(crossing["source_is_clean"])
        self.assertTrue(crossing["crosses_pit"])
        self.assertFalse(crossing["is_clean"])
        retired = self.model.archive_comparison("session-1", mode="participant", participant_id="nr-77")
        self.assertEqual(retired["comparison"]["participant_id"], "nr-77")
        self.assertEqual(retired["points"][0]["benchmark_pace_5_ms"], None)
        with self.assertRaises(ReadValidationError):
            self.model.archive_comparison("session-1", mode="participant", participant_id="ours")
        with self.assertRaises(ScopeNotFoundError):
            self.model.archive_comparison("session-1", mode="participant", participant_id="not-in-class")

    def test_archive_comparison_uses_each_result_grid_last_without_inventing_lap_numbers(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        baseline_cell, linked_cell, raw_cell = self._add_result_last_cells(
            (
                ("r_i", "108000000", 10_500_000),
                ("r_c", "107200000", 12_000_000),
                ("r_c", "106900000", 12_500_000),
            )
        )
        self.connection.execute(
            """
            UPDATE laps
            SET duration_source_cell_observation_id = ?, duration_source_message_id = 1,
                duration_source_key = 'raw:2', duration_source_kind = 'RESULT_GRID_LAST',
                sectors_json = ?, sectors_source_cell_observation_ids_json = ?
            WHERE id = 'lap-12'
            """,
            (
                linked_cell,
                json.dumps(
                    {
                        "sector_1": "34800000",
                        "sector_2": None,
                        "sector_3": "36500000",
                    }
                ),
                json.dumps(
                    {
                        "sector_1": 101,
                        "sector_2": 102,
                        "sector_3": 103,
                    }
                ),
            ),
        )
        payload = {
            "schema_version": "timing-archive.v1",
            "observed_at_us": 10_000_000,
            "computed": {"session": {"ours_participant_id": "ours"}},
            "class_participants": [
                {
                    "measured": {
                        "participant_id": "ours",
                        "start_number": "21",
                        "team_name": "BALCHUG Racing",
                        "class_name": "CN PRO",
                        "is_ours": True,
                        "state": {"state_kind": "ON_TRACK"},
                    },
                    "computed": {"participant_id": "ours", "is_ours": True, "pace_5_ms": 107_200},
                }
            ],
        }
        self._add_playback_snapshot(10_000_000, payload)
        payload["observed_at_us"] = 13_000_000
        self._add_playback_snapshot(13_000_000, payload)
        self.connection.commit()

        comparison = self.model.archive_comparison("session-1")
        raw = comparison["lap_series"]["ours_raw"]
        self.assertEqual([item["duration_ms"] for item in raw], [108_000, 107_200, 106_900])
        self.assertEqual(
            [item["timeline_kind"] for item in raw],
            ["snapshot_baseline", "confirmed_lap", "table_observation"],
        )
        self.assertEqual([item["lap_number"] for item in raw], [None, 12, None])
        self.assertEqual([item["board_observed_at_us"] for item in raw], [10_500_000, 12_000_000, 12_500_000])
        self.assertEqual(raw[1]["completed_at_us"], 12_000_000)
        self.assertEqual(raw[2]["completed_at_us"], None)
        self.assertEqual(raw[2]["source"]["cell_observation_id"], raw_cell)
        self.assertEqual(raw[0]["source"]["cell_observation_id"], baseline_cell)
        self.assertIsNone(raw[0]["sectors"])
        self.assertEqual(
            raw[1]["sectors"],
            {
                "sector_1": {"duration_ms": 34_800, "source_cell_observation_id": 101},
                "sector_2": None,
                "sector_3": {"duration_ms": 36_500, "source_cell_observation_id": 103},
            },
        )
        self.assertIsNone(raw[2]["sectors"])
        self.assertIn("individual source cell", comparison["semantics"]["lap_series_sectors"])
        self.assertIn("not an invented finish crossing", comparison["semantics"]["lap_series_ours_raw"])

    def test_archive_manifest_capture_lap_events_uses_confirmed_last_ledger(self):
        """A refresh is excluded while sparse confirmed r_c LAST is retained."""

        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        _baseline_cell, confirmed_cell, refresh_cell, unlinked_delta_cell = self._add_result_last_cells(
            (
                ("r_i", "108000000", 10_500_000),
                ("r_c", "107200000", 12_000_000),
                # A later r_c can repaint a whole result table too. This
                # exact repeat of lap 12 is classified REFRESH_REPEAT, not a
                # new captured lap, despite its r_c transport handle.
                ("r_c", "107200000", 12_250_000),
                # A changed sparse r_c LAST can be ledger-confirmed without
                # an exact completed-lap link. It advances the local capture
                # counter, but its provider lap number remains unknown.
                ("r_c", "106900000", 12_750_000),
            )
        )
        refresh_message_id = self.connection.execute(
            "SELECT source_message_id FROM participant_result_cell_observations WHERE id = ?",
            (refresh_cell,),
        ).fetchone()[0]
        confirmed_message_id = self.connection.execute(
            "SELECT source_message_id FROM participant_result_cell_observations WHERE id = ?",
            (confirmed_cell,),
        ).fetchone()[0]
        self.connection.execute(
            """
            UPDATE laps
            SET duration_ms = 107200, duration_source_cell_observation_id = ?, duration_source_message_id = ?,
                duration_source_key = 'raw:1', duration_source_kind = 'RESULT_GRID_LAST'
            WHERE id = 'lap-11'
            """,
            (refresh_cell, refresh_message_id),
        )
        self.connection.execute(
            """
            UPDATE laps
            SET duration_source_cell_observation_id = ?, duration_source_message_id = ?,
                duration_source_key = 'raw:2', duration_source_kind = 'RESULT_GRID_LAST'
            WHERE id = 'lap-12'
            """,
            (confirmed_cell, confirmed_message_id),
        )
        self._add_last_cell_ledger(_baseline_cell, classification="UNCONFIRMED")
        self._add_last_cell_ledger(
            confirmed_cell,
            classification="CONFIRMED_LAP",
            linked_lap_id="lap-12",
        )
        self._add_last_cell_ledger(
            refresh_cell,
            classification="REFRESH_REPEAT",
            linked_lap_id="lap-11",
        )
        self._add_last_cell_ledger(unlinked_delta_cell, classification="CONFIRMED_LAP")
        payload = {
            "schema_version": "timing-archive.v1",
            "observed_at_us": 10_000_000,
            "computed": {"session": {"ours_participant_id": "ours"}},
        }
        self._add_playback_snapshot(10_000_000, payload)
        payload["observed_at_us"] = 13_000_000
        self._add_playback_snapshot(13_000_000, payload)
        self.connection.commit()

        manifest = self.model.archive_manifest("session-1")

        self.assertEqual(len(manifest["capture_lap_events"]), 2)
        event = manifest["capture_lap_events"][0]
        self.assertEqual(event["capture_at_us"], 12_000_000)
        self.assertEqual(event["completed_at_us"], 12_000_000)
        self.assertEqual(event["lap_number"], 12)
        self.assertEqual(event["duration_ms"], 107_200)
        self.assertEqual(event["timeline_kind"], "confirmed_lap")
        self.assertEqual(event["source"]["cell_observation_id"], confirmed_cell)
        self.assertEqual(event["source"]["handle"], "r_c")
        self.assertEqual(event["source"]["classification"], "CONFIRMED_LAP")
        self.assertNotEqual(event["source"]["cell_observation_id"], refresh_cell)
        sparse = manifest["capture_lap_events"][1]
        self.assertEqual(sparse["capture_at_us"], 12_750_000)
        self.assertEqual(sparse["duration_ms"], 106_900)
        self.assertIsNone(sparse["lap_number"])
        self.assertIsNone(sparse["completed_at_us"])
        self.assertEqual(sparse["source"]["cell_observation_id"], unlinked_delta_cell)
        self.assertEqual(sparse["source"]["classification"], "CONFIRMED_LAP")
        self.assertIn("REFRESH_REPEAT table refreshes", manifest["semantics"]["capture_lap_events"])

    def test_archive_fallback_lap_sectors_require_result_grid_last_provenance(self):
        [source_cell] = self._add_result_last_cells((("r_c", "107200000", 12_000_000),))
        sectors_json = json.dumps(
            {
                "sector_1": "34800000",
                "sector_2": "35900000",
                "sector_3": "36500000",
            }
        )
        source_ids_json = json.dumps(
            {
                "sector_1": 101,
                "sector_2": 102,
                "sector_3": 103,
            }
        )
        self.connection.execute(
            """
            UPDATE laps
            SET sectors_json = ?, sectors_source_cell_observation_ids_json = ?,
                duration_source_cell_observation_id = ?, duration_source_kind = 'RESULT_GRID_LAST'
            WHERE id = 'lap-12'
            """,
            (sectors_json, source_ids_json, source_cell),
        )
        rows = _archive_comparison_lap_rows(
            self.connection,
            heat_id=self.heat_id,
            participant_ids=["ours"],
            first_at_us=10_000_000,
            last_at_us=13_000_000,
            include_unplaced=True,
            clip_to_archive_range=False,
        )["ours"]
        proven = next(row for row in rows if row["lap_number"] == 12)
        self.assertEqual(proven["sectors"]["sector_1"]["duration_ms"], 34_800)

        self.connection.execute("UPDATE laps SET duration_source_kind = NULL WHERE id = 'lap-12'")
        rows_without_last_provenance = _archive_comparison_lap_rows(
            self.connection,
            heat_id=self.heat_id,
            participant_ids=["ours"],
            first_at_us=10_000_000,
            last_at_us=13_000_000,
            include_unplaced=True,
            clip_to_archive_range=False,
        )["ours"]
        unproven = next(row for row in rows_without_last_provenance if row["lap_number"] == 12)
        self.assertIsNone(unproven["sectors"])

    def test_archive_raw_last_fails_closed_on_duplicate_last_columns(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        self._add_result_last_cells((("r_i", "108000000", 10_500_000),))
        layout_id = self.connection.execute(
            "SELECT id FROM result_layout_versions WHERE source_heat_id = ? ORDER BY id DESC LIMIT 1",
            (self.heat_id,),
        ).fetchone()[0]
        self.connection.execute(
            """
            INSERT INTO result_column_definitions(
              layout_version_id,column_index,source_name_raw,canonical_key,raw_definition_json
            ) VALUES (?,1,'LAST duplicate','last_lap','{}')
            """,
            (layout_id,),
        )
        self.connection.commit()

        self.assertEqual(
            _archive_result_last_rows(
                self.connection,
                heat_id=self.heat_id,
                participant_ids=["ours"],
                first_at_us=10_000_000,
                last_at_us=11_000_000,
            ),
            {},
        )

    def test_archive_baseline_last_does_not_hide_confirmed_legacy_laps(self):
        self._add_result_last_cells((("r_i", "108000000", 10_500_000),))
        self.connection.commit()

        rows = _archive_raw_last_or_lap_rows(
            self.connection,
            heat_id=self.heat_id,
            participant_ids=["ours"],
            first_at_us=10_000_000,
            last_at_us=13_000_000,
            clip_to_archive_range=True,
        )["ours"]
        self.assertEqual(rows[0]["timeline_kind"], "snapshot_baseline")
        self.assertEqual([row["lap_number"] for row in rows[1:]], [11, 12])

    def test_bounded_archive_laps_keep_breaks_when_non_clean_rows_are_decimated(self):
        laps = [
            {"lap_number": 1, "is_clean": True},
            {"lap_number": 2, "is_clean": False},
            {"lap_number": 3, "is_clean": True},
            {"lap_number": 4, "is_clean": True},
            {"lap_number": 5, "is_clean": False},
            {"lap_number": 6, "is_clean": True},
        ]
        with patch("timing.read_api.MAX_ARCHIVE_COMPARISON_LAPS_PER_PARTICIPANT", 3):
            bounded = _bounded_archive_lap_rows(laps)
        self.assertEqual([lap["lap_number"] for lap in bounded], [1, 3, 6])
        self.assertTrue(all(lap["break_before"] for lap in bounded))

    def test_archive_manifest_exposes_timeservice_clock_separately_from_seek_time(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        for observed_at_us in (2_000_000, 7_000_000):
            self._add_playback_snapshot(
                observed_at_us,
                {
                    "schema_version": "timing-archive.v1",
                    "observed_at_us": observed_at_us,
                    "computed": {"session": {"ours_participant_id": "ours"}},
                },
            )
        self.connection.execute(
            """
            INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us)
            VALUES ('clock-run','session-1','test',1000000)
            """
        )
        self.connection.execute(
            """
            INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us)
            VALUES ('clock-connection','clock-run',0,1000000)
            """
        )
        provider_first = 10_000_000
        provider_last = 15_000_000
        offset = 2_000_000 - (TIME_SERVICE_EPOCH_UNIX_US + provider_first)
        self.connection.execute(
            """
            INSERT INTO connection_clock_calibrations(
              ingest_connection_id,source_heat_id,calibration_key,provider_timestamp_kind,offset_us,sample_count,
              median_abs_deviation_us,valid_from_provider_us,valid_to_provider_us,valid_from_observed_at_us,
              valid_to_observed_at_us,source_message_id,source_key,created_at_us
            ) VALUES ('clock-connection',?,'clock-1','ts_time',?,1,NULL,?,NULL,2000000,NULL,NULL,'clock:1',2000000)
            """,
            (self.heat_id, offset, provider_first),
        )
        self.connection.execute(
            """
            INSERT INTO connection_clock_samples(
              ingest_connection_id,source_heat_id,provider_timestamp_raw,provider_timestamp_us,provider_timestamp_kind,
              received_at_us,source_message_id,source_key,source_event_key,created_at_us
            ) VALUES
              ('clock-connection',?,'10000000',10000000,'ts_time',2000000,NULL,'clock:1','clock:1',2000000),
              ('clock-connection',?,'15000000',15000000,'ts_time',7000000,NULL,'clock:2','clock:2',7000000)
            """,
            (self.heat_id, self.heat_id),
        )
        self.connection.commit()

        manifest = self.model.archive_manifest("session-1")
        axes = manifest["time_axes"]
        self.assertEqual(axes["playback"]["id"], "capture_received")
        self.assertEqual(axes["playback"]["origin_received_at_us"], 2_000_000)
        self.assertEqual(axes["source"]["id"], "timeservice")
        self.assertEqual([anchor["provider_ts_time_us"] for anchor in axes["source"]["anchors"]], [provider_first, provider_last])
        self.assertEqual(axes["source"]["anchors"][0]["calibrated_utc_at_us"], 2_000_000)

    def test_archive_requires_a_stopped_session_with_a_projection(self):
        with self.assertRaises(ReadValidationError):
            self.model.archive_manifest("session-1")
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        self.connection.commit()
        with self.assertRaises(ArchiveProjectionMissingError):
            self.model.archive_manifest("session-1")

    def test_archive_window_ends_at_finish_and_marks_carried_flag(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        for observed_at_us in (2_000_000, 5_000_000, 7_000_000, 9_000_000):
            self._add_playback_snapshot(
                observed_at_us,
                {
                    "schema_version": "timing-archive.v1",
                    "observed_at_us": observed_at_us,
                    "measured": {"track_flag": {"flag": "GREEN"}},
                    "computed": {"session": {"position_overall": 1}},
                },
            )
        self.connection.executemany(
            """
            INSERT INTO track_flag_periods(
              source_heat_id,flag,started_at_us,ended_at_us,source_key,created_at_us
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                (self.heat_id, "RED", 1_000_000, 3_000_000, "flag:red", 1_000_000),
                (self.heat_id, "FINISH", 6_000_000, None, "flag:finish", 6_000_000),
            ),
        )
        self.connection.commit()

        manifest = self.model.archive_manifest("session-1")
        self.assertEqual(manifest["range"], {
            "first_at_us": 2_000_000,
            "last_at_us": 7_000_000,
            "source_point_count": 3,
            "downsampled": False,
        })
        self.assertEqual([point["observed_at_us"] for point in manifest["keyframes"]], [2_000_000, 5_000_000, 7_000_000])
        self.assertEqual(manifest["heat"]["coverage"]["kind"], "partial_capture")
        self.assertEqual(manifest["heat"]["coverage"]["missing_prefix_us"], 1_000_000)
        self.assertEqual(manifest["heat"]["coverage"]["finish_at_us"], 6_000_000)
        self.assertEqual(manifest["heat"]["coverage"]["omitted_tail_point_count"], 1)
        red = next(flag for flag in manifest["markers"]["flags"] if flag["flag"] == "RED")
        self.assertTrue(red["carried_into_range"])
        self.assertEqual(red["started_at_us"], 2_000_000)

        self.assertEqual(self.model.archive_snapshot("session-1", at_us=7_000_000)["playback"]["effective_at_us"], 7_000_000)
        with self.assertRaises(ReadValidationError):
            self.model.archive_snapshot("session-1", at_us=9_000_000)

    def test_superseded_archive_is_hidden_from_list_but_remains_directly_readable(self):
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        self._add_playback_snapshot(
            2_000_000,
            {
                "schema_version": "timing-archive.v1",
                "observed_at_us": 2_000_000,
                "measured": {"track_flag": {"flag": "GREEN"}},
                "computed": {"session": {"position_overall": 1}},
            },
        )
        self.connection.execute(
            """
            INSERT INTO archive_session_replacements(
              superseded_session_id,canonical_session_id,recording_sha256,frame_count,
              capture_first_at_us,capture_last_at_us,reason,created_at_us
            ) VALUES ('session-1','pending-1',?,2,1000000,2000000,'recovered_raw_capture',3000000)
            """,
            ("a" * 64,),
        )
        self.connection.commit()

        self.assertEqual(self.model.archived_sessions()["items"], [])
        self.assertEqual(self.model.archive_manifest("session-1")["range"]["first_at_us"], 2_000_000)


if __name__ == "__main__":
    unittest.main()
