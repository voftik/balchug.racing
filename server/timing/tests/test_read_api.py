import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.read_api import (
    ArchiveProjectionMissingError,
    MetricScopeRequest,
    ReadValidationError,
    ScopeNotFoundError,
    TimingReadModel,
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
              exited_lap,pit_lane_ms,completed,entered_source_key,exited_source_key,created_at_us,updated_at_us
            ) VALUES ('pit-1',?,'ours',1,5000000,5030000,5,5,30000,1,'pit:in','pit:out',?,?)
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
        with self.assertRaises(ScopeNotFoundError):
            self.model.laps("session-1", participant_id="not-a-crew")

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

    def test_archive_requires_a_stopped_session_with_a_projection(self):
        with self.assertRaises(ReadValidationError):
            self.model.archive_manifest("session-1")
        self.connection.execute("UPDATE analysis_sessions SET lifecycle = 'stopped' WHERE id = 'session-1'")
        self.connection.commit()
        with self.assertRaises(ArchiveProjectionMissingError):
            self.model.archive_manifest("session-1")


if __name__ == "__main__":
    unittest.main()
