import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from timing.db import connect, migrate
from timing.normalization import TIME_SERVICE_EPOCH_UNIX_US
from timing.read_api import (
    ArchiveProjectionMissingError,
    MetricScopeRequest,
    ReadValidationError,
    ScopeNotFoundError,
    TimingReadModel,
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
        self.assertEqual(pits["items"][0]["provenance"], "measured")
        with self.assertRaises(ScopeNotFoundError):
            self.model.laps("session-1", participant_id="not-a-crew")

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

    def test_archive_intervals_rederive_source_gap_without_source_laps(self):
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
                        # This is the old immutable materialization.  The
                        # reader must derive the source interval separately.
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
        self.assertEqual(partial["gap_to_behind_ms"], 1_246)
        self.assertIsNone(partial["lap_delta_to_behind"])
        self.assertEqual(explicit["lap_count_scope"], "source_grid")
        self.assertIsNone(explicit["gap_to_behind_ms"])
        self.assertEqual(explicit["lap_delta_to_behind"], 10)

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
