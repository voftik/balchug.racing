import json
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path

from timing.db import connect, migrate
from timing.metric_store import (
    MetricMaterializationResult,
    MetricSampleCandidate,
    MetricStoreError,
    load_heat_metric_input,
    load_metric_history,
    materialize_metric_samples,
)


class MetricStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        self.connection = connect(self.path)
        self.source_heat_id = self._seed_facts()
        self.connection.commit()

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def _seed_facts(self):
        timestamp = 1_000_000
        self.connection.execute(
            """
            INSERT INTO timing_sources(slug,source_url,adapter_version,created_at_us)
            VALUES ('igora','https://example.test/igora','test',?)
            """,
            (timestamp,),
        )
        source_id = self.connection.execute("SELECT id FROM timing_sources WHERE slug='igora'").fetchone()[0]
        self.connection.execute(
            """
            INSERT INTO analysis_sessions(
              id,source_id,mode,lifecycle,race_duration_s,required_pits,our_participant_id,
              our_class,identity_state,started_at_us,created_at_us,updated_at_us
            ) VALUES ('session-1',?,'race','active',14400,3,'ours','CN PRO','resolved',?,?,?)
            """,
            (source_id, timestamp, timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO source_heats(
              analysis_session_id,generation,external_name,provider_started_at_us,created_at_us
            ) VALUES ('session-1',1,'Race - Heat 1',?,?)
            """,
            (timestamp, timestamp),
        )
        heat_id = self.connection.execute("SELECT id FROM source_heats").fetchone()[0]
        self._insert_participant(
            heat_id,
            participant_id="ours",
            number="21",
            team="BALCHUG Racing",
            car="Ligier JS53 evo2",
            class_name="CN PRO",
            ours=True,
            position_class=2,
            position_overall=4,
            laps=12,
            state_kind="ON_TRACK",
        )
        self._insert_participant(
            heat_id,
            participant_id="leader",
            number="9",
            team="Competitor",
            car="Norma",
            class_name="CN PRO",
            ours=False,
            position_class=1,
            position_overall=2,
            laps=12,
            state_kind="ON_TRACK",
        )
        self._insert_participant(
            heat_id,
            participant_id="gt",
            number="90",
            team="GT Team",
            car="Audi R8",
            class_name="GT PRO",
            ours=False,
            position_class=1,
            position_overall=1,
            laps=13,
            state_kind="IN_PIT",
        )
        self.connection.execute(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,
              flag,is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us
            ) VALUES
              ('lap-11',?,'ours',11,11000000,107500,'[35000,36000,36500]','GREEN',0,0,0,1,'frame:11',?),
              ('lap-12',?,'ours',12,12000000,107200,'[34800,35900,36500]','GREEN',0,0,0,1,'frame:12',?)
            """,
            (heat_id, timestamp, heat_id, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO pit_stops(
              id,source_heat_id,participant_id,stop_number,entered_at_us,exited_at_us,entered_lap,
              exited_lap,pit_lane_ms,pit_lane_duration_source_kind,completed,entered_source_key,exited_source_key,created_at_us,updated_at_us
            ) VALUES ('pit-1',?,'ours',1,5000000,5030000,5,5,30000,'RESULT_L_PIT',1,'frame:5','frame:5-out',?,?)
            """,
            (heat_id, timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO tire_stints(
              id,source_heat_id,participant_id,stint_number,started_at_us,ended_at_us,started_lap,
              ended_lap,completed_laps,source_key,created_at_us,updated_at_us
            ) VALUES
              ('stint-1',?,'ours',1,1000000,5030000,0,5,5,'frame:5',?,?),
              ('stint-2',?,'ours',2,5030000,NULL,5,NULL,7,'frame:5-out',?,?)
            """,
            (heat_id, timestamp, timestamp, heat_id, timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO track_flag_current(
              source_heat_id,flag,provider_code,provider_label,started_at_us,source_key,updated_at_us,
              start_provider_ts_raw,observed_started_at_us,calibrated_started_at_us
            ) VALUES (?,'RED','2','Red flag',13000000,'frame:13',13050000,'13000000',13050000,13000000)
            """,
            (heat_id,),
        )
        self.connection.execute(
            """
            INSERT INTO heat_statistics_current(
              source_heat_id,heat_name_raw,participants_started,total_laps,total_pitstops,
              safety_car_count,code_60_count,full_course_yellow_count,raw_payload_json,
              source_key,source_event_key,observed_at_us,updated_at_us
            ) VALUES (?,'Race - Heat 1',30,401,66,0,0,0,'{}','stats:13','stats:13',13060000,13060000)
            """,
            (heat_id,),
        )
        self.connection.execute(
            """
            INSERT INTO statistics_class_best_laps(
              source_heat_id,class_name_key,lap_time_us,start_number_raw,event_fingerprint,
              raw_record_json,source_key,source_event_key,observed_at_us,updated_at_us
            ) VALUES (?,'cn pro',107100000,'9','best-cn-pro','{}','stats:13','stats:13:best',13060000,13060000)
            """,
            (heat_id,),
        )
        self.connection.execute(
            """
            INSERT INTO state_ticks(
              source_heat_id,observed_second,observed_at_us,source_key,state_hash,freshness_ms,created_at_us
            ) VALUES (?,13,13900000,'tick:13','hash',125,?)
            """,
            (heat_id, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO ingest_gaps(analysis_session_id,source_heat_id,started_at_us,reason,created_at_us)
            VALUES ('session-1',?,14000000,'socket_closed',?)
            """,
            (heat_id, timestamp),
        )
        return heat_id

    def _insert_participant(
        self,
        heat_id,
        *,
        participant_id,
        number,
        team,
        car,
        class_name,
        ours,
        position_class,
        position_overall,
        laps,
        state_kind,
    ):
        timestamp = 1_000_000
        class_key = class_name.casefold()
        self.connection.execute(
            """
            INSERT INTO participants(
              id,source_heat_id,external_key,start_number,team_name,car_name,class_name,class_name_key,
              is_ours,active,first_seen_at_us,last_seen_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                participant_id,
                heat_id,
                f"nr:{number}",
                number,
                team,
                car,
                class_name,
                class_key,
                int(ours),
                1,
                timestamp,
                13_800_000,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO participant_state_current(
              source_heat_id,participant_id,position_overall,position_class,laps,state,state_raw,state_kind,
              current_driver_name,last_lap_ms,best_lap_ms,gap_ms,gap_raw,gap_kind,diff_ms,diff_raw,diff_kind,
              source_key,updated_at_us,provider_pit_count
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                heat_id,
                participant_id,
                position_overall,
                position_class,
                laps,
                state_kind,
                f"S{state_kind}",
                state_kind,
                "Driver",
                107_200,
                107_100,
                1_250,
                "1.250",
                "TIME",
                1_250,
                "1.250",
                "TIME",
                f"frame:{participant_id}",
                13_800_000,
                1,
            ),
        )

    def test_loads_immutable_normalized_facts_and_automatic_class_scope(self):
        snapshot = load_heat_metric_input(self.connection, self.source_heat_id)

        self.assertEqual(snapshot.session.mode, "race")
        self.assertEqual(snapshot.session.race_duration_s, 14_400)
        self.assertEqual(snapshot.session.required_pits, 3)
        self.assertEqual(snapshot.current_flag.flag, "RED")
        self.assertEqual(snapshot.current_flag.started_at_us, 13_000_000)
        self.assertEqual(snapshot.current_flag.calibrated_started_at_us, 13_000_000)
        self.assertEqual(snapshot.statistics.total_laps, 401)
        self.assertEqual(snapshot.latest_tick.freshness_ms, 125)
        self.assertEqual(snapshot.open_ingest_gap.reason, "socket_closed")
        self.assertEqual(snapshot.observed_at_us, 13_900_000)

        ours = snapshot.our_participant
        self.assertEqual(ours.id, "ours")
        self.assertEqual([lap.lap_number for lap in ours.laps], [11, 12])
        self.assertEqual(ours.pit_stops[0].pit_lane_ms, 30_000)
        self.assertEqual(ours.pit_stops[0].pit_lane_duration_source_kind, "RESULT_L_PIT")
        self.assertEqual(ours.active_tire_stint.stint_number, 2)
        self.assertEqual(ours.active_tire_stint.completed_laps, 7)

        class_scope = snapshot.current_class_scope
        self.assertEqual(class_scope.key, "cn pro")
        self.assertEqual([participant.id for participant in class_scope.participants], ["leader", "ours"])
        self.assertEqual(class_scope.class_best_lap_ms, 107_100)
        self.assertEqual(class_scope.class_best_start_number, "9")
        self.assertEqual(snapshot.class_scope("gt pro").participants[0].id, "gt")

        with self.assertRaises(FrozenInstanceError):
            snapshot.session.mode = "practice"
        with self.assertRaises(AttributeError):
            snapshot.participants.append(ours)

    def test_rejects_missing_heat(self):
        with self.assertRaisesRegex(MetricStoreError, "does not exist"):
            load_heat_metric_input(self.connection, 999)

    def test_sparsifies_changed_samples_at_periodic_and_event_boundaries(self):
        sample = lambda value, event=False: MetricSampleCandidate(
            "participant", "ours", {"pace_5_ms": value, "nested": {"z": 1, "a": None}}, event
        )
        first = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=1_100_000,
            metric_version=1,
            source_key="frame:1",
            samples=[sample(107_200)],
        )
        self.assertEqual(first.inserted, (first.written[0],))

        skipped = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=2_100_000,
            metric_version=1,
            source_key="frame:2",
            samples=[sample(107_000)],
        )
        self.assertEqual(skipped.written, ())
        self.assertEqual(skipped.skipped[0].scope_key, "ours")

        event = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=3_100_000,
            metric_version=1,
            source_key="frame:3",
            samples=[sample(107_000, event=True)],
        )
        self.assertEqual(len(event.inserted), 1)

        periodic = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=5_100_000,
            metric_version=1,
            source_key="frame:5",
            samples=[sample(106_900)],
        )
        self.assertEqual(len(periodic.inserted), 1)

        same_second_event = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=5_500_000,
            metric_version=1,
            source_key="frame:5b",
            samples=[sample(106_800, event=True)],
        )
        self.assertEqual(len(same_second_event.updated), 1)
        retry = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=5_500_000,
            metric_version=1,
            source_key="frame:5b",
            samples=[sample(106_800, event=True)],
        )
        self.assertEqual(retry.written, ())

        rows = self.connection.execute(
            """
            SELECT observed_second,observed_at_us,values_json,source_key
            FROM metric_samples
            WHERE source_heat_id = ? AND scope_kind = 'participant' AND scope_key = 'ours'
            ORDER BY observed_second
            """,
            (self.source_heat_id,),
        ).fetchall()
        self.assertEqual([row["observed_second"] for row in rows], [1, 3, 5])
        self.assertEqual(rows[-1]["observed_at_us"], 5_500_000)
        self.assertEqual(rows[-1]["source_key"], "frame:5b")
        self.assertEqual(json.loads(rows[-1]["values_json"])["pace_5_ms"], 106_800)

    def test_materializes_current_scope_each_newer_tick_while_history_stays_sparse(self):
        first = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=1_100_000,
            metric_version=1,
            source_key="frame:1",
            samples=[MetricSampleCandidate("participant", "ours", {"pace_5_ms": 107_200})],
        )
        self.assertEqual(first.current_inserted, (first.current_written[0],))

        # This value changed, but it is inside the same five-second history
        # bucket. The live panel must still receive the newer state.
        second = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=2_100_000,
            metric_version=1,
            source_key="frame:2",
            samples=[MetricSampleCandidate("participant", "ours", {"pace_5_ms": 107_000})],
        )
        self.assertEqual(second.written, ())
        self.assertEqual(second.current_updated, (second.current_written[0],))

        current = self.connection.execute(
            """
            SELECT observed_at_us,metric_version,values_json,source_key
            FROM metric_current
            WHERE source_heat_id = ? AND scope_kind = 'participant' AND scope_key = 'ours'
            """,
            (self.source_heat_id,),
        ).fetchone()
        self.assertEqual(current["observed_at_us"], 2_100_000)
        self.assertEqual(current["metric_version"], 1)
        self.assertEqual(current["source_key"], "frame:2")
        self.assertEqual(json.loads(current["values_json"]), {"pace_5_ms": 107_000})

        history = self.connection.execute(
            """
            SELECT observed_at_us,values_json
            FROM metric_samples
            WHERE source_heat_id = ? AND scope_kind = 'participant' AND scope_key = 'ours'
            """,
            (self.source_heat_id,),
        ).fetchone()
        self.assertEqual(history["observed_at_us"], 1_100_000)
        self.assertEqual(json.loads(history["values_json"]), {"pace_5_ms": 107_200})

        older = materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=2_000_000,
            metric_version=1,
            source_key="late-frame:1",
            samples=[MetricSampleCandidate("participant", "ours", {"pace_5_ms": 106_900})],
        )
        self.assertEqual(older.current_written, ())
        self.assertEqual(older.current_skipped[0].scope_key, "ours")
        self.assertEqual(
            self.connection.execute(
                """
                SELECT observed_at_us,values_json FROM metric_current
                WHERE source_heat_id = ? AND scope_kind = 'participant' AND scope_key = 'ours'
                """,
                (self.source_heat_id,),
            ).fetchone()["observed_at_us"],
            2_100_000,
        )

    def test_loads_validated_ordered_sparse_history(self):
        candidate = MetricSampleCandidate("session", "session-1", {"pace_5_ms": 107_200})
        materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=1_100_000,
            metric_version=1,
            source_key="frame:1",
            samples=[candidate],
        )
        materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=6_100_000,
            metric_version=1,
            source_key="frame:6",
            samples=[MetricSampleCandidate("session", "session-1", {"pace_5_ms": 107_000})],
        )
        history = load_metric_history(
            self.connection,
            source_heat_id=self.source_heat_id,
            scope_kind="session",
            scope_key="session-1",
            since_at_us=2_000_000,
            metric_version=1,
        )
        self.assertEqual([(point.observed_at_us, point.values) for point in history], [(6_100_000, {"pace_5_ms": 107_000})])
        with self.assertRaisesRegex(MetricStoreError, "Unsupported metric scope"):
            load_metric_history(
                self.connection,
                source_heat_id=self.source_heat_id,
                scope_kind="unknown",
                scope_key="session-1",
            )

    def test_formula_version_upgrade_starts_a_separate_history_window(self):
        candidate = MetricSampleCandidate("session", "session-1", {"gap_to_ahead_ms": 1_250})
        materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=1_100_000,
            metric_version=1,
            source_key="frame:v1",
            samples=[candidate],
        )
        materialize_metric_samples(
            self.connection,
            source_heat_id=self.source_heat_id,
            observed_at_us=2_100_000,
            metric_version=2,
            source_key="frame:v2",
            samples=[candidate],
        )

        legacy = load_metric_history(
            self.connection,
            source_heat_id=self.source_heat_id,
            scope_kind="session",
            scope_key="session-1",
            metric_version=1,
        )
        current = load_metric_history(
            self.connection,
            source_heat_id=self.source_heat_id,
            scope_kind="session",
            scope_key="session-1",
            metric_version=2,
        )

        self.assertEqual([point.observed_at_us for point in legacy], [1_100_000])
        self.assertEqual([point.observed_at_us for point in current], [2_100_000])

    def test_rejects_ambiguous_or_invalid_materialization_requests(self):
        candidate = MetricSampleCandidate("participant", "ours", {"pace_5_ms": 107_200})
        with self.assertRaisesRegex(MetricStoreError, "Duplicate metric scope"):
            materialize_metric_samples(
                self.connection,
                source_heat_id=self.source_heat_id,
                observed_at_us=1_000_000,
                metric_version=1,
                source_key="frame:1",
                samples=[candidate, candidate],
            )
        with self.assertRaisesRegex(MetricStoreError, "NaN"):
            materialize_metric_samples(
                self.connection,
                source_heat_id=self.source_heat_id,
                observed_at_us=1_000_000,
                metric_version=1,
                source_key="frame:1",
                samples=[MetricSampleCandidate("participant", "ours", {"pace_5_ms": float("nan")})],
            )


if __name__ == "__main__":
    unittest.main()
