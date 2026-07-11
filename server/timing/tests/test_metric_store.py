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
    _green_covers_interval,
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

    def _insert_raw_result_message(self, *, sequence, observed_at_us, handle, connection_id="raw-connection"):
        """Persist the minimal immutable frame/message pair used by raw LAST tests."""

        if self.connection.execute("SELECT 1 FROM ingest_runs WHERE id = 'raw-run'").fetchone() is None:
            self.connection.execute(
                """
                INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us)
                VALUES ('raw-run','session-1','test',1000000)
                """
            )
        if self.connection.execute("SELECT 1 FROM ingest_connections WHERE id = ?", (connection_id,)).fetchone() is None:
            ordinal = int(
                self.connection.execute("SELECT COUNT(*) FROM ingest_connections WHERE ingest_run_id = 'raw-run'").fetchone()[0]
            ) + 1
            self.connection.execute(
                """
                INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us)
                VALUES (?,'raw-run',?,1000000)
                """
                ,
                (connection_id, ordinal),
            )
        cursor = self.connection.execute(
            """
            INSERT INTO feed_frames(
              analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,monotonic_ns,
              raw_payload,raw_sha256,decode_state,created_at_us
            ) VALUES ('session-1',?,?,?,?,'{}','raw-sha','decoded',?)
            """,
            (connection_id, sequence, observed_at_us, sequence, observed_at_us),
        )
        frame_id = int(cursor.lastrowid)
        cursor = self.connection.execute(
            """
            INSERT INTO feed_messages(frame_id,ordinal,handle,args_json,compressed,created_at_us)
            VALUES (?,0,?,'[]',0,?)
            """,
            (frame_id, handle, observed_at_us),
        )
        return frame_id, int(cursor.lastrowid)

    def _seed_no_laps_last_history(self):
        """Seed r_i/r_c cells without using the normalizer under test elsewhere."""

        _, initial_message_id = self._insert_raw_result_message(sequence=100, observed_at_us=1_500_000, handle="r_i")
        layout_id = int(
            self.connection.execute(
                """
                INSERT INTO result_layout_versions(
                  source_heat_id,version_ordinal,layout_fingerprint,raw_layout_json,source_message_id,
                  source_key,observed_at_us,created_at_us
                ) VALUES (?,0,'raw-no-laps','{}',?,'raw:100',1500000,1500000)
                """,
                (self.source_heat_id, initial_message_id),
            ).lastrowid
        )
        self.connection.executemany(
            """
            INSERT INTO result_column_definitions(
              layout_version_id,column_index,source_name_raw,source_parameter_raw,display_name_raw,
              canonical_key,raw_definition_json
            ) VALUES (?,?,?,?,?,?,?)
            """,
            [
                (layout_id, 0, "LAST", None, None, "last_lap", "{}"),
                (layout_id, 1, "STATE", None, None, "state", "{}"),
            ],
        )

        def cell(message_id, sequence, ordinal, column, value, observed_at_us):
            cursor = self.connection.execute(
                """
                INSERT INTO participant_result_cell_observations(
                  source_heat_id,participant_id,layout_version_id,provider_row_index,column_index,
                  raw_value_json,value_text,source_message_id,source_key,source_change_ordinal,
                  observed_at_us,created_at_us
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    self.source_heat_id,
                    "ours",
                    layout_id,
                    0,
                    column,
                    json.dumps([value]),
                    value,
                    message_id,
                    f"raw:{sequence}",
                    ordinal,
                    observed_at_us,
                    observed_at_us,
                ),
            )
            return int(cursor.lastrowid)

        initial_last_id = cell(initial_message_id, 100, 0, 0, "108000000", 1_500_000)
        cell(initial_message_id, 100, 1, 1, "E1500000", 1_500_000)
        raw_ids = []
        for sequence, observed_at_us, value in (
            (101, 2_000_000, "107500000"),
            (102, 3_000_000, "107400000"),
            (103, 4_000_000, "9223372036854775807"),
        ):
            _, message_id = self._insert_raw_result_message(
                sequence=sequence, observed_at_us=observed_at_us, handle="r_c"
            )
            raw_ids.append(cell(message_id, sequence, 0, 0, value, observed_at_us))
        _, reconnect_message_id = self._insert_raw_result_message(sequence=104, observed_at_us=5_000_000, handle="r_i")
        cell(reconnect_message_id, 104, 0, 0, "107300000", 5_000_000)
        _, final_message_id = self._insert_raw_result_message(sequence=105, observed_at_us=6_000_000, handle="r_c")
        raw_ids.append(cell(final_message_id, 105, 0, 0, "107400000", 6_000_000))

        self.connection.execute(
            """
            INSERT INTO track_flag_periods(
              source_heat_id,flag,started_at_us,source_key,created_at_us
            ) VALUES (?,'GREEN',1000000,'raw:green',1000000)
            """,
            (self.source_heat_id,),
        )
        tracker_id = int(
            self.connection.execute(
                """
                INSERT INTO tracker_passing_observations(
                  source_heat_id,participant_id,event_fingerprint,raw_passing_json,source_message_id,
                  source_key,source_event_key,observed_at_us,created_at_us
                ) VALUES (?,'ours','raw-tracker-r-c','{}',?,'raw:102','raw:tracker-r-c',3000000,3000000)
                """,
                (
                    self.source_heat_id,
                    self.connection.execute(
                        "SELECT id FROM feed_messages WHERE frame_id = (SELECT id FROM feed_frames WHERE ingest_connection_id='raw-connection' AND frame_sequence=102)"
                    ).fetchone()[0],
                ),
            ).lastrowid
        )
        self.connection.execute(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,flag,
              is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us,
              completion_passing_observation_id,duration_source_cell_observation_id,duration_source_kind
            ) VALUES ('duplicate-r-c',?,'ours',97,3000000,107400,NULL,'GREEN',0,0,0,1,'raw:102',3000000,?,?,'RESULT_GRID_LAST')
            """,
            (self.source_heat_id, tracker_id, raw_ids[1]),
        )
        self.connection.execute(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,flag,
              is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us,
              completion_passing_observation_id,duration_source_cell_observation_id,duration_source_kind
            ) VALUES ('tracker-r-i',?,'ours',98,1500000,108000,NULL,'GREEN',0,0,0,1,'raw:100',1500000,?,?,'RESULT_GRID_LAST')
            """,
            (self.source_heat_id, tracker_id, initial_last_id),
        )
        self.connection.execute(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,flag,
              is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us,
              completion_passing_observation_id,duration_source_cell_observation_id,duration_source_kind
            ) VALUES ('tracker-unlinked',?,'ours',99,4000000,NULL,NULL,'GREEN',0,0,0,1,'raw:103',4000000,?,NULL,NULL)
            """,
            (self.source_heat_id, tracker_id),
        )
        return raw_ids

    def _seed_explicit_lap_after_no_laps(self):
        """Add an explicit-LAPS layout after raw no-LAPS LAST facts."""

        self._seed_no_laps_last_history()
        _, layout_message_id = self._insert_raw_result_message(sequence=106, observed_at_us=7_000_000, handle="r_i")
        layout_id = int(
            self.connection.execute(
                """
                INSERT INTO result_layout_versions(
                  source_heat_id,version_ordinal,layout_fingerprint,raw_layout_json,source_message_id,
                  source_key,observed_at_us,created_at_us
                ) VALUES (?,1,'explicit-laps','{}',?,'raw:106',7000000,7000000)
                """,
                (self.source_heat_id, layout_message_id),
            ).lastrowid
        )
        self.connection.executemany(
            """
            INSERT INTO result_column_definitions(
              layout_version_id,column_index,source_name_raw,source_parameter_raw,display_name_raw,
              canonical_key,raw_definition_json
            ) VALUES (?,?,?,?,?,?,?)
            """,
            [
                (layout_id, 0, "LAST", None, None, "last_lap", "{}"),
                (layout_id, 1, "LAPS", None, None, "laps", "{}"),
            ],
        )
        _, message_id = self._insert_raw_result_message(sequence=107, observed_at_us=8_000_000, handle="r_c")
        duration_cell_id = int(
            self.connection.execute(
                """
                INSERT INTO participant_result_cell_observations(
                  source_heat_id,participant_id,layout_version_id,provider_row_index,column_index,
                  raw_value_json,value_text,source_message_id,source_key,source_change_ordinal,
                  observed_at_us,created_at_us
                ) VALUES (?,'ours',?,0,0,'["106900000"]','106900000',?,'raw:107',0,8000000,8000000)
                RETURNING id
                """,
                (self.source_heat_id, layout_id, message_id),
            ).fetchone()[0]
        )
        self.connection.execute(
            """
            INSERT INTO laps(
              id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,sectors_json,flag,
              is_in_lap,is_out_lap,crosses_pit,is_clean,source_message_id,source_key,created_at_us,
              completion_passing_observation_id,duration_source_cell_observation_id,duration_source_kind
            ) VALUES ('explicit-after-raw',?,'ours',100,8000000,106900,NULL,'GREEN',0,0,0,1,?,'raw:107',8000000,NULL,?,'RESULT_GRID_LAST')
            """,
            (self.source_heat_id, message_id, duration_cell_id),
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

    def test_loads_each_raw_r_c_last_without_inventing_a_provider_lap_number(self):
        raw_ids = self._seed_no_laps_last_history()
        self.connection.commit()

        ours = load_heat_metric_input(self.connection, self.source_heat_id).our_participant
        raw_laps = [lap for lap in ours.laps if lap.timing_event_id is not None]

        # r_i is only a reconnect/baseline snapshot.  The sentinel is not a
        # lap time.  Every valid r_c LAST remains an independent raw sample,
        # including an equal consecutive duration.
        self.assertEqual([lap.timing_event_id for lap in raw_laps], [raw_ids[0], raw_ids[1], raw_ids[3]])
        self.assertEqual([lap.lap_number for lap in raw_laps], [None, None, None])
        self.assertEqual([lap.capture_sequence for lap in raw_laps], [1, 2, 3])
        self.assertEqual([lap.duration_ms for lap in raw_laps], [107_500, 107_400, 107_400])
        self.assertEqual([lap.is_clean for lap in raw_laps], [False, True, False])
        self.assertEqual(ours.latest_timing_event_id, raw_ids[3])
        # Tracker rows stay in the durable chronology for tyre age, but they
        # cannot dilute timing counts once raw no-LAPS LAST facts exist.
        ineligible = [lap for lap in ours.laps if not lap.timing_eligible]
        self.assertEqual([lap.lap_number for lap in ineligible], [11, 12, 98, 99])
        self.assertEqual([lap.duration_ms for lap in ineligible], [107_500, 107_200, 108_000, None])
        self.assertIn(98, [lap.lap_number for lap in ours.laps])
        self.assertIn(99, [lap.lap_number for lap in ours.laps])

    def test_raw_last_and_later_explicit_laps_are_both_timing_eligible(self):
        self._seed_explicit_lap_after_no_laps()
        self.connection.commit()

        ours = load_heat_metric_input(self.connection, self.source_heat_id).our_participant
        explicit = next(lap for lap in ours.laps if lap.lap_number == 100)
        self.assertTrue(explicit.timing_eligible)
        self.assertEqual(explicit.duration_ms, 106_900)
        self.assertTrue(any(lap.timing_event_id is not None for lap in ours.laps))

    def test_ambiguous_last_layout_is_not_a_raw_timing_source(self):
        self._seed_no_laps_last_history()
        layout_id = self.connection.execute(
            "SELECT id FROM result_layout_versions WHERE layout_fingerprint = 'raw-no-laps'"
        ).fetchone()[0]
        self.connection.execute(
            """
            INSERT INTO result_column_definitions(
              layout_version_id,column_index,source_name_raw,source_parameter_raw,display_name_raw,
              canonical_key,raw_definition_json
            ) VALUES (?,2,'LAST DUPLICATE',NULL,NULL,'last_lap','{}')
            """,
            (layout_id,),
        )
        self.connection.commit()

        ours = load_heat_metric_input(self.connection, self.source_heat_id).our_participant
        self.assertEqual([lap for lap in ours.laps if lap.timing_event_id is not None], [])

    def test_overlapping_non_green_flag_rejects_raw_last_clean_classification(self):
        self.assertFalse(
            _green_covers_interval(
                (("GREEN", 1_000_000, None), ("RED", 2_000_000, 3_000_000)),
                started_at_us=1_500_000,
                ended_at_us=3_500_000,
            )
        )

    def test_session_level_open_gap_is_visible_to_metric_input(self):
        self.connection.execute("DELETE FROM ingest_gaps WHERE source_heat_id = ?", (self.source_heat_id,))
        self.connection.execute(
            """
            INSERT INTO ingest_gaps(analysis_session_id,source_heat_id,started_at_us,reason,created_at_us)
            VALUES ('session-1',NULL,15000000,'connection_reset',15000000)
            """
        )
        self.connection.commit()

        snapshot = load_heat_metric_input(self.connection, self.source_heat_id)
        self.assertEqual(snapshot.open_ingest_gap.reason, "connection_reset")

    def test_no_laps_r_c_requires_an_initial_snapshot_from_the_same_connection(self):
        self._seed_no_laps_last_history()
        layout_id = self.connection.execute(
            "SELECT id FROM result_layout_versions WHERE layout_fingerprint = 'raw-no-laps'"
        ).fetchone()[0]
        _, message_id = self._insert_raw_result_message(
            sequence=1,
            observed_at_us=7_000_000,
            handle="r_c",
            connection_id="raw-reconnect",
        )
        cell_id = int(
            self.connection.execute(
                """
                INSERT INTO participant_result_cell_observations(
                  source_heat_id,participant_id,layout_version_id,provider_row_index,column_index,
                  raw_value_json,value_text,source_message_id,source_key,source_change_ordinal,
                  observed_at_us,created_at_us
                ) VALUES (?,'ours',?,0,0,'["106900000"]','106900000',?,'raw:reconnect',0,7000000,7000000)
                RETURNING id
                """,
                (self.source_heat_id, layout_id, message_id),
            ).fetchone()[0]
        )
        self.connection.commit()

        ours = load_heat_metric_input(self.connection, self.source_heat_id).our_participant
        self.assertNotIn(cell_id, [lap.timing_event_id for lap in ours.laps if lap.timing_event_id is not None])

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
