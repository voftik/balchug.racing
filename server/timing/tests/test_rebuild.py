import base64
import gzip
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from timing.db import connect
from timing.importer import import_recording
from timing.normalization import TIME_SERVICE_EPOCH_UNIX_US
from timing.read_api import TimingReadModel
from timing.rebuild import RebuildError, plan_rebuild, rebuild_session
from timing.sse import read_cursor_window


class TimingRebuildTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.events = self.root / "events.ndjson"
        self.database = self.root / "timing.db"
        self._write_recording()
        self.session_id = import_recording(self.database, self.events)

    def tearDown(self):
        self.temporary.cleanup()

    def _write_recording(self):
        first = json.dumps(
            {
                "M": [
                    ["h_i", {"n": "Practice", "s": 1_000_000, "f": 6}],
                    ["s_i", 1_000_000],
                    [
                        "r_i",
                        {
                            "l": {
                                "h": [
                                    {"n": "POS"},
                                    {"n": "NR"},
                                    {"n": "STATE"},
                                    {"n": "TEAM"},
                                    {"n": "DRIVER IN CAR"},
                                    {"n": "CLS"},
                                    {"n": "PIC"},
                                    {"n": "LAPS"},
                                ]
                            },
                            "r": [
                                [0, 0, "1"],
                                [0, 1, "21"],
                                [0, 2, "E1000000"],
                                [0, 3, "BALCHUG Racing"],
                                [0, 4, "Лобода Михаил"],
                                [0, 5, "CN PRO"],
                                [0, 6, "1"],
                                [0, 7, "4"],
                            ],
                        },
                    ],
                ]
            },
            separators=(",", ":"),
        )
        second = json.dumps(
            {"M": [["h_h", {"f": 2}], ["s_t", 2_000_000]]}, separators=(",", ":")
        )
        records = [
            {"v": 1, "kind": "connected", "received_at": "2000-01-01T03:00:01Z", "monotonic_ns": 1},
            {
                "v": 1,
                "kind": "frame",
                "received_at": "2000-01-01T03:00:01Z",
                "monotonic_ns": 2,
                "sequence": 1,
                "text_b64": base64.b64encode(first.encode()).decode(),
            },
            {
                "v": 1,
                "kind": "frame",
                "received_at": "2000-01-01T03:00:02Z",
                "monotonic_ns": 3,
                "sequence": 2,
                "text_b64": base64.b64encode(second.encode()).decode(),
            },
        ]
        self.events.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")

    def _write_interval_recording(self, path):
        """Record a complete current result grid followed by source deltas."""

        headers = ("POS", "NR", "STATE", "TEAM", "CLS", "PIC", "LAPS", "GAP", "DIFF", "LAST")

        def initial_grid(lap, leader_last, balchug_last):
            rows = []
            leader = ("1", "9", "E1000000", "Про Моторспорт", "CN PRO", "1", str(lap), "0.000", None, str(leader_last))
            balchug = ("2", "21", "E1000000", "BALCHUG Racing", "CN PRO", "2", str(lap), "1.246", "1.246", str(balchug_last))
            for row_index, values in enumerate((leader, balchug)):
                rows.extend([row_index, column, value] for column, value in enumerate(values) if value is not None)
            return {"l": {"h": [{"n": header} for header in headers]}, "r": rows}

        def result_delta(lap, leader_last, balchug_last):
            changes = []
            for row_index, values in enumerate(
                (
                    ((6, str(lap)), (7, "0.000"), (9, str(leader_last))),
                    ((6, str(lap)), (7, "1.246"), (8, "1.246"), (9, str(balchug_last))),
                )
            ):
                changes.extend([row_index, column, value] for column, value in values)
            return changes

        frames = [
            {
                "M": [
                    ["h_i", {"n": "Practice", "s": 1_000_000, "f": 6}],
                    ["s_i", 1_000_000],
                    ["r_i", initial_grid(8, 107_000_000, 108_000_000)],
                ]
            }
        ]
        for offset, lap in enumerate((9, 10, 11), start=1):
            frames.append(
                {
                    "M": [
                        ["s_t", 1_000_000 + offset * 120_000_000],
                        ["r_c", result_delta(lap, 107_000_000 + offset * 100_000, 108_000_000 + offset * 100_000)],
                    ]
                }
            )
        records = [{"v": 1, "kind": "connected", "received_at": "2000-01-01T03:00:01Z", "monotonic_ns": 1}]
        for index, frame in enumerate(frames, start=1):
            records.append(
                {
                    "v": 1,
                    "kind": "frame",
                    "received_at": f"2000-01-01T03:0{index}:01Z",
                    "monotonic_ns": index + 1,
                    "sequence": index,
                    "text_b64": base64.b64encode(json.dumps(frame, separators=(",", ":")).encode()).decode(),
                }
            )
        path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")

    def _write_anomalous_timestamp_recording(self, path):
        """A stopped capture with a valid clock and a low numeric grid TsTime."""

        provider_now = 837_026_446_926_000
        received = TIME_SERVICE_EPOCH_UNIX_US + provider_now
        headers = [
            {"n": "NR"}, {"n": "STATE"}, {"n": "TEAM"}, {"n": "CLS"},
            {"n": "LAPS"}, {"n": "PIT"}, {"n": "L-PIT"}, {"n": "STINT"},
        ]
        frames = [
            {
                "M": [
                    ["s_i", provider_now],
                    [
                        "r_i",
                        {
                            "l": {"h": headers},
                            "r": [
                                [0, 0, "77"], [0, 1, f"E{provider_now}"],
                                [0, 2, "Anomaly Racing"], [0, 3, "CN PRO"],
                                [0, 4, "5"], [0, 5, "0"], [0, 6, "L0"], [0, 7, f"S{provider_now}"],
                            ],
                        },
                    ],
                ]
            },
            {
                "M": [
                    ["s_t", provider_now + 1_000_000],
                    ["r_c", [[0, 1, "E120000000"], [0, 7, "S120000000"]]],
                ]
            },
            {
                "M": [
                    ["s_t", provider_now + 2_000_000],
                    ["r_c", [[0, 1, "SIn Pit"], [0, 5, "1"], [0, 6, "S120000000"]]],
                ]
            },
        ]

        def received_at(offset_us):
            return datetime.fromtimestamp((received + offset_us) / 1_000_000, timezone.utc).isoformat().replace("+00:00", "Z")

        records = [{"v": 1, "kind": "connected", "received_at": received_at(0), "monotonic_ns": 1}]
        for index, frame in enumerate(frames):
            records.append(
                {
                    "v": 1,
                    "kind": "frame",
                    "received_at": received_at(index * 1_000_000),
                    "monotonic_ns": index + 2,
                    "sequence": index + 1,
                    "text_b64": base64.b64encode(json.dumps(frame, separators=(",", ":")).encode()).decode(),
                }
            )
        path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")

    def test_rebuilds_stopped_session_from_unchanged_raw_frames(self):
        reader = connect(self.database, readonly=True)
        try:
            before_metric = json.loads(
                reader.execute("SELECT values_json FROM metric_current WHERE scope_kind = 'session'").fetchone()[0]
            )
            before_raw = reader.execute("SELECT id,raw_payload FROM feed_frames ORDER BY id").fetchall()
            before_heat_id = reader.execute("SELECT id FROM source_heats").fetchone()[0]
            plan = plan_rebuild(self.database, self.session_id)
            self.assertEqual(plan.decoded_frames, 2)
            self.assertEqual((plan.pending_frames, plan.failed_frames, plan.ingest_gaps), (0, 0, 0))
        finally:
            reader.close()

        result = rebuild_session(self.database, self.session_id)
        self.assertEqual(result.frames_replayed, 2)
        self.assertGreaterEqual(result.metric_current, 3)
        self.assertGreaterEqual(result.stream_events, 2)

        reader = connect(self.database, readonly=True)
        try:
            after_metric = json.loads(
                reader.execute("SELECT values_json FROM metric_current WHERE scope_kind = 'session'").fetchone()[0]
            )
            self.assertEqual(after_metric["ours_class_key"], before_metric["ours_class_key"])
            self.assertEqual(after_metric["track_flag"], before_metric["track_flag"])
            self.assertEqual(after_metric["channel_status"], "LIVE")
            self.assertEqual(
                reader.execute("SELECT lifecycle FROM analysis_sessions WHERE id = ?", (self.session_id,)).fetchone()[0],
                "stopped",
            )
            playback = [
                json.loads(gzip.decompress(bytes(row[0])))
                for row in reader.execute("SELECT payload FROM playback_snapshots ORDER BY observed_at_us")
            ]
            self.assertTrue(playback)
            self.assertTrue(
                all(snapshot["computed"]["session"]["channel_status"] == "LIVE" for snapshot in playback)
            )
            self.assertTrue(all(snapshot["session"]["lifecycle"] == "active" for snapshot in playback))
            self.assertEqual(reader.execute("SELECT id,raw_payload FROM feed_frames ORDER BY id").fetchall(), before_raw)
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM feed_frames WHERE processed_at_us IS NULL").fetchone()[0], 0)
            self.assertEqual(reader.execute("SELECT flag FROM track_flag_current").fetchone()[0], "RED")
            self.assertGreater(reader.execute("SELECT id FROM source_heats").fetchone()[0], before_heat_id)
            self.assertTrue(
                read_cursor_window(self.session_id, cursor=plan.previous_stream_cursor, database=self.database).requires_reset(
                    plan.previous_stream_cursor
                )
            )
            self.assertEqual(reader.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertIsNone(reader.execute("PRAGMA foreign_key_check").fetchone())
        finally:
            reader.close()

    def test_rebuild_keeps_anomalous_raw_timestamps_and_rejects_false_utc(self):
        events = self.root / "anomalous-events.ndjson"
        database = self.root / "anomalous-timing.db"
        self._write_anomalous_timestamp_recording(events)
        session_id = import_recording(database, events)

        reader = connect(database, readonly=True)
        try:
            raw_before = reader.execute("SELECT id,raw_payload FROM feed_frames ORDER BY id").fetchall()
            state = reader.execute(
                """
                SELECT state_raw,state_kind,state_timer_target_provider_us,state_timer_target_at_us
                FROM participant_state_observations
                WHERE state_raw = 'E120000000'
                """
            ).fetchone()
            self.assertEqual(tuple(state), ("E120000000", "ON_TRACK", 120_000_000, None))
            stint = reader.execute(
                """
                SELECT driver_stint_raw,driver_stint_kind,driver_stint_provider_ts_time,driver_stint_at_us
                FROM participant_state_observations
                WHERE driver_stint_raw = 'S120000000'
                ORDER BY id LIMIT 1
                """
            ).fetchone()
            self.assertEqual(tuple(stint), ("S120000000", "START_TS", 120_000_000, None))
            pit = reader.execute(
                """
                SELECT pit.entered_at_us,frame.received_at_us,pit.entered_at_source_kind
                FROM pit_stops AS pit
                JOIN feed_messages AS message ON message.id = pit.entered_source_message_id
                JOIN feed_frames AS frame ON frame.id = message.frame_id
                """
            ).fetchone()
            self.assertEqual((pit[0], pit[1], pit[2]), (pit[1], pit[1], None))
        finally:
            reader.close()

        result = rebuild_session(database, session_id)
        self.assertEqual(result.frames_replayed, 3)

        reader = connect(database, readonly=True)
        try:
            self.assertEqual(reader.execute("SELECT id,raw_payload FROM feed_frames ORDER BY id").fetchall(), raw_before)
            state = reader.execute(
                """
                SELECT state_raw,state_kind,state_timer_target_provider_us,state_timer_target_at_us
                FROM participant_state_observations
                WHERE state_raw = 'E120000000'
                """
            ).fetchone()
            self.assertEqual(tuple(state), ("E120000000", "ON_TRACK", 120_000_000, None))
            stint = reader.execute(
                """
                SELECT driver_stint_raw,driver_stint_kind,driver_stint_provider_ts_time,driver_stint_at_us
                FROM participant_state_observations
                WHERE driver_stint_raw = 'S120000000'
                ORDER BY id LIMIT 1
                """
            ).fetchone()
            self.assertEqual(tuple(stint), ("S120000000", "START_TS", 120_000_000, None))
            pit = reader.execute(
                """
                SELECT pit.entered_at_us,frame.received_at_us,pit.entered_at_source_kind
                FROM pit_stops AS pit
                JOIN feed_messages AS message ON message.id = pit.entered_source_message_id
                JOIN feed_frames AS frame ON frame.id = message.frame_id
                """
            ).fetchone()
            self.assertEqual((pit[0], pit[1], pit[2]), (pit[1], pit[1], None))
            self.assertEqual(reader.execute("PRAGMA foreign_key_check").fetchall(), [])
        finally:
            reader.close()

    def test_rebuild_preserves_source_proven_interval_relation_from_raw_result_grid(self):
        events = self.root / "interval-events.ndjson"
        database = self.root / "interval-timing.db"
        self._write_interval_recording(events)
        session_id = import_recording(database, events)

        result = rebuild_session(database, session_id)
        self.assertEqual(result.frames_replayed, 4)

        reader = connect(database, readonly=True)
        try:
            current = reader.execute(
                """
                SELECT gap_fact.interval_kind AS gap_kind,gap_fact.raw_value AS gap_raw,
                       gap_fact.interval_ms AS gap_ms,gap_fact.value_kind AS gap_value_kind,
                       gap_fact.source_cell_observation_id AS gap_cell_id,
                       gap_fact.source_handle AS gap_handle,gap_fact.observation_kind AS gap_observation_kind,
                       gap_fact.source_position_overall AS gap_subject_position,
                       gap_fact.source_laps AS gap_subject_laps,
                       gap_target.start_number AS gap_target_number,
                       gap_fact.target_position_overall AS gap_target_position,
                       gap_fact.target_laps AS gap_target_laps,
                       gap_fact.relation_kind AS gap_relation_kind,
                       diff_fact.interval_kind AS diff_kind,diff_fact.interval_ms AS diff_ms,
                       diff_fact.source_cell_observation_id AS diff_cell_id,
                       diff_fact.source_handle AS diff_handle,diff_fact.observation_kind AS diff_observation_kind
                FROM participant_state_current AS state
                JOIN participants AS participant ON participant.id = state.participant_id
                JOIN participant_interval_source_facts AS gap_fact ON gap_fact.id = state.gap_interval_fact_id
                JOIN participants AS gap_target ON gap_target.id = gap_fact.target_participant_id
                JOIN participant_interval_source_facts AS diff_fact ON diff_fact.id = state.diff_interval_fact_id
                WHERE participant.start_number = '21'
                """
            ).fetchone()
            self.assertIsNotNone(current)
            self.assertEqual(
                (
                    current["gap_kind"], current["gap_raw"], current["gap_ms"], current["gap_value_kind"],
                    current["gap_handle"], current["gap_observation_kind"], current["gap_subject_position"],
                    current["gap_subject_laps"], current["gap_target_number"], current["gap_target_position"],
                    current["gap_target_laps"], current["gap_relation_kind"], current["diff_kind"], current["diff_ms"],
                    current["diff_handle"], current["diff_observation_kind"],
                ),
                (
                    "GAP", "1.246", 1_246, "TIME", "r_c", "DELTA", 2, 11,
                    "9", 1, 11, "OVERALL_LEADER", "DIFF", 1_246, "r_c", "DELTA",
                ),
            )
            self.assertIsNotNone(current["gap_cell_id"])
            self.assertIsNotNone(current["diff_cell_id"])
            observed_at_us = reader.execute("SELECT MAX(observed_at_us) FROM playback_snapshots").fetchone()[0]
            self.assertIsNotNone(observed_at_us)
        finally:
            reader.close()

        archive = TimingReadModel(database).archive_snapshot(session_id, at_us=observed_at_us)["snapshot"]["archive_intervals"]
        relation = archive["relations"]["class_leader"]
        self.assertEqual((relation["status"], relation["value_ms"], relation["relation_kind"]), ("VALID", 1_246, "GAP_TO_OVERALL_LEADER"))
        self.assertEqual((relation["ours_state_kind"], relation["target_state_kind"], relation["ours_laps"], relation["target_laps"]), ("ON_TRACK", "ON_TRACK", 11, 11))
        self.assertEqual(len(relation["source_facts"]), 1)
        source = relation["source_facts"][0]
        self.assertEqual(
            (source["field_kind"], source["raw_value"], source["value_ms"], source["value_kind"], source["source_handle"], source["observation_kind"]),
            ("GAP", "1.246", 1_246, "TIME", "r_c", "DELTA"),
        )
        self.assertEqual(source["cell_observation_id"], current["gap_cell_id"])
        self.assertEqual(archive["gap_to_class_leader_ms"], 1_246)

    def test_rejects_an_active_session_before_mutating_raw_or_derived_data(self):
        writer = connect(self.database)
        try:
            writer.execute("UPDATE analysis_sessions SET lifecycle = 'active' WHERE id = ?", (self.session_id,))
            writer.commit()
        finally:
            writer.close()
        with self.assertRaises(RebuildError):
            rebuild_session(self.database, self.session_id)
        reader = connect(self.database, readonly=True)
        try:
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM metric_current").fetchone()[0], 3)
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM feed_frames").fetchone()[0], 2)
        finally:
            reader.close()

    def test_rejects_a_raw_retention_floor_before_mutating_derived_state(self):
        """A surviving tail is not sufficient evidence for a full rebuild."""

        writer = connect(self.database)
        try:
            writer.execute(
                """
                INSERT INTO timing_raw_retention_floors(
                  analysis_session_id,deleted_through_frame_id,deleted_through_received_at_us,
                  checkpoint_id,created_at_us,updated_at_us
                ) VALUES (?,?,?,?,?,?)
                """,
                (self.session_id, 1, 1, None, 1, 1),
            )
            writer.commit()
        finally:
            writer.close()

        with self.assertRaisesRegex(RebuildError, "raw retention floor"):
            plan_rebuild(self.database, self.session_id)
        with self.assertRaisesRegex(RebuildError, "raw retention floor"):
            rebuild_session(self.database, self.session_id)

        reader = connect(self.database, readonly=True)
        try:
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM feed_frames").fetchone()[0], 2)
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM source_heats").fetchone()[0], 1)
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM metric_current").fetchone()[0], 3)
        finally:
            reader.close()

    def test_rejects_incomplete_raw_evidence_before_mutating_derived_state(self):
        for decode_state in ("pending", "failed"):
            with self.subTest(decode_state=decode_state):
                writer = connect(self.database)
                try:
                    writer.execute(
                        "UPDATE feed_frames SET decode_state = ? WHERE analysis_session_id = ? AND id = 1",
                        (decode_state, self.session_id),
                    )
                    writer.commit()
                finally:
                    writer.close()
                with self.assertRaisesRegex(RebuildError, "incomplete raw evidence"):
                    rebuild_session(self.database, self.session_id)
                reader = connect(self.database, readonly=True)
                try:
                    self.assertEqual(reader.execute("SELECT COUNT(*) FROM metric_current").fetchone()[0], 3)
                    self.assertEqual(reader.execute("SELECT COUNT(*) FROM source_heats").fetchone()[0], 1)
                finally:
                    reader.close()
                writer = connect(self.database)
                try:
                    writer.execute(
                        "UPDATE feed_frames SET decode_state = 'decoded' WHERE analysis_session_id = ? AND id = 1",
                        (self.session_id,),
                    )
                    writer.commit()
                finally:
                    writer.close()

    def test_rejects_closed_reconnect_gap_before_mutating_derived_state(self):
        writer = connect(self.database)
        try:
            heat_id = writer.execute("SELECT id FROM source_heats WHERE analysis_session_id = ?", (self.session_id,)).fetchone()[0]
            writer.execute(
                """
                INSERT INTO ingest_gaps(
                  analysis_session_id,source_heat_id,started_at_us,ended_at_us,reason,created_at_us
                ) VALUES (?,?,?,?,?,?)
                """,
                (self.session_id, heat_id, 1_500_000, 1_600_000, "socket_closed", 1_500_000),
            )
            writer.commit()
        finally:
            writer.close()
        with self.assertRaisesRegex(RebuildError, "persisted ingest gap"):
            rebuild_session(self.database, self.session_id)
        reader = connect(self.database, readonly=True)
        try:
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM metric_current").fetchone()[0], 3)
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM source_heats").fetchone()[0], 1)
        finally:
            reader.close()

    def test_uses_first_nonempty_decoded_frame_as_the_rebuilt_heat_start(self):
        writer = connect(self.database)
        try:
            first_frame_id = writer.execute(
                "SELECT MIN(id) FROM feed_frames WHERE analysis_session_id = ?", (self.session_id,)
            ).fetchone()[0]
            second_received_at_us = writer.execute(
                """
                SELECT received_at_us FROM feed_frames
                WHERE analysis_session_id = ? ORDER BY id LIMIT 1 OFFSET 1
                """,
                (self.session_id,),
            ).fetchone()[0]
            writer.execute("DELETE FROM feed_messages WHERE frame_id = ?", (first_frame_id,))
            writer.commit()
        finally:
            writer.close()

        rebuild_session(self.database, self.session_id)
        reader = connect(self.database, readonly=True)
        try:
            heat = reader.execute("SELECT created_at_us FROM source_heats WHERE analysis_session_id = ?", (self.session_id,)).fetchone()
            self.assertIsNotNone(heat)
            self.assertEqual(heat["created_at_us"], second_received_at_us)
        finally:
            reader.close()

    def test_does_not_create_a_phantom_heat_for_empty_decoded_frames(self):
        writer = connect(self.database)
        try:
            writer.execute(
                "DELETE FROM feed_messages WHERE frame_id IN (SELECT id FROM feed_frames WHERE analysis_session_id = ?)",
                (self.session_id,),
            )
            writer.commit()
        finally:
            writer.close()

        result = rebuild_session(self.database, self.session_id)
        self.assertEqual(result.source_heats, 0)
        self.assertEqual(result.metric_current, 0)
        reader = connect(self.database, readonly=True)
        try:
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM source_heats WHERE analysis_session_id = ?", (self.session_id,)).fetchone()[0], 0)
        finally:
            reader.close()


if __name__ == "__main__":
    unittest.main()
