import base64
import json
import tempfile
import unittest
from pathlib import Path

from timing.db import connect
from timing.importer import import_recording
from timing.rebuild import RebuildError, plan_rebuild, rebuild_session


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

    def test_rebuilds_stopped_session_from_unchanged_raw_frames(self):
        reader = connect(self.database, readonly=True)
        try:
            before_metric = json.loads(
                reader.execute("SELECT values_json FROM metric_current WHERE scope_kind = 'session'").fetchone()[0]
            )
            before_raw = reader.execute("SELECT id,raw_payload FROM feed_frames ORDER BY id").fetchall()
            self.assertEqual(plan_rebuild(self.database, self.session_id).decoded_frames, 2)
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
            self.assertEqual(after_metric["channel_status"], "OFFLINE")
            self.assertEqual(reader.execute("SELECT id,raw_payload FROM feed_frames ORDER BY id").fetchall(), before_raw)
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM feed_frames WHERE processed_at_us IS NULL").fetchone()[0], 0)
            self.assertEqual(reader.execute("SELECT flag FROM track_flag_current").fetchone()[0], "RED")
            self.assertEqual(reader.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertIsNone(reader.execute("PRAGMA foreign_key_check").fetchone())
        finally:
            reader.close()

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


if __name__ == "__main__":
    unittest.main()
