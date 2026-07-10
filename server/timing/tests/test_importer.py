import base64
import json
import tempfile
import unittest
from pathlib import Path

from timing.db import connect
from timing.importer import import_recording


class RecordingImporterTests(unittest.TestCase):
    def test_replays_raw_frames_through_the_live_normalizer_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = root / "events.ndjson"
            raw = json.dumps(
                {
                    "M": [
                        ["s_i", 1_000_000],
                        ["h_i", {"n": "Practice", "f": 2}],
                        [
                            "r_i",
                            {
                                "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "CLS"}, {"n": "STATE"}]},
                                "r": [[0, 0, "21"], [0, 1, "BALCHUG Racing"], [0, 2, "CN PRO"], [0, 3, "E1000000"]],
                            },
                        ],
                    ]
                },
                separators=(",", ":"),
            )
            records = [
                {"v": 1, "kind": "connected", "received_at": "2000-01-01T03:00:01Z", "monotonic_ns": 1, "timekeeper_id": "recorded"},
                {"v": 1, "kind": "frame", "received_at": "2000-01-01T03:00:01Z", "monotonic_ns": 2, "sequence": 1, "text_b64": base64.b64encode(raw.encode()).decode()},
                {"v": 1, "kind": "recording_finished", "received_at": "2000-01-01T03:00:02Z", "monotonic_ns": 3},
            ]
            events.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")
            database = root / "timing.db"
            session_id = import_recording(database, events)
            connection = connect(database, readonly=True)
            try:
                self.assertEqual(connection.execute("SELECT lifecycle FROM analysis_sessions WHERE id=?", (session_id,)).fetchone()[0], "stopped")
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM feed_frames").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT flag FROM track_flag_current").fetchone()[0], "RED")
                participant = connection.execute("SELECT start_number,team_name,class_name,is_ours FROM participants").fetchone()
                self.assertEqual(tuple(participant), ("21", "BALCHUG Racing", "CN PRO", 1))
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
