import json
import tempfile
import unittest
from pathlib import Path

from timing.protocol import Bootstrap
from timing.recording import RecordingWriter, record_with_reconnect
from timing.replay import replay_file


class RecordingReplayTests(unittest.TestCase):
    def test_versioned_fixture_covers_pit_reconnect_and_heat_reset(self):
        fixture = Path(__file__).parent.parent / "fixtures" / "signalr-replay-v1.ndjson"
        reducer = replay_file(fixture)
        self.assertEqual(reducer.latest_heat["n"], "Race Heat 2")
        self.assertEqual(reducer.result_rows()[1]["laps"], "0")
        self.assertEqual(len(reducer.tracker_passings), 2)
        self.assertEqual(reducer.tracker_passings[0][4], -1)
        self.assertEqual(reducer.tracker_passings[1][4], 1)

    def test_replay_is_deterministic_and_applies_sparse_deltas(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "capture"
            with RecordingWriter(root, "https://example.test/track", "test", ("s", "h", "r", "t", "a")) as writer:
                writer.write_event(
                    "frame",
                    {
                        "wire_bytes": 12,
                        "cursor": "c1",
                        "groups_token": "g1",
                        "raw": "{}",
                        "messages": [
                            {"handle": "r_i", "args": [{"l": {"h": [{"n": "laps", "p": ""}, {"n": "SectorTimes", "p": "1"}]}, "r": [[5, 0, "4"]]}], "compressed": True},
                            {"handle": "r_c", "args": [[[5, 16, "34189000"], [1, 20, "5"]]], "compressed": False},
                            {"handle": "t_p", "args": [[[7, "", 3562700, 5183000, 2, 47036, False, 837021057308000]]], "compressed": False},
                            {"handle": "h_h", "args": [{"f": 6}], "compressed": False},
                        ],
                    },
                )
            first = replay_file(root / "events.ndjson")
            second = replay_file(root / "events.ndjson")
            self.assertEqual(first.state_hash(), second.state_hash())
            self.assertEqual(first.result_cells["5:16"], ["34189000"])
            self.assertEqual(first.result_rows()[5]["laps"], "4")
            self.assertEqual(first.result_columns()[1], "SectorTimes(1)")
            self.assertEqual(first.latest_heat, {"f": 6})
            self.assertEqual(first.tracker_passings[-1][0], 7)
            self.assertEqual(first.messages_applied, 4)

    def test_recording_manifest_tracks_payload_size(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "capture"
            writer = RecordingWriter(root, "https://example.test/track", "test", ("s",))
            writer.write_frame("{\"M\":[]}", {"C": "cursor"}, [])
            writer.close("test")
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["frames"], 1)
            self.assertEqual(manifest["bytes_received"], len('{"M":[]}'.encode("utf-8")))
            self.assertEqual(manifest["stop_reason"], "test")


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnects_after_socket_close_and_records_gap(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            async def frames(self):
                self.calls += 1
                yield (
                    Bootstrap("https://example.test", f"tid-{self.calls}", None),
                    '{"M":[]}',
                    {"C": f"cursor-{self.calls}"},
                    [],
                )
                if self.calls > 1:
                    await __import__("asyncio").sleep(60)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "capture"
            writer = RecordingWriter(root, "https://example.test/track", "test", ("s",))
            stop_reason = await record_with_reconnect(FakeClient(), writer, 0.02, backoff=(0,))
            writer.close(stop_reason)
            kinds = [json.loads(line)["kind"] for line in (root / "events.ndjson").read_text(encoding="utf-8").splitlines()]
            self.assertEqual(stop_reason, "duration_reached")
            self.assertGreaterEqual(kinds.count("connected"), 2)
            self.assertIn("disconnected", kinds)

    async def test_raw_frame_is_durable_when_decode_fails(self):
        class BrokenFrameClient:
            def __init__(self):
                self.calls = 0

            async def raw_frames(self):
                self.calls += 1
                if self.calls == 1:
                    yield Bootstrap("https://example.test", "tid-bad", None), "not-json"
                    return
                await __import__("asyncio").sleep(60)
                if False:
                    yield None

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "capture"
            writer = RecordingWriter(root, "https://example.test/track", "test", ("s",))
            stop_reason = await record_with_reconnect(BrokenFrameClient(), writer, 0.02, backoff=(0,))
            writer.close(stop_reason)
            records = [json.loads(line) for line in (root / "events.ndjson").read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(record["kind"] == "frame" and record["text_b64"] == "bm90LWpzb24=" for record in records))
            self.assertTrue(any(record["kind"] == "parse_error" for record in records))


if __name__ == "__main__":
    unittest.main()
