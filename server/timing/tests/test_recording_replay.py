import json
import tempfile
import time
import tracemalloc
import unittest
from pathlib import Path

from timing.protocol import Bootstrap
from timing.recording import RecordingWriter, record_with_reconnect
from timing.replay import replay_file


class RecordingReplayTests(unittest.TestCase):
    def test_versioned_fixture_covers_pit_reconnect_and_heat_reset(self):
        fixture = Path(__file__).parent.parent / "fixtures" / "signalr-replay-v1.ndjson"
        records = [json.loads(line) for line in fixture.read_text(encoding="utf-8").splitlines()]
        messages = [
            message
            for record in records
            if record.get("kind") == "decoded"
            for message in record.get("messages", [])
        ]
        flags = [
            message["args"][0]["f"]
            for message in messages
            if message["handle"] in {"h_i", "h_h"} and "f" in message["args"][0]
        ]
        result_values = [
            change[2]
            for message in messages
            if message["handle"] == "r_c"
            for batch in message["args"]
            for change in batch
        ]
        reducer = replay_file(fixture)

        self.assertEqual(sum(record["kind"] == "connected" for record in records), 2)
        self.assertEqual(sum(record["kind"] == "disconnected" for record in records), 1)
        self.assertIn(3, flags)
        self.assertGreaterEqual(flags.count(6), 2)
        self.assertIn("4", result_values)
        self.assertIn("In Pit", result_values)
        self.assertIn("OutLap", result_values)
        self.assertEqual(reducer.latest_heat["n"], "Race Heat 2")
        self.assertEqual(reducer.result_rows()[1]["laps"], "0")
        self.assertEqual(len(reducer.tracker_passings), 2)
        self.assertEqual(reducer.tracker_passings[0][4], -1)
        self.assertEqual(reducer.tracker_passings[1][4], 1)

    def test_partial_heat_delta_keeps_green_flag_and_result_meta_is_not_a_car(self):
        fixture = Path(__file__).parent.parent / "fixtures" / "signalr-replay-v1.ndjson"
        reducer = replay_file(fixture)
        reducer.apply("h_i", [{"n": "Heat", "f": 6}])
        reducer.apply("h_h", [{"r": 123}])
        reducer.apply("r_c", [[[-1, -1, "source-meta"], [1, 0, "1"]]])
        self.assertEqual(reducer.latest_heat["f"], 6)
        self.assertEqual(reducer.latest_heat["r"], 123)
        self.assertNotIn(-1, reducer.result_rows())
        self.assertEqual(reducer.result_meta_changes[-1], [-1, -1, "source-meta"])

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
    async def test_four_hour_virtual_soak_reconnects_and_replays_deterministically(self):
        class VirtualClock:
            def __init__(self):
                self.value = 0.0

            def __call__(self):
                return self.value

            async def sleep(self, seconds):
                self.value += seconds
                await __import__("asyncio").sleep(0)

        class FourHourClient:
            def __init__(self, clock):
                self.clock = clock
                self.calls = 0

            async def raw_frames(self):
                self.calls += 1
                bootstrap = Bootstrap("https://example.test", f"tid-{self.calls}", None)
                while self.clock.value < 4 * 60 * 60:
                    self.clock.value += 1
                    raw = json.dumps(
                        {"C": str(int(self.clock.value)), "M": [["s_t", int(self.clock.value)]]},
                        separators=(",", ":"),
                    )
                    yield bootstrap, raw
                    if self.calls == 1 and self.clock.value >= 2 * 60 * 60:
                        return

        clock = VirtualClock()
        client = FourHourClient(clock)
        tracemalloc.start()
        started = time.perf_counter()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / "capture"
                writer = RecordingWriter(root, "https://example.test", "test", ("s",))
                stop_reason = await record_with_reconnect(
                    client,
                    writer,
                    4 * 60 * 60,
                    backoff=(1,),
                    clock=clock,
                    sleep=clock.sleep,
                )
                writer.close(stop_reason)
                first = replay_file(root / "events.ndjson")
                second = replay_file(root / "events.ndjson")
                manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        finally:
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        self.assertEqual(stop_reason, "duration_reached")
        self.assertEqual(client.calls, 2)
        self.assertEqual(manifest["connections"], 2)
        self.assertGreaterEqual(manifest["frames"], 14_000)
        self.assertEqual(first.state_hash(), second.state_hash())
        self.assertEqual(first.latest_server_time, 4 * 60 * 60)
        self.assertLess(time.perf_counter() - started, 20)
        self.assertLess(peak_bytes, 64 * 1024 * 1024)

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
