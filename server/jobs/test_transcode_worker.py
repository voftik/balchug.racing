#!/usr/bin/env python3
"""Focused regression tests for durable HLS queue behaviour."""
import importlib.util
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("transcode_worker.py")


def load_worker():
    fake_common = types.ModuleType("common")
    fake_common.make_id = lambda key: "item-id"
    fake_common.S3_BUCKET = "bucket"
    fake_common.s3 = lambda: None
    fake_common.db = lambda: None
    previous = sys.modules.get("common")
    sys.modules["common"] = fake_common
    try:
        spec = importlib.util.spec_from_file_location("transcode_worker_under_test", SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if previous is None:
            sys.modules.pop("common", None)
        else:
            sys.modules["common"] = previous


class TranscodeWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.worker = load_worker()

    def test_transcode_discards_damaged_packets_and_cleans_temp_dir(self):
        class FakeS3:
            def generate_presigned_url(self, *_args, **_kwargs):
                return "https://example.invalid/source.mp4"

        class FakeRow(dict):
            def __getitem__(self, key):
                return super().__getitem__(key)

        class FakeConnection:
            def execute(self, *_args):
                return self

            def fetchone(self):
                return FakeRow({"hls_key": ""})

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as temp_root, \
                mock.patch.object(self.worker.C, "s3", return_value=FakeS3()), \
                mock.patch.object(self.worker.C, "db", return_value=FakeConnection()), \
                mock.patch.object(self.worker.tempfile, "mkdtemp", return_value=temp_root), \
                mock.patch.object(self.worker, "run_ffmpeg", side_effect=RuntimeError("broken input")) as run:
            with self.assertRaisesRegex(RuntimeError, "broken input"):
                self.worker.transcode("stream_records/2026-07-11/live_140000/source.mp4")

        command = run.call_args.args[0]
        self.assertIn("+genpts+discardcorrupt", command)
        self.assertIn("ignore_err", command)
        self.assertFalse(pathlib.Path(temp_root).exists())

    def test_live_broadcast_preempts_vod_encode_and_keeps_error_visible(self):
        class FakeProcess:
            returncode = None

            def poll(self):
                return None

            def terminate(self):
                self.terminated = True

            def wait(self, timeout=None):
                self.returncode = -15
                return self.returncode

        process = FakeProcess()
        with mock.patch.object(self.worker.subprocess, "Popen", return_value=process), \
                mock.patch.object(self.worker, "live_active", return_value=True), \
                mock.patch.object(self.worker.time, "sleep"):
            with self.assertRaisesRegex(self.worker.LiveBroadcastStarted, "deferred"):
                self.worker.run_ffmpeg(["ffmpeg", "-i", "source"])

        self.assertTrue(process.terminated)


if __name__ == "__main__":
    unittest.main()
