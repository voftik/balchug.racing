#!/usr/bin/env python3
"""Safety tests for fix_gaps.py that do not require boto3 or an S3 bucket."""
import contextlib
import importlib.util
import io
import pathlib
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("fix_gaps.py")


class NotFound(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakePaginator:
    def __init__(self, store):
        self.store = store

    def paginate(self, Bucket, Prefix):
        return [{"Contents": [{"Key": key} for key in sorted(self.store) if key.startswith(Prefix)]}]


class FakeS3:
    def __init__(self, store):
        self.store = dict(store)
        self.fail_copy_from = None

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise NotFound()
        return {"Body": io.BytesIO(self.store[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise NotFound()
        return {}

    def copy(self, CopySource, Bucket, Key):
        source = CopySource["Key"]
        if source == self.fail_copy_from:
            raise RuntimeError("injected copy failure")
        self.store[Key] = self.store[source]

    def delete_object(self, Bucket, Key):
        self.store.pop(Key, None)

    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise AssertionError(name)
        return FakePaginator(self.store)


class FakeConnection:
    def execute(self, *args):
        return self

    def commit(self):
        pass

    def close(self):
        pass


def load_fix_gaps():
    """Load the job with a minimal common-module stub instead of boto3."""
    fake_common = types.ModuleType("common")
    fake_common.S3_BUCKET = "test-bucket"
    fake_common.s3 = lambda: object()
    fake_common.make_id = lambda key: "test-item"
    fake_common.db = lambda: None

    previous = sys.modules.get("common")
    sys.modules["common"] = fake_common
    try:
        spec = importlib.util.spec_from_file_location("fix_gaps_under_test", SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if previous is None:
            sys.modules.pop("common", None)
        else:
            sys.modules["common"] = previous


class FixGapsSafetyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = load_fix_gaps()

    def test_empty_dts_is_explicit_scan_failure(self):
        with mock.patch.object(self.job.subprocess, "check_output", return_value=b""):
            with self.assertRaisesRegex(self.job.ScanError, "видеопакеты не найдены"):
                self.job.video_dts_list("https://example.invalid/video?X-Amz-Signature=secret")

    def test_scan_failure_hides_presigned_url_and_returns_nonzero(self):
        signed_url = "https://example.invalid/video?X-Amz-Signature=must-not-leak"
        failure = subprocess.CalledProcessError(1, ["ffprobe", signed_url])
        output = io.StringIO()
        with mock.patch.object(self.job, "presign", return_value=signed_url), \
             mock.patch.object(self.job.subprocess, "check_output", side_effect=failure), \
             contextlib.redirect_stdout(output):
            code = self.job.main(["--key", "stream_records/2026-01-01/live_120000/source.mp4"])

        self.assertEqual(code, 1)
        self.assertNotIn("X-Amz-Signature", output.getvalue())
        self.assertIn("скан DTS", output.getvalue())

    def test_apply_requires_mp4box_before_multiregion_repair(self):
        key = "stream_records/2026-01-01/live_120000/source.mp4"
        info = {"key": key, "regions": [(0.0, 10.0), (50.0, 60.0)],
                "content": 20.0, "container": 60.0, "broken": True}
        output = io.StringIO()
        with mock.patch.object(self.job, "load_state", return_value=None), \
             mock.patch.object(self.job, "scan", return_value=info), \
             mock.patch.object(self.job.shutil, "which", return_value=None), \
             mock.patch.object(self.job, "rebuild") as rebuild, \
             contextlib.redirect_stdout(output):
            code = self.job.main(["--apply", "--key", key])

        self.assertEqual(code, 1)
        rebuild.assert_not_called()
        self.assertIn("MP4Box не найден", output.getvalue())

    def test_apply_does_not_repair_any_file_after_scan_failure(self):
        bad_key = "stream_records/2026-01-01/live_080000/source.mp4"
        good_key = "stream_records/2026-01-01/live_120000/source.mp4"
        info = {"key": good_key, "regions": [(1.0, 10.0)],
                "content": 9.0, "container": 10.0, "broken": True}
        output = io.StringIO()

        def fake_scan(_s3, key):
            if key == bad_key:
                raise self.job.ScanError("скан DTS: видеопакеты не найдены")
            return info

        with mock.patch.object(self.job, "load_state", return_value=None), \
             mock.patch.object(self.job, "scan", side_effect=fake_scan), \
             mock.patch.object(self.job, "rebuild") as rebuild, \
             contextlib.redirect_stdout(output):
            code = self.job.main(["--apply", "--key", bad_key, "--key", good_key])

        self.assertEqual(code, 1)
        rebuild.assert_not_called()
        self.assertIn("ремонт не начат", output.getvalue())

    def test_apply_failure_returns_nonzero_without_exception_details(self):
        key = "stream_records/2026-01-01/live_120000/source.mp4"
        info = {"key": key, "regions": [(1.0, 10.0)],
                "content": 9.0, "container": 10.0, "broken": True}
        output = io.StringIO()
        with mock.patch.object(self.job, "load_state", return_value=None), \
             mock.patch.object(self.job, "scan", return_value=info), \
             mock.patch.object(self.job, "rebuild",
                               side_effect=RuntimeError("https://example.invalid/?secret")), \
             contextlib.redirect_stdout(output):
            code = self.job.main(["--apply", "--key", key])

        self.assertEqual(code, 1)
        self.assertNotIn("example.invalid", output.getvalue())
        self.assertIn("RuntimeError", output.getvalue())

    def test_resume_uses_staged_video_after_partial_publish_failure(self):
        key = "stream_records/2026-01-01/live_120000/source.mp4"
        item_id = self.job.C.make_id(key)
        stage = f"repair_staging/gaps/{item_id}"
        state_key = f"repair_state/gaps/{item_id}.json"
        annotation = "annotations/2026-01-01/live_120000/live_120000_annotation.json"
        state = {
            "version": 1,
            "state_key": state_key,
            "target_key": key,
            "duration": 42.0,
            "target_thumb_key": "thumbnails/2026-01-01/live_120000/source.mp4.jpg",
            "target_annotation_key": annotation,
            "staged_video_key": f"{stage}/source.mp4",
            "staged_thumb_key": f"{stage}/thumb.jpg",
            "staged_annotation_key": f"{stage}/annotation.json",
        }
        s3 = FakeS3({
            key: b"old-video",
            state["staged_video_key"]: b"fixed-video",
            state["staged_thumb_key"]: b"thumb",
            state["staged_annotation_key"]: b"annotation",
            state_key: b"state",
        })

        with tempfile.TemporaryDirectory() as queue, \
                mock.patch.object(self.job, "S3_RETRY_COUNT", 1), \
                mock.patch.object(self.job, "HLS_QUEUE", queue), \
                mock.patch.object(self.job.C, "db", return_value=FakeConnection()):
            s3.fail_copy_from = state["staged_annotation_key"]
            with self.assertRaises(RuntimeError):
                self.job.commit_state(s3, state)
            self.assertEqual(s3.store[key], b"fixed-video")
            self.assertIn(state["staged_video_key"], s3.store)

            s3.fail_copy_from = None
            self.job.commit_state(s3, state)
            self.assertEqual(s3.store[key], b"fixed-video")
            self.assertEqual(s3.store[annotation], b"annotation")
            self.assertNotIn(state["staged_video_key"], s3.store)
            self.assertNotIn(state_key, s3.store)


if __name__ == "__main__":
    unittest.main()
