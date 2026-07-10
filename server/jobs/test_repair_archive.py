#!/usr/bin/env python3
"""Resume-safety tests for repair_archive.py without boto3 or a real bucket."""
import importlib.util
import io
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("repair_archive.py")


class NotFound(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakePaginator:
    def __init__(self, store):
        self.store = store

    def paginate(self, Bucket, Prefix):
        keys = [{"Key": key} for key in sorted(self.store) if key.startswith(Prefix)]
        return [{"Contents": keys}]


class FakeS3:
    def __init__(self, store):
        self.store = dict(store)
        self.fail_copy_from = None

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise NotFound()
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.store[Key] = Body

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
        self.assert_name(name)
        return FakePaginator(self.store)

    @staticmethod
    def assert_name(name):
        if name != "list_objects_v2":
            raise AssertionError(name)


def load_repair_archive():
    fake_common = types.ModuleType("common")
    fake_common.S3_BUCKET = "test-bucket"
    fake_common.make_id = lambda key: "id-" + key.replace("/", "-")
    fake_common.db = lambda: None

    fake_annotator = types.ModuleType("annotator")
    fake_annotator.annotation_key = lambda date, rec_id: f"annotations/{date}/{rec_id}/{rec_id}_annotation.json"

    old_common = sys.modules.get("common")
    old_annotator = sys.modules.get("annotator")
    sys.modules["common"] = fake_common
    sys.modules["annotator"] = fake_annotator
    try:
        spec = importlib.util.spec_from_file_location("repair_archive_under_test", SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if old_common is None:
            sys.modules.pop("common", None)
        else:
            sys.modules["common"] = old_common
        if old_annotator is None:
            sys.modules.pop("annotator", None)
        else:
            sys.modules["annotator"] = old_annotator


class RepairArchiveResumeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = load_repair_archive()

    def test_resume_republishes_staging_without_concatenating_target_again(self):
        date, rec_id = "2026-01-01", "live_120000"
        target = f"stream_records/{date}/{rec_id}/source.mp4"
        secondary = f"stream_records/{date}/live_121000/source.mp4"
        stage = f"repair_staging/archive/{date}/{rec_id}"
        state_key = f"repair_state/archive/{date}/{rec_id}.json"
        annotation = f"annotations/{date}/{rec_id}/{rec_id}_annotation.json"
        state = {
            "version": 1,
            "state_key": state_key,
            "target_key": target,
            "target_thumb_key": f"thumbnails/{date}/{rec_id}/source.mp4.jpg",
            "target_annotation_key": annotation,
            "staged_video_key": f"{stage}/source.mp4",
            "staged_thumb_key": f"{stage}/thumb.jpg",
            "staged_annotation_key": f"{stage}/annotation.json",
            "sources": [
                {"key": target, "date": date, "rec_id": rec_id},
                {"key": secondary, "date": date, "rec_id": "live_121000"},
            ],
        }
        s3 = FakeS3({
            target: b"old-first-part",
            secondary: b"old-second-part",
            state["staged_video_key"]: b"merged-once",
            state["staged_thumb_key"]: b"thumb",
            state["staged_annotation_key"]: b"annotation",
            state_key: b"state",
        })

        with tempfile.TemporaryDirectory() as queue, \
                mock.patch.object(self.job, "S3_RETRY_COUNT", 1), \
                mock.patch.object(self.job, "HLS_QUEUE", queue), \
                mock.patch.object(self.job, "db_delete") as db_delete, \
                mock.patch.object(self.job, "db_clear_hls") as db_clear:
            s3.fail_copy_from = state["staged_annotation_key"]
            with self.assertRaises(RuntimeError):
                self.job.commit_state(s3, state)

            self.assertEqual(s3.store[target], b"merged-once")
            self.assertIn(secondary, s3.store)
            self.assertIn(state["staged_video_key"], s3.store)
            self.assertIn(state_key, s3.store)

            s3.fail_copy_from = None
            self.job.commit_state(s3, state)

            self.assertEqual(s3.store[target], b"merged-once")
            self.assertEqual(s3.store[annotation], b"annotation")
            self.assertNotIn(secondary, s3.store)
            self.assertNotIn(state["staged_video_key"], s3.store)
            self.assertNotIn(state_key, s3.store)
            db_delete.assert_called()
            db_clear.assert_called_once()


if __name__ == "__main__":
    unittest.main()
