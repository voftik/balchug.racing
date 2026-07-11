#!/usr/bin/env python3
"""Regression tests for archive session boundaries without S3 or ffmpeg."""
import datetime
import importlib.util
import os
import pathlib
import sys
import types
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("session_merger.py")


def load_merger(gap_minutes="30"):
    fake_common = types.ModuleType("common")
    fake_annotator = types.ModuleType("annotator")
    previous_common = sys.modules.get("common")
    previous_annotator = sys.modules.get("annotator")
    sys.modules["common"] = fake_common
    sys.modules["annotator"] = fake_annotator
    try:
        with mock.patch.dict(os.environ, {"SESSION_GAP_MIN": gap_minutes}, clear=False):
            spec = importlib.util.spec_from_file_location("session_merger_under_test", SCRIPT)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
    finally:
        if previous_common is None:
            sys.modules.pop("common", None)
        else:
            sys.modules["common"] = previous_common
        if previous_annotator is None:
            sys.modules.pop("annotator", None)
        else:
            sys.modules["annotator"] = previous_annotator


def part(hour, minute, second, *, closed_after=60):
    start = datetime.datetime(2026, 7, 11, hour, minute, second)
    return {
        "path": f"/tmp/{hour:02d}{minute:02d}{second:02d}.flv",
        "start_dt": start,
        "start_ts": start.timestamp(),
        "end_ts": start.timestamp() + closed_after,
        "size": 1024 * 1024,
        "busy": False,
    }


class SessionMergerBoundaryTests(unittest.TestCase):
    def test_default_boundary_is_thirty_minutes(self):
        merger = load_merger()
        self.assertEqual(merger.GAP_SEC, 30 * 60)

    def test_fifty_one_minute_program_break_is_not_merged(self):
        merger = load_merger()
        early = part(7, 24, 36, closed_after=274)
        qualifying = part(8, 20, 28, closed_after=165)

        groups = merger.group_sessions([early, qualifying])

        self.assertEqual(groups, [[early], [qualifying]])

    def test_short_reconnect_is_kept_in_one_session(self):
        merger = load_merger()
        first = part(14, 0, 0, closed_after=600)
        reconnect = part(14, 20, 0, closed_after=600)

        self.assertEqual(merger.group_sessions([first, reconnect]), [[first, reconnect]])

    def test_failed_finalization_leaves_original_flv_untouched(self):
        merger = load_merger()
        candidate = part(14, 0, 0)
        with mock.patch.object(merger, "part_start_time", return_value=0.0), \
                mock.patch.object(merger, "run", side_effect=RuntimeError("ffmpeg unavailable")), \
                mock.patch.object(merger, "cleanup") as cleanup:
            with self.assertRaisesRegex(RuntimeError, "ffmpeg unavailable"):
                merger.finalize([candidate])
        cleanup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
