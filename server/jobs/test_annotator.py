#!/usr/bin/env python3
"""Regression coverage for deterministic race archive classification."""
import importlib.util
import pathlib
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("annotator.py")


def load_annotator():
    spec = importlib.util.spec_from_file_location("annotator_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AnnotatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.annotator = load_annotator()

    def test_scheduled_long_race_cannot_be_reclassified_by_vision(self):
        vision = {
            "session_type": "Квалификация",
            "display_title": "Ошибочная квалификация",
            "summary": "Кадр передаёт общую атмосферу трассы.",
        }
        with mock.patch.object(self.annotator, "_llm_refine", return_value=vision):
            annotation = self.annotator.annotate(
                date="2026-07-11",
                start_hms="14:00:00",
                duration_sec=4 * 60 * 60,
                video_key="stream_records/2026-07-11/live_140000/source.mp4",
                video_path="/tmp/race.mp4",
                use_llm=True,
            )

        self.assertEqual(annotation["session_info"]["session_type"], "Гонка")
        self.assertIn("Гонка", annotation["ui_metadata"]["display_title"])
        self.assertIn("Кадр передаёт", annotation["ai_annotation"]["session_summary"])


if __name__ == "__main__":
    unittest.main()
