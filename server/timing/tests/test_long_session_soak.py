import tempfile
import unittest
from pathlib import Path

from timing.long_session_soak import _percentile, run_soak


class LongSessionSoakTests(unittest.TestCase):
    def test_percentile_interpolates_ordered_samples(self):
        self.assertEqual(_percentile([4.0, 1.0, 3.0, 2.0], 0.0), 1.0)
        self.assertEqual(_percentile([4.0, 1.0, 3.0, 2.0], 0.5), 2.5)
        self.assertEqual(_percentile([4.0, 1.0, 3.0, 2.0], 1.0), 4.0)

    def test_small_no_laps_soak_preserves_source_invariants(self):
        with tempfile.TemporaryDirectory() as directory:
            report = run_soak(
                Path(directory) / "timing.db",
                participants=3,
                stages=(1,),
                lap_interval_s=1_800,
                samples=2,
                warmups=0,
                p95_limit_ms=10_000,
                p99_limit_ms=10_000,
            )

        self.assertTrue(report["passed"])
        stage = report["stages"][0]
        self.assertEqual(stage["laps_per_participant"], 2)
        self.assertEqual(stage["timing_events"], 6)
        self.assertEqual(stage["completed_pits"], 0)


if __name__ == "__main__":
    unittest.main()
