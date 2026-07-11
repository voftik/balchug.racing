import unittest

from timing.result_grid import ResultGrid, ResultGridStateError


class ResultGridTests(unittest.TestCase):
    def test_snapshot_and_sparse_updates_use_dynamic_header_aliases(self):
        grid = ResultGrid()
        grid.apply_snapshot(
            {
                "l": {"h": [{"n": "NR"}, {"n": "TEAM"}, {"n": "STATE"}, {"n": "new field"}]},
                "r": [[4, 0, "21"], [4, 1, "BALCHUG Racing"], [4, 2, "E837026446926000"]],
            }
        )
        grid.apply_changes([[4, 2, "SIn Pit"], [4, 3, "future data", "style"]])

        self.assertEqual(
            grid.row_values(4),
            {
                "start_number": "21",
                "team_name": "BALCHUG Racing",
                "state": "SIn Pit",
                "unknown:new field:": "future data",
            },
        )
        self.assertEqual(grid.rows[4][3].presentation, ("style",))

    def test_negative_or_malformed_delta_is_metadata_not_a_participant(self):
        grid = ResultGrid()
        grid.apply_changes([[-1, -1, "source metadata"], ["row", 1, "bad"], [5, 1]])
        self.assertEqual(grid.rows, {})
        self.assertEqual(len(grid.metadata_changes), 3)

    def test_initial_snapshot_clears_previous_rows_and_remove_is_conservative(self):
        grid = ResultGrid()
        grid.apply_snapshot({"l": {"h": [{"n": "NR"}]}, "r": [[1, 0, "21"], [2, 0, "9"]]})
        grid.apply_snapshot({"r": [[3, 0, "29"]]})
        self.assertEqual(grid.all_rows(), {3: {"start_number": "29"}})
        grid.remove_rows([[3], [-1], ["not-a-row"]])
        self.assertEqual(grid.rows, {})

    def test_layout_swap_remaps_rows_by_canonical_identity_and_accepts_sparse_delta(self):
        grid = ResultGrid()
        grid.apply_snapshot(
            {
                "l": {"h": [{"n": "NR"}, {"n": "POS"}, {"n": "PIC"}]},
                "r": [[0, 0, "21"], [0, 1, "3"], [0, 2, "1"]],
            }
        )
        self.assertEqual(
            grid.row_values(0),
            {"start_number": "21", "position_overall": "3", "position_class": "1"},
        )

        # The live provider adds LAPS and moves PIC/POS, then immediately sends
        # r_c without another r_i. Existing cells move by header identity.
        grid.apply_layout_update(
            {"h": [{"n": "NR"}, {"n": "PIC"}, {"n": "LAPS"}, {"n": "POS"}]}
        )
        grid.apply_changes([[0, 2, "28"], [0, 3, "4"]])

        self.assertFalse(grid.schema_pending)
        self.assertTrue(grid.schema_ready)
        self.assertEqual(
            grid.row_values(0),
            {
                "start_number": "21",
                "position_class": "1",
                "laps": "28",
                "position_overall": "4",
            },
        )

    def test_layout_update_drops_removed_field_and_remaps_unknown_field_by_identity(self):
        grid = ResultGrid()
        grid.apply_snapshot(
            {
                "l": {"h": [{"n": "NR"}, {"n": "LAPS"}, {"n": "futureMetric"}, {"n": "LAST"}]},
                "r": [[0, 0, "21"], [0, 1, "28"], [0, 2, "raw fact"], [0, 3, "107200000"]],
            }
        )

        grid.apply_layout_update(
            {"h": [{"n": "LAST"}, {"n": "futureMetric"}, {"n": "NR"}]}
        )
        grid.apply_changes([[0, 0, "106900000"]])

        self.assertEqual(
            grid.row_values(0),
            {
                "last_lap": "106900000",
                "unknown:futureMetric:": "raw fact",
                "start_number": "21",
            },
        )
        self.assertNotIn("laps", grid.row_values(0))

    def test_display_caption_is_a_safe_fallback_for_a_new_provider_name(self):
        grid = ResultGrid()
        grid.apply_snapshot(
            {
                "l": {
                    "h": [
                        {"n": "startnumber", "c": "NR"},
                        {"n": "completedLapCounterV2", "c": "LAPS"},
                        {"n": "lastRoundTime", "c": "LAST"},
                    ]
                },
                "r": [[0, 0, "21"], [0, 1, "29"], [0, 2, "107200000"]],
            }
        )

        self.assertTrue(grid.schema_ready)
        self.assertEqual(
            grid.row_values(0),
            {"start_number": "21", "laps": "29", "last_lap": "107200000"},
        )

        # Two visible LAPS captions are ambiguous even when their provider
        # names differ, so neither is allowed into tactical state.
        grid.apply_layout_update(
            {
                "h": [
                    {"n": "completedLapCounterV2", "c": "LAPS"},
                    {"n": "completedLapCounterV3", "c": "LAPS"},
                ]
            }
        )
        self.assertFalse(grid.schema_ready)
        self.assertEqual(grid.schema_conflicts, {"laps": (0, 1)})
        self.assertEqual(grid.rows, {})

    def test_layout_update_with_duplicate_canonical_headers_stays_fail_closed(self):
        grid = ResultGrid()
        grid.apply_snapshot({"l": {"h": [{"n": "NR"}]}, "r": [[0, 0, "21"]]})
        grid.apply_layout_update({"h": [{"n": "NR"}, {"n": "startnumber"}]})
        grid.apply_changes([[0, 0, "21"]])

        self.assertFalse(grid.schema_ready)
        self.assertEqual(grid.schema_conflicts, {"start_number": (0, 1)})
        self.assertEqual(grid.rows, {})

    def test_duplicate_canonical_headers_are_fail_closed(self):
        grid = ResultGrid()
        grid.apply_snapshot(
            {
                "l": {"h": [{"n": "POS"}, {"n": "Position"}, {"n": "NR"}]},
                "r": [[0, 0, "3"], [0, 1, "4"], [0, 2, "21"]],
            }
        )

        self.assertFalse(grid.schema_pending)
        self.assertFalse(grid.schema_ready)
        self.assertEqual(grid.schema_conflicts, {"position_overall": (0, 1)})
        self.assertEqual(grid.rows, {})
        self.assertEqual(grid.row_values(0), {})
        self.assertEqual(grid.all_rows(), {})
        self.assertEqual(grid.snapshot()["schema_conflicts"], {"position_overall": [0, 1]})

        # Subsequent sparse deltas cannot revive an ambiguous schema.
        grid.apply_changes([[0, 2, "21"]])
        self.assertEqual(grid.rows, {})

    def test_checkpoint_snapshot_restores_sparse_cells_without_reinterpreting_layout(self):
        grid = ResultGrid()
        grid.apply_snapshot(
            {
                "l": {"h": [{"n": "NR"}, {"n": "STATE"}, {"n": "future"}]},
                "r": [[4, 0, "21"], [4, 1, "E1000000"], [4, 2, "source", "style"]],
            }
        )
        grid.apply_changes([[-1, -1, "metadata"]])

        restored = ResultGrid()
        restored.restore_snapshot(grid.snapshot())

        self.assertEqual(restored.snapshot(), grid.snapshot())
        self.assertEqual(restored.row_values(4)["state"], "E1000000")
        self.assertEqual(restored.rows[4][2].presentation, ("style",))

        invalid = grid.snapshot()
        invalid["schema_conflicts"] = {"state": [1, 2]}
        with self.assertRaises(ResultGridStateError):
            ResultGrid().restore_snapshot(invalid)


if __name__ == "__main__":
    unittest.main()
