import unittest

from timing.result_grid import ResultGrid


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

    def test_layout_swap_clears_rows_and_ignores_deltas_until_next_snapshot(self):
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

        # A new layout moves PIC before POS. Sparse values observed before r_i
        # cannot safely be assigned to the new headers.
        grid.set_layout({"h": [{"n": "NR"}, {"n": "PIC"}, {"n": "POS"}]})
        grid.apply_changes([[0, 1, "1"], [0, 2, "3"]])

        self.assertTrue(grid.schema_pending)
        self.assertFalse(grid.schema_ready)
        self.assertEqual(grid.rows, {})
        self.assertEqual(grid.all_rows(), {})

        grid.apply_snapshot({"r": [[0, 0, "21"], [0, 1, "1"], [0, 2, "3"]]})
        self.assertTrue(grid.schema_ready)
        self.assertEqual(
            grid.row_values(0),
            {"start_number": "21", "position_class": "1", "position_overall": "3"},
        )

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


if __name__ == "__main__":
    unittest.main()
