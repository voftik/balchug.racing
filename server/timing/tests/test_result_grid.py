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


if __name__ == "__main__":
    unittest.main()
