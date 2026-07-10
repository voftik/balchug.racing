"""Command-line entry point for timing.db migrations."""

from __future__ import annotations

import argparse
import json
import sys

from .config import timing_db_path
from .db import migrate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Migrate Balchug timing.db")
    parser.add_argument("--db", default=None, help="override TIMING_DB")
    args = parser.parse_args(argv)
    database = timing_db_path(args.db)
    print(json.dumps({"database": str(database), "applied": migrate(database)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
