"""Create a verified online backup of the live timing database."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import timing_db_path
from .db import backup_database


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Back up Balchug timing.db without stopping its writer")
    parser.add_argument("--db", default=None, help="override TIMING_DB")
    parser.add_argument("--output", required=True, help="destination SQLite backup path")
    args = parser.parse_args(argv)
    database = timing_db_path(args.db)
    destination = Path(args.output)
    backup_database(database, destination)
    print(json.dumps({"database": str(database), "backup": str(destination)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
