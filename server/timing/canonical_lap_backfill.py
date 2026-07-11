"""Operator CLI for an idempotent canonical-lap rebuild from retained RAW."""

from __future__ import annotations

import argparse
import json

from .canonical_laps import rebuild_canonical_heat
from .config import timing_db_path
from .db import connect, migrate
from .gap_coordinates import write_current_gap_snapshot


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="timing.db path; defaults to TIMING_DB")
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--heat-id", type=int)
    selection.add_argument("--session-id")
    selection.add_argument("--all", action="store_true")
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    path = timing_db_path(arguments.db)
    migrate(path)
    connection = connect(path)
    try:
        if arguments.heat_id is not None:
            heat_ids = [arguments.heat_id]
        elif arguments.session_id is not None:
            heat_ids = [
                int(row[0])
                for row in connection.execute(
                    "SELECT id FROM source_heats WHERE analysis_session_id = ? ORDER BY generation,id",
                    (arguments.session_id,),
                )
            ]
        else:
            heat_ids = [int(row[0]) for row in connection.execute("SELECT id FROM source_heats ORDER BY id")]
        if not heat_ids:
            raise SystemExit("no matching source heats")
        result: dict[str, dict[str, int]] = {}
        connection.execute("BEGIN IMMEDIATE")
        try:
            for heat_id in heat_ids:
                exists = connection.execute("SELECT 1 FROM source_heats WHERE id = ?", (heat_id,)).fetchone()
                if exists is None:
                    raise SystemExit(f"source heat {heat_id} does not exist")
                result[str(heat_id)] = rebuild_canonical_heat(connection, heat_id)
                result[str(heat_id)]["current_gap_snapshot"] = int(
                    write_current_gap_snapshot(connection, heat_id)
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
