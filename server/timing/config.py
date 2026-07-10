"""Configuration and time helpers shared by timing services."""

from __future__ import annotations

import os
import time
from pathlib import Path


DEFAULT_TIMING_DB = Path("/var/lib/balchug/timing.db")


def timing_db_path(value: str | None = None) -> Path:
    return Path(value or os.environ.get("TIMING_DB") or DEFAULT_TIMING_DB)


def now_us() -> int:
    """UTC wall-clock time in integer microseconds for SQLite hot tables."""
    return time.time_ns() // 1_000
