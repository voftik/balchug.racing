"""SQLite connection, migration and online backup utilities for timing.db."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import now_us, timing_db_path


MIGRATIONS_DIR = Path(__file__).with_name("migrations")


class MigrationError(RuntimeError):
    """The database does not match the immutable migration history."""


class CheckpointError(RuntimeError):
    """A checkpoint cannot be decoded or conflicts with an existing state."""


@dataclass(frozen=True)
class Migration:
    version: str
    path: Path
    sql: str
    checksum: str


def connect(path: str | Path | None = None, *, readonly: bool = False) -> sqlite3.Connection:
    """Open a timing connection with the concurrency rules required by ingest."""
    database = timing_db_path(str(path) if path is not None else None)
    if readonly:
        uri = f"file:{database}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
    else:
        database.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(database, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=5000")
    if not readonly:
        connection.execute("PRAGMA journal_mode=WAL")
        # The raw frame is the recoverable source of truth. At this event rate,
        # FULL is worth the durability guarantee of a committed WAL record.
        connection.execute("PRAGMA synchronous=FULL")
    return connection


def discover_migrations(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    migrations: list[Migration] = []
    for path in sorted(directory.glob("[0-9][0-9][0-9][0-9]_*.sql")):
        sql = path.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=path.stem.split("_", 1)[0],
                path=path,
                sql=sql,
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            )
        )
    if not migrations:
        raise MigrationError(f"No migrations found in {directory}")
    return migrations


def _quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _statements(sql: str) -> list[str]:
    """Split one migration using SQLite's own statement-completeness parser."""
    result: list[str] = []
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            if statement:
                result.append(statement)
            buffer = ""
    if buffer.strip():
        raise MigrationError("Migration ended with an incomplete SQL statement")
    return result


def migrate(path: str | Path | None = None, *, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply each immutable migration exactly once and return applied versions."""
    connection = connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version TEXT PRIMARY KEY,
              checksum TEXT NOT NULL,
              applied_at_us INTEGER NOT NULL
            )
            """
        )
        connection.commit()
        applied: list[str] = []
        for migration in discover_migrations(directory):
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT checksum FROM schema_migrations WHERE version = ?", (migration.version,)
                ).fetchone()
                if row:
                    if row["checksum"] != migration.checksum:
                        raise MigrationError(f"Migration checksum changed: {migration.path.name}")
                    connection.commit()
                    continue
                for statement in _statements(migration.sql):
                    connection.execute(statement)
                connection.execute(
                    "INSERT INTO schema_migrations(version, checksum, applied_at_us) VALUES (?,?,?)",
                    (migration.version, migration.checksum, now_us()),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            applied.append(migration.version)
        return applied
    finally:
        connection.close()


def backup_database(source: str | Path, destination: str | Path) -> None:
    """Create a consistent SQLite backup while a WAL writer may be active."""
    source_connection = connect(source, readonly=True)
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination_path.with_suffix(destination_path.suffix + ".part")
    temporary_path.unlink(missing_ok=True)
    destination_connection = sqlite3.connect(temporary_path)
    try:
        source_connection.backup(destination_connection)
        if destination_connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("timing database backup failed integrity_check")
        if destination_connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RuntimeError("timing database backup failed foreign_key_check")
    finally:
        destination_connection.close()
        source_connection.close()
    os.replace(temporary_path, destination_path)


def encode_checkpoint(state: Any) -> tuple[str, bytes, str]:
    """Encode a deterministic, compact reducer state for restart/replay."""
    serialized = json.dumps(state, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "gzip", gzip.compress(serialized, compresslevel=6, mtime=0), hashlib.sha256(serialized).hexdigest()


def decode_checkpoint(codec: str, payload: bytes) -> Any:
    """Decode a stored checkpoint without silently accepting an unknown codec."""
    try:
        if codec == "gzip":
            serialized = gzip.decompress(payload)
        elif codec == "identity":
            serialized = payload
        else:
            raise CheckpointError(f"Unsupported checkpoint codec: {codec}")
        return json.loads(serialized)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckpointError("Checkpoint payload could not be decoded") from exc


def save_checkpoint(
    connection: sqlite3.Connection,
    *,
    source_heat_id: int,
    source_frame_id: int | None,
    source_key: str,
    observed_at_us: int,
    state: Any,
) -> bool:
    """Insert one checkpoint once; a divergent replay at the same tick fails."""
    codec, payload, state_hash = encode_checkpoint(state)
    existing = connection.execute(
        "SELECT state_hash FROM state_checkpoints WHERE source_heat_id = ? AND observed_at_us = ?",
        (source_heat_id, observed_at_us),
    ).fetchone()
    if existing:
        if existing["state_hash"] != state_hash:
            raise CheckpointError("Checkpoint conflicts with existing state at the same source tick")
        return False
    connection.execute(
        """
        INSERT INTO state_checkpoints(
          source_heat_id,source_frame_id,source_key,observed_at_us,state_hash,codec,payload,created_at_us
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        (source_heat_id, source_frame_id, source_key, observed_at_us, state_hash, codec, payload, now_us()),
    )
    return True


def load_latest_checkpoint(connection: sqlite3.Connection, source_heat_id: int) -> tuple[sqlite3.Row, Any] | None:
    """Return the newest decoded state and its immutable provenance row."""
    row = connection.execute(
        "SELECT * FROM state_checkpoints WHERE source_heat_id = ? ORDER BY observed_at_us DESC LIMIT 1",
        (source_heat_id,),
    ).fetchone()
    return (row, decode_checkpoint(row["codec"], row["payload"])) if row else None
