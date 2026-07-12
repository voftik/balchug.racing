"""Bounded operational health and durable incident transitions for timing."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config import now_us, timing_db_path
from .db import connect, discover_migrations
from .read_api import TimingReadError, read_ingest_health


LOGGER = logging.getLogger(__name__)
OPERATIONS_SCHEMA_VERSION = "timing-operations.v1"
KNOWN_HANDLES = frozenset(
    {
        "s_i", "s_t", "h_i", "h_h", "r_l", "r_i", "r_c", "r_d",
        "t_i", "t_p", "a_i", "a_u", "a_r", "m_i", "m_c", "m_d", "m_a",
    }
)
SEVERITY_ORDER = {"CRITICAL": 0, "WARNING": 1}
ALLOWED_DETAIL_FIELDS = frozenset(
    {
        "active_session_count",
        "age_ms",
        "binding_mismatch_count",
        "checksum_mismatch_count",
        "database_total_bytes",
        "error_type",
        "failed_frame_count",
        "free_bytes",
        "free_ratio",
        "future_version_count",
        "handle_count",
        "missing",
        "missing_count",
        "observation_count",
        "open_gap",
        "pending_frame_count",
        "queue_lag_ms",
        "reconnect_count",
        "rejected_checkpoint_count",
        "runtime_checkpoint_count",
        "state",
        "stopped_at_us",
        "unknown_column_count",
        "validation_status",
        "window_s",
    }
)
SAFE_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]{0,79}\Z")


def _environment_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _environment_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if 0 < value < 1 else default


@dataclass(frozen=True)
class OperationalThresholds:
    live_max_age_ms: int = 3_000
    offline_age_ms: int = 10_000
    reconnect_window_s: int = 300
    reconnect_warning_count: int = 4
    reconnect_critical_count: int = 10
    disk_warning_free_bytes: int = 10 * 1024**3
    disk_critical_free_bytes: int = 2 * 1024**3
    disk_warning_free_ratio: float = 0.10
    disk_critical_free_ratio: float = 0.05
    database_warning_bytes: int = 8 * 1024**3
    database_critical_bytes: int = 16 * 1024**3
    unknown_handle_window_s: int = 600

    @classmethod
    def from_environment(cls) -> "OperationalThresholds":
        return cls(
            disk_warning_free_bytes=_environment_int(
                "TIMING_DISK_WARNING_FREE_BYTES", cls.disk_warning_free_bytes
            ),
            disk_critical_free_bytes=_environment_int(
                "TIMING_DISK_CRITICAL_FREE_BYTES", cls.disk_critical_free_bytes
            ),
            disk_warning_free_ratio=_environment_float(
                "TIMING_DISK_WARNING_FREE_RATIO", cls.disk_warning_free_ratio
            ),
            disk_critical_free_ratio=_environment_float(
                "TIMING_DISK_CRITICAL_FREE_RATIO", cls.disk_critical_free_ratio
            ),
            database_warning_bytes=_environment_int(
                "TIMING_DATABASE_WARNING_BYTES", cls.database_warning_bytes
            ),
            database_critical_bytes=_environment_int(
                "TIMING_DATABASE_CRITICAL_BYTES", cls.database_critical_bytes
            ),
        )


@dataclass(frozen=True)
class OperationalMonitorSettings:
    interval_s: float = 5.0
    initial_delay_s: float = 1.0


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_identifier(value: object, *, fallback: str = "UNKNOWN") -> str:
    return value if isinstance(value, str) and SAFE_IDENTIFIER_PATTERN.fullmatch(value) else fallback


def _safe_details(value: object) -> dict[str, int | float | str | bool | None]:
    """Keep incident storage bounded to non-sensitive operational metadata."""

    if not isinstance(value, Mapping):
        return {}
    result: dict[str, int | float | str | bool | None] = {}
    for key, item in sorted(value.items()):
        if key not in ALLOWED_DETAIL_FIELDS or not (
            item is None or isinstance(item, (int, float, str, bool))
        ):
            continue
        result[str(key)] = _safe_identifier(item) if key == "error_type" else item
    return result


def _json_count(value: object) -> int:
    if not isinstance(value, str):
        return 0
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return 0
    return len(decoded) if isinstance(decoded, (list, dict)) else 0


def _age_ms(observed_at_us: int | None, at_us: int) -> int | None:
    return max(0, (at_us - observed_at_us) // 1_000) if observed_at_us is not None else None


def _alert(
    alerts: list[dict[str, Any]],
    code: str,
    severity: str,
    scope_kind: str,
    scope_key: str,
    observed_at_us: int,
    **details: int | float | str | bool | None,
) -> None:
    alerts.append(
        {
            "code": code,
            "severity": severity,
            "scope_kind": scope_kind,
            "scope_key": scope_key,
            "observed_at_us": observed_at_us,
            "details": _safe_details(details),
        }
    )


def _disk_health(
    database: Path,
    thresholds: OperationalThresholds,
    alerts: list[dict[str, Any]],
    at_us: int,
    disk_usage: Callable[[str | os.PathLike[str]], Any],
) -> dict[str, Any]:
    wal = Path(f"{database}-wal")
    shm = Path(f"{database}-shm")
    stat_error: OSError | None = None
    sizes: dict[str, int] = {}
    for name, path in (("database", database), ("wal", wal), ("shm", shm)):
        try:
            sizes[name] = path.stat().st_size if path.exists() else 0
        except OSError as error:
            sizes[name] = 0
            stat_error = error
    database_bytes = sizes["database"]
    database_total_bytes = sum(sizes.values())
    try:
        usage = disk_usage(database.parent)
        total_bytes = int(usage.total)
        free_bytes = int(usage.free)
        free_ratio = free_bytes / total_bytes if total_bytes > 0 else 0.0
        status = "HEALTHY"
        if (
            free_bytes <= thresholds.disk_critical_free_bytes
            or free_ratio <= thresholds.disk_critical_free_ratio
        ):
            status = "CRITICAL"
            _alert(
                alerts,
                "DISK_SPACE_LOW",
                "CRITICAL",
                "system",
                "timing-storage",
                at_us,
                free_bytes=free_bytes,
                free_ratio=round(free_ratio, 6),
            )
        elif (
            free_bytes <= thresholds.disk_warning_free_bytes
            or free_ratio <= thresholds.disk_warning_free_ratio
        ):
            status = "WARNING"
            _alert(
                alerts,
                "DISK_SPACE_LOW",
                "WARNING",
                "system",
                "timing-storage",
                at_us,
                free_bytes=free_bytes,
                free_ratio=round(free_ratio, 6),
            )
        disk = {
            "status": status,
            "total_bytes": total_bytes,
            "free_bytes": free_bytes,
            "free_ratio": round(free_ratio, 6),
        }
    except (OSError, ValueError) as error:
        _alert(
            alerts,
            "DISK_STATUS_UNAVAILABLE",
            "WARNING",
            "system",
            "timing-storage",
            at_us,
            error_type=type(error).__name__,
        )
        disk = {"status": "UNKNOWN", "total_bytes": None, "free_bytes": None, "free_ratio": None}
    if stat_error is not None:
        _alert(
            alerts,
            "DATABASE_SIZE_UNAVAILABLE",
            "WARNING",
            "system",
            "timing-database",
            at_us,
            error_type=type(stat_error).__name__,
        )
    if database_total_bytes >= thresholds.database_critical_bytes:
        _alert(
            alerts,
            "DATABASE_SIZE_HIGH",
            "CRITICAL",
            "system",
            "timing-database",
            at_us,
            database_total_bytes=database_total_bytes,
        )
    elif database_total_bytes >= thresholds.database_warning_bytes:
        _alert(
            alerts,
            "DATABASE_SIZE_HIGH",
            "WARNING",
            "system",
            "timing-database",
            at_us,
            database_total_bytes=database_total_bytes,
        )
    return {
        "database_bytes": database_bytes,
        "wal_bytes": sizes["wal"],
        "database_total_bytes": database_total_bytes,
        "disk": disk,
    }


def _migration_health(
    connection: sqlite3.Connection,
    alerts: list[dict[str, Any]],
    at_us: int,
) -> dict[str, Any]:
    expected = discover_migrations()
    applied = {
        row["version"]: row["checksum"]
        for row in connection.execute("SELECT version,checksum FROM schema_migrations")
    }
    expected_versions = {migration.version: migration.checksum for migration in expected}
    missing = sorted(set(expected_versions) - set(applied))
    mismatched = sorted(
        version for version, checksum in expected_versions.items() if applied.get(version) not in {None, checksum}
    )
    future = sorted(set(applied) - set(expected_versions))
    status = "CURRENT" if not missing and not mismatched and not future else "DEGRADED"
    if status != "CURRENT":
        _alert(
            alerts,
            "SCHEMA_MIGRATION_MISMATCH",
            "CRITICAL",
            "system",
            "timing-database",
            at_us,
            missing_count=len(missing),
            checksum_mismatch_count=len(mismatched),
            future_version_count=len(future),
        )
    return {
        "status": status,
        "expected_version": expected[-1].version,
        "applied_version": max(applied) if applied else None,
        "missing_versions": missing,
        "checksum_mismatch_versions": mismatched,
        "future_versions": future,
    }


def _worker_health(
    connection: sqlite3.Connection,
    thresholds: OperationalThresholds,
    alerts: list[dict[str, Any]],
    at_us: int,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT state,active_session_count,started_at_us,ready_at_us,heartbeat_at_us,
               stopped_at_us,stop_reason
        FROM timing_worker_heartbeats WHERE worker_kind='timing-ingest'
        """
    ).fetchone()
    if row is None:
        _alert(alerts, "WORKER_OFFLINE", "CRITICAL", "worker", "timing-ingest", at_us, missing=True)
        return {"status": "OFFLINE", "state": "MISSING", "age_ms": None, "active_session_count": None}
    age_ms = _age_ms(int(row["heartbeat_at_us"]), at_us)
    if row["state"] != "READY" or age_ms is None or age_ms > thresholds.offline_age_ms:
        status = "OFFLINE"
        _alert(
            alerts,
            "WORKER_OFFLINE",
            "CRITICAL",
            "worker",
            "timing-ingest",
            at_us,
            state=row["state"],
            age_ms=age_ms,
        )
    elif age_ms > thresholds.live_max_age_ms:
        status = "STALE"
        _alert(
            alerts,
            "WORKER_STALE",
            "WARNING",
            "worker",
            "timing-ingest",
            at_us,
            age_ms=age_ms,
        )
    else:
        status = "LIVE"
    return {
        "status": status,
        "state": row["state"],
        "age_ms": age_ms,
        "active_session_count": int(row["active_session_count"]),
        "started_at_us": int(row["started_at_us"]),
        "ready_at_us": row["ready_at_us"],
        "heartbeat_at_us": int(row["heartbeat_at_us"]),
        "stopped_at_us": row["stopped_at_us"],
        "stop_reason": (
            _safe_identifier(row["stop_reason"])
            if row["stop_reason"] is not None
            else None
        ),
    }


def _latest_schema_health(connection: sqlite3.Connection, session_id: str) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT contract.status,contract.missing_required_keys_json,
               contract.binding_mismatches_json,contract.unknown_columns_json,
               contract.observed_at_us
        FROM result_schema_contract_observations AS contract
        JOIN source_heats AS heat ON heat.id = contract.source_heat_id
        WHERE heat.analysis_session_id = ?
        ORDER BY heat.generation DESC,contract.observed_at_us DESC,contract.id DESC
        LIMIT 1
        """,
        (session_id,),
    ).fetchone()
    if row is None:
        return {
            "status": "PENDING",
            "observed_at_us": None,
            "missing_required_count": 0,
            "binding_mismatch_count": 0,
            "unknown_column_count": 0,
        }
    return {
        "status": row["status"],
        "observed_at_us": int(row["observed_at_us"]),
        "missing_required_count": _json_count(row["missing_required_keys_json"]),
        "binding_mismatch_count": _json_count(row["binding_mismatches_json"]),
        "unknown_column_count": _json_count(row["unknown_columns_json"]),
    }


def _unknown_handles(
    connection: sqlite3.Connection,
    session_id: str,
    *,
    from_at_us: int,
) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in KNOWN_HANDLES)
    rows = connection.execute(
        f"""
        SELECT message.handle,COUNT(*) AS observation_count,MAX(frame.received_at_us) AS latest_at_us
        FROM feed_messages AS message
        JOIN feed_frames AS frame ON frame.id = message.frame_id
        WHERE frame.analysis_session_id = ? AND frame.received_at_us >= ?
          AND message.handle NOT IN ({placeholders})
        GROUP BY message.handle ORDER BY observation_count DESC,message.handle
        LIMIT 20
        """,
        (session_id, from_at_us, *sorted(KNOWN_HANDLES)),
    )
    return [
        {
            "handle": _safe_identifier(row["handle"], fallback="REDACTED"),
            "observation_count": int(row["observation_count"]),
            "latest_at_us": int(row["latest_at_us"]),
        }
        for row in rows
    ]


def _session_health(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    database: Path,
    thresholds: OperationalThresholds,
    alerts: list[dict[str, Any]],
    at_us: int,
) -> dict[str, Any]:
    session_id = row["id"]
    try:
        ingest = read_ingest_health(session_id, database=database)
    except TimingReadError as error:
        _alert(
            alerts,
            "SESSION_HEALTH_UNAVAILABLE",
            "CRITICAL",
            "session",
            session_id,
            at_us,
            error_type=type(error).__name__,
        )
        return {
            "session_id": session_id,
            "source_slug": row["source_slug"],
            "mode": row["mode"],
            "status": "OFFLINE",
            "error_type": type(error).__name__,
        }
    raw = ingest["raw"]
    processing = ingest["processing"]
    latest_frame = raw["latest_frame"]
    latest_at_us = int(latest_frame["received_at_us"]) if latest_frame is not None else None
    source_age_ms = _age_ms(
        latest_at_us if latest_at_us is not None else int(row["started_at_us"]),
        at_us,
    )
    open_gap = ingest["open_gap"]
    if open_gap is not None or source_age_ms is None or source_age_ms > thresholds.offline_age_ms:
        source_status = "OFFLINE"
        _alert(
            alerts,
            "SOURCE_OFFLINE",
            "CRITICAL",
            "session",
            session_id,
            at_us,
            age_ms=source_age_ms,
            open_gap=open_gap is not None,
        )
    elif source_age_ms > thresholds.live_max_age_ms:
        source_status = "STALE"
        _alert(
            alerts,
            "SOURCE_STALE",
            "WARNING",
            "session",
            session_id,
            at_us,
            age_ms=source_age_ms,
        )
    else:
        source_status = "CONNECTING" if latest_frame is None else "LIVE"

    oldest_pending = connection.execute(
        """
        SELECT MIN(received_at_us) FROM feed_frames
        WHERE analysis_session_id = ? AND processed_at_us IS NULL AND decode_state <> 'failed'
        """,
        (session_id,),
    ).fetchone()[0]
    queue_lag_ms = _age_ms(int(oldest_pending), at_us) if oldest_pending is not None else 0
    if processing["pending_frame_count"] and queue_lag_ms > thresholds.offline_age_ms:
        _alert(
            alerts,
            "PROCESSING_QUEUE_LAG",
            "CRITICAL",
            "session",
            session_id,
            at_us,
            pending_frame_count=int(processing["pending_frame_count"]),
            queue_lag_ms=queue_lag_ms,
        )
    elif processing["pending_frame_count"] and queue_lag_ms > thresholds.live_max_age_ms:
        _alert(
            alerts,
            "PROCESSING_QUEUE_LAG",
            "WARNING",
            "session",
            session_id,
            at_us,
            pending_frame_count=int(processing["pending_frame_count"]),
            queue_lag_ms=queue_lag_ms,
        )
    if processing["failed_frame_count"]:
        _alert(
            alerts,
            "FRAME_DECODE_FAILURE",
            "CRITICAL",
            "session",
            session_id,
            at_us,
            failed_frame_count=int(processing["failed_frame_count"]),
        )

    reconnect_window_start = at_us - thresholds.reconnect_window_s * 1_000_000
    reconnect_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM ingest_gaps WHERE analysis_session_id = ? AND started_at_us >= ?",
            (session_id, reconnect_window_start),
        ).fetchone()[0]
    )
    if reconnect_count >= thresholds.reconnect_critical_count:
        _alert(
            alerts,
            "RECONNECT_STORM",
            "CRITICAL",
            "session",
            session_id,
            at_us,
            reconnect_count=reconnect_count,
            window_s=thresholds.reconnect_window_s,
        )
    elif reconnect_count >= thresholds.reconnect_warning_count:
        _alert(
            alerts,
            "RECONNECT_STORM",
            "WARNING",
            "session",
            session_id,
            at_us,
            reconnect_count=reconnect_count,
            window_s=thresholds.reconnect_window_s,
        )

    checkpoint = ingest["runtime_checkpoints"]
    checkpoint_status = checkpoint["latest_validation"]["status"]
    rejected_checkpoint_count = int(
        checkpoint["latest_validation"]["rejected_newer_or_incompatible_checkpoint_count"]
    )
    if processing["processed_frame_count"] and checkpoint["latest"] is None:
        code = "CHECKPOINT_INVALID" if checkpoint["runtime_checkpoint_count"] else "CHECKPOINT_MISSING"
        severity = "CRITICAL" if checkpoint["runtime_checkpoint_count"] else "WARNING"
        _alert(
            alerts,
            code,
            severity,
            "session",
            session_id,
            at_us,
            runtime_checkpoint_count=int(checkpoint["runtime_checkpoint_count"]),
            validation_status=checkpoint_status,
        )
    elif rejected_checkpoint_count:
        _alert(
            alerts,
            "CHECKPOINT_INVALID",
            "WARNING",
            "session",
            session_id,
            at_us,
            rejected_checkpoint_count=rejected_checkpoint_count,
            validation_status=checkpoint_status,
        )

    schema = _latest_schema_health(connection, session_id)
    if schema["status"] == "DEGRADED":
        _alert(
            alerts,
            "RESULT_SCHEMA_DEGRADED",
            "CRITICAL",
            "session",
            session_id,
            at_us,
            missing_required_count=schema["missing_required_count"],
            binding_mismatch_count=schema["binding_mismatch_count"],
            unknown_column_count=schema["unknown_column_count"],
        )
    elif schema["status"] == "PENDING" and source_age_ms > thresholds.live_max_age_ms:
        _alert(
            alerts,
            "RESULT_SCHEMA_PENDING",
            "WARNING",
            "session",
            session_id,
            at_us,
            age_ms=source_age_ms,
        )

    unknown = _unknown_handles(
        connection,
        session_id,
        from_at_us=at_us - thresholds.unknown_handle_window_s * 1_000_000,
    )
    if unknown:
        _alert(
            alerts,
            "UNKNOWN_SOURCE_HANDLE",
            "WARNING",
            "session",
            session_id,
            at_us,
            handle_count=len(unknown),
            observation_count=sum(item["observation_count"] for item in unknown),
        )

    failed_run = connection.execute(
        """
        SELECT stop_reason,stopped_at_us FROM ingest_runs
        WHERE analysis_session_id = ? AND stopped_at_us >= ? AND stop_reason LIKE 'error:%'
        ORDER BY stopped_at_us DESC LIMIT 1
        """,
        (session_id, reconnect_window_start),
    ).fetchone()
    if failed_run is not None:
        _alert(
            alerts,
            "INGEST_RUN_FAILED",
            "CRITICAL",
            "session",
            session_id,
            at_us,
            error_type=str(failed_run["stop_reason"]).split(":", 1)[-1],
            stopped_at_us=int(failed_run["stopped_at_us"]),
        )

    return {
        "session_id": session_id,
        "source_slug": row["source_slug"],
        "mode": row["mode"],
        "started_at_us": int(row["started_at_us"]),
        "status": source_status,
        "source": {
            "status": source_status,
            "age_ms": source_age_ms,
            "latest_frame_at_us": latest_at_us,
            "open_gap": open_gap is not None,
        },
        "processing": {
            "retained_frame_count": int(raw["retained_frame_count"]),
            "processed_frame_count": int(processing["processed_frame_count"]),
            "pending_frame_count": int(processing["pending_frame_count"]),
            "failed_frame_count": int(processing["failed_frame_count"]),
            "queue_lag_ms": queue_lag_ms,
        },
        "checkpoint": {
            "status": checkpoint_status,
            "count": int(checkpoint["runtime_checkpoint_count"]),
            "eligible_count": int(checkpoint["eligible_runtime_checkpoint_count"]),
            "latest_at_us": checkpoint["latest"]["observed_at_us"] if checkpoint["latest"] else None,
        },
        "schema": schema,
        "reconnects": {"window_s": thresholds.reconnect_window_s, "count": reconnect_count},
        "unknown_handles": unknown,
        "last_restore": (
            {
                "outcome": _safe_identifier(ingest["last_restore"].get("outcome")),
                "reason": _safe_identifier(ingest["last_restore"].get("reason")),
                "replayed_tail_frames": int(
                    ingest["last_restore"].get("replayed_tail_frames", 0)
                ),
                "created_at_us": int(ingest["last_restore"].get("created_at_us", 0)),
            }
            if isinstance(ingest["last_restore"], Mapping)
            else None
        ),
    }


def collect_operational_health(
    database: str | Path | None = None,
    *,
    observed_at_us: int | None = None,
    thresholds: OperationalThresholds | None = None,
    disk_usage: Callable[[str | os.PathLike[str]], Any] = shutil.disk_usage,
) -> dict[str, Any]:
    """Read one bounded health snapshot without mutating timing state."""

    at_us = now_us() if observed_at_us is None else observed_at_us
    settings = thresholds or OperationalThresholds.from_environment()
    database_path = timing_db_path(str(database) if database is not None else None)
    alerts: list[dict[str, Any]] = []
    storage = _disk_health(database_path, settings, alerts, at_us, disk_usage)
    try:
        connection = connect(database_path, readonly=True)
    except (OSError, sqlite3.Error) as error:
        _alert(
            alerts,
            "DATABASE_UNAVAILABLE",
            "CRITICAL",
            "system",
            "timing-database",
            at_us,
            error_type=type(error).__name__,
        )
        return _finalize_health(
            at_us,
            settings,
            storage,
            {"connected": False, "schema": None},
            {"status": "UNKNOWN", "state": "UNKNOWN", "age_ms": None},
            [],
            alerts,
        )
    try:
        schema = _migration_health(connection, alerts, at_us)
        worker = _worker_health(connection, settings, alerts, at_us)
        active_rows = connection.execute(
            """
            SELECT session.id,session.mode,session.started_at_us,source.slug AS source_slug
            FROM analysis_sessions AS session
            JOIN timing_sources AS source ON source.id = session.source_id
            WHERE session.lifecycle = 'active'
            ORDER BY source.slug,session.started_at_us
            """
        ).fetchall()
        sessions = [
            _session_health(connection, row, database_path, settings, alerts, at_us)
            for row in active_rows
        ]
    except (OSError, sqlite3.Error, TimingReadError, ValueError, TypeError) as error:
        _alert(
            alerts,
            "DATABASE_UNAVAILABLE",
            "CRITICAL",
            "system",
            "timing-database",
            at_us,
            error_type=type(error).__name__,
        )
        schema = None
        worker = {"status": "UNKNOWN", "state": "UNKNOWN", "age_ms": None}
        sessions = []
    finally:
        connection.close()
    return _finalize_health(
        at_us,
        settings,
        storage,
        {"connected": True, "schema": schema},
        worker,
        sessions,
        alerts,
    )


def _finalize_health(
    at_us: int,
    thresholds: OperationalThresholds,
    storage: Mapping[str, Any],
    database: Mapping[str, Any],
    worker: Mapping[str, Any],
    sessions: Sequence[Mapping[str, Any]],
    alerts: list[dict[str, Any]],
) -> dict[str, Any]:
    alerts.sort(
        key=lambda item: (
            SEVERITY_ORDER.get(item["severity"], 9),
            item["code"],
            item["scope_kind"],
            item["scope_key"],
        )
    )
    status = "CRITICAL" if any(item["severity"] == "CRITICAL" for item in alerts) else (
        "DEGRADED" if alerts else "HEALTHY"
    )
    incomplete_codes = {"DATABASE_UNAVAILABLE", "SESSION_HEALTH_UNAVAILABLE"}
    return {
        "schema_version": OPERATIONS_SCHEMA_VERSION,
        "observed_at_us": at_us,
        "ok": status != "CRITICAL",
        "ready": status != "CRITICAL",
        "incident_reconciliation_safe": not any(
            item["code"] in incomplete_codes for item in alerts
        ),
        "status": status,
        "active_sessions": len(sessions),
        "database": {**dict(database), **dict(storage)},
        "worker": dict(worker),
        "sessions": [dict(item) for item in sessions],
        "alerts": alerts,
        "thresholds": asdict(thresholds),
    }


def reconcile_operational_incidents(
    connection: sqlite3.Connection,
    alerts: Sequence[Mapping[str, Any]],
    *,
    observed_at_us: int,
) -> list[dict[str, Any]]:
    """Open/update/resolve incidents exactly once per code and scope."""

    current = {
        (str(alert["code"]), str(alert["scope_kind"]), str(alert["scope_key"])): alert
        for alert in alerts
    }
    if not current and connection.execute(
        "SELECT 1 FROM timing_operational_incidents WHERE status='OPEN' LIMIT 1"
    ).fetchone() is None:
        return []
    transitions: list[dict[str, Any]] = []
    connection.execute("BEGIN IMMEDIATE")
    try:
        open_rows = {
            (row["incident_code"], row["scope_kind"], row["scope_key"]): row
            for row in connection.execute(
                "SELECT * FROM timing_operational_incidents WHERE status='OPEN'"
            )
        }
        for key, alert in current.items():
            details_json = _canonical_json(_safe_details(alert.get("details")))
            existing = open_rows.get(key)
            if existing is None:
                cursor = connection.execute(
                    """
                    INSERT INTO timing_operational_incidents(
                      incident_code,scope_kind,scope_key,severity,status,details_json,
                      opened_at_us,last_seen_at_us,occurrence_count,created_at_us,updated_at_us
                    ) VALUES (?,?,?,?, 'OPEN', ?,?,?,1,?,?)
                    """,
                    (
                        key[0], key[1], key[2], alert["severity"], details_json,
                        observed_at_us, observed_at_us, observed_at_us, observed_at_us,
                    ),
                )
                transitions.append(
                    {
                        "action": "OPENED",
                        "incident_id": int(cursor.lastrowid),
                        "code": key[0],
                        "severity": alert["severity"],
                        "scope_kind": key[1],
                        "scope_key": key[2],
                    }
                )
            else:
                connection.execute(
                    """
                    UPDATE timing_operational_incidents
                    SET severity=?,details_json=?,last_seen_at_us=?,
                        occurrence_count=occurrence_count+1,updated_at_us=?
                    WHERE id=?
                    """,
                    (alert["severity"], details_json, observed_at_us, observed_at_us, existing["id"]),
                )
                if existing["severity"] != alert["severity"]:
                    transitions.append(
                        {
                            "action": "ESCALATED" if alert["severity"] == "CRITICAL" else "DEESCALATED",
                            "incident_id": int(existing["id"]),
                            "code": key[0],
                            "severity": alert["severity"],
                            "scope_kind": key[1],
                            "scope_key": key[2],
                        }
                    )
        for key, existing in open_rows.items():
            if key in current:
                continue
            connection.execute(
                """
                UPDATE timing_operational_incidents
                SET status='RESOLVED',resolved_at_us=?,updated_at_us=?
                WHERE id=? AND status='OPEN'
                """,
                (observed_at_us, observed_at_us, existing["id"]),
            )
            transitions.append(
                {
                    "action": "RESOLVED",
                    "incident_id": int(existing["id"]),
                    "code": key[0],
                    "severity": existing["severity"],
                    "scope_kind": key[1],
                    "scope_key": key[2],
                }
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return transitions


def reconcile_health_report(
    database: str | Path | None,
    report: Mapping[str, Any],
) -> list[dict[str, Any]]:
    connection = connect(database)
    try:
        return reconcile_operational_incidents(
            connection,
            report.get("alerts", []),
            observed_at_us=int(report["observed_at_us"]),
        )
    finally:
        connection.close()


def read_operational_incidents(
    database: str | Path | None = None,
    *,
    open_only: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")
    connection = connect(database, readonly=True)
    try:
        where = "WHERE status='OPEN'" if open_only else ""
        rows = connection.execute(
            f"""
            SELECT id,incident_code,scope_kind,scope_key,severity,status,details_json,
                   opened_at_us,last_seen_at_us,resolved_at_us,occurrence_count
            FROM timing_operational_incidents {where}
            ORDER BY CASE status WHEN 'OPEN' THEN 0 ELSE 1 END,
                     CASE severity WHEN 'CRITICAL' THEN 0 ELSE 1 END,
                     opened_at_us DESC,id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        connection.close()
    return {
        "schema_version": OPERATIONS_SCHEMA_VERSION,
        "items": [
            {
                "id": int(row["id"]),
                "code": row["incident_code"],
                "scope_kind": row["scope_kind"],
                "scope_key": row["scope_key"],
                "severity": row["severity"],
                "status": row["status"],
                "details": json.loads(row["details_json"]),
                "opened_at_us": int(row["opened_at_us"]),
                "last_seen_at_us": int(row["last_seen_at_us"]),
                "resolved_at_us": row["resolved_at_us"],
                "occurrence_count": int(row["occurrence_count"]),
            }
            for row in rows
        ],
    }


class OperationalMonitor:
    """Run health collection outside the ingest event loop hot path."""

    def __init__(
        self,
        database: str | Path | None = None,
        *,
        settings: OperationalMonitorSettings = OperationalMonitorSettings(),
    ):
        self.database = database
        self.settings = settings

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(0.0, self.settings.initial_delay_s))
            return
        except asyncio.TimeoutError:
            pass
        while not stop_event.is_set():
            try:
                report = await asyncio.to_thread(collect_operational_health, self.database)
                transitions = (
                    await asyncio.to_thread(reconcile_health_report, self.database, report)
                    if report.get("incident_reconciliation_safe", True)
                    else []
                )
                for transition in transitions:
                    LOGGER.log(
                        logging.ERROR if transition["severity"] == "CRITICAL" else logging.WARNING,
                        "timing operational incident transition",
                        extra={
                            "event": "operational_incident",
                            "incident_action": transition["action"],
                            "incident_code": transition["code"],
                            "severity": transition["severity"],
                            "scope_kind": transition["scope_kind"],
                            "scope_key": transition["scope_key"],
                        },
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Observability must never terminate the recorder it observes.
                LOGGER.error(
                    "timing operational monitor iteration failed",
                    extra={
                        "event": "operational_monitor_failed",
                        "error_type": type(error).__name__,
                    },
                )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=max(0.25, self.settings.interval_s))
            except asyncio.TimeoutError:
                pass
