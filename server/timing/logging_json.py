"""Allowlisted JSON logs for long-lived timing services."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime


ALLOWED_FIELDS = (
    "event",
    "session_id",
    "source_slug",
    "error_type",
    "restart_delay_s",
    "incident_action",
    "incident_code",
    "severity",
    "scope_kind",
    "scope_key",
    "status",
)
ALLOWED_MESSAGES = frozenset(
    {
        "timing ingest task crashed",
        "timing operational incident transition",
        "timing operational monitor iteration failed",
        "timing source connection failed",
    }
)


class SafeJsonFormatter(logging.Formatter):
    """Serialize only operational metadata, never arbitrary record extras."""

    def format(self, record: logging.LogRecord) -> str:
        message = (
            record.msg
            if isinstance(record.msg, str) and record.msg in ALLOWED_MESSAGES
            else "unstructured log suppressed"
        )
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name if record.name.startswith("timing.") else "external",
            "message": message,
        }
        for field in ALLOWED_FIELDS:
            value = getattr(record, field, None)
            if isinstance(value, (str, int, float, bool)):
                payload[field] = value
        if record.exc_info and record.exc_info[0] is not None:
            payload.setdefault("error_type", record.exc_info[0].__name__)
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def configure_json_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(SafeJsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
