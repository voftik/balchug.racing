"""Minimal systemd notification support without a platform dependency."""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping


def watchdog_interval_s(environment: Mapping[str, str] | None = None) -> float | None:
    """Return a conservative heartbeat interval for the current watchdog."""

    values = os.environ if environment is None else environment
    raw = values.get("WATCHDOG_USEC")
    if not raw:
        return None
    try:
        watchdog_us = int(raw)
    except ValueError:
        return None
    if watchdog_us <= 0:
        return None
    watchdog_pid = values.get("WATCHDOG_PID")
    if watchdog_pid:
        try:
            if int(watchdog_pid) != os.getpid():
                return None
        except ValueError:
            return None
    return max(0.5, watchdog_us / 2_000_000)


def notify(message: str, environment: Mapping[str, str] | None = None) -> bool:
    """Send one datagram to ``NOTIFY_SOCKET`` and fail closed when unavailable."""

    values = os.environ if environment is None else environment
    address = values.get("NOTIFY_SOCKET")
    if not address or not message:
        return False
    target: str | bytes = address
    if address.startswith("@"):
        target = b"\0" + address[1:].encode("utf-8")
    client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        client.connect(target)
        client.sendall(message.encode("utf-8"))
    except OSError:
        return False
    finally:
        client.close()
    return True
