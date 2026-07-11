"""Durable NDJSON recorder for raw live timing SignalR frames."""

from __future__ import annotations

import json
import os
import time
import asyncio
import base64
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from .protocol import Bootstrap, SignalRMessage


FORMAT_VERSION = 1


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


@dataclass
class RecordingWriter:
    """Append raw frames first; derived state can always be rebuilt later."""

    directory: Path
    source_url: str
    track: str
    groups: tuple[str, ...]
    started_at: str = field(default_factory=utc_now)
    frames: int = 0
    messages: int = 0
    bytes_received: int = 0
    connections: int = 0
    frame_sequence: int = 0

    def __post_init__(self) -> None:
        self.directory = Path(self.directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self._events_path = self.directory / "events.ndjson"
        self._events = self._events_path.open("a", encoding="utf-8")
        self._started_monotonic_ns = time.monotonic_ns()
        self.write_event("recording_started", {"source_url": self.source_url, "track": self.track})
        self.write_manifest()

    @property
    def manifest_path(self) -> Path:
        return self.directory / "manifest.json"

    def write_event(self, kind: str, payload: dict[str, Any]) -> None:
        record = {
            "v": FORMAT_VERSION,
            "kind": kind,
            "received_at": utc_now(),
            "monotonic_ns": time.monotonic_ns() - self._started_monotonic_ns,
            **payload,
        }
        self._events.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._events.flush()

    def connected(self, bootstrap: Bootstrap) -> None:
        self.connections += 1
        self.write_event(
            "connected",
            {
                "connection": self.connections,
                "timekeeper_id": bootstrap.timekeeper_id,
                "display_marker": bootstrap.display_marker,
            },
        )

    def disconnected(self, reason: str) -> None:
        self.write_event("disconnected", {"reason": reason})

    def write_raw_frame(self, raw_text: str) -> int:
        """Append the exact source text before attempting to parse it."""
        wire_bytes = len(raw_text.encode("utf-8"))
        self.frame_sequence += 1
        self.frames += 1
        self.bytes_received += wire_bytes
        self.write_event(
            "frame",
            {
                "sequence": self.frame_sequence,
                "wire_bytes": wire_bytes,
                "text_b64": base64.b64encode(raw_text.encode("utf-8")).decode("ascii"),
            },
        )
        return self.frame_sequence

    def write_decoded_frame(
        self,
        frame_sequence: int,
        envelope: dict[str, Any],
        messages: Iterable[SignalRMessage],
    ) -> None:
        decoded = [message.as_dict() for message in messages]
        self.messages += len(decoded)
        self.write_event(
            "decoded",
            {
                "frame_sequence": frame_sequence,
                "cursor": envelope.get("C"),
                "groups_token": envelope.get("G"),
                "messages": decoded,
            },
        )

    def parse_error(self, frame_sequence: int, error: Exception) -> None:
        self.write_event(
            "parse_error",
            {"frame_sequence": frame_sequence, "error_type": type(error).__name__, "error": str(error)},
        )

    def write_frame(
        self,
        raw_text: str,
        envelope: dict[str, Any],
        messages: Iterable[SignalRMessage],
    ) -> None:
        sequence = self.write_raw_frame(raw_text)
        self.write_decoded_frame(sequence, envelope, messages)

    def write_manifest(self, *, finished_at: str | None = None, stop_reason: str | None = None) -> None:
        _atomic_write_json(
            self.manifest_path,
            {
                "format_version": FORMAT_VERSION,
                "source_url": self.source_url,
                "track": self.track,
                "groups": list(self.groups),
                "started_at": self.started_at,
                "finished_at": finished_at,
                "stop_reason": stop_reason,
                "frames": self.frames,
                "messages": self.messages,
                "bytes_received": self.bytes_received,
                "connections": self.connections,
                "events_file": self._events_path.name,
            },
        )

    def close(self, stop_reason: str = "completed") -> None:
        if self._events.closed:
            return
        self.write_event("recording_finished", {"reason": stop_reason})
        self.write_manifest(finished_at=utc_now(), stop_reason=stop_reason)
        self._events.close()

    def __enter__(self) -> "RecordingWriter":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


async def record_with_reconnect(
    client: Any,
    writer: RecordingWriter,
    seconds: float,
    *,
    backoff: tuple[float, ...] = (1, 2, 5, 10, 30),
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> str:
    """Record one source for a bounded duration with explicit reconnect gaps.

    A fresh ``client.frames()`` call refetches the provider bootstrap/token. The
    raw log therefore records every break instead of pretending the state was
    continuous across a socket reconnect.
    """
    loop = asyncio.get_running_loop()
    now = clock or loop.time
    deadline = now() + seconds
    failures = 0
    while now() < deadline:
        raw_stream = getattr(client, "raw_frames", None)
        stream = (raw_stream() if raw_stream is not None else client.frames()).__aiter__()
        connected = False
        reason = "socket_closed"
        try:
            while True:
                remaining = deadline - now()
                if remaining <= 0:
                    return "duration_reached"
                try:
                    item = await asyncio.wait_for(anext(stream), timeout=remaining)
                except StopAsyncIteration:
                    break
                if raw_stream is not None:
                    bootstrap, raw_text = item
                    if not connected:
                        writer.connected(bootstrap)
                        connected = True
                        failures = 0
                    sequence = writer.write_raw_frame(raw_text)
                    try:
                        from .protocol import decode_envelope
                        envelope, messages = decode_envelope(raw_text)
                    except Exception as exc:
                        writer.parse_error(sequence, exc)
                        continue
                    writer.write_decoded_frame(sequence, envelope, messages)
                else:
                    bootstrap, raw_text, envelope, messages = item
                    writer.write_frame(raw_text, envelope, messages)
                    if not connected:
                        writer.connected(bootstrap)
                        connected = True
                        failures = 0
        except asyncio.TimeoutError:
            return "duration_reached"
        except Exception as exc:  # source failures are durable events, not a crash loop
            reason = f"error:{type(exc).__name__}"
            writer.disconnected(f"{reason}:{exc}")
        finally:
            close = getattr(stream, "aclose", None)
            if close is not None:
                await close()

        if now() >= deadline:
            return "duration_reached"
        if connected and reason == "socket_closed":
            writer.disconnected(reason)
        delay = backoff[min(failures, len(backoff) - 1)]
        failures += 1
        await sleep(min(delay, max(0, deadline - now())))
    return "duration_reached"
