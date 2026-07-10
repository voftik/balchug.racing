"""Minimal client for the Time Service ASP.NET SignalR live timing feed.

This module intentionally models only the small wire contract used by the
collector.  It does not import or copy the provider's browser client.
"""

from __future__ import annotations

import asyncio
import json
import re
import secrets
from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterable
from urllib.parse import urljoin

try:  # Kept optional so offline replay tests can run without network packages.
    import aiohttp
except ImportError:  # pragma: no cover - exercised by runtime configuration
    aiohttp = None

try:
    from lzstring import LZString
except ImportError:  # pragma: no cover - exercised by runtime configuration
    LZString = None


DEFAULT_GROUPS = ("s", "h", "r", "t", "a")
CLIENT_PROTOCOL = "1.5"


class ProtocolError(RuntimeError):
    """The upstream feed did not match the observed SignalR wire contract."""


@dataclass(frozen=True)
class Bootstrap:
    source_url: str
    timekeeper_id: str
    display_marker: str | None


@dataclass(frozen=True)
class SignalRMessage:
    """A single provider handle and its positional arguments."""

    handle: str
    args: tuple[Any, ...]
    compressed: bool = False

    @property
    def data(self) -> Any:
        """The usual one-argument message payload, otherwise all arguments."""
        return self.args[0] if len(self.args) == 1 else list(self.args)

    def as_dict(self) -> dict[str, Any]:
        return {"handle": self.handle, "args": list(self.args), "compressed": self.compressed}


def _require_runtime_dependency(name: str, value: Any) -> None:
    if value is None:
        raise RuntimeError(f"{name} is required; install requirements.txt before recording live timing")


def parse_bootstrap(html: str, source_url: str) -> Bootstrap:
    """Extract the dynamic timekeeper id from the provider's HTML bootstrap."""
    marker_name = "new liveTiming.LiveTimingApp("
    marker_at = html.find(marker_name)
    config: dict[str, Any] | None = None
    if marker_at >= 0:
        object_at = html.find("{", marker_at + len(marker_name))
        if object_at >= 0:
            try:
                decoded, _ = json.JSONDecoder().raw_decode(html[object_at:])
            except json.JSONDecodeError:
                decoded = None
            if isinstance(decoded, dict):
                config = decoded
    if config is None:
        # Fallback retains compatibility with a minified bootstrap that wraps
        # the application object before calling the constructor.
        tid = re.search(r'"tid"\s*:\s*"(?P<value>[^"]+)"', html)
        marker = re.search(r'"dm"\s*:\s*"(?P<value>[^"]+)"', html)
        if not tid:
            raise ProtocolError("LiveTiming bootstrap did not contain tid")
        timekeeper_id = tid.group("value")
        display_marker = marker.group("value") if marker else None
    else:
        timekeeper_id = config.get("tid")
        display_marker = config.get("dm")
        if not isinstance(timekeeper_id, str) or not timekeeper_id:
            raise ProtocolError("LiveTiming app bootstrap did not contain a usable tid")
    return Bootstrap(
        source_url=source_url,
        timekeeper_id=timekeeper_id,
        display_marker=display_marker if isinstance(display_marker, str) else None,
    )


def _without_checksum(value: str) -> str:
    """Initial LZString payloads may be suffixed by ``::checksum``."""
    return value.split("::", 1)[0]


def decode_utf16_payload(value: str) -> list[Any]:
    """Decode the provider's LZString UTF-16 initial snapshot."""
    _require_runtime_dependency("lzstring", LZString)
    compressed = _without_checksum(value)
    decoder = LZString()
    try:
        decoded = decoder.decompressFromUTF16(compressed)
    except TypeError:
        # lzstring 1.0.4 mirrors JavaScript's character arithmetic and raises
        # on Python 3.14 strings. Feeding code units preserves the JavaScript
        # contract without changing the compressed bytes.
        decoded = decoder.decompressFromUTF16([ord(character) for character in compressed])
    if decoded is None:
        raise ProtocolError("Unable to decompress initial SignalR payload")
    try:
        parsed = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ProtocolError("Decoded initial SignalR payload was not JSON") from exc
    if not isinstance(parsed, list):
        raise ProtocolError("Decoded initial SignalR payload was not a message list")
    return parsed


def _messages_from_entries(entries: Iterable[Any], *, compressed: bool) -> list[SignalRMessage]:
    messages: list[SignalRMessage] = []
    for entry in entries:
        if not isinstance(entry, list) or not entry or not isinstance(entry[0], str):
            continue
        messages.append(SignalRMessage(entry[0], tuple(entry[1:]), compressed=compressed))
    return messages


def decode_envelope(raw_text: str) -> tuple[dict[str, Any], list[SignalRMessage]]:
    """Parse one SignalR envelope and expand an initial compressed snapshot."""
    try:
        envelope = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ProtocolError("SignalR frame was not JSON") from exc
    if not isinstance(envelope, dict):
        raise ProtocolError("SignalR frame was not an object")

    messages: list[SignalRMessage] = []
    for entry in envelope.get("M", []):
        if not isinstance(entry, list) or not entry or not isinstance(entry[0], str):
            continue
        handle = entry[0]
        if handle == "_" and len(entry) >= 2 and isinstance(entry[1], str):
            messages.extend(_messages_from_entries(decode_utf16_payload(entry[1]), compressed=True))
        else:
            messages.append(SignalRMessage(handle, tuple(entry[1:])))
    return envelope, messages


class LiveTimingClient:
    """One upstream server-side SignalR connection for one timing source."""

    def __init__(self, source_url: str, groups: Iterable[str] = DEFAULT_GROUPS, *, timeout: float = 20.0):
        self.source_url = source_url.rstrip("/")
        self.groups = tuple(groups)
        self.timeout = timeout

    @property
    def group_string(self) -> str:
        return ",".join(self.groups)

    @property
    def origin(self) -> str:
        match = re.match(r"^(https?://[^/]+)", self.source_url)
        if not match:
            raise ProtocolError(f"Invalid source URL: {self.source_url}")
        return match.group(1)

    def _endpoint(self, path: str) -> str:
        return urljoin(f"{self.origin}/", path.lstrip("/"))

    async def bootstrap(self, session: Any) -> Bootstrap:
        async with session.get(self.source_url) as response:
            response.raise_for_status()
            return parse_bootstrap(await response.text(), self.source_url)

    async def negotiate(self, session: Any, bootstrap: Bootstrap) -> dict[str, Any]:
        params = {
            "clientProtocol": CLIENT_PROTOCOL,
            "_tk": bootstrap.timekeeper_id,
            "_gr": self.group_string,
        }
        if bootstrap.display_marker:
            params["_tkdm"] = bootstrap.display_marker
        async with session.get(self._endpoint("/lt/negotiate"), params=params) as response:
            response.raise_for_status()
            negotiated = await response.json(content_type=None)
        if not negotiated.get("TryWebSockets") or not negotiated.get("ConnectionToken"):
            raise ProtocolError("SignalR negotiation did not provide a WebSocket token")
        return negotiated

    async def raw_frames(self) -> AsyncIterator[tuple[Bootstrap, str]]:
        """Yield unmodified WebSocket text frames until the upstream closes.

        The recorder persists every raw frame before trying to parse it. Keeping
        this iterator to one socket makes reconnect gaps explicit in its durable
        log rather than silently stitching two source states together.
        """
        _require_runtime_dependency("aiohttp", aiohttp)
        # ``total`` must stay open for the full race. Connect/bootstrap failures
        # are bounded separately, while the caller owns the recording duration.
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=self.timeout, sock_read=None)
        # Bootstrap and negotiate are ordinary upstream requests. Only the
        # WebSocket carries an Origin, and it must be the provider origin.
        async with aiohttp.ClientSession(timeout=timeout) as session:
            bootstrap = await self.bootstrap(session)
            negotiated = await self.negotiate(session, bootstrap)
            params = {
                "transport": "webSockets",
                "clientProtocol": CLIENT_PROTOCOL,
                "_tk": bootstrap.timekeeper_id,
                "_gr": self.group_string,
                "connectionToken": negotiated["ConnectionToken"],
                "tid": str(secrets.randbelow(11)),
            }
            if bootstrap.display_marker:
                params["_tkdm"] = bootstrap.display_marker
            websocket_url = self._endpoint("/lt/connect").replace("https://", "wss://", 1).replace("http://", "ws://", 1)
            # The provider already pushes at its own cadence. Sending client
            # heartbeats is unnecessary traffic and can mask source silence.
            async with session.ws_connect(websocket_url, params=params, heartbeat=None, origin=self.origin) as ws:
                async for frame in ws:
                    if frame.type == aiohttp.WSMsgType.TEXT:
                        yield bootstrap, frame.data
                    elif frame.type == aiohttp.WSMsgType.ERROR:
                        raise ws.exception() or ProtocolError("Upstream WebSocket error")
                    elif frame.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                        return

    async def frames(self) -> AsyncIterator[tuple[Bootstrap, str, dict[str, Any], list[SignalRMessage]]]:
        """Yield decoded frames for diagnostics that do not need raw persistence."""
        async for bootstrap, raw_text in self.raw_frames():
            envelope, messages = decode_envelope(raw_text)
            yield bootstrap, raw_text, envelope, messages


async def collect_for(
    client: LiveTimingClient,
    seconds: float,
) -> AsyncIterator[tuple[Bootstrap, str, dict[str, Any], list[SignalRMessage]]]:
    """Bound a single connection for the recorder CLI and tests."""
    deadline = asyncio.get_running_loop().time() + seconds
    stream = client.frames().__aiter__()
    try:
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return
            try:
                item = await asyncio.wait_for(anext(stream), timeout=remaining)
            except (StopAsyncIteration, asyncio.TimeoutError):
                return
            yield item
    finally:
        close = getattr(stream, "aclose", None)
        if close is not None:
            await close()
