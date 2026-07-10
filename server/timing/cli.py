"""CLI for recording and replaying Time Service live timing frames."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from .protocol import DEFAULT_GROUPS, LiveTimingClient
from .recording import RecordingWriter, record_with_reconnect
from .replay import replay_file


TRACK_URLS = {
    "igora": "https://livetiming.getraceresults.com/igora",
    "moscow": "https://livetiming.getraceresults.com/moscowraceway",
}


def _recording_dir(root: Path, track: str) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return root / f"{track}-{stamp}"


async def record(args: argparse.Namespace) -> int:
    source_url = args.url or TRACK_URLS[args.track]
    directory = _recording_dir(Path(args.output), args.track)
    client = LiveTimingClient(source_url, DEFAULT_GROUPS, timeout=args.timeout)
    writer = RecordingWriter(directory, source_url, args.track, DEFAULT_GROUPS)
    try:
        stop_reason = await record_with_reconnect(client, writer, args.seconds)
    except KeyboardInterrupt:
        stop_reason = "interrupted"
    finally:
        writer.close(stop_reason)
    print(json.dumps({"recording": str(directory), "manifest": str(writer.manifest_path)}, ensure_ascii=False))
    return 0


def replay(args: argparse.Namespace) -> int:
    source = Path(args.recording)
    events = source / "events.ndjson" if source.is_dir() else source
    reducer = replay_file(events)
    print(json.dumps({"events": str(events), "messages": reducer.messages_applied, "state_hash": reducer.state_hash()}, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Balchug Racing live timing recorder/replay")
    commands = parser.add_subparsers(dest="command", required=True)

    record_parser = commands.add_parser("record", help="record one live timing WebSocket")
    record_parser.add_argument("--track", choices=sorted(TRACK_URLS), default="igora")
    record_parser.add_argument("--url", help="override timing source URL")
    record_parser.add_argument("--seconds", type=float, default=60.0)
    record_parser.add_argument("--timeout", type=float, default=20.0)
    record_parser.add_argument("--output", default="var/timing-recordings")
    record_parser.set_defaults(handler=record)

    replay_parser = commands.add_parser("replay", help="replay events.ndjson and print a stable state hash")
    replay_parser.add_argument("recording")
    replay_parser.set_defaults(handler=replay)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = args.handler(args)
    return asyncio.run(result) if asyncio.iscoroutine(result) else result


if __name__ == "__main__":
    sys.exit(main())
