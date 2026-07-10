"""Replay a raw recorder NDJSON capture through the production DB normalizer."""

from __future__ import annotations

import argparse
import base64
import json
import time
from pathlib import Path
from typing import Any

from .db import connect, migrate
from .ingest_store import RawIngestStore
from .lifecycle import create_session, start_session, stop_session
from .normalization import received_at_to_unix_us
from .normalizer_writer import TimingNormalizer
from .protocol import Bootstrap
from .replay import iter_records


def _received_at_us(record: dict[str, Any]) -> int:
    parsed = received_at_to_unix_us(record.get("received_at"))
    if parsed is None:
        raise ValueError("Recorder event is missing a valid received_at timestamp")
    return parsed


def import_recording(
    database: str | Path,
    recording: str | Path,
    *,
    source_slug: str = "igora",
    mode: str = "practice",
) -> str:
    """Import raw v1 frames into a stopped analysis session and return its id.

    The normalizer only consumes the raw ``frame`` event text. Its optional
    ``decoded`` siblings are diagnostics and never become a second source of
    facts, so replay uses the same parser and database path as live ingest.
    """
    database_path = Path(database)
    events = Path(recording)
    if events.is_dir():
        events = events / "events.ndjson"
    migrate(database_path)
    connection = connect(database_path)
    try:
        seed = f"import:{events.resolve()}"
        draft = create_session(
            connection,
            source_slug=source_slug,
            mode=mode,
            idempotency_key=f"{seed}:create",
        ).session
        session = start_session(
            connection,
            session_id=draft.id,
            idempotency_key=f"{seed}:start",
        ).session
        store = RawIngestStore(connection, analysis_session_id=session.id)
        store.start_run(reducer_version="timeservice-signalr-normalizer-v1-replay")
        normalizer = TimingNormalizer(session.id)
        upstream = None
        sequence = 0
        for record in iter_records(events):
            kind = record.get("kind")
            if kind == "connected":
                if upstream is not None:
                    store.close_connection(upstream, reason="recording_reconnect", disconnected_at_us=_received_at_us(record))
                upstream = store.open_connection(
                    Bootstrap(
                        source_url="recording://timeservice",
                        timekeeper_id=str(record.get("timekeeper_id") or "recorded"),
                        display_marker=str(record["display_marker"]) if record.get("display_marker") is not None else None,
                    ),
                    connected_at_us=_received_at_us(record),
                )
                continue
            if kind == "disconnected":
                if upstream is not None:
                    store.close_connection(
                        upstream,
                        reason=str(record.get("reason") or "recording_disconnect"),
                        disconnected_at_us=_received_at_us(record),
                    )
                    upstream = None
                continue
            if kind != "frame":
                continue
            if upstream is None:
                upstream = store.open_connection(
                    Bootstrap("recording://timeservice", "recorded", None), connected_at_us=_received_at_us(record)
                )
            raw_b64 = record.get("text_b64")
            if not isinstance(raw_b64, str):
                raise ValueError("Recording frame does not contain text_b64 raw source bytes")
            try:
                raw_text = base64.b64decode(raw_b64, validate=True).decode("utf-8")
            except (ValueError, UnicodeDecodeError) as error:
                raise ValueError("Recording frame text_b64 is not UTF-8 source text") from error
            record_sequence = record.get("sequence")
            sequence = record_sequence if type(record_sequence) is int and record_sequence > 0 else sequence + 1
            frame = store.persist_raw_frame(
                upstream,
                sequence=sequence,
                raw_text=raw_text,
                received_at_us=_received_at_us(record),
                monotonic_ns=int(record.get("monotonic_ns") or time.monotonic_ns()),
            )
            messages = store.decode_frame(frame)
            if messages:
                normalizer(connection, frame, messages)
            store.mark_processed(frame)
        if upstream is not None:
            store.close_connection(upstream, reason="recording_finished")
        store.finish_run(reason="recording_imported")
        stop_session(connection, session_id=session.id, idempotency_key=f"{seed}:stop")
        return session.id
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay a raw timing recording into timing.db")
    parser.add_argument("recording", help="events.ndjson or a recording directory")
    parser.add_argument("--db", required=True, help="target SQLite timing.db")
    parser.add_argument("--source", choices=("igora", "moscow"), default="igora")
    parser.add_argument("--mode", choices=("practice", "qualifying"), default="practice")
    args = parser.parse_args(argv)
    session_id = import_recording(args.db, args.recording, source_slug=args.source, mode=args.mode)
    print(json.dumps({"session_id": session_id, "database": str(Path(args.db))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
