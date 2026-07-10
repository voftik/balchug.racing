# Live Timing Recorder

This package is the isolated protocol spike for Time Service live timing. It
uses one server-side SignalR WebSocket, records raw frames before deriving any
state, and can replay the result deterministically without a browser.

## Run locally

Install the project requirements into the environment used for the command,
then run from the repository root:

```bash
PYTHONPATH=server python3 -m timing.cli record --track igora --seconds 60
PYTHONPATH=server python3 -m timing.cli replay var/timing-recordings/igora-<timestamp>
PYTHONPATH=server python3 -m unittest discover -s server/timing/tests -v
```

The recorder produces an immutable directory:

```text
igora-20260710T120000Z/
  manifest.json       # connection/count/byte summary
  events.ndjson       # one append-only event per connection/frame
```

`events.ndjson` records the receive time and monotonic time. Every `frame`
record is appended and flushed first, with the exact UTF-8 source text encoded
as `text_b64`; a subsequent `decoded` record links the parsed provider handles
by frame sequence. A malformed upstream message therefore remains replayable.
Raw frames are the source of truth; all future table normalization and strategy
metrics can be rebuilt from them.

## Reconnect policy

The CLI reconnects with backoff `1, 2, 5, 10, 30` seconds. Every socket break
becomes an explicit `disconnected` event, and each reconnect fetches a fresh
bootstrap and SignalR token. This keeps a source gap visible instead of
inventing data.

## Current known handles

- `h_h`: heat/flag state
- `r_l`, `r_i`, `r_c`, `r_d`: dynamic results layout/snapshot/cell deltas
- `t_i`, `t_p`: tracker snapshot and loop/pit passings
- `a_i`, `a_u`: aggregate statistics
- `s_i`, `s_t`: server time

Unknown handles are retained in raw recording and counted by the replay reducer;
they do not crash recording.
