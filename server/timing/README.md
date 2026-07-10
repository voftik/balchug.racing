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

## Timing database

`timing.db` is deliberately separate from the archive catalogue database. It
uses WAL with one ingest writer and short-lived API readers:

```bash
PYTHONPATH=server python3 -m timing.migrate --db /var/lib/balchug/timing.db
```

Raw feed events are retained for seven days after a stopped session; normalized
laps, pits, flags and metrics are retained for replay and analysis. SQLite's
online backup API is used for backups, so a running ingest writer is not paused.

Review retention without deleting anything, then apply the same policy only
after inspecting its counts:

```bash
PYTHONPATH=server python3 -m timing.retention --db /var/lib/balchug/timing.db
PYTHONPATH=server python3 -m timing.retention --db /var/lib/balchug/timing.db --apply
```

Create a consistent backup without stopping a writer. The copy is checked with
SQLite `integrity_check` and `foreign_key_check` before it is published:

```bash
PYTHONPATH=server python3 -m timing.backup \
  --db /var/lib/balchug/timing.db \
  --output /var/lib/balchug/backups/timing-$(date -u +%Y%m%dT%H%M%SZ).db
```

## Capacity planning

The active Igora Practice capture measured roughly 1.2 MB in 25 minutes, or
about 70 MB of raw WebSocket data over 24 hours at that cadence. Race traffic
is deliberately budgeted much higher: reserve 500 MB of raw data and 1 GB total
for a 24-hour analysis session, then keep at least 10 GB free under
`/var/lib/balchug` before race day. The seven-day raw-retention policy is only
applied to stopped sessions after a checkpoint exists; normalized analytics and
backups are not removed by that command.

## Engineer session lifecycle

The timing lifecycle API is a separate loopback service on port `8091`, exposed
by nginx as `/api/timing/`. It owns session intent only; it never opens an
upstream WebSocket inside an HTTP request. The ingest supervisor added later
observes active rows in `timing.db`.

Write calls require an `Authorization: Bearer` value matching `ENGINEER_TOKEN`
in `/etc/balchug/secrets.env`, plus an `Idempotency-Key`. This is deliberately
not the archive `ADMIN_TOKEN` and is never returned by `/api/boris`.

```text
POST /api/timing/sources/igora/sessions
POST /api/timing/sessions/{id}/start
POST /api/timing/sessions/{id}/stop
POST /api/timing/sessions/{id}/abort
GET  /api/timing/sources/igora/sessions/active
```

The create body is intentionally constrained:

```json
{"mode":"practice"}
{"mode":"qualifying"}
{"mode":"race","race_duration_s":14400,"required_pits":2}
```

Race duration accepts only 4, 6, 12 or 24 hours in seconds; required pits are
2 through 8. Extra fields and all manual identity, class, tyre, fuel or driver
configuration are rejected.
