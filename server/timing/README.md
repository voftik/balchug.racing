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

## Live dashboard read API

The engineer panel reads normalized facts and calculated metrics from the same
timing database through a separate, read-only API surface. These endpoints are
public same-origin reads; they never accept a driver, tyre, fuel, competitor,
or other tactical input. Lifecycle writes above remain the only bearer-token
operations.

```text
GET /api/timing/sessions/{id}/state
GET /api/timing/sessions/{id}/metrics
GET /api/timing/sessions/{id}/metrics/history?scope_kind=participant&scope_key={participant_id}
GET /api/timing/sessions/{id}/laps?participant_id={participant_id}&limit=200
GET /api/timing/sessions/{id}/pit-stops?participant_id={participant_id}&limit=200
GET /api/timing/sessions/{id}/stream
```

All responses use `timing-live.v1`. A state snapshot includes a durable stream
cursor, measured provider facts, computed tactical metrics, and explicit
system assumptions (a completed pit-out starts fresh tyres; tyre age is
completed laps in that reconstructed stint). The API computes freshness when
it reads: `LIVE` through 3 seconds, `STALE` through 10 seconds, then
`OFFLINE`; an open source gap, Finish flag, or stopped/aborted analysis session
is immediately offline. Historical charts are constrained to 24 hours and at
most 720 points; lap and pit detail requests are bounded to 500 rows.

`/stream` is server-sent events. Its first response is a `snapshot`; reconnect
with the browser's `Last-Event-ID` to receive only unseen durable `state`,
`metric`, `lap`, `flag`, `pit`, and `alert` events. If history was retained
away, a generation changes, or a slow client falls behind its bounded queue,
the stream sends `reset` with a complete new snapshot. Heartbeats and
freshness-only `quality` events have no cursor and cannot hide a stale source.

## Ingest worker operation

`balchug-timing-ingest.service` runs `python -m timing.worker` as `www-data`.
It is a long-lived supervisor, not an HTTP-request worker and not a replacement
for the lifecycle API. It polls `analysis_sessions` in `timing.db` and opens one
upstream connection only for each session whose durable lifecycle state is
`active`. With no active session, the process remains idle and performs no
provider polling. Starting, stopping, or aborting a session remains an API
operation; the worker never creates or changes session intent on its own.

Each source frame is committed to SQLite before it is decoded or normalized.
The worker records connection attempts, clean closes, failures, and explicit
source gaps. On a disconnect it obtains a new bootstrap and SignalR token, then
reconnects with bounded backoff (`1, 2, 5, 10, 30` seconds). It never fills a
gap with inferred telemetry. On process restart it discovers durable active
sessions and replays decoded-but-unprocessed frames in receive order before
accepting newer frames, so a restart cannot skip the raw source record.

Deploy runs database migrations before enabling or restarting this unit. Its
working database is `/var/lib/balchug/timing.db`; it has no archive-catalogue
database access and writes only as `www-data`. Operational checks are:

```bash
systemctl status balchug-timing-ingest
journalctl -u balchug-timing-ingest -f
```

An operator stop or abort is authoritative: the worker observes the lifecycle
change, closes its connection, persists the final run state, and returns to the
idle supervisor loop.

## Normalized timing facts

The worker keeps the raw grid and then writes a dynamic-layout version plus one
source-cell observation for every non-metadata `r_i`/`r_c` update. Current
typed fields include absolute `POS`, crew `NR`, `TEAM`, `DRIVER IN CAR`, `CLS`,
class `PIC`, timing values, `PIT`, `STATE`, and tracker passings. `NR=21` and
`BALCHUG Racing` are automatic identity evidence; driver and class are always
observed from the source and are never operator inputs. `POS` remains absolute;
all class calculations use `PIC` and observed `CLS`.

`STATE` is stored losslessly. `E<TsTime>` is an on-track timer target,
`SIn Pit` is `IN_PIT`, `SOutLap` is `OUT_LAP`, and future literals remain
`UNKNOWN` rather than being guessed. A completed observed pit closes the active
tyre stint and starts the next one. When a layout has no LAPS column, finish
loop passings from the dynamic tracker topology count completed stint laps; no
manual tyre-age field is accepted.

Track flags retain their provider code and canonical state: Not started, Ready,
Red, Safety Car, Code 60, Finish, Green, and FCY. A live `h_i`/`h_h.f` change
opens a provisional interval at exact frame receive time. Statistics `a_u.i`
then reconciles the same interval using raw provider start/end TsTime values and
a per-connection calibrated UTC time. The raw boundary, receive observation,
calibrated boundary, and source message are all retained; an open Int64-max end
remains open rather than being fabricated.

The Statistics tab is persisted both as raw merged state and typed summaries,
best-lap history, class best laps, caution history, leader history inputs, and
car identity observations. An offline capture can be exercised through the
same raw parser/normalizer path without a browser:

```bash
PYTHONPATH=server python3 -m timing.importer \
  /var/lib/balchug/timing/captures/igora-<timestamp>/events.ndjson \
  --db /private/tmp/igora-replay.db --source igora --mode practice
```
