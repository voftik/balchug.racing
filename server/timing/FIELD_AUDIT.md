# Current Live Field Audit

Observed from the active Igora SignalR capture on 2026-07-10. The production
source contract is the current result-table schema below. The normalizer still
resolves headers rather than trusting column indexes, so an upstream fault or
schema drift is retained as raw evidence and fails closed instead of silently
relabeling a metric.

## Current source state

- Provider heat name: `Practice - Open-Pit`.
- Initial snapshot flag code was `f=6`, normalized as `GREEN`; the same capture
  later observed `f=2` (`RED`), subsequent `GREEN` transitions and `FINISH`.
  The current code is always an event-derived state, never a configured value.
- Our current source row: start number `21`, team `BALCHUG Racing`, driver
  `Киракозов Кирилл`, class `CN PRO`, class position `1`. `21` is the
  permanent Balchug Racing race-entry number and is an automatic identity
  anchor, not an engineer-entered parameter.
- The driver is intentionally observed from the feed, not configured as a
  constant. `Лобода Михаил` remains a valid future observation, not a hardcoded
  identity rule.

The engineer's selected mode (Practice, Qualifying or Race) is stored separately
from the provider heat name. A race strategy session can therefore run against a
provider heat whose display title is not itself called “Race”.

## Result grid: fixed current wire contract

The visual dashboard labels and the provider wire header are different. The
following `n`/`p` values are the stable production contract. It is validated
for every raw layout as `time-service-result-grid-v1`, stored in
`result_schema_contract_observations`, and reported as `CURRENT` or
`DEGRADED`. The implementation never relies on the displayed column index.

| Provider header (`n`, `p`) | Visual dashboard column | Persistent/query-ready destination | Notes |
|---|---|---|---|
| `position` | `POS` | `participant_state_current.position_overall` | Absolute overall position; never substituted for class position |
| `startnumber` | `NR` | `participants.start_number` | `21` is the known Balchug entry number; a conflicting team/number observation is an identity conflict, not a silent match |
| `State` | `STATE` | `participant_state_current.state_raw/state_kind` | Raw source value plus canonical state; source may show a running timer, `In Pit`, `OutLap` or future tokens |
| `Team name` | `TEAM` | participant + identity segment `team_name` | `BALCHUG Racing` identifies our car |
| `CurrentDriver` | `DRIVER IN CAR` | current state + identity segment `driver_name_raw` | A change opens a new automatic driver segment |
| `class` | `CLS` | participant + identity segment `class_name` | `CN PRO` scopes tactics and competitor selection |
| `position_in_class` | `PIC` | `participant_state_current.position_class` | Position within class; drives class-only tactical comparisons |
| `hole` | `GAP` | immutable `participant_interval_source_facts(GAP)` + current pointer | Exact source cell with message, time, subject/target position, state and lap context; only this fact may feed a gap metric |
| `fastestRoundTime` | `BEST` | `best_lap_ms` | Invalid source sentinel remains raw-only |
| `lastRoundTime` | `LAST` | current `last_lap_ms` plus immutable `LAST` cell ledger | The only source of a lap duration; every canonical source cell is retained with an explicit admission status before it can enter timing analytics |
| `CurrentDriverStintTime` | `STINT` | `current_driver_stint_raw` | Source-specific driver-stint representation, preserved independently from tyre logic |
| `PitTime` | `L-PIT` | `pit_time_raw`, source pit facts and computed `pit_stops` | Pit-lane source field; it is not stationary service time |
| `pitstops` | `PIT` | provider pit count plus reconciled `pit_stops` | Counter corroborates automatic pit cycles; it cannot manufacture a completed stop |
| `SectorTimes`, `p=1/2/3` | `SECT 1/2/3` | `last_sectors_json` / `laps.sectors_json` | Per-sector values are retained with exact source cells and linked only to a provable `LAST` boundary |

`CAR`, `LAPS` and `DIFF` are recognized optional source fields but are absent
from the current wire layout. Their absence is explicit rather than inferred:
car identity can still arrive from Statistics identity facts, official lap
totals stay unavailable until a real `LAPS` column appears, and no DIFF value
is synthesized. `sectionMarker` is a currently observed optional display field.
Additional fields remain queryable raw facts, but they do not change the
meaning of the fixed timing fields above.

Values in sparse `r_c` cells can be prefixed (`E`, `S`, `L`) and must remain raw
until their per-column semantics are decoded; they are not universally integers.

### LAST and LAPS

`LAST` is the source of a lap duration. It is not recomputed from tracker
timestamps. Every canonical `LAST` cell is written to the immutable
`result_last_cell_ledger` with one of four statuses:

- `CONFIRMED_LAP`: the only status admitted to pace, archive capture counters,
  tactical alerts and capture-local tyre age;
- `REFRESH_REPEAT`: an unchanged value in a dense aggregate row block, not a
  new lap;
- `UNCONFIRMED`: incomplete or ambiguous evidence, retained for audit only;
- `INVALID`: a sentinel or invalid duration, retained raw only.

A sparse changed (or first observed) `r_c LAST` after the accepted `r_i`
baseline for the same connection is a confirmed timing event. Equal sparse
values remain unconfirmed: equal consecutive real laps are possible. A dense
aggregate block has at least two transmitted rows and at least 95% of the
current layout columns for those rows. Only an equal `LAST` in that block is a
`REFRESH_REPEAT`; a changed or new value still fails closed as unconfirmed.

`LAPS`, when present in the current source schema, remains the official lap
count. When it is absent, the archive's capture counter and tyre age count only
confirmed ledger events since the relevant capture/stint boundary; that local
count is never presented as an official total. The horizontal coordinate of a
`LAST` fact is the time that the table observation arrived, not an invented
finish-crossing timestamp. `r_i` is an audit baseline, while tracker `t_p` or
an explicit `LAPS` cell may attach a provider lap number to the same confirmed
`LAST` source cell without replacing its duration.

## STATE interpretation contract

`STATE` is not a lap-time field. The normalizer preserves every raw value in
`state_raw` and maps only recognized status forms to `state_kind`:

- `S<literal>` is a source literal: `SIn Pit` -> `IN_PIT`, `SOutLap` ->
  `OUT_LAP`, and another literal stays `UNKNOWN` until mapped;
- `E<TsTime>` is an on-track timer target, not a lap/pit duration ->
  `ON_TRACK` when the TsTime is valid;
- a new or unrecognized token -> `UNKNOWN`, never a guessed pit or zero time.

Time Service `TsTime` is integer microseconds from a `2000-01-01` epoch, but
the provider clock is not automatically UTC: the current feed is offset by
about three hours from receive time. The normalizer stores raw TsTime and
calibrates provider-clock-to-UTC per connection from `s_i`/`s_t` plus frame
receive time (median offset). A `*_at_us` field stays `NULL` until calibrated;
the system never presents epoch arithmetic as a guessed UTC timestamp.
`E<TsTime>` is retained separately as a state timer target so it cannot be
confused with `LAST`, `BEST` or `L-PIT`.

Pit entry/exit is recorded from observed `STATE` transitions and the source
`PIT` count, while `t_p` passings are stored independently as corroborating raw
track evidence. The original STATE observation remains available for replay;
unknown source literals never manufacture a pit stop.

`participant_state_current.source_*` identifies the latest materialized table
row, which may be a sparse `LAST`, `GAP` or sector update. Exact STATE
provenance is stored separately in `state_source_cell_observation_id`,
`state_source_message_id`, `state_source_key` and `state_observed_at_us` and
is preserved until a new source `STATE` cell arrives. A row in
`participant_state_observations` is written only when that source message
actually contains `STATE`, `PIT`, `L-PIT` or `STINT`; a `LAST`-only or
`LAPS`-only sparse update cannot create a synthetic `UNKNOWN` state event.

## Heat, flag, tracker and statistics handles

| Handle | Current payload | Storage/behavior |
|---|---|---|
| `h_i` | Initial heat object including name, clock and `f` flag code | Merge into current heat/flag state |
| `h_h` | Partial heat patches | Patch, never replace the initial heat object |
| `r_i` | Initial layout plus sparse result cells | Build dynamic header schema and table state |
| `r_c` | Sparse table cell updates | Apply only non-negative row/column cells; retain metadata deltas raw |
| `a_i`, `a_u`, `a_r` | Aggregate statistics and history updates | `a_u.q`/`a_u.b` carry car identity, fast-lap facts and source flag intervals; raw first, then merged statistics current/samples |
| `t_i` | Tracker layout, transponder data, classes and paths | Raw/checkpoint source for track topology |
| `t_p` | Loop/sector/pit passings | Persist to `tracker_passings`, link to participant |
| `t_q` | Provider tracker auxiliary update | Preserve raw until its semantics are established |
| `s_i`, `s_t` | Provider server time | Preserve for source-time reconciliation |

Known current flag codes: `-1/0` not started, `1` ready, `2` red, `3`
safety-car/yellow, `4` Code 60, `5` finish, `6` green, `7` FCY. The normalizer
stores both the code and canonical flag period. A future unknown numeric code
or text label is stored as its own `UNKNOWN` period with the original value; it
never silently becomes Green or merges with a different unknown status.

### Flag time and reconciliation

`h_i/h_h.f` is the immediate current-state signal. A changed code opens or
closes a provisional period at the precise frame receive time and records its
source key. The provider can send a bare `{f: 2}` patch, so that receive time is
not falsely represented as a source timestamp.

`a_u.i` is the authoritative caution-history reconciliation: each record has a
flag kind `k`, raw provider start `f`, raw provider end `t`, clock-stopped `s`
and remark `r`. After provider-clock calibration, its start/end replace the
provisional observed boundaries while both raw values and receive provenance are
retained. `9223372036854775807` means that the period is still open.

The aggregate Statistics stream can lag the live `h_h` signal. An old open
history record therefore cannot reopen a period after the next live flag has
already arrived: it remains closed at that next frame's observed time until the
provider supplies an exact end boundary, which then replaces the fallback.

Current capture example: the transition to `f=2` arrived at
`16:20:48.364Z`; caution history records the Red Flag start as raw
`837026446926000`, calibrated to `16:20:46.926Z`. Repeated `h_h` or `a_u.i`
updates for the same source period update it in place and never create a second
RED event.

## Statistics tab contract

The provider's Statistics screen is fed by the same `a_i` initial snapshot,
`a_u` patches and `a_r` reset handle captured by the recorder. It is not a
second page scrape. The following `a_u` compact keys are confirmed against the
provider's own view model and must be normalized in addition to retaining the
merged raw payload:

| Key | Meaning |
|---|---|
| `h` | heat name |
| `g`, `f` | green-flag and finish-flag source timestamps |
| `p`, `a`, `n` | participants started, classified and not classified |
| `pt`, `pp`, `ptz` | participants on track, in pit zone and tank zone |
| `o`, `x` | total laps and total pitstops for all participants |
| `e`, `y`, `r`, `fy` | leader laps under green, safety car, Code 60 and FCY |
| `c`, `s`, `fc` | number of safety cars, Code 60s and FCYs |
| `u`, `t`, `fu` | total duration under safety car, Code 60 and FCY |

`a_u.b` is best-lap history and `a_u.q` is best lap per class. Their records
carry `r` lap number, `i` lap time, `t` source time of day, `a` average speed,
`d` driver, `n` team, `c` vehicle and `s` race number; `q` additionally carries
`m` class and `p` provider class ordering. They enrich CAR identity only when
the current source observation matches the same entry; they never overwrite a
different participant from an older heat.

The same stream also supplies `l` leader history, `d` aggregated leader laps
and `i` caution history (`k`, start `f`, end `t`, clock-stopped `s`, remark
`r`). Caution history is typed and reconciles flag periods; `l` and `d` remain
in the merged raw statistics payload pending their dedicated tactical metrics.
Unknown compact keys or changed record shapes stay in raw storage rather than
being guessed.

## Coverage gate

The recorder already persists every raw frame and all decoded handles before
normalization. The timing database schema includes query-ready tables for:

- dynamic participant identity (`TEAM`, `DRIVER IN CAR`, `CAR`, `CLS`);
- result grid, raw/parsed timing values and sectors;
- heat flags and their periods;
- tracker passings and aggregate source statistics;
- laps, pits, automatic tyre stints and metric snapshots.

The normalizer fills these tables from raw frames. Unknown headers and handles
remain in raw storage and are surfaced as schema drift rather than discarded.
