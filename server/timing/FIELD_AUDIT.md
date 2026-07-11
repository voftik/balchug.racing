# Versioned Live Field Audit

Observed from the active Igora SignalR captures on 2026-07-10 and the race on
2026-07-11. The production
source emits a versioned result-table layout. There is no assumption that the
current column set or order is final. The normalizer resolves each `r_i`/`r_l`
header rather than trusting column indexes, so schema drift is retained as raw
evidence and only the dependent metric fails closed instead of silently
relabeling a field.

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

## Result grid: dynamic versioned wire contract

The visual dashboard labels and the provider wire header can differ. The
following `n`/`p` values are known semantic bindings, not a promise that every
future layout contains them. Every raw layout is diagnosed as
`time-service-result-grid-v1`, stored in
`result_schema_contract_observations`, and reported as `CURRENT` or
`DEGRADED`. A degraded layout still processes every unambiguous field actually
present; an absent field disables only calculations that require it. The
implementation never relies on the displayed column index.

| Provider header (`n`, `p`) | Visual dashboard column | Persistent/query-ready destination | Notes |
|---|---|---|---|
| `position` | `POS` | `participant_state_current.position_overall` | Absolute overall position; never substituted for class position |
| `startnumber` | `NR` | `participants.start_number` | `21` is the known Balchug entry number; a conflicting team/number observation is an identity conflict, not a silent match |
| `State` | `STATE` | `participant_state_current.state_raw/state_kind` | Raw source value plus canonical state; source may show a running timer, `In Pit`, `OutLap`, final `Finshd` or future tokens |
| `Team name` | `TEAM` | participant + identity segment `team_name` | `BALCHUG Racing` identifies our car |
| `CurrentDriver` | `DRIVER IN CAR` | current state + identity segment `driver_name_raw` | A change opens a new automatic driver segment |
| `class` | `CLS` | participant + identity segment `class_name` | `CN PRO` scopes tactics and competitor selection |
| `position_in_class` | `PIC` | `participant_state_current.position_class` | Position within class; drives class-only tactical comparisons |
| `hole` | `GAP` | immutable source fact + one-Hz full-table `participant_gap_coordinates` | Same-lap values are cumulative from the first car in their completed-lap group; `-- N laps --` starts a new group. Mixed lap/time coordinates are reconstructed before class filtering |
| `fastestRoundTime` | `BEST` | `best_lap_ms` | Invalid source sentinel remains raw-only |
| `lastRoundTime` | `LAST` | current `last_lap_ms`, immutable ledger and `canonical_laps.source_duration_*` | The only source of a lap duration. Exact Tracker chronology may identify a new lap even when two consecutive `LAST` values are equal |
| `CurrentDriverStintTime` | `STINT` | `current_driver_stint_raw` | Source-specific driver-stint representation, preserved independently from tyre logic |
| `PitTime` | `L-PIT` | `pit_time_raw`, source pit facts and computed `pit_stops` | Pit-lane source field; it is not stationary service time |
| `pitstops` | `PIT` | provider pit count plus reconciled `pit_stops` | Counter corroborates automatic pit cycles; it cannot manufacture a completed stop |
| `SectorTimes`, `p=1/2/3` | `SECT 1/2/3` | source cells plus `canonical_lap_sectors` | Source values remain authoritative; raw Tracker sector boundaries and durations are stored independently for reconciliation |

`CAR`, `LAPS` and `DIFF` are recognized optional source fields. `LAPS` was
absent at race start and was added by a live `r_l` layout update during the
2026-07-11 race; `CAR` and `DIFF` remained absent. The reducer remaps retained
cells by canonical header identity across that update and accepts subsequent
sparse `r_c` messages without waiting for an `r_i` that the provider does not
send. Car identity can still arrive from Statistics facts, Tracker independently
reconstructs the completed-lap total, and no DIFF value is synthesized.
`sectionMarker` is a currently observed optional display field.
Additional fields remain queryable raw facts. A new provider name can use its
explicit visible caption as a conservative semantic fallback; duplicate
matches fail closed. On `r_l`, retained cells are remapped by semantic/raw
identity, removed columns are dropped, and the following sparse `r_c` is
interpreted only against that active version.

Values in sparse `r_c` cells can be prefixed (`E`, `S`, `L`) and must remain raw
until their per-column semantics are decoded; they are not universally integers.

### LAST and LAPS

`LAST` is the source of a lap duration. It is not recomputed from tracker
timestamps. Every canonical `LAST` cell is written to the immutable
`result_last_cell_ledger` with one of four statuses:

- `CONFIRMED_LAP`: the only status admitted to pace and lap-time tactical
  alerts; it never increments a lap count or tyre age;
- `REFRESH_REPEAT`: an unchanged value in a dense aggregate row block, not a
  new lap;
- `UNCONFIRMED`: incomplete or ambiguous evidence, retained for audit only;
- `INVALID`: a sentinel or invalid duration, retained raw only.

A sparse `r_c LAST` is first matched to one still-unlinked canonical Tracker
lap by exact provider duration and a bounded observation window. This makes
equal consecutive real laps distinct when each has its own physical boundary.
The legacy fallback still treats an unmatched equal sparse value as
unconfirmed. A dense
aggregate block has at least two transmitted rows and at least 95% of the
current layout columns for those rows. Only an equal `LAST` in that block is a
`REFRESH_REPEAT`; a changed or new value still fails closed as unconfirmed.

`LAPS`, when present, remains a direct provider count. When it is absent, the
official green timestamp from Statistics starts lap 1 and each subsequent
main-finish or pit-finish Tracker boundary increments the exact completed-lap
count. A capture that starts after green is explicitly partial: it exposes only
an observed lower bound and never promotes its local ordinal to an official
total. `LAST` supplies duration, `SECT 1/2/3` supply sector duration, and Tracker
supplies exact raw provider start/end timestamps. None is substituted for
another. In particular, neither a changed nor a confirmed `LAST` value advances
the stint age; without a Tracker boundary that age remains unchanged.

### Canonical Tracker chronology

The `t_i` topology defines the main finish, sector loops and pit path. Every
physical `t_p` observation is retained. A lap boundary is only a passing whose
start distance is zero and whose destination is either the main timing loop or
the pit timing loop; a pit exit is not a completed lap. The initial
service/start passing is only corroboration for the Statistics green timestamp.

`canonical_laps` stores the exact provider start/end, calibrated UTC where
available, coverage status and source `LAST` reconciliation. Every `t_p` inside
the interval is linked in `canonical_lap_tracker_passings`; all three source and
Tracker sector facts remain separately queryable in `canonical_lap_sectors`.
The projection is deterministically rebuildable from immutable RAW data.

### Mixed GAP coordinates

During the race the provider groups the absolute table by completed laps. A
row such as `-- 41 laps --` means that the row starts the 41-completed-lap
group, not that the participant is 41 laps behind. Numeric rows below it are
cumulative offsets from that group's first car, not pairwise values to sum.

The reducer snapshots the full absolute table once per receive second before
selecting `CN PRO`. Each participant therefore has a lexicographic coordinate:
whole completed-lap deficit to the absolute leader plus the source time within
its own lap group. The dashboard formats both components, for example
`18 кругов + 6:20.111`. It never sums all visible numeric GAP rows and never
silently converts a lap marker to zero milliseconds.

## STATE interpretation contract

`STATE` is not a lap-time field. The normalizer preserves every raw value in
`state_raw` and maps only recognized status forms to `state_kind`:

- `S<literal>` is a source literal: `SIn Pit` -> `IN_PIT`, `SOutLap` ->
  `OUT_LAP`, and the observed final `SFinshd` -> `FINISHED`;
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

For `STATE=E<TsTime>`, `STINT=S/P<TsTime>` and `L-PIT=S<TsTime>`, a calibrated
instant is published only when it lies within 26 hours of the source frame
receive time (24 hours of race duration plus a two-hour reserve). The raw cell
and parsed provider TsTime remain unchanged when that guard rejects a value;
the derived `*_at_us` and its calibration reference are `NULL`. An invalid
`L-PIT` source timestamp therefore uses the observed STATE/PIT boundary rather
than inventing a pit entry in 2000.

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
