# Current Live Field Audit

Observed from the active Igora SignalR capture on 2026-07-10. This is a source
contract, not a promise that every heat will expose the same layout. The
normalizer must always use dynamic headers rather than fixed column indexes.

## Current source state

- Provider heat name: `Practice - Open-Pit`.
- Provider flag code: `f=6`, normalized as `GREEN`.
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

## Result grid: current dynamic headers

| Source header | Persistent/query-ready destination | Notes |
|---|---|---|
| `position` / `POS` | `participant_state_current.position_overall` | Absolute overall position; never substituted for class position |
| `marker` | `participant_state_current.marker` | Preserve source marker/status |
| `startnumber` / `NR` | `participants.start_number` | `21` is the known Balchug entry number; a conflicting team/number observation is an identity conflict, not a silent match |
| `State` / `STATE` | `participant_state_current.state_raw/state_kind` | Raw source value plus canonical state; source may show a running timer, `In Pit`, `OutLap` or future tokens |
| `Team name` / `TEAM` | participant + identity segment `team_name` | `BALCHUG Racing` identifies our car |
| `CurrentDriver` / `DRIVER IN CAR` | current state + identity segment `driver_name_raw` | A change opens a new automatic driver segment |
| `class` / `CLS` | participant + identity segment `class_name` | `CN PRO` scopes tactics and competitor selection |
| `position_in_class` / `PIC` | `participant_state_current.position_class` | Position within class; drives class-only tactical comparisons |
| `hole` | raw/current gap fields | Semantics are validated before converting to milliseconds |
| `fastestRoundTime` | `best_lap_ms` | Invalid source sentinel remains raw-only |
| `lastRoundTime` | `last_lap_ms` | This layout has no separate `LAPS` column |
| `CurrentDriverStintTime` | `current_driver_stint_raw` | Source-specific time representation |
| `PitTime` | `pit_time_raw`, computed `pit_stops` | Do not call this stationary service time |
| `pitstops` | `pit_stops` and strategy counters | Reconciles automatic pit cycles |
| `SectorTimes(1..3)` | `last_sectors_json` / `laps.sectors_json` | Dynamic number of sectors is supported |
| `sectionMarker` | tracker/current state context | Keep raw and reconcile with `t_p` |

`CAR`, `LAPS` and `DIFF` are not present in this particular result layout. Car
identity is currently available in `a_u.q`/`a_u.b` statistics records as `c`
and is joined only to the matching source observation by start number plus
available team/class. Otherwise it remains nullable: the system must not invent
a car model from an earlier heat.

Values in sparse `r_c` cells can be prefixed (`E`, `S`, `L`) and must remain raw
until their per-column semantics are decoded; they are not universally integers.

## STATE interpretation contract

`STATE` is not a lap-time field. The normalizer preserves every raw value in
`state_raw` and maps only recognized status forms to `state_kind`:

- a running source timer or recognized elapsed-state encoding -> `ON_TRACK`;
- `In Pit` and equivalent source forms -> `IN_PIT`;
- `OutLap` and equivalent source forms -> `OUT_LAP`;
- a new or unrecognized token -> `UNKNOWN`, never a guessed pit or zero time.

Pit entry/exit is not inferred from one text cell alone. The state transition is
reconciled with `t_p.lastPassingIsInPit` and the source `PIT` count, then
debounced before creating or closing a `pit_stops` record. This keeps a transient
or reordered table update from manufacturing a stop, while keeping the original
STATE observation available for replay.

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
stores both the code and canonical flag period.

## Coverage gate

The recorder already persists every raw frame and all decoded handles before
normalization. The timing database schema includes query-ready tables for:

- dynamic participant identity (`TEAM`, `DRIVER IN CAR`, `CAR`, `CLS`);
- result grid, raw/parsed timing values and sectors;
- heat flags and their periods;
- tracker passings and aggregate source statistics;
- laps, pits, automatic tyre stints and metric snapshots.

The next normalizer task fills those tables. Unknown headers and handles remain
in raw storage and are surfaced as schema drift rather than discarded.
