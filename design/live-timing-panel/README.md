# Live timing engineer panel

This document is the implementation contract for GitHub issues #25, #18 and
#19. The prototype is the production telemetry page itself with a disposable
client-side fixture enabled by `?demo=1` or the 24-hour upper-bound fixture by
`?demo=24h`. Demo values never reach the timing API or timing database.

## Operator contract

- Practice and Qualifying have no parameters.
- Race has exactly two parameters: duration (4/6/12/24 hours) and required
  pit stops (2 through 8).
- Authentication is a separate access step. It is not a tactical input and the
  token is retained only in `sessionStorage` until the tab closes.
- Team, car, driver, class, laps, tyres, flags, gaps, sectors and pit facts are
  always derived from the normalized source feed.
- The panel never reads or changes the third-party iframe DOM. Opening,
  closing, pinning and tab changes leave the iframe element and connection in
  place.

## Annotated wireframes

### Desktop overlay and docked layout

```text
session mode bar: Practice | Qualifying | Race        lifecycle / time / Stop
+-------------------------------------------------------------------------+
|56 px rail| 440-520 px overlay panel       | interactive timing iframe   |
|flag      | flag strip                     |                              |
|PIC       | session / freshness / identity |                              |
|tyres     | PIC | ahead | behind           |                              |
|pits      | Pace5 | tyre age | pits         |                              |
|open      | tabs + comparison + view       |                              |
+-------------------------------------------------------------------------+
```

At 1440 px and wider, Pin changes the same workspace into two stable tracks:
panel on the left and iframe on the right. It does not recreate either node.

![Desktop 1440 x 900](screenshots/desktop-1440x900.png)

![Wide desktop 2048 x 1152](screenshots/wide-2048x1152.png)

### Tablet overlay

At 768-1439 px the panel overlays the left side of the iframe. There is no
backdrop, so the remaining timing table stays visible. Pin is unavailable.

![Tablet 768 x 1024](screenshots/tablet-768x1024.png)

### Mobile full screen

Below 768 px the open panel is a full-viewport work surface with safe-area
padding and internal scrolling. The page scroll is locked while open. Close
restores the prior page/iframe position and iframe keyboard eligibility.

![Mobile 390 x 844](screenshots/mobile-390x844.png)

The Race dialog contains only the two permitted race parameters:

![Race dialog](screenshots/race-dialog-390x844.png)

## Information hierarchy

1. Flag and source freshness are always visible.
2. The decision strip exposes PIC/OA, confirmed gaps, Pace5, tyre age and pit
   obligation without scrolling.
3. Tabs separate repeated decisions: Overview, Pace, Intervals, Pits, Class
   and Events.
4. Overview uses full-width bands and one bordered metric grid. It does not
   nest cards.
5. Class view prioritizes `PIC | Team/Car | Pace5 | Tyres | Pits | Compare`.
   `Last` is intentionally hidden in the compact panel because it duplicates a
   nearby pace decision and would remove the team identity column.

## Comparison selection

BALCHUG is pinned as the red series. Automatic mode selects at most three
unique cars in this order:

1. class leader;
2. immediate class car ahead;
3. immediate class car behind;
4. nearest remaining PIC when a slot is still free.

Manual mode is display-only. It searches number/team/driver/car, allows up to
three competitors, retains a disappeared selection as `OUT`, and persists by
track/session. Colors are assigned once per participant and reused in every
view: blue, teal, amber, violet. Color is paired with labels, points and line
shape rather than carrying meaning alone.

## Chart contract

`uPlot 1.6.32` is vendored under `web/assets/vendor/uplot`; there is no CDN and
no custom chart engine. Charts use Canvas, `spanGaps: false`, exact points plus
readable lines, a shared cursor sync key, and at most BALCHUG plus three
competitors. The tooltip shows lap, exact value and full team identity.

![Exact point tooltip](screenshots/chart-tooltip-1440x900.png)

The API keeps each visible history at no more than 720 points. A local
`?demo=24h` measurement used 720 points and four series:

| Measurement | Result |
| --- | ---: |
| 100 Overview -> Pace switches, p50 | 15.7 ms |
| p95 | 17.3 ms |
| maximum | 29.5 ms |
| heap after idle GC | 4.6 MB |
| retained chart roots/canvases | 1 / 1 |
| canvas nonblank check | 32,598 opaque; 31,159 colored pixels |

The implementation phase in #19 adds source-backed lap, interval, flag and pit
markers without changing these layout or selection contracts.

## Responsive matrix

| Viewport | Closed | Open | Pin | Decision strip | Class priorities |
| --- | --- | --- | --- | --- | --- |
| 390 x 844 | 52 px rail | fixed full viewport | no | 3 x 2 | PIC, team, Pace5, tyres, pits |
| 768 x 1024 | 56 px rail | 440 px overlay | no | 3 x 2 | same |
| 1440 x 900 | 56 px rail | 440-520 px overlay | yes | 3 x 2 | same |
| 2048 x 1152 | 56 px rail | 520 px overlay/docked | yes | 3 x 2 | same |

## Component states

| State | Header/rail | Main view | Actions |
| --- | --- | --- | --- |
| mode not started | OFFLINE, empty values | explicit start prompt | mode buttons enabled |
| connecting | connecting badge, frozen values | prior snapshot or loader | duplicate start disabled |
| identity unresolved | flag/freshness remain live | strategy suppressed | no identity form |
| no completed laps | position/state if present | timing metrics are dashes | no synthetic zero |
| LIVE | green freshness badge | one-second updates | Stop available |
| STALE >3 s | amber STALE | last values frozen | source warning only |
| OFFLINE >10 s | red OFFLINE | last values frozen | reconnect is automatic |
| heat changed/reset | new generation | complete snapshot/reset | selections retained by session |
| competitor disappeared | unchanged series identity | legend says OUT | manual removal remains available |
| stopped/aborted | OFFLINE | final snapshot, then idle launcher | modes unlock after active read clears |
| replay fixture | Replay badge | deterministic interactive data | never writes API/DB |

## API to view-model mapping

| UI field | API path |
| --- | --- |
| mode/lifecycle/race parameters | `session.*` |
| heat | `heat.external_name` |
| LIVE/STALE/OFFLINE | `freshness.status` |
| flag and flag clock | `measured.track_flag.*`, session metric `flag_phase_elapsed_s` |
| team/driver/car/class/POS/PIC/state | `measured.participants[]` |
| our identity | `session.our_participant_id`, participant `is_ours` |
| Last/Best/Pace3/5/10 | participant metric values |
| tyre age/stint | `tyre_age_laps`, `stint_*` |
| completed/required pits | participant `pits_completed`, `session.required_pits` |
| ahead/behind | session metric IDs and `gap_to_ahead_ms/gap_to_behind_ms` |
| comparison rows | participant metrics filtered to our class |
| alerts/events | session `alerts`, stream flag/pit/lap/alert events |
| charts | bounded `metrics/history`, `laps`, `pit-stops` endpoints |

Lifecycle calls use the existing contract: create draft, then start; stop is an
explicit separate action. Each write carries `Authorization: Bearer` and a
fresh `Idempotency-Key`. SSE `snapshot/reset` is applied directly; other
durable events trigger one batched state refresh, never one request per event.

## Tokens

The panel extends existing `site.css` tokens rather than introducing a second
theme:

- panel/paper: `#fff`, `--paper`, `--line`;
- primary text: `--navy`, supporting text: `--muted`;
- BALCHUG/action: `--red`;
- competitors: `#1976b8`, `#148477`, `#b96d00`, `#7356a5`;
- positive operational state: `#16804b`;
- spacing steps: 4, 6, 8, 10, 12, 14, 18 px;
- radii: 4-8 px;
- timing values: tabular numerals; Russo One only for compact headings/data;
- no gradients, decorative blobs, nested cards or moving number animations.

## Accessibility and interaction

- tabs implement `tablist/tab/tabpanel`, arrow keys, Home and End;
- all commands have visible text or an aria-label and a portal tooltip;
- Escape closes comparison first, then a non-pinned overlay;
- mobile open traps Tab within the panel and removes iframe from tab order;
- touch controls are at least 44 px on mobile;
- status meaning is not color-only;
- `prefers-reduced-motion` removes panel transitions;
- tooltip positioning uses viewport coordinates, flips at edges, and is
  suppressed after pointer activation until the pointer leaves.
