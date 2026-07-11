# Automatic Tactical Metrics Contract

This document is the normative, machine-readable contract for issue #16. The
JSON object below is the canonical catalog. A consumer must merge a group's
`inputs`, `modes`, `window`, `null_when`, and `display` into each member, then
apply member fields as overrides. Unknown is `null`; it is never coerced to
zero. `participant_id` and `competitor_id` are automatic source identities,
not engineer inputs.

```json
{
  "version": 1,
  "allowed_live_session_inputs": {
    "mode": ["practice", "qualifying", "race"],
    "race_only": {
      "race_duration_s": [14400, 21600, 43200, 86400],
      "required_pits": [2, 3, 4, 5, 6, 7, 8]
    },
    "forbidden": [
      "manual_team", "manual_car", "manual_driver", "manual_class",
      "manual_tyre_age", "fuel", "compound", "service_time",
      "driver_limits", "confidence"
    ]
  },
  "dimensions": {
    "ours": "The participant resolved from live source identity. BALCHUG Racing is primary evidence and NR=21 is a fallback/cross-check; a driver is never hard-coded.",
    "class_participant": "Every currently resolved participant in the ours.class_name scope.",
    "competitor_id": "A class participant output dimension. The UI may select/filter it without writing state or changing any calculation.",
    "stint_id": "An automatically reconstructed stint. A completed pit_in -> pit_out starts the next stint.",
    "sector_index": "A dynamic source sector index; it exists only if the active result layout supplies valid sector values."
  },
  "global": {
    "evaluation": {
      "current_tick": "Recalculate at most once per second from ordered normalized facts.",
      "history": "Persist chart points every 5 seconds and at domain-event boundaries only.",
      "replay": "The same ordered facts and session configuration must produce the same values and alerts independent of wall-clock replay speed."
    },
    "null_policy": {
      "value": "null",
      "ui": "Hide the value or render an em dash. Do not substitute zero, estimate a missing fact, or show a confidence/provenance badge.",
      "channel_indicator": ["LIVE", "STALE", "OFFLINE"],
      "source_gap": "A source gap, reconnect, unresolved identity, invalid timing value, or unmet metric gate makes the affected calculation null. Existing last-known P0 facts may remain visible with channel_status=STALE/OFFLINE."
    },
    "time_basis": {
      "event_at_us": "Use the normalized calibrated provider instant when available; otherwise use the persisted receive-time boundary only where a provisional live state is explicitly permitted. Recompute when a later reconciliation supplies the calibrated instant.",
      "durations": "Use event boundaries, never browser time. Pit duration is full pit-lane time from confirmed pit_in to confirmed pit_out, not stationary/service time.",
      "remaining_time": "For race, max(0, race_duration_s - session_elapsed_s). For practice/qualifying it is source-supplied only; otherwise null."
    },
    "lap_and_stint_rules": {
      "completed_lap": "A provider-confirmed finish-loop crossing, using the grid LAPS value when present or the dynamic tracker topology when it is absent. Do not double count both sources. Public official lap totals remain null when the grid does not expose LAPS; tracker crossings remain available only for observed stint and chronology logic.",
      "last_timing_stream": "Every valid changed r_c LAST cell from a no-LAPS layout is retained in source frame order as a timing event without inventing a provider lap number. An r_i snapshot is only a reconnect baseline. Pace may combine those raw LAST events with later explicit-LAPS rows only when the latter retain exact source-cell provenance; tracker-only and pre-provenance legacy rows remain available for tyre/stint chronology but are excluded from timing formulas.",
      "clean_lap": "A valid timed lap whose full lap interval is Green, is not an in-lap or out-lap, has no pit crossing, and does not intersect a feed/source gap.",
      "tyre_age_laps": "The number of provider-confirmed completed laps after the current stint start. At confirmed pit_out it is 0. The first partial stint begins at analysis activation and counts from that point until its first pit; it is not presented as a manual correction.",
      "mandatory_pit": "Count only a completed, ordered pit_in -> pit_out pair. A raw provider PIT counter alone cannot complete a mandatory stop."
    },
    "statistics": {
      "median": "Median of valid samples.",
      "mad": "median(abs(sample - median(samples))).",
      "percentiles": "P10/P90 use the nearest-rank percentile of the stated valid sample set.",
      "robust_slope": "Theil-Sen median pairwise slope in ms per lap.",
      "pace_n": "median of the last N clean laps in the stated scope; exactly N valid laps are required.",
      "class_pace_5": "median(Pace5) across eligible current class participants."
    },
    "display_signs": {
      "pace_delta_ms": "ours Pace5 minus reference Pace5. Negative means Balchug is faster; positive means Balchug is slower.",
      "interval_ms": "A non-negative time distance. For lapped cars, time distance is null and lap_delta is displayed instead.",
      "lap_delta_to_class_leader": "ours.completed_laps minus class_leader.completed_laps. Zero means same lap; negative means Balchug is lap(s) down.",
      "position_change": "previous position minus current position. Positive means positions gained; negative means positions lost.",
      "closure_ahead_ms_per_lap": "previous gap_to_ahead minus current gap_to_ahead, divided by Balchug completed laps. Positive label: dogonyaem (closing). Negative label: ahead_pulls_away. A provider TIME GAP without a source LAPS axis still yields closure_ms_per_min, but closure_ms_per_lap stays null.",
      "closure_behind_ms_per_lap": "current gap_to_behind minus previous gap_to_behind, divided by Balchug completed laps. Positive label: otryvaemsya (pulling away). Negative label: nas_dogonyayut (being caught). A provider TIME GAP without a source LAPS axis still yields closure_ms_per_min, but closure_ms_per_lap stays null.",
      "closure_ms_per_min": "The corresponding signed gap change scaled to one minute with the same direction labels.",
      "projected_gap_ms": "Positive means the current order remains separated in its reference direction; zero or negative means a projected catch/pass within the stated horizon.",
      "stint_trend_ms_per_lap": "Positive means lap time worsens with stint age; negative means it improves.",
      "schedule_deviation_s": "current session elapsed minus the next even-pit target. Positive means the current stint is beyond that reference target; negative means time remains before it.",
      "pit_offset": "competitor completed mandatory pit pairs minus Balchug completed mandatory pit pairs. Positive means the competitor has made more stops."
    }
  },
  "metric_groups": [
    {
      "id": "session_and_flag",
      "priority": "P0",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["analysis_session", "source_heat", "track_flag_current", "feed_connection"],
      "window": "current tick",
      "null_when": ["required source fact absent"],
      "display": {"surface": "decision_strip", "hide_when_null": true},
      "members": [
        {
          "key": "channel_status",
          "unit": "enum(LIVE|STALE|OFFLINE)",
          "formula": "LIVE while a fresh ingest connection supplies current facts; STALE when the latest facts exceed the internal freshness threshold but remain retained; OFFLINE when no live upstream connection is usable.",
          "window": "current tick",
          "null_when": [],
          "display": {"sign": "enum", "only_channel_health_indicator": true}
        },
        {
          "key": "track_flag",
          "unit": "enum(NOT_STARTED|READY|RED|SAFETY_CAR|CODE_60|FINISH|GREEN|FCY|UNKNOWN)",
          "formula": "Canonical current track flag from h_i/h_h, reconciled by Statistics caution history when available; retain provider raw code separately.",
          "window": "current phase",
          "null_when": ["no current flag observation"],
          "display": {"sign": "enum"}
        },
        {
          "key": "flag_phase_elapsed_s",
          "unit": "s",
          "formula": "max(0, evaluation_at_us - authoritative_flag_phase_start_at_us) / 1000000. The authoritative start is calibrated when reconciled, otherwise the persisted provisional receive boundary.",
          "window": "current phase",
          "null_when": ["track_flag or phase boundary absent"],
          "display": {"sign": "non_negative_duration"}
        },
        {
          "key": "heat_name",
          "unit": "text",
          "formula": "Latest non-empty Statistics heat name, otherwise the active source heat name.",
          "window": "current tick",
          "null_when": ["no heat name"],
          "display": {"sign": "text"}
        },
        {
          "key": "session_elapsed_s",
          "unit": "s",
          "formula": "max(0, evaluation_at_us - session_started_at_us) / 1000000; prefer the normalized heat start when it is known, otherwise analysis activation.",
          "window": "current tick",
          "null_when": ["no session start boundary"],
          "display": {"sign": "non_negative_duration"}
        },
        {
          "key": "session_remaining_s",
          "unit": "s",
          "formula": "For race: max(0, race_duration_s - session_elapsed_s). For other modes: source scheduled remaining time when normalized.",
          "window": "current tick",
          "null_when": ["practice/qualifying source has no scheduled end", "session_elapsed_s absent"],
          "display": {"sign": "non_negative_duration"}
        }
      ]
    },
    {
      "id": "identity_position_and_interval",
      "priority": "P0",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["resolved ours identity", "current participant grid", "completed-lap ledger"],
      "window": "current tick",
      "null_when": ["ours identity unresolved", "current grid row absent"],
      "display": {"surface": "decision_strip", "hide_when_null": true},
      "members": [
        {
          "key": "ours_identity",
          "unit": "record(start_number,team,driver,car,class)",
          "formula": "Latest identity segment from source observations. NR=21 and BALCHUG Racing resolve the crew; driver, car, and class remain observed fields.",
          "window": "current identity segment",
          "null_when": ["identity conflict or no live source identity"],
          "display": {"sign": "text_and_record"}
        },
        {
          "key": "position_overall",
          "unit": "rank",
          "formula": "Latest source POS. POS is absolute overall position and is never used as class position.",
          "window": "current tick",
          "null_when": ["POS missing or invalid"],
          "display": {"sign": "lower_is_better"}
        },
        {
          "key": "position_class",
          "unit": "rank",
          "formula": "Latest source PIC in ours.class_name.",
          "window": "current tick",
          "null_when": ["PIC or class missing"],
          "display": {"sign": "lower_is_better"}
        },
        {
          "key": "completed_laps",
          "unit": "lap",
          "formula": "Current official provider-grid LAPS count. It remains null when the result layout exposes no LAPS column; tracker crossings are not promoted to a whole-session public total.",
          "window": "current tick",
          "null_when": ["no authoritative lap source"],
          "display": {"sign": "non_negative_count"}
        },
        {
          "key": "lap_delta_to_class_leader",
          "unit": "lap",
          "formula": "ours.completed_laps - class_leader.completed_laps.",
          "window": "current tick",
          "null_when": ["ours or class leader lap count absent"],
          "display": {"sign": "lap_delta_to_class_leader"}
        },
        {
          "key": "class_leader_id",
          "unit": "participant_id",
          "formula": "Class participant with PIC=1, falling back to the best valid class order only when PIC is absent for every class row.",
          "window": "current tick",
          "null_when": ["class order unresolved"],
          "display": {"sign": "reference"}
        },
        {
          "key": "class_ahead_id",
          "unit": "participant_id",
          "formula": "Class participant immediately ahead of ours by PIC.",
          "window": "current tick",
          "null_when": ["ours is class leader or class order unresolved"],
          "display": {"sign": "reference"}
        },
        {
          "key": "class_behind_id",
          "unit": "participant_id",
          "formula": "Class participant immediately behind ours by PIC.",
          "window": "current tick",
          "null_when": ["ours is last in class or class order unresolved"],
          "display": {"sign": "reference"}
        },
        {
          "key": "gap_to_class_leader_ms",
          "unit": "ms",
          "formula": "Official normalized time interval from ours to class leader. If explicit LAPS exist for both rows they must match; if the grid has no LAPS column, a provider TIME GAP/DIFF remains usable while lap delta stays null.",
          "window": "current tick",
          "null_when": ["not same lap", "official interval absent or invalid"],
          "display": {"sign": "interval_ms", "lapped_fallback": "lap_delta_to_class_leader"}
        },
        {
          "key": "gap_to_ahead_ms",
          "unit": "ms",
          "formula": "Official normalized time interval from ours to class_ahead_id. If explicit LAPS exist for both rows they must match; if the grid has no LAPS column, a provider TIME GAP/DIFF remains usable while lap delta stays null.",
          "window": "current tick",
          "null_when": ["no class_ahead_id", "not same lap", "official interval absent or invalid"],
          "display": {"sign": "interval_ms", "lapped_fallback": "lap delta"}
        },
        {
          "key": "gap_to_behind_ms",
          "unit": "ms",
          "formula": "Official normalized time interval from ours to class_behind_id. If explicit LAPS exist for both rows they must match; if the grid has no LAPS column, a provider TIME GAP/DIFF remains usable while lap delta stays null.",
          "window": "current tick",
          "null_when": ["no class_behind_id", "not same lap", "official interval absent or invalid"],
          "display": {"sign": "interval_ms", "lapped_fallback": "lap delta"}
        }
      ]
    },
    {
      "id": "lap_state_stint_and_obligations",
      "priority": "P0",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["lap ledger", "participant STATE observations", "tracker passings", "pit/stint ledger", "analysis_session"],
      "window": "current tick",
      "null_when": ["ours identity unresolved"],
      "display": {"surface": "decision_strip", "hide_when_null": true},
      "members": [
        {
          "key": "last_lap_ms",
          "unit": "ms",
          "formula": "Most recent valid completed lap time for ours.",
          "window": "last completed lap",
          "null_when": ["no valid completed lap"],
          "display": {"sign": "lower_is_better"}
        },
        {
          "key": "best_lap_ms",
          "unit": "ms",
          "formula": "Minimum valid completed lap time for ours in the active heat.",
          "window": "active heat",
          "null_when": ["no valid completed lap"],
          "display": {"sign": "lower_is_better"}
        },
        {
          "key": "last_to_best_delta_ms",
          "unit": "ms",
          "formula": "last_lap_ms - best_lap_ms.",
          "window": "last completed lap",
          "null_when": ["last_lap_ms or best_lap_ms absent"],
          "display": {"sign": "non_negative_is_slower"}
        },
        {
          "key": "delta_to_class_best_ms",
          "unit": "ms",
          "formula": "ours.best_lap_ms - current class best valid lap time.",
          "window": "active heat",
          "null_when": ["ours or class best lap absent"],
          "display": {"sign": "negative_is_faster"}
        },
        {
          "key": "current_state",
          "unit": "enum(ON_TRACK|IN_LAP|IN_PIT|OUT_LAP|UNKNOWN)",
          "formula": "Use normalized STATE first. IN_LAP is emitted only with explicit source evidence or a confirmed pit-entry transition before pit state; otherwise preserve ON_TRACK/UNKNOWN rather than guessing.",
          "window": "current tick",
          "null_when": ["no state evidence"],
          "display": {"sign": "enum"}
        },
        {
          "key": "stint_number",
          "unit": "count",
          "formula": "1 plus the number of completed pit_in -> pit_out pairs since analysis activation.",
          "window": "current stint",
          "null_when": ["no ours stint ledger"],
          "display": {"sign": "non_negative_count"}
        },
        {
          "key": "stint_elapsed_s",
          "unit": "s",
          "formula": "evaluation_at_us minus current stint start boundary. The initial partial stint starts at analysis activation; later stints start at confirmed pit_out.",
          "window": "current stint",
          "null_when": ["stint boundary absent"],
          "display": {"sign": "non_negative_duration"}
        },
        {
          "key": "tyre_age_laps",
          "unit": "lap",
          "formula": "Count provider-confirmed completed laps after the current stint start. Reset to 0 at each confirmed pit_out.",
          "window": "current stint",
          "null_when": ["stint or lap ledger absent"],
          "display": {"sign": "non_negative_count"}
        },
        {
          "key": "pits_completed",
          "unit": "count",
          "formula": "Number of ours completed pit_in -> pit_out pairs in the active session.",
          "window": "active session",
          "null_when": ["pit ledger absent"],
          "display": {"sign": "non_negative_count"}
        },
        {
          "key": "pits_required",
          "unit": "count",
          "formula": "analysis_session.required_pits.",
          "window": "active session",
          "null_when": ["mode is not race"],
          "display": {"sign": "non_negative_count"}
        },
        {
          "key": "pits_remaining",
          "unit": "count",
          "formula": "max(0, pits_required - pits_completed).",
          "window": "active session",
          "null_when": ["pits_required or pits_completed absent"],
          "display": {"sign": "non_negative_count"}
        },
        {
          "key": "expected_remaining_laps_range",
          "unit": "record(min_laps,max_laps)",
          "formula": "For race, use remaining_time_ms divided by [Pace5 + Consistency10, max(1, Pace5 - Consistency10)], rounded outward to [floor,ceil].",
          "window": "current Pace5/Consistency10",
          "null_when": ["mode is not race", "remaining time, Pace5, or Consistency10 absent"],
          "display": {"sign": "ordered_non_negative_range"}
        }
      ]
    },
    {
      "id": "rolling_pace",
      "priority": "P1",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["clean completed laps", "current class order"],
      "window": "last N clean laps in active heat/current class",
      "null_when": ["fewer than N clean laps", "ours identity unresolved"],
      "display": {"surface": "pace_panel", "hide_when_null": true},
      "members": [
        {
          "key": "pace_3_ms",
          "unit": "ms/lap",
          "formula": "median(last 3 ours clean laps).",
          "window": "N=3",
          "null_when": ["fewer than 3 ours clean laps"],
          "display": {"sign": "lower_is_faster"}
        },
        {
          "key": "pace_5_ms",
          "unit": "ms/lap",
          "formula": "median(last 5 ours clean laps).",
          "window": "N=5",
          "null_when": ["fewer than 5 ours clean laps"],
          "display": {"sign": "lower_is_faster"}
        },
        {
          "key": "pace_10_ms",
          "unit": "ms/lap",
          "formula": "median(last 10 ours clean laps).",
          "window": "N=10",
          "null_when": ["fewer than 10 ours clean laps"],
          "display": {"sign": "lower_is_faster"}
        },
        {
          "key": "class_pace_5_ms",
          "unit": "ms/lap",
          "formula": "median(Pace5) for all eligible class_participant rows.",
          "window": "each participant N=5",
          "null_when": ["no eligible class Pace5"],
          "display": {"sign": "lower_is_faster"}
        },
        {
          "key": "pace_delta_to_reference_ms",
          "unit": "ms/lap",
          "formula": "ours.pace_5_ms - reference.pace_5_ms, emitted for references class_leader, class_ahead, class_behind, class_median, and every competitor_id.",
          "window": "N=5 for both sides",
          "null_when": ["ours or reference Pace5 absent"],
          "display": {"sign": "pace_delta_ms", "dimension": "reference"}
        },
        {
          "key": "pace_rank_class",
          "unit": "rank",
          "formula": "1 + count(eligible class Pace5 strictly lower than ours Pace5); equal values share rank by deterministic participant_id tie-break without changing the count.",
          "window": "current class Pace5",
          "null_when": ["ours Pace5 absent"],
          "display": {"sign": "lower_is_better"}
        },
        {
          "key": "position_change",
          "unit": "record(overall,class)",
          "formula": "previous position minus current position, emitted at windows 60s, 300s, 900s, and session_start for both POS and PIC.",
          "window": "60s|300s|900s|session_start",
          "null_when": ["no position snapshot at or before window start", "current position absent"],
          "display": {"sign": "position_change", "dimension": "basis_and_window"}
        }
      ]
    },
    {
      "id": "lap_quality_and_track_evolution",
      "priority": "P1",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["clean-lap classification", "class Pace5 history", "track flags", "source gaps"],
      "window": "stated metric window",
      "null_when": ["clean-lap inputs unavailable"],
      "display": {"surface": "pace_panel", "hide_when_null": true},
      "members": [
        {
          "key": "consistency_10_ms",
          "unit": "ms/lap",
          "formula": "1.4826 * MAD(last 10 ours clean laps).",
          "window": "N=10",
          "null_when": ["fewer than 10 ours clean laps"],
          "display": {"sign": "lower_is_more_consistent"}
        },
        {
          "key": "clean_lap_p10_p90_ms",
          "unit": "record(p10_ms,p90_ms,spread_ms)",
          "formula": "P10, P90, and P90-P10 of the last 10 ours clean laps.",
          "window": "N=10",
          "null_when": ["fewer than 10 ours clean laps"],
          "display": {"sign": "spread_non_negative"}
        },
        {
          "key": "clean_lap_ratio_current_stint",
          "unit": "ratio(0..1)",
          "formula": "clean timed laps / all classifiable timed completed laps in the current ours stint.",
          "window": "current stint",
          "null_when": ["no classifiable timed lap in current stint"],
          "display": {"sign": "higher_is_cleaner"}
        },
        {
          "key": "slow_lap_anomaly",
          "unit": "event(record lap_id,threshold_ms,excess_ms)",
          "formula": "For a newly completed lap: lap_time_ms > prior Pace10 + max(2000, 3 * prior MAD10). The 10-lap baseline excludes the candidate lap.",
          "window": "event; prior N=10 clean laps",
          "null_when": ["candidate is not a valid timed lap", "fewer than 10 prior clean laps"],
          "display": {"sign": "positive excess is slower", "event_only": true}
        },
        {
          "key": "track_evolution_class_ms",
          "unit": "ms/lap",
          "formula": "current ClassPace5 - ClassPace5 sampled at or immediately before now-600s.",
          "window": "10 minutes",
          "null_when": ["either class Pace5 sample absent", "sample interval intersects source gap"],
          "display": {"sign": "positive_is_class_slower"}
        }
      ]
    },
    {
      "id": "sectors",
      "priority": "P1",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["dynamic result layout sector columns", "valid sectorized laps", "current class participants"],
      "window": "active heat/current class",
      "null_when": ["active layout has no valid sector columns"],
      "display": {"surface": "sector_panel", "hide_when_null": true, "omit_group_when_all_null": true},
      "members": [
        {
          "key": "last_sector_ms",
          "unit": "ms",
          "formula": "Most recent valid sector value for ours, emitted per sector_index.",
          "window": "last completed sectorized lap",
          "null_when": ["sector value absent or invalid"],
          "display": {"sign": "lower_is_faster", "dimension": "sector_index"}
        },
        {
          "key": "personal_best_sector_ms",
          "unit": "ms",
          "formula": "Minimum valid ours sector value in active heat, emitted per sector_index.",
          "window": "active heat",
          "null_when": ["no valid own sector value"],
          "display": {"sign": "lower_is_faster", "dimension": "sector_index"}
        },
        {
          "key": "class_best_sector_ms",
          "unit": "ms",
          "formula": "Minimum valid sector value among current class participants in active heat, emitted per sector_index.",
          "window": "active heat/current class",
          "null_when": ["no valid class sector value"],
          "display": {"sign": "lower_is_faster", "dimension": "sector_index"}
        },
        {
          "key": "ideal_lap_ms",
          "unit": "ms/lap",
          "formula": "sum(personal_best_sector_ms for every active sector_index).",
          "window": "active heat",
          "null_when": ["any active sector has no own personal best"],
          "display": {"sign": "lower_is_faster"}
        },
        {
          "key": "potential_to_best_ms",
          "unit": "ms/lap",
          "formula": "best_lap_ms - ideal_lap_ms.",
          "window": "active heat",
          "null_when": ["best_lap_ms or ideal_lap_ms absent", "result is negative due to inconsistent source timing"],
          "display": {"sign": "non_negative_potential"}
        },
        {
          "key": "sector_delta_to_competitor_ms",
          "unit": "ms",
          "formula": "ours personal_best_sector_ms - competitor personal_best_sector_ms, emitted for each competitor_id and sector_index.",
          "window": "active heat",
          "null_when": ["either personal best sector absent"],
          "display": {"sign": "negative_is_faster", "dimensions": ["competitor_id", "sector_index"]}
        },
        {
          "key": "largest_sector_loss",
          "unit": "record(sector_index,delta_ms,competitor_id)",
          "formula": "Maximum positive sector_delta_to_competitor_ms across valid sectors for a competitor.",
          "window": "active heat",
          "null_when": ["no valid competitor sector delta", "all deltas are not positive"],
          "display": {"sign": "positive_is_time_lost", "dimension": "competitor_id"}
        }
      ]
    },
    {
      "id": "stint_and_tyre_pace",
      "priority": "P1",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["stint ledger", "clean laps with tyre_age_laps", "class participant stints"],
      "window": "current or stated stint",
      "null_when": ["stint ledger unavailable"],
      "display": {"surface": "stint_panel", "hide_when_null": true},
      "members": [
        {
          "key": "stint_pace_5_ms",
          "unit": "ms/lap",
          "formula": "median(last 5 ours clean laps that belong to the current stint).",
          "window": "current stint, N=5",
          "null_when": ["fewer than 5 current-stint clean laps"],
          "display": {"sign": "lower_is_faster"}
        },
        {
          "key": "stint_best_lap_ms",
          "unit": "ms/lap",
          "formula": "Minimum ours clean lap time in the current stint.",
          "window": "current stint",
          "null_when": ["no current-stint clean lap"],
          "display": {"sign": "lower_is_faster"}
        },
        {
          "key": "stint_consistency_10_ms",
          "unit": "ms/lap",
          "formula": "1.4826 * MAD(last 10 ours clean laps in current stint).",
          "window": "current stint, N=10",
          "null_when": ["fewer than 10 current-stint clean laps"],
          "display": {"sign": "lower_is_more_consistent"}
        },
        {
          "key": "stint_trend_ms_per_lap",
          "unit": "ms/lap",
          "formula": "Theil-Sen slope of clean lap_time_ms against tyre_age_laps in the current stint.",
          "window": "current stint, minimum 6 clean laps with distinct tyre ages",
          "null_when": ["fewer than 6 eligible points", "no tyre-age variation"],
          "display": {"sign": "stint_trend_ms_per_lap"}
        },
        {
          "key": "stint_cumulative_pace_change_ms",
          "unit": "ms/lap",
          "formula": "stint_trend_ms_per_lap * (current tyre_age_laps - minimum tyre_age_laps used by the trend).",
          "window": "current stint trend sample",
          "null_when": ["stint_trend_ms_per_lap absent"],
          "display": {"sign": "positive_is_slower_since_stint_start"}
        },
        {
          "key": "stint_pace_delta_previous_ms",
          "unit": "ms/lap",
          "formula": "current stint_pace_5_ms - previous completed stint Pace5.",
          "window": "current and immediately previous stint",
          "null_when": ["either stint has fewer than 5 clean laps"],
          "display": {"sign": "negative_is_faster"}
        },
        {
          "key": "pace_delta_near_tyre_age_ms",
          "unit": "ms/lap",
          "formula": "median(ours current-stint clean laps with age within ours_age +/-2) minus the equivalent median for competitor_id; emit only when abs(current ages) <=2 and each side has at least 3 eligible laps.",
          "window": "current stints, tyre-age band +/-2 laps",
          "null_when": ["competitor not in class", "age difference exceeds 2", "fewer than 3 eligible laps on either side"],
          "display": {"sign": "pace_delta_ms", "dimension": "competitor_id"}
        },
        {
          "key": "stint_abrupt_deterioration",
          "unit": "event(record lap_ids,threshold_ms)",
          "formula": "Emit when two consecutive current-stint clean laps each exceed their own prior Pace10 + max(2000, 3 * prior MAD10) baseline.",
          "window": "event; prior N=10 clean laps",
          "null_when": ["two eligible candidate laps or either baseline absent"],
          "display": {"sign": "event_only"}
        },
        {
          "key": "stint_summary",
          "unit": "record(stint_id,completed_laps,elapsed_s,pace_5_ms,best_lap_ms,consistency_10_ms)",
          "formula": "One derived record for every automatically reconstructed ours stint.",
          "window": "all stints in active session",
          "null_when": ["no stint ledger"],
          "display": {"sign": "comparison_series", "dimension": "stint_id"}
        }
      ]
    },
    {
      "id": "interval_and_battle",
      "priority": "P1",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["same-lap official gaps", "current class order", "Pace5", "clean-lap history", "track_flag", "participant state", "session_remaining_s"],
      "window": "30s|60s|180s unless stated otherwise",
      "null_when": ["ours or compared participant is not same lap", "either is in pit/in lap/out lap", "flag is not GREEN", "source gap intersects the window"],
      "display": {"surface": "battle_panel", "hide_when_null": true},
      "members": [
        {
          "key": "closure_ahead",
          "unit": "record(ms_per_lap,ms_per_min,label)",
          "formula": "For each window, (gap_to_ahead at window start - current gap_to_ahead) / ours completed laps in window; ms_per_min uses the same numerator divided by elapsed wall time. label is dogonyaem, ahead_pulls_away, or stable.",
          "window": "30s|60s|180s",
          "null_when": ["group gates", "no positive Balchug lap delta in window"],
          "display": {"sign": "closure_ahead_ms_per_lap", "dimension": "window"}
        },
        {
          "key": "closure_behind",
          "unit": "record(ms_per_lap,ms_per_min,label)",
          "formula": "For each window, (current gap_to_behind - gap_to_behind at window start) / ours completed laps in window; ms_per_min uses the same numerator divided by elapsed wall time. label is otryvaemsya, nas_dogonyayut, or stable.",
          "window": "30s|60s|180s",
          "null_when": ["group gates", "no positive Balchug lap delta in window"],
          "display": {"sign": "closure_behind_ms_per_lap", "dimension": "window"}
        },
        {
          "key": "catch_range",
          "unit": "record(min_laps,max_laps,min_s,max_s,direction)",
          "formula": "For ahead use every positive valid closure_ahead estimate; for behind use abs(negative valid closure_behind). Divide current interval by each directional rate, then take min/max. Convert each bound with ours Pace5.",
          "window": "valid 30s|60s|180s estimates",
          "null_when": ["fewer than 2 directional closure estimates", "Pace5 absent", "group gates"],
          "display": {"sign": "ordered_range", "dimension": "direction(ahead|behind)"}
        },
        {
          "key": "required_pace_to_catch_ahead_ms",
          "unit": "ms/lap",
          "formula": "ahead Pace5 - gap_to_ahead_ms / (session_remaining_s * 1000 / ours Pace5). This is the maximum average ours pace that erases the current ahead gap by calculated finish.",
          "window": "current Pace5 and remaining race time",
          "null_when": ["mode is not race", "session_remaining_s <=0", "ours/ahead Pace5 or gap absent", "group gates"],
          "display": {"sign": "lower_than_or_equal_means_catch"}
        },
        {
          "key": "required_pace_to_defend_behind_ms",
          "unit": "ms/lap",
          "formula": "behind Pace5 + gap_to_behind_ms / (session_remaining_s * 1000 / ours Pace5). This is the maximum average ours pace that holds the current behind gap by calculated finish.",
          "window": "current Pace5 and remaining race time",
          "null_when": ["mode is not race", "session_remaining_s <=0", "ours/behind Pace5 or gap absent", "group gates"],
          "display": {"sign": "lower_than_or_equal_means_defend"}
        },
        {
          "key": "projected_gap_ms",
          "unit": "ms",
          "formula": "For ahead: gap_to_ahead_ms - closure_ahead_60s.ms_per_lap * horizon_laps. For behind: gap_to_behind_ms + closure_behind_60s.ms_per_lap * horizon_laps.",
          "window": "horizon_laps=5|10, closure=60s",
          "null_when": ["closure 60s absent", "group gates"],
          "display": {"sign": "projected_gap_ms", "dimensions": ["direction(ahead|behind)", "horizon_laps"]}
        },
        {
          "key": "class_density",
          "unit": "count",
          "formula": "Count other same-lap class participants with valid official absolute interval to ours <= threshold_ms.",
          "window": "current tick, threshold=5000|10000|30000ms",
          "null_when": ["ours lap or class participant intervals unavailable"],
          "display": {"sign": "non_negative_count", "dimension": "threshold_ms"}
        }
      ]
    },
    {
      "id": "pit_history_and_equal_schedule",
      "priority": "P1",
      "modes": ["practice", "qualifying", "race"],
      "inputs": ["pit/stint ledger", "class participant stints", "analysis_session", "session_elapsed_s", "session_remaining_s"],
      "window": "active session/current race",
      "null_when": ["pit ledger unavailable"],
      "display": {"surface": "pit_panel", "hide_when_null": true},
      "members": [
        {
          "key": "pit_history",
          "unit": "record(pit_number,pit_in_at_us,pit_out_at_us,pit_in_lap,pit_out_lap,pit_lane_duration_ms)",
          "formula": "One record per completed pit_in -> pit_out pair for ours and each class_participant. duration = pit_out_at_us - pit_in_at_us.",
          "window": "active session",
          "null_when": ["no completed pit pair"],
          "display": {"sign": "duration_and_history", "dimension": "participant_id"}
        },
        {
          "key": "total_pit_lane_time_ms",
          "unit": "ms",
          "formula": "sum(pit_lane_duration_ms) over completed pit pairs.",
          "window": "active session",
          "null_when": ["no completed pit pair"],
          "display": {"sign": "non_negative_duration", "dimension": "participant_id"}
        },
        {
          "key": "median_pit_lane_time_ms",
          "unit": "ms",
          "formula": "median(completed pit_lane_duration_ms), emitted for ours and class aggregate.",
          "window": "active session",
          "null_when": ["no completed pit pair in requested scope"],
          "display": {"sign": "lower_is_shorter", "dimension": "scope(ours|class)"}
        },
        {
          "key": "pit_lane_anomaly",
          "unit": "event(record pit_id,median_ms,mad_ms,excess_ms)",
          "formula": "For a new completed pit, abs(duration_ms - scope median_ms) > 3 * scope MAD. The relevant scope needs at least 3 completed durations before the candidate pit.",
          "window": "event; completed pit history",
          "null_when": ["fewer than 3 baseline completed durations", "candidate duration absent"],
          "display": {"sign": "positive excess is unusual", "event_only": true, "dimension": "scope(ours|class)"}
        },
        {
          "key": "competitor_stint_state",
          "unit": "record(stint_number,stint_elapsed_s,tyre_age_laps,pits_completed)",
          "formula": "Current automatically reconstructed stint state for every class_participant.",
          "window": "current tick",
          "null_when": ["competitor stint ledger absent"],
          "display": {"sign": "record", "dimension": "competitor_id"}
        },
        {
          "key": "pit_offset",
          "unit": "count",
          "formula": "competitor pits_completed - ours pits_completed for each competitor_id.",
          "window": "active session",
          "null_when": ["ours or competitor completed pit count absent"],
          "display": {"sign": "pit_offset", "dimension": "competitor_id"}
        },
        {
          "key": "remaining_equal_stint_s",
          "unit": "s",
          "formula": "session_remaining_s / (pits_remaining + 1).",
          "window": "current race",
          "null_when": ["mode is not race", "session_remaining_s or pits_remaining absent"],
          "display": {"sign": "non_negative_duration"}
        },
        {
          "key": "initial_equal_stint_target_s",
          "unit": "s",
          "formula": "race_duration_s / (required_pits + 1).",
          "window": "race configuration",
          "null_when": ["mode is not race"],
          "display": {"sign": "non_negative_duration"}
        },
        {
          "key": "stop_load_per_hour",
          "unit": "pit/hour",
          "formula": "pits_remaining / (session_remaining_s / 3600).",
          "window": "current race",
          "null_when": ["mode is not race", "session_remaining_s <=0", "pits_remaining absent"],
          "display": {"sign": "non_negative_rate"}
        },
        {
          "key": "next_equal_pit_target_elapsed_s",
          "unit": "s",
          "formula": "(pits_completed + 1) * initial_equal_stint_target_s.",
          "window": "current race",
          "null_when": ["mode is not race", "pits_completed >= required_pits", "target absent"],
          "display": {"sign": "session_elapsed_coordinate"}
        },
        {
          "key": "stint_schedule_deviation_s",
          "unit": "s",
          "formula": "session_elapsed_s - next_equal_pit_target_elapsed_s.",
          "window": "current race",
          "null_when": ["next_equal_pit_target_elapsed_s or session_elapsed_s absent"],
          "display": {"sign": "schedule_deviation_s"}
        }
      ]
    },
    {
      "id": "p2_strategy",
      "priority": "P2",
      "modes": ["race"],
      "inputs": ["P0/P1 facts", "completed pit cycles", "completed stints", "historical normalized sessions for same source/class"],
      "window": "current race plus comparable persisted observations",
      "null_when": ["p2_history_eligible=false", "flag/pit/lap/gap gate for the individual metric fails"],
      "display": {"surface": "strategy_panel", "hide_when_null": true, "feature_gated": true},
      "p2_history_eligible": "A deterministic, versioned internal policy must require at least 3 comparable completed observations for the specific model/flag condition. It is not an engineer input and is not displayed as confidence.",
      "members": [
        {
          "key": "green_pit_cycle_loss_ms",
          "unit": "ms",
          "formula": "observed completed Green pit-cycle elapsed time - expected clean-lap equivalent time for the same normalized distance/time span.",
          "window": "completed Green pit cycles",
          "null_when": ["no comparable completed Green cycles", "clean-lap equivalent cannot be determined"],
          "display": {"sign": "positive_is_pit_time_lost"}
        },
        {
          "key": "neutralized_pit_cycle_loss_ms",
          "unit": "ms",
          "formula": "Same as green_pit_cycle_loss_ms, partitioned by SAFETY_CAR, FCY, or CODE_60 flag phase.",
          "window": "completed pit cycles by flag kind",
          "null_when": ["no comparable completed cycle for the active neutralization kind"],
          "display": {"sign": "positive_is_pit_time_lost", "dimension": "flag_kind"}
        },
        {
          "key": "projected_rejoin",
          "unit": "record(ahead_id,behind_id,projected_gap_ms)",
          "formula": "Apply the observed relevant pit-cycle loss to the current same-lap class timeline and resolve nearest projected class neighbors after a pit now.",
          "window": "current race",
          "null_when": ["no applicable observed pit-cycle loss", "cars not comparable on class timeline"],
          "display": {"sign": "projected_gap_ms"}
        },
        {
          "key": "virtual_class_order",
          "unit": "record(rank,net_gap_ms)",
          "formula": "Official class timeline adjusted only by remaining mandatory completed-pit debt multiplied by observed comparable pit-cycle loss.",
          "window": "current race",
          "null_when": ["any participant has no applicable pit-loss input", "class timeline not comparable"],
          "display": {"sign": "lower_rank_is_better"}
        },
        {
          "key": "competitor_next_pit_range_laps",
          "unit": "record(min_laps,max_laps)",
          "formula": "min/max of prior completed stint lengths for competitor_id in comparable normalized sessions, restricted to the current observed flag context.",
          "window": "comparable completed stints",
          "null_when": ["fewer than 3 comparable completed stints"],
          "display": {"sign": "ordered_non_negative_range", "dimension": "competitor_id"}
        },
        {
          "key": "projected_finish",
          "unit": "record(class_rank,net_gap_ms)",
          "formula": "Project each class participant on Pace5 through remaining race time and apply remaining mandatory-pit debt using observed relevant pit-cycle loss.",
          "window": "remaining race",
          "null_when": ["Pace5, remaining time, or all pit-debt losses unavailable", "participants on incomparable laps"],
          "display": {"sign": "lower_rank_is_better"}
        },
        {
          "key": "pit_scenario",
          "unit": "record(scenario,projected_rejoin,virtual_rank,net_gap_ms)",
          "formula": "Evaluate only pit_now, pit_plus_5_laps, and pit_plus_10_laps using current Pace5 and observed relevant pit-cycle loss; no manually authored scenario exists.",
          "window": "current race",
          "null_when": ["P2 inputs absent", "scenario crosses non-Green/pit/source-gap interval"],
          "display": {"sign": "comparison_series", "dimension": "scenario"}
        },
        {
          "key": "undercut_overcut_delta_ms",
          "unit": "ms",
          "formula": "For each 3|5 lap horizon, compare pit_now with deferred pit timing using observed Pace difference at comparable tyre ages and observed pit-cycle loss.",
          "window": "horizon_laps=3|5",
          "null_when": ["no comparable tyre-age pace and pit-loss samples"],
          "display": {"sign": "negative_favors_pit_now", "dimension": "horizon_laps"}
        },
        {
          "key": "cheap_stop_opportunity",
          "unit": "event(record flag_kind,green_loss_ms,neutralized_loss_ms,saving_ms)",
          "formula": "Emit only when current neutralized pit-cycle loss is lower than comparable Green pit-cycle loss by a positive measured saving.",
          "window": "current neutralization",
          "null_when": ["not SAFETY_CAR/FCY/CODE_60", "comparable Green or neutralized loss absent"],
          "display": {"sign": "positive saving favors pit", "event_only": true}
        },
        {
          "key": "pace_to_finish_ms",
          "unit": "ms/lap",
          "formula": "For each class neighbor, solve the maximum average ours pace that attacks or defends the current/projection target by calculated finish after required pit debt.",
          "window": "remaining race",
          "null_when": ["remaining time, comparable timeline, Pace5, or pit-debt input absent"],
          "display": {"sign": "lower_than_or_equal_means_target_met", "dimension": "attack_or_defend"}
        }
      ]
    }
  ],
  "alerts": {
    "schema": "Every alert is an event {key,severity,at_us,fact,numeric_consequence}. It has no aggregate strategy score and never carries a confidence field.",
    "rules": [
      {
        "key": "source_offline_or_ours_missing",
        "severity": "critical",
        "modes": ["practice", "qualifying", "race"],
        "condition": "channel_status=OFFLINE OR a previously resolved ours row disappears from a fresh source grid.",
        "numeric_consequence": "last_fresh_at_us or missing_duration_s"
      },
      {
        "key": "red_flag_or_session_reset",
        "severity": "critical",
        "modes": ["practice", "qualifying", "race"],
        "condition": "track_flag changes to RED OR normalized source heat reset is observed.",
        "numeric_consequence": "flag_phase_elapsed_s or reset sequence"
      },
      {
        "key": "mandatory_pits_infeasible",
        "severity": "critical",
        "modes": ["race"],
        "condition": "pits_remaining > 0 AND session_remaining_s is less than pits_remaining multiplied by the observed applicable pit-cycle duration.",
        "numeric_consequence": "time_deficit_s",
        "null_when": "no observed applicable pit-cycle duration"
      },
      {
        "key": "ours_pit_too_long",
        "severity": "critical",
        "modes": ["practice", "qualifying", "race"],
        "condition": "An open ours pit exceeds the class/ours completed-pit median plus 3*MAD after at least 3 baseline durations.",
        "numeric_consequence": "current_excess_ms",
        "null_when": "open pit start or baseline unavailable"
      },
      {
        "key": "competitor_pit_transition",
        "severity": "action",
        "modes": ["practice", "qualifying", "race"],
        "condition": "A selected-or-nearest class competitor confirms pit_in or pit_out.",
        "numeric_consequence": "pit_count and duration when closed"
      },
      {
        "key": "catch_or_threat_crossing",
        "severity": "action",
        "modes": ["race"],
        "condition": "A valid catch_range crosses a versioned internal tactical threshold in either direction.",
        "numeric_consequence": "catch_range_laps",
        "null_when": "catch_range absent"
      },
      {
        "key": "flag_changed",
        "severity": "action",
        "modes": ["practice", "qualifying", "race"],
        "condition": "Canonical track_flag changes.",
        "numeric_consequence": "flag_phase_elapsed_s"
      },
      {
        "key": "even_schedule_deviation",
        "severity": "action",
        "modes": ["race"],
        "condition": "stint_schedule_deviation_s crosses a versioned internal warning threshold.",
        "numeric_consequence": "stint_schedule_deviation_s",
        "null_when": "schedule metric absent"
      },
      {
        "key": "stint_pace_deteriorated",
        "severity": "action",
        "modes": ["practice", "qualifying", "race"],
        "condition": "stint_abrupt_deterioration is emitted.",
        "numeric_consequence": "threshold_ms and lap excess"
      },
      {
        "key": "projected_order_changed",
        "severity": "action",
        "modes": ["race"],
        "condition": "projected_rejoin or virtual_class_order changes compared with the prior eligible calculation.",
        "numeric_consequence": "rank/gap delta",
        "null_when": "P2 metric absent"
      },
      {
        "key": "new_best_lap",
        "severity": "information",
        "modes": ["practice", "qualifying", "race"],
        "condition": "A valid new own or class best lap is confirmed.",
        "numeric_consequence": "lap_time_ms and delta"
      },
      {
        "key": "class_position_changed",
        "severity": "information",
        "modes": ["practice", "qualifying", "race"],
        "condition": "ours position_class changes.",
        "numeric_consequence": "position_change"
      },
      {
        "key": "pit_completed_new_stint",
        "severity": "information",
        "modes": ["practice", "qualifying", "race"],
        "condition": "ours or a class participant completes pit_in -> pit_out; its tyre_age_laps is reset to 0.",
        "numeric_consequence": "pit_lane_duration_ms and stint_number"
      },
      {
        "key": "class_leader_or_neighbor_changed",
        "severity": "information",
        "modes": ["practice", "qualifying", "race"],
        "condition": "class_leader_id, class_ahead_id, or class_behind_id changes.",
        "numeric_consequence": "old/new participant_id"
      }
    ]
  },
  "implementation_invariants": [
    "All class calculations use observed CLS and PIC. POS remains absolute overall position.",
    "All raw source facts and event times remain replayable; the metrics layer consumes normalized facts and does not scrape browser DOM.",
    "No LLM recomputes or overwrites metrics. It may consume the persisted deterministic output only.",
    "The dashboard may choose competitors to display, but selection is a client filter over already calculated class-participant outputs and never a persisted operational input.",
    "Fuel, compounds, tyre temperatures/pressures, driver limits, stationary service time, and causes of slow laps are unavailable and must not be inferred or rendered."
  ]
}
```

## Deliberately Internal Thresholds

The catalog fixes P2 eligibility at three comparable completed observations.
The accepted product contract does not set the numeric catch/threat and
even-schedule alert thresholds. The engine must version those constants in
code, keep them out of the dashboard and session API, and return `null`/no
alert until they are defined. They are not manual race-engineer inputs.
