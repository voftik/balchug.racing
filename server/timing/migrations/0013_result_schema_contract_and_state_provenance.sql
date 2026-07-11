-- The current Time Service table is a stable production contract. Keep a
-- durable diagnostic per raw layout while continuing to retain every unknown
-- header/cell in the source ledger.
CREATE TABLE result_schema_contract_observations (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  layout_version_id INTEGER NOT NULL REFERENCES result_layout_versions(id) ON DELETE CASCADE,
  contract_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('CURRENT','DEGRADED')),
  required_keys_json TEXT NOT NULL,
  present_keys_json TEXT NOT NULL,
  missing_required_keys_json TEXT NOT NULL,
  binding_mismatches_json TEXT NOT NULL,
  optional_present_keys_json TEXT NOT NULL,
  unknown_columns_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(layout_version_id, contract_name)
);
CREATE INDEX result_schema_contract_observations_heat_status
  ON result_schema_contract_observations(source_heat_id,status,observed_at_us DESC,id DESC);

-- Before the sector parameter grammar was recognized, the current production
-- layout was retained correctly but SectorTimes p=1..3 had NULL canonical
-- keys. Reattach every fixed-contract header to its explicit semantic key;
-- raw layouts and cell observations remain unchanged.
UPDATE result_column_definitions
SET canonical_key = CASE
  WHEN source_name_raw = 'position' THEN 'position_overall'
  WHEN source_name_raw = 'marker' THEN 'marker'
  WHEN source_name_raw = 'startnumber' THEN 'start_number'
  WHEN source_name_raw = 'State' THEN 'state'
  WHEN source_name_raw = 'Team name' THEN 'team_name'
  WHEN source_name_raw = 'CurrentDriver' THEN 'current_driver'
  WHEN source_name_raw = 'class' THEN 'class_name'
  WHEN source_name_raw = 'position_in_class' THEN 'position_class'
  WHEN source_name_raw = 'hole' THEN 'gap'
  WHEN source_name_raw = 'fastestRoundTime' THEN 'best_lap'
  WHEN source_name_raw = 'lastRoundTime' THEN 'last_lap'
  WHEN source_name_raw = 'CurrentDriverStintTime' THEN 'driver_stint'
  WHEN source_name_raw = 'PitTime' THEN 'pit_time'
  WHEN source_name_raw = 'pitstops' THEN 'pit_stops'
  WHEN source_name_raw = 'SectorTimes' AND source_parameter_raw = '1' THEN 'sector_1'
  WHEN source_name_raw = 'SectorTimes' AND source_parameter_raw = '2' THEN 'sector_2'
  WHEN source_name_raw = 'SectorTimes' AND source_parameter_raw = '3' THEN 'sector_3'
  WHEN source_name_raw = 'sectionMarker' THEN 'section_marker'
  WHEN source_name_raw = 'car' THEN 'car_name'
  WHEN source_name_raw = 'laps' THEN 'laps'
  WHEN source_name_raw = 'diff' THEN 'diff'
  ELSE canonical_key
END
WHERE source_name_raw IN (
  'position','marker','startnumber','State','Team name','CurrentDriver','class',
  'position_in_class','hole','fastestRoundTime','lastRoundTime',
  'CurrentDriverStintTime','PitTime','pitstops','SectorTimes','sectionMarker',
  'car','laps','diff'
);

-- `source_message_id/source_key/updated_at_us` on the current row identify the
-- latest materialized table update. Preserve the independent source of STATE
-- so a sparse LAST/GAP update cannot pretend to be a new state observation.
ALTER TABLE participant_state_current ADD COLUMN state_source_message_id INTEGER
  REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN state_source_key TEXT;
ALTER TABLE participant_state_current ADD COLUMN state_observed_at_us INTEGER;
CREATE INDEX participant_state_current_state_source
  ON participant_state_current(state_source_message_id)
  WHERE state_source_message_id IS NOT NULL;

UPDATE participant_state_current
SET state_source_message_id = (
      SELECT observation.source_message_id
      FROM participant_result_cell_observations AS observation
      WHERE observation.id = participant_state_current.state_source_cell_observation_id
    ),
    state_source_key = (
      SELECT observation.source_key
      FROM participant_result_cell_observations AS observation
      WHERE observation.id = participant_state_current.state_source_cell_observation_id
    ),
    state_observed_at_us = (
      SELECT observation.observed_at_us
      FROM participant_result_cell_observations AS observation
      WHERE observation.id = participant_state_current.state_source_cell_observation_id
    )
WHERE state_source_cell_observation_id IS NOT NULL;

-- Earlier versions wrote a synthetic UNKNOWN observation for generic sparse
-- row updates. It is derived noise, not source evidence; raw cells remain
-- untouched and every actual STATE/PIT/L-PIT/STINT observation is retained.
DELETE FROM participant_state_observations
WHERE state_cell_observation_id IS NULL
  AND provider_pit_count_cell_observation_id IS NULL
  AND pit_time_cell_observation_id IS NULL
  AND driver_stint_cell_observation_id IS NULL
  AND state_raw IS NULL
  AND state_kind = 'UNKNOWN'
  AND provider_pit_count_raw IS NULL
  AND provider_pit_count IS NULL
  AND pit_time_raw IS NULL
  AND driver_stint_raw IS NULL;
