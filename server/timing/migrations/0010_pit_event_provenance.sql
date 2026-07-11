-- PIT, L-PIT and STATE are sparse result-grid cells. Keep their exact cell
-- provenance separate so a cached L-PIT display value cannot become the
-- duration or boundary of a later pit event.
ALTER TABLE participant_state_current ADD COLUMN state_source_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN provider_pit_count_source_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN pit_time_source_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN pit_time_source_message_id INTEGER
  REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN pit_time_source_key TEXT;
ALTER TABLE participant_state_current ADD COLUMN pit_time_observed_at_us INTEGER;
ALTER TABLE participant_state_current ADD COLUMN driver_stint_kind TEXT
  CHECK(driver_stint_kind IS NULL OR driver_stint_kind IN ('START_TS','POINT_TS','DURATION','UNKNOWN'));
ALTER TABLE participant_state_current ADD COLUMN driver_stint_provider_ts_time INTEGER;
ALTER TABLE participant_state_current ADD COLUMN driver_stint_at_us INTEGER;
ALTER TABLE participant_state_current ADD COLUMN driver_stint_calibration_id INTEGER
  REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN driver_stint_duration_ms INTEGER;
ALTER TABLE participant_state_current ADD COLUMN driver_stint_source_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN driver_stint_source_message_id INTEGER
  REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN driver_stint_source_key TEXT;
ALTER TABLE participant_state_current ADD COLUMN driver_stint_observed_at_us INTEGER;

ALTER TABLE participant_state_observations ADD COLUMN state_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_observations ADD COLUMN provider_pit_count_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_observations ADD COLUMN pit_time_raw TEXT;
ALTER TABLE participant_state_observations ADD COLUMN pit_time_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_observations ADD COLUMN driver_stint_raw TEXT;
ALTER TABLE participant_state_observations ADD COLUMN driver_stint_kind TEXT
  CHECK(driver_stint_kind IS NULL OR driver_stint_kind IN ('START_TS','POINT_TS','DURATION','UNKNOWN'));
ALTER TABLE participant_state_observations ADD COLUMN driver_stint_provider_ts_time INTEGER;
ALTER TABLE participant_state_observations ADD COLUMN driver_stint_at_us INTEGER;
ALTER TABLE participant_state_observations ADD COLUMN driver_stint_calibration_id INTEGER
  REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_observations ADD COLUMN driver_stint_duration_ms INTEGER;
ALTER TABLE participant_state_observations ADD COLUMN driver_stint_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;

ALTER TABLE pit_stops ADD COLUMN entered_state_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE pit_stops ADD COLUMN entered_pit_count_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE pit_stops ADD COLUMN entered_at_source_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE pit_stops ADD COLUMN entered_at_source_message_id INTEGER
  REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE pit_stops ADD COLUMN entered_at_source_key TEXT;
ALTER TABLE pit_stops ADD COLUMN entered_at_source_kind TEXT
  CHECK(entered_at_source_kind IS NULL OR entered_at_source_kind = 'RESULT_L_PIT_S');
ALTER TABLE pit_stops ADD COLUMN exited_state_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE pit_stops ADD COLUMN exited_pit_count_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE pit_stops ADD COLUMN pit_lane_duration_source_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE pit_stops ADD COLUMN pit_lane_duration_source_message_id INTEGER
  REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE pit_stops ADD COLUMN pit_lane_duration_source_key TEXT;
ALTER TABLE pit_stops ADD COLUMN pit_lane_duration_source_kind TEXT
  CHECK(pit_lane_duration_source_kind IS NULL OR pit_lane_duration_source_kind = 'RESULT_L_PIT');

CREATE INDEX pit_stops_duration_source_cell
  ON pit_stops(pit_lane_duration_source_cell_observation_id)
  WHERE pit_lane_duration_source_cell_observation_id IS NOT NULL;
