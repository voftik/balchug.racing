-- Normalizer storage. This migration keeps provider facts, their receive-time
-- observations and calibrated UTC values separate. A NULL calibrated value is
-- intentional until a connection clock calibration is available.

-- A results layout is dynamic per heat. Its fingerprint makes initial snapshots
-- and reconnect replays idempotent without relying on fixed provider indexes.
CREATE TABLE result_layout_versions (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  version_ordinal INTEGER NOT NULL CHECK(version_ordinal >= 0),
  layout_fingerprint TEXT NOT NULL,
  raw_layout_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, version_ordinal),
  UNIQUE(source_heat_id, layout_fingerprint)
);
CREATE INDEX result_layout_versions_heat_time
  ON result_layout_versions(source_heat_id, observed_at_us DESC);

CREATE TABLE result_column_definitions (
  layout_version_id INTEGER NOT NULL REFERENCES result_layout_versions(id) ON DELETE CASCADE,
  column_index INTEGER NOT NULL CHECK(column_index >= 0),
  source_name_raw TEXT,
  source_parameter_raw TEXT,
  display_name_raw TEXT,
  canonical_key TEXT,
  raw_definition_json TEXT NOT NULL,
  PRIMARY KEY(layout_version_id, column_index)
);
CREATE INDEX result_column_definitions_canonical
  ON result_column_definitions(canonical_key) WHERE canonical_key IS NOT NULL;

-- Every changed result cell is retained with its exact dynamic column. The
-- current materialization is deliberately separate from raw frame retention so
-- changed/unknown columns can still be queried without hard-coded positions.
CREATE TABLE participant_result_cell_observations (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT REFERENCES participants(id) ON DELETE SET NULL,
  layout_version_id INTEGER NOT NULL REFERENCES result_layout_versions(id) ON DELETE CASCADE,
  provider_row_index INTEGER NOT NULL CHECK(provider_row_index >= 0),
  column_index INTEGER NOT NULL CHECK(column_index >= 0),
  raw_value_json TEXT NOT NULL,
  value_text TEXT,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_change_ordinal INTEGER NOT NULL CHECK(source_change_ordinal >= 0),
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, source_key, source_change_ordinal)
);
CREATE INDEX result_cell_observations_participant_time
  ON participant_result_cell_observations(participant_id, observed_at_us);
CREATE INDEX result_cell_observations_column_time
  ON participant_result_cell_observations(source_heat_id, layout_version_id, column_index, observed_at_us);

-- Normalized identity keys are produced by the writer, never supplied by an
-- engineer. Raw values remain alongside them for later re-matching.
ALTER TABLE participants ADD COLUMN identity_key TEXT;
ALTER TABLE participants ADD COLUMN start_number_key TEXT;
ALTER TABLE participants ADD COLUMN team_name_key TEXT;
ALTER TABLE participants ADD COLUMN car_name_key TEXT;
ALTER TABLE participants ADD COLUMN class_name_key TEXT;
ALTER TABLE participants ADD COLUMN identity_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE participants ADD COLUMN identity_source_key TEXT;
ALTER TABLE participants ADD COLUMN identity_observed_at_us INTEGER;
CREATE INDEX participants_identity_match
  ON participants(source_heat_id, start_number_key, team_name_key, class_name_key);

ALTER TABLE participant_identity_segments ADD COLUMN start_number_raw TEXT;
ALTER TABLE participant_identity_segments ADD COLUMN start_number_key TEXT;
ALTER TABLE participant_identity_segments ADD COLUMN team_name_key TEXT;
ALTER TABLE participant_identity_segments ADD COLUMN car_name_key TEXT;
ALTER TABLE participant_identity_segments ADD COLUMN class_name_key TEXT;
ALTER TABLE participant_identity_segments ADD COLUMN identity_fingerprint TEXT;
ALTER TABLE participant_identity_segments ADD COLUMN observed_at_us INTEGER;
ALTER TABLE participant_identity_segments ADD COLUMN ended_observed_at_us INTEGER;
CREATE UNIQUE INDEX participant_identity_segments_one_open
  ON participant_identity_segments(source_heat_id, participant_id) WHERE ended_at_us IS NULL;
CREATE UNIQUE INDEX participant_identity_segments_idempotency
  ON participant_identity_segments(source_heat_id, participant_id, source_key, identity_fingerprint)
  WHERE identity_fingerprint IS NOT NULL;

CREATE TABLE participant_identity_observations (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT REFERENCES participants(id) ON DELETE SET NULL,
  external_key_raw TEXT,
  transponder_id_raw TEXT,
  start_number_raw TEXT,
  start_number_key TEXT,
  team_name_raw TEXT,
  team_name_key TEXT,
  car_name_raw TEXT,
  car_name_key TEXT,
  class_name_raw TEXT,
  class_name_key TEXT,
  driver_name_raw TEXT,
  driver_name_key TEXT,
  identity_fingerprint TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, source_event_key)
);
CREATE INDEX participant_identity_observations_match
  ON participant_identity_observations(source_heat_id, start_number_key, team_name_key, class_name_key, observed_at_us DESC);

-- Server clock values are Time Service timestamps. Samples preserve the raw
-- provider value; calibration rows define UTC after the 2000-epoch conversion
-- plus the recorded connection-specific offset.
CREATE TABLE connection_clock_samples (
  id INTEGER PRIMARY KEY,
  ingest_connection_id TEXT NOT NULL REFERENCES ingest_connections(id) ON DELETE CASCADE,
  source_heat_id INTEGER REFERENCES source_heats(id) ON DELETE SET NULL,
  provider_timestamp_raw TEXT NOT NULL,
  provider_timestamp_us INTEGER,
  provider_timestamp_kind TEXT NOT NULL,
  received_at_us INTEGER NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(ingest_connection_id, source_event_key)
);
CREATE INDEX clock_samples_connection_time
  ON connection_clock_samples(ingest_connection_id, received_at_us);

CREATE TABLE connection_clock_calibrations (
  id INTEGER PRIMARY KEY,
  ingest_connection_id TEXT NOT NULL REFERENCES ingest_connections(id) ON DELETE CASCADE,
  source_heat_id INTEGER REFERENCES source_heats(id) ON DELETE SET NULL,
  calibration_key TEXT NOT NULL,
  provider_timestamp_kind TEXT NOT NULL,
  offset_us INTEGER NOT NULL,
  sample_count INTEGER NOT NULL CHECK(sample_count > 0),
  median_abs_deviation_us INTEGER,
  valid_from_provider_us INTEGER,
  valid_to_provider_us INTEGER,
  valid_from_observed_at_us INTEGER NOT NULL,
  valid_to_observed_at_us INTEGER,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(ingest_connection_id, calibration_key)
);
CREATE INDEX clock_calibrations_connection_validity
  ON connection_clock_calibrations(ingest_connection_id, valid_from_observed_at_us DESC);

-- STATE is an observation, not a lap or pit duration. E<TsTime> has a raw
-- target, a provider timestamp and (only after calibration) a UTC target.
ALTER TABLE participant_state_current ADD COLUMN state_timer_target_raw TEXT;
ALTER TABLE participant_state_current ADD COLUMN state_timer_target_provider_us INTEGER;
ALTER TABLE participant_state_current ADD COLUMN state_timer_target_at_us INTEGER;
ALTER TABLE participant_state_current ADD COLUMN state_timer_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN state_timer_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN state_timer_source_key TEXT;
ALTER TABLE participant_state_current ADD COLUMN state_timer_observed_at_us INTEGER;
ALTER TABLE participant_state_current ADD COLUMN provider_pit_count INTEGER;
ALTER TABLE participant_state_current ADD COLUMN provider_pit_count_raw TEXT;
ALTER TABLE participant_state_current ADD COLUMN provider_pit_count_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN provider_pit_count_source_key TEXT;
ALTER TABLE participant_state_current ADD COLUMN provider_pit_count_observed_at_us INTEGER;

CREATE TABLE participant_state_observations (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT REFERENCES participants(id) ON DELETE SET NULL,
  layout_version_id INTEGER REFERENCES result_layout_versions(id) ON DELETE SET NULL,
  provider_row_index INTEGER NOT NULL CHECK(provider_row_index >= 0),
  state_raw TEXT,
  state_kind TEXT NOT NULL DEFAULT 'UNKNOWN',
  state_timer_target_raw TEXT,
  state_timer_target_provider_us INTEGER,
  state_timer_target_at_us INTEGER,
  state_timer_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL,
  provider_pit_count_raw TEXT,
  provider_pit_count INTEGER,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, source_event_key)
);
CREATE INDEX participant_state_observations_vehicle_time
  ON participant_state_observations(participant_id, observed_at_us);

-- The current normalized passing table gains raw track-side values. The
-- append-only observation table deduplicates a physical passing across a
-- reconnect, where the provider source key necessarily changes.
ALTER TABLE tracker_passings ADD COLUMN raw_speed_mm_s INTEGER;
ALTER TABLE tracker_passings ADD COLUMN provider_passed_at_provider_us INTEGER;
ALTER TABLE tracker_passings ADD COLUMN provider_passed_at_kind TEXT;
ALTER TABLE tracker_passings ADD COLUMN clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL;
ALTER TABLE tracker_passings ADD COLUMN event_fingerprint TEXT;
ALTER TABLE tracker_passings ADD COLUMN observed_at_us INTEGER;
ALTER TABLE tracker_passings ADD COLUMN raw_passing_json TEXT;
CREATE UNIQUE INDEX tracker_passings_event_dedupe
  ON tracker_passings(source_heat_id, event_fingerprint) WHERE event_fingerprint IS NOT NULL;

CREATE TABLE tracker_passing_observations (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT REFERENCES participants(id) ON DELETE SET NULL,
  transponder_id_raw TEXT,
  start_number_raw TEXT,
  start_distance_mm INTEGER,
  stop_distance_mm INTEGER,
  sector_id INTEGER,
  raw_speed_mm_s INTEGER,
  is_in_pit INTEGER CHECK(is_in_pit IN (0, 1)),
  provider_passed_at_raw TEXT,
  provider_passed_at_provider_us INTEGER,
  provider_passed_at_kind TEXT,
  passed_at_us INTEGER,
  clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL,
  event_fingerprint TEXT NOT NULL,
  raw_passing_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, event_fingerprint),
  UNIQUE(source_heat_id, source_event_key)
);
CREATE INDEX tracker_passing_observations_vehicle_time
  ON tracker_passing_observations(participant_id, passed_at_us);

-- Immediate h_h transitions are provisional receive-time boundaries. a_u.i
-- reconciliation later supplies raw provider timestamps and calibrated UTC.
ALTER TABLE track_flag_periods ADD COLUMN start_provider_ts_raw TEXT;
ALTER TABLE track_flag_periods ADD COLUMN end_provider_ts_raw TEXT;
ALTER TABLE track_flag_periods ADD COLUMN start_provider_ts_us INTEGER;
ALTER TABLE track_flag_periods ADD COLUMN end_provider_ts_us INTEGER;
ALTER TABLE track_flag_periods ADD COLUMN observed_started_at_us INTEGER;
ALTER TABLE track_flag_periods ADD COLUMN observed_ended_at_us INTEGER;
ALTER TABLE track_flag_periods ADD COLUMN calibrated_started_at_us INTEGER;
ALTER TABLE track_flag_periods ADD COLUMN calibrated_ended_at_us INTEGER;
ALTER TABLE track_flag_periods ADD COLUMN start_clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL;
ALTER TABLE track_flag_periods ADD COLUMN end_clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL;
ALTER TABLE track_flag_periods ADD COLUMN source_flag_kind_raw TEXT;
ALTER TABLE track_flag_periods ADD COLUMN clock_stopped_raw TEXT;
ALTER TABLE track_flag_periods ADD COLUMN remark_raw TEXT;
ALTER TABLE track_flag_periods ADD COLUMN reconciliation_key TEXT;
ALTER TABLE track_flag_periods ADD COLUMN reconciliation_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE track_flag_periods ADD COLUMN reconciliation_source_key TEXT;
ALTER TABLE track_flag_periods ADD COLUMN reconciled_at_us INTEGER;
CREATE UNIQUE INDEX track_flag_periods_reconciliation
  ON track_flag_periods(source_heat_id, reconciliation_key) WHERE reconciliation_key IS NOT NULL;

ALTER TABLE track_flag_current ADD COLUMN start_provider_ts_raw TEXT;
ALTER TABLE track_flag_current ADD COLUMN start_provider_ts_us INTEGER;
ALTER TABLE track_flag_current ADD COLUMN observed_started_at_us INTEGER;
ALTER TABLE track_flag_current ADD COLUMN calibrated_started_at_us INTEGER;
ALTER TABLE track_flag_current ADD COLUMN start_clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL;
ALTER TABLE track_flag_current ADD COLUMN source_flag_kind_raw TEXT;
ALTER TABLE track_flag_current ADD COLUMN reconciliation_key TEXT;
ALTER TABLE track_flag_current ADD COLUMN reconciliation_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE track_flag_current ADD COLUMN reconciliation_source_key TEXT;
ALTER TABLE track_flag_current ADD COLUMN reconciled_at_us INTEGER;

-- Typed a_i/a_u statistics complement the existing raw merged JSON. No value
-- here is an engineer input; all rows are source-derived and idempotent.
CREATE TABLE heat_statistics_current (
  source_heat_id INTEGER PRIMARY KEY REFERENCES source_heats(id) ON DELETE CASCADE,
  heat_name_raw TEXT,
  green_flag_provider_ts_raw TEXT,
  green_flag_provider_ts_us INTEGER,
  green_flag_at_us INTEGER,
  green_flag_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL,
  finish_flag_provider_ts_raw TEXT,
  finish_flag_provider_ts_us INTEGER,
  finish_flag_at_us INTEGER,
  finish_flag_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL,
  participants_started INTEGER,
  participants_classified INTEGER,
  participants_not_classified INTEGER,
  participants_on_track INTEGER,
  participants_in_pit_zone INTEGER,
  participants_in_tank_zone INTEGER,
  total_laps INTEGER,
  total_pitstops INTEGER,
  leader_laps_green INTEGER,
  leader_laps_safety_car INTEGER,
  leader_laps_code_60 INTEGER,
  leader_laps_full_course_yellow INTEGER,
  safety_car_count INTEGER,
  code_60_count INTEGER,
  full_course_yellow_count INTEGER,
  safety_car_total_time_raw TEXT,
  code_60_total_time_raw TEXT,
  full_course_yellow_total_time_raw TEXT,
  raw_payload_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL
);

CREATE TABLE heat_statistics_samples (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  observed_second INTEGER NOT NULL,
  observed_at_us INTEGER NOT NULL,
  heat_name_raw TEXT,
  green_flag_provider_ts_raw TEXT,
  green_flag_provider_ts_us INTEGER,
  green_flag_at_us INTEGER,
  finish_flag_provider_ts_raw TEXT,
  finish_flag_provider_ts_us INTEGER,
  finish_flag_at_us INTEGER,
  participants_started INTEGER,
  participants_classified INTEGER,
  participants_not_classified INTEGER,
  participants_on_track INTEGER,
  participants_in_pit_zone INTEGER,
  participants_in_tank_zone INTEGER,
  total_laps INTEGER,
  total_pitstops INTEGER,
  leader_laps_green INTEGER,
  leader_laps_safety_car INTEGER,
  leader_laps_code_60 INTEGER,
  leader_laps_full_course_yellow INTEGER,
  safety_car_count INTEGER,
  code_60_count INTEGER,
  full_course_yellow_count INTEGER,
  safety_car_total_time_raw TEXT,
  code_60_total_time_raw TEXT,
  full_course_yellow_total_time_raw TEXT,
  raw_payload_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  PRIMARY KEY(source_heat_id, observed_second)
);
CREATE INDEX heat_statistics_samples_time
  ON heat_statistics_samples(source_heat_id, observed_at_us);

CREATE TABLE statistics_best_lap_history (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  event_fingerprint TEXT NOT NULL,
  time_of_day_raw TEXT,
  time_of_day_provider_us INTEGER,
  time_of_day_at_us INTEGER,
  clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL,
  lap_time_raw TEXT,
  lap_time_us INTEGER,
  lap_number INTEGER,
  start_number_raw TEXT,
  start_number_key TEXT,
  team_name_raw TEXT,
  team_name_key TEXT,
  driver_name_raw TEXT,
  driver_name_key TEXT,
  car_name_raw TEXT,
  car_name_key TEXT,
  average_speed_raw TEXT,
  average_speed_kph REAL,
  provider_rank INTEGER,
  raw_record_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, event_fingerprint),
  UNIQUE(source_heat_id, source_event_key)
);
CREATE INDEX statistics_best_lap_history_time
  ON statistics_best_lap_history(source_heat_id, time_of_day_at_us);
CREATE INDEX statistics_best_lap_history_entry
  ON statistics_best_lap_history(source_heat_id, start_number_key, lap_time_us);

CREATE TABLE statistics_class_best_laps (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  class_name_raw TEXT,
  class_name_key TEXT NOT NULL,
  lap_time_raw TEXT,
  lap_time_us INTEGER,
  lap_number INTEGER,
  start_number_raw TEXT,
  start_number_key TEXT,
  team_name_raw TEXT,
  team_name_key TEXT,
  driver_name_raw TEXT,
  driver_name_key TEXT,
  car_name_raw TEXT,
  car_name_key TEXT,
  time_of_day_raw TEXT,
  time_of_day_provider_us INTEGER,
  time_of_day_at_us INTEGER,
  clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL,
  average_speed_raw TEXT,
  average_speed_kph REAL,
  provider_class_order INTEGER,
  event_fingerprint TEXT NOT NULL,
  raw_record_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  PRIMARY KEY(source_heat_id, class_name_key)
);
CREATE INDEX statistics_class_best_laps_entry
  ON statistics_class_best_laps(source_heat_id, start_number_key, lap_time_us);

CREATE TABLE statistics_caution_history (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  reconciliation_key TEXT NOT NULL,
  flag_kind_raw TEXT,
  start_provider_ts_raw TEXT,
  end_provider_ts_raw TEXT,
  start_provider_ts_us INTEGER,
  end_provider_ts_us INTEGER,
  started_at_us INTEGER,
  ended_at_us INTEGER,
  start_clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL,
  end_clock_calibration_id INTEGER REFERENCES connection_clock_calibrations(id) ON DELETE SET NULL,
  clock_stopped_raw TEXT,
  clock_stopped INTEGER CHECK(clock_stopped IN (0, 1)),
  remark_raw TEXT,
  raw_record_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, reconciliation_key),
  UNIQUE(source_heat_id, source_event_key)
);
CREATE INDEX statistics_caution_history_time
  ON statistics_caution_history(source_heat_id, started_at_us);
