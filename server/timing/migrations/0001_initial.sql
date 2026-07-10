-- Immutable timing data plane. All timestamps are UTC integer microseconds.
-- A raw SignalR frame is durable before its decoded messages or derived state.

CREATE TABLE timing_sources (
  id INTEGER PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  source_url TEXT NOT NULL,
  adapter_version TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  last_seen_at_us INTEGER
);

CREATE TABLE analysis_sessions (
  id TEXT PRIMARY KEY,
  source_id INTEGER NOT NULL REFERENCES timing_sources(id),
  mode TEXT NOT NULL CHECK(mode IN ('practice', 'qualifying', 'race')),
  lifecycle TEXT NOT NULL CHECK(lifecycle IN ('draft', 'active', 'stopped', 'aborted')),
  race_duration_s INTEGER CHECK(race_duration_s IN (14400, 21600, 43200, 86400)),
  required_pits INTEGER CHECK(required_pits BETWEEN 2 AND 8),
  our_participant_id TEXT,
  our_class TEXT,
  identity_state TEXT NOT NULL DEFAULT 'pending' CHECK(identity_state IN ('pending', 'resolved', 'unresolved')),
  started_at_us INTEGER,
  stopped_at_us INTEGER,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  CHECK((mode = 'race' AND race_duration_s IS NOT NULL AND required_pits IS NOT NULL)
        OR (mode <> 'race' AND race_duration_s IS NULL AND required_pits IS NULL))
);
CREATE UNIQUE INDEX one_active_session_per_source
  ON analysis_sessions(source_id) WHERE lifecycle = 'active';

CREATE TABLE source_heats (
  id INTEGER PRIMARY KEY,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  generation INTEGER NOT NULL,
  external_name TEXT,
  provider_started_at_us INTEGER,
  provider_finished_at_us INTEGER,
  created_at_us INTEGER NOT NULL,
  UNIQUE(analysis_session_id, generation)
);

CREATE TABLE ingest_runs (
  id TEXT PRIMARY KEY,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  reducer_version TEXT NOT NULL,
  started_at_us INTEGER NOT NULL,
  stopped_at_us INTEGER,
  stop_reason TEXT
);
CREATE INDEX ingest_runs_session_time ON ingest_runs(analysis_session_id, started_at_us);

CREATE TABLE ingest_connections (
  id TEXT PRIMARY KEY,
  ingest_run_id TEXT NOT NULL REFERENCES ingest_runs(id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL,
  timekeeper_id TEXT,
  display_marker TEXT,
  connected_at_us INTEGER NOT NULL,
  disconnected_at_us INTEGER,
  disconnect_reason TEXT,
  UNIQUE(ingest_run_id, ordinal)
);

CREATE TABLE feed_frames (
  id INTEGER PRIMARY KEY,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  ingest_connection_id TEXT NOT NULL REFERENCES ingest_connections(id) ON DELETE CASCADE,
  frame_sequence INTEGER NOT NULL,
  received_at_us INTEGER NOT NULL,
  monotonic_ns INTEGER NOT NULL,
  upstream_cursor TEXT,
  groups_token TEXT,
  raw_payload BLOB NOT NULL,
  raw_sha256 TEXT NOT NULL,
  decode_state TEXT NOT NULL DEFAULT 'pending' CHECK(decode_state IN ('pending', 'decoded', 'failed')),
  decode_error TEXT,
  processed_at_us INTEGER,
  created_at_us INTEGER NOT NULL,
  UNIQUE(ingest_connection_id, frame_sequence)
);
CREATE INDEX feed_frames_pending
  ON feed_frames(analysis_session_id, frame_sequence) WHERE processed_at_us IS NULL;
CREATE INDEX feed_frames_session_time ON feed_frames(analysis_session_id, received_at_us);

CREATE TABLE feed_messages (
  id INTEGER PRIMARY KEY,
  frame_id INTEGER NOT NULL REFERENCES feed_frames(id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL,
  handle TEXT NOT NULL,
  args_json TEXT NOT NULL,
  compressed INTEGER NOT NULL CHECK(compressed IN (0, 1)),
  source_time_raw TEXT,
  source_at_us INTEGER,
  created_at_us INTEGER NOT NULL,
  UNIQUE(frame_id, ordinal)
);
CREATE INDEX feed_messages_handle ON feed_messages(handle, id);

CREATE TABLE ingest_gaps (
  id INTEGER PRIMARY KEY,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  source_heat_id INTEGER REFERENCES source_heats(id) ON DELETE SET NULL,
  ingest_connection_id TEXT REFERENCES ingest_connections(id) ON DELETE SET NULL,
  started_at_us INTEGER NOT NULL,
  ended_at_us INTEGER,
  reason TEXT NOT NULL,
  created_at_us INTEGER NOT NULL
);
CREATE INDEX ingest_gaps_session_started ON ingest_gaps(analysis_session_id, started_at_us);

CREATE TABLE participants (
  id TEXT PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  external_key TEXT NOT NULL,
  transponder_id TEXT,
  start_number TEXT,
  team_name TEXT,
  car_name TEXT,
  class_name TEXT,
  is_ours INTEGER NOT NULL DEFAULT 0 CHECK(is_ours IN (0, 1)),
  active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
  first_seen_at_us INTEGER NOT NULL,
  last_seen_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, external_key)
);
CREATE INDEX participants_heat_class ON participants(source_heat_id, class_name, active);

-- TEAM, DRIVER IN CAR and CAR are source observations. A driver is not part of
-- vehicle identity; a changed driver closes one segment and starts another.
CREATE TABLE participant_identity_segments (
  id TEXT PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  team_name TEXT,
  car_name TEXT,
  class_name TEXT,
  driver_name_raw TEXT,
  driver_name_key TEXT,
  started_at_us INTEGER NOT NULL,
  ended_at_us INTEGER,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, participant_id, started_at_us)
);
CREATE INDEX identity_segments_current
  ON participant_identity_segments(participant_id, ended_at_us);

CREATE TABLE state_ticks (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  observed_second INTEGER NOT NULL,
  observed_at_us INTEGER NOT NULL,
  source_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  state_hash TEXT NOT NULL,
  freshness_ms INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  PRIMARY KEY(source_heat_id, observed_second)
);

CREATE TABLE state_checkpoints (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  source_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  state_hash TEXT NOT NULL,
  codec TEXT NOT NULL CHECK(codec IN ('gzip', 'identity')),
  payload BLOB NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, observed_at_us)
);
CREATE INDEX checkpoints_heat_time ON state_checkpoints(source_heat_id, observed_at_us DESC);

CREATE TABLE participant_state_current (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  position_overall INTEGER,
  position_class INTEGER,
  marker TEXT,
  laps INTEGER,
  state TEXT,
  state_raw TEXT,
  state_kind TEXT,
  current_driver_name TEXT,
  current_driver_stint_raw TEXT,
  last_lap_ms INTEGER,
  last_lap_number INTEGER,
  best_lap_ms INTEGER,
  best_lap_number INTEGER,
  last_sectors_json TEXT,
  best_sectors_json TEXT,
  last_speeds_json TEXT,
  gap_ms INTEGER,
  gap_raw TEXT,
  gap_kind TEXT,
  diff_ms INTEGER,
  diff_raw TEXT,
  diff_kind TEXT,
  sector_json TEXT,
  speed_kph REAL,
  pit_time_raw TEXT,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  updated_at_us INTEGER NOT NULL,
  PRIMARY KEY(source_heat_id, participant_id)
);

CREATE TABLE laps (
  id TEXT PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  lap_number INTEGER NOT NULL,
  completed_at_us INTEGER,
  duration_ms INTEGER,
  sectors_json TEXT,
  flag TEXT,
  is_in_lap INTEGER NOT NULL DEFAULT 0 CHECK(is_in_lap IN (0, 1)),
  is_out_lap INTEGER NOT NULL DEFAULT 0 CHECK(is_out_lap IN (0, 1)),
  crosses_pit INTEGER NOT NULL DEFAULT 0 CHECK(crosses_pit IN (0, 1)),
  is_clean INTEGER NOT NULL DEFAULT 0 CHECK(is_clean IN (0, 1)),
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, participant_id, lap_number)
);
CREATE INDEX laps_participant_time ON laps(participant_id, completed_at_us);

CREATE TABLE pit_stops (
  id TEXT PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  stop_number INTEGER NOT NULL,
  entered_at_us INTEGER NOT NULL,
  exited_at_us INTEGER,
  entered_lap INTEGER,
  exited_lap INTEGER,
  pit_lane_ms INTEGER,
  completed INTEGER NOT NULL DEFAULT 0 CHECK(completed IN (0, 1)),
  entered_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  entered_source_key TEXT NOT NULL,
  exited_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  exited_source_key TEXT,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, participant_id, stop_number)
);
CREATE INDEX pits_participant_time ON pit_stops(participant_id, entered_at_us);

CREATE TABLE track_flag_periods (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  flag TEXT NOT NULL,
  provider_code TEXT,
  provider_label TEXT,
  started_at_us INTEGER NOT NULL,
  ended_at_us INTEGER,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  ended_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  ended_source_key TEXT,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, source_key)
);
CREATE INDEX flags_heat_time ON track_flag_periods(source_heat_id, started_at_us);

CREATE TABLE track_flag_current (
  source_heat_id INTEGER PRIMARY KEY REFERENCES source_heats(id) ON DELETE CASCADE,
  flag TEXT NOT NULL,
  provider_code TEXT,
  provider_label TEXT,
  started_at_us INTEGER NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  updated_at_us INTEGER NOT NULL
);

CREATE TABLE tracker_passings (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT REFERENCES participants(id) ON DELETE SET NULL,
  transponder_id TEXT NOT NULL,
  start_number TEXT,
  distance_mm INTEGER,
  stop_distance_mm INTEGER,
  sector_id INTEGER,
  speed_kph REAL,
  is_in_pit INTEGER NOT NULL CHECK(is_in_pit IN (0, 1)),
  passed_at_us INTEGER,
  provider_passed_at_raw TEXT,
  path_id TEXT,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  message_ordinal INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_key, message_ordinal)
);
CREATE INDEX tracker_passings_vehicle_time ON tracker_passings(participant_id, passed_at_us);

CREATE TABLE source_statistics_current (
  source_heat_id INTEGER PRIMARY KEY REFERENCES source_heats(id) ON DELETE CASCADE,
  observed_at_us INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  updated_at_us INTEGER NOT NULL
);

CREATE TABLE source_statistics_samples (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  observed_second INTEGER NOT NULL,
  observed_at_us INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  PRIMARY KEY(source_heat_id, observed_second)
);

-- Every completed pit automatically starts a new tyre stint. There is no
-- manual override, compound field or tyre-change control in this product.
CREATE TABLE tire_stints (
  id TEXT PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  stint_number INTEGER NOT NULL,
  started_at_us INTEGER NOT NULL,
  ended_at_us INTEGER,
  started_lap INTEGER,
  ended_lap INTEGER,
  completed_laps INTEGER NOT NULL DEFAULT 0,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, participant_id, stint_number)
);
CREATE INDEX tire_stints_participant_time ON tire_stints(participant_id, started_at_us);

-- One compact metric snapshot per scope and five-second (or event) bucket.
-- This avoids a high-volume EAV table for every metric of every participant.
CREATE TABLE metric_samples (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  scope_kind TEXT NOT NULL CHECK(scope_kind IN ('participant', 'class', 'session')),
  scope_key TEXT NOT NULL,
  observed_second INTEGER NOT NULL,
  observed_at_us INTEGER NOT NULL,
  metric_version INTEGER NOT NULL,
  values_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  PRIMARY KEY(source_heat_id, scope_kind, scope_key, observed_second)
);
CREATE INDEX metrics_chart ON metric_samples(source_heat_id, scope_kind, scope_key, observed_second);

CREATE TABLE stream_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  source_heat_id INTEGER REFERENCES source_heats(id) ON DELETE SET NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_us INTEGER NOT NULL
);
CREATE INDEX stream_session_id ON stream_events(analysis_session_id, id);

CREATE TABLE strategy_advisories (
  id TEXT PRIMARY KEY,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  created_at_us INTEGER NOT NULL,
  data_cutoff_us INTEGER NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  input_json TEXT NOT NULL,
  output_json TEXT NOT NULL
);
