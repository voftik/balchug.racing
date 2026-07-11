-- In the endurance-race layout GAP is a mixed display coordinate. A row with
-- ``-- N laps --`` starts a completed-lap group; following TIME rows are
-- cumulative offsets from that group's first car. Persist a one-Hz projection
-- of the whole absolute table before any class filtering.

CREATE TABLE gap_coordinate_snapshots (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  observed_second INTEGER NOT NULL,
  observed_at_us INTEGER NOT NULL,
  source_frame_id INTEGER NOT NULL REFERENCES feed_frames(id) ON DELETE CASCADE,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  leader_completed_laps INTEGER,
  participant_count INTEGER NOT NULL CHECK(participant_count >= 0),
  positioned_participant_count INTEGER NOT NULL CHECK(positioned_participant_count >= 0),
  resolved_coordinate_count INTEGER NOT NULL CHECK(resolved_coordinate_count >= 0),
  lap_group_count INTEGER NOT NULL CHECK(lap_group_count >= 0),
  completeness TEXT NOT NULL CHECK(completeness IN ('COMPLETE','PARTIAL','UNRESOLVED')),
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, observed_second)
);
CREATE INDEX gap_coordinate_snapshots_heat_time
  ON gap_coordinate_snapshots(source_heat_id,observed_at_us,id);

CREATE TABLE participant_gap_coordinates (
  snapshot_id INTEGER NOT NULL REFERENCES gap_coordinate_snapshots(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  source_position_overall INTEGER,
  source_position_class INTEGER,
  raw_gap_value TEXT,
  display_value_kind TEXT NOT NULL CHECK(display_value_kind IN (
    'LAP_GROUP','TIME','EMPTY','UNKNOWN'
  )),
  lap_group_completed_laps INTEGER,
  time_from_lap_group_leader_ms INTEGER,
  lap_group_leader_participant_id TEXT REFERENCES participants(id) ON DELETE SET NULL,
  lap_group_leader_position_overall INTEGER,
  gap_to_overall_leader_laps INTEGER,
  gap_to_overall_leader_residual_ms INTEGER,
  coordinate_status TEXT NOT NULL CHECK(coordinate_status IN (
    'EXACT','GROUP_UNRESOLVED','VALUE_UNSUPPORTED','POSITION_UNRESOLVED'
  )),
  source_cell_observation_id INTEGER
    REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL,
  source_cell_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_cell_key TEXT,
  source_cell_observed_at_us INTEGER,
  created_at_us INTEGER NOT NULL,
  PRIMARY KEY(snapshot_id,participant_id)
);
CREATE INDEX participant_gap_coordinates_participant_time
  ON participant_gap_coordinates(participant_id,snapshot_id);
CREATE INDEX participant_gap_coordinates_position
  ON participant_gap_coordinates(snapshot_id,source_position_overall);
