-- A finish-loop passing supplies chronology only.  The result-grid LAST cell
-- remains the authoritative source for a lap duration (and its sector cells).
-- Keep both immutable links so a published lap can be audited without
-- conflating the tracker boundary with the timing value.
ALTER TABLE laps ADD COLUMN completion_passing_observation_id INTEGER
  REFERENCES tracker_passing_observations(id) ON DELETE SET NULL;
ALTER TABLE laps ADD COLUMN duration_source_cell_observation_id INTEGER
  REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL;
ALTER TABLE laps ADD COLUMN duration_source_message_id INTEGER
  REFERENCES feed_messages(id) ON DELETE SET NULL;
ALTER TABLE laps ADD COLUMN duration_source_key TEXT;
ALTER TABLE laps ADD COLUMN duration_source_kind TEXT
  CHECK(duration_source_kind IS NULL OR duration_source_kind = 'RESULT_GRID_LAST');
ALTER TABLE laps ADD COLUMN sectors_source_cell_observation_ids_json TEXT;

CREATE INDEX laps_completion_passing_observation
  ON laps(completion_passing_observation_id)
  WHERE completion_passing_observation_id IS NOT NULL;
CREATE INDEX laps_duration_source_cell_observation
  ON laps(duration_source_cell_observation_id)
  WHERE duration_source_cell_observation_id IS NOT NULL;
CREATE INDEX tracker_passing_observations_source_message
  ON tracker_passing_observations(source_message_id, id);
CREATE INDEX result_cell_observations_source_message
  ON participant_result_cell_observations(source_message_id, participant_id, id);
