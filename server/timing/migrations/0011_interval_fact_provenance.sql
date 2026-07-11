-- GAP and DIFF are sparse result-grid cells.  A materialized grid can retain
-- the last display value across later frames, so preserve each exact cell as
-- an append-only fact and let the current row point at that source fact.
-- Invalid provider forms (for example "1 lap") remain facts with a NULL
-- interval_ms; they are never coerced into a time interval.
CREATE TABLE participant_interval_source_facts (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  interval_kind TEXT NOT NULL CHECK(interval_kind IN ('GAP','DIFF')),
  raw_value TEXT,
  interval_ms INTEGER,
  value_kind TEXT CHECK(value_kind IS NULL OR value_kind = 'TIME'),
  source_cell_observation_id INTEGER NOT NULL UNIQUE
    REFERENCES participant_result_cell_observations(id) ON DELETE CASCADE,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  source_change_ordinal INTEGER NOT NULL CHECK(source_change_ordinal >= 0),
  source_handle TEXT NOT NULL CHECK(source_handle IN ('r_i','r_c')),
  observation_kind TEXT NOT NULL CHECK(observation_kind IN ('SNAPSHOT_BASELINE','DELTA')),
  observed_at_us INTEGER NOT NULL,
  source_layout_version_id INTEGER REFERENCES result_layout_versions(id) ON DELETE SET NULL,
  source_provider_row_index INTEGER NOT NULL CHECK(source_provider_row_index >= 0),
  source_position_overall INTEGER,
  source_position_class INTEGER,
  source_laps INTEGER,
  source_state_kind TEXT,
  relation_kind TEXT CHECK(relation_kind IS NULL OR relation_kind IN ('OVERALL_LEADER','OVERALL_AHEAD')),
  target_participant_id TEXT REFERENCES participants(id) ON DELETE SET NULL,
  target_position_overall INTEGER,
  target_state_kind TEXT,
  target_laps INTEGER,
  created_at_us INTEGER NOT NULL
);
CREATE INDEX participant_interval_source_facts_participant_time
  ON participant_interval_source_facts(source_heat_id, participant_id, observed_at_us, id);
CREATE INDEX participant_interval_source_facts_message
  ON participant_interval_source_facts(source_message_id, participant_id, id);
CREATE INDEX participant_interval_source_facts_target_time
  ON participant_interval_source_facts(source_heat_id, target_participant_id, observed_at_us, id)
  WHERE target_participant_id IS NOT NULL;

ALTER TABLE participant_state_current ADD COLUMN gap_interval_fact_id INTEGER
  REFERENCES participant_interval_source_facts(id) ON DELETE SET NULL;
ALTER TABLE participant_state_current ADD COLUMN diff_interval_fact_id INTEGER
  REFERENCES participant_interval_source_facts(id) ON DELETE SET NULL;
CREATE INDEX participant_state_current_gap_interval_fact
  ON participant_state_current(gap_interval_fact_id)
  WHERE gap_interval_fact_id IS NOT NULL;
CREATE INDEX participant_state_current_diff_interval_fact
  ON participant_state_current(diff_interval_fact_id)
  WHERE diff_interval_fact_id IS NOT NULL;
