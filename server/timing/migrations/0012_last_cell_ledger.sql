-- Canonical result-grid LAST cells are immutable source observations, but a
-- provider ``r_c`` can also repaint an unchanged value for many rows. Keep a
-- separate, auditable classification for every observed canonical LAST cell
-- before tactical code decides whether it is a new timing event.
CREATE TABLE result_schema_baselines (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  ingest_connection_id TEXT NOT NULL REFERENCES ingest_connections(id) ON DELETE CASCADE,
  layout_version_id INTEGER NOT NULL REFERENCES result_layout_versions(id) ON DELETE CASCADE,
  layout_generation INTEGER NOT NULL CHECK(layout_generation >= 0),
  source_frame_id INTEGER NOT NULL REFERENCES feed_frames(id) ON DELETE CASCADE,
  source_message_id INTEGER NOT NULL UNIQUE REFERENCES feed_messages(id) ON DELETE CASCADE,
  source_message_ordinal INTEGER NOT NULL CHECK(source_message_ordinal >= 0),
  source_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  created_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, ingest_connection_id, source_message_id)
);
CREATE INDEX result_schema_baselines_connection_layout_rank
  ON result_schema_baselines(
    source_heat_id,ingest_connection_id,layout_version_id,layout_generation,
    source_frame_id,source_message_ordinal,id
  );

CREATE TABLE result_last_cell_ledger (
  source_cell_observation_id INTEGER PRIMARY KEY
    REFERENCES participant_result_cell_observations(id) ON DELETE CASCADE,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT REFERENCES participants(id) ON DELETE SET NULL,
  layout_version_id INTEGER NOT NULL REFERENCES result_layout_versions(id) ON DELETE CASCADE,
  source_frame_id INTEGER NOT NULL REFERENCES feed_frames(id) ON DELETE CASCADE,
  source_message_id INTEGER NOT NULL REFERENCES feed_messages(id) ON DELETE CASCADE,
  source_message_ordinal INTEGER NOT NULL CHECK(source_message_ordinal >= 0),
  source_key TEXT NOT NULL,
  source_change_ordinal INTEGER NOT NULL CHECK(source_change_ordinal >= 0),
  source_handle TEXT NOT NULL CHECK(source_handle IN ('r_i','r_c')),
  observed_at_us INTEGER NOT NULL,
  duration_ms INTEGER,
  classification TEXT NOT NULL CHECK(classification IN (
    'CONFIRMED_LAP','REFRESH_REPEAT','UNCONFIRMED','INVALID'
  )),
  classification_reason TEXT NOT NULL,
  predecessor_source_cell_observation_id INTEGER
    REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL,
  schema_baseline_id INTEGER REFERENCES result_schema_baselines(id) ON DELETE SET NULL,
  linked_lap_id TEXT REFERENCES laps(id) ON DELETE SET NULL,
  sectors_json TEXT,
  sectors_source_cell_observation_ids_json TEXT,
  created_at_us INTEGER NOT NULL
);
CREATE INDEX result_last_cell_ledger_participant_rank
  ON result_last_cell_ledger(
    source_heat_id,participant_id,source_frame_id,source_message_ordinal,
    source_change_ordinal,source_cell_observation_id
  );
CREATE INDEX result_last_cell_ledger_classification_rank
  ON result_last_cell_ledger(
    source_heat_id,classification,source_frame_id,source_message_ordinal,
    source_change_ordinal,source_cell_observation_id
  );
CREATE INDEX result_last_cell_ledger_linked_lap
  ON result_last_cell_ledger(linked_lap_id)
  WHERE linked_lap_id IS NOT NULL;
