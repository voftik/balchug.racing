-- Race Control / ScreenMessagesList (m_*) source ledger.
--
-- The current provider payload has no event-occurrence timestamp.  Therefore
-- observed_at_us is the exact UTC time at which this recorder received the
-- source frame.  provider_occurred_at_us is deliberately nullable and must
-- remain NULL unless a future provider payload actually supplies that value;
-- it must never be inferred from observed_at_us or a rendered dashboard.
--
-- One source invocation can carry a complete m_i snapshot, so the immutable
-- ledger permits one row per snapshot member (or one marker for an empty
-- snapshot) using source_change_ordinal.  The original provider payload stays
-- intact on every row for replay and later LLM analysis.
CREATE TABLE race_control_message_observations (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  source_handle TEXT NOT NULL,
  operation TEXT NOT NULL CHECK(operation IN (
    'INITIAL_SNAPSHOT','UPSERT','DELETE','CLEAR','UNKNOWN'
  )),
  message_id_raw TEXT,
  text_raw TEXT,
  line INTEGER,
  modality INTEGER,
  background_color_raw TEXT,
  font_color_raw TEXT,
  provider_occurred_at_us INTEGER,
  raw_record_json TEXT,
  raw_payload_json TEXT NOT NULL,
  source_frame_id INTEGER NOT NULL REFERENCES feed_frames(id) ON DELETE CASCADE,
  source_message_id INTEGER NOT NULL REFERENCES feed_messages(id) ON DELETE CASCADE,
  source_message_ordinal INTEGER NOT NULL CHECK(source_message_ordinal >= 0),
  source_key TEXT NOT NULL,
  source_change_ordinal INTEGER NOT NULL CHECK(source_change_ordinal >= 0),
  observed_at_us INTEGER NOT NULL CHECK(observed_at_us >= 0),
  created_at_us INTEGER NOT NULL CHECK(created_at_us >= 0),
  CHECK(
    (operation = 'INITIAL_SNAPSHOT' AND source_handle = 'm_i')
    OR (operation = 'UPSERT' AND source_handle = 'm_c')
    OR (operation = 'DELETE' AND source_handle = 'm_d')
    OR (operation = 'CLEAR' AND source_handle = 'm_a')
    OR operation = 'UNKNOWN'
  ),
  CHECK(operation NOT IN ('UPSERT','DELETE') OR message_id_raw IS NOT NULL),
  UNIQUE(source_heat_id, source_key, source_change_ordinal)
);
CREATE INDEX race_control_message_observations_heat_time
  ON race_control_message_observations(source_heat_id, observed_at_us, id);
CREATE INDEX race_control_message_observations_message
  ON race_control_message_observations(source_message_id, source_change_ordinal);
CREATE INDEX race_control_message_observations_message_time
  ON race_control_message_observations(source_heat_id, message_id_raw, observed_at_us, id)
  WHERE message_id_raw IS NOT NULL;

-- This is the materialized provider message list, not a replacement for the
-- immutable ledger above.  Inactive rows are retained so a DELETE, CLEAR or a
-- later m_i snapshot reconciliation remains auditable.  Snapshot membership
-- establishes first_observation_kind, but never claims the message originated
-- at first_observed_at_us.
CREATE TABLE race_control_messages_current (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  message_id_raw TEXT NOT NULL,
  text_raw TEXT,
  line INTEGER,
  modality INTEGER,
  background_color_raw TEXT,
  font_color_raw TEXT,
  provider_occurred_at_us INTEGER,
  raw_record_json TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
  first_observation_kind TEXT NOT NULL CHECK(first_observation_kind IN (
    'INITIAL_SNAPSHOT','UPSERT'
  )),
  first_observed_at_us INTEGER NOT NULL CHECK(first_observed_at_us >= 0),
  first_source_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL,
  first_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  first_source_key TEXT NOT NULL,
  first_source_change_ordinal INTEGER NOT NULL CHECK(first_source_change_ordinal >= 0),
  first_observation_id INTEGER
    REFERENCES race_control_message_observations(id) ON DELETE SET NULL,
  last_action TEXT NOT NULL CHECK(last_action IN (
    'INITIAL_SNAPSHOT','UPSERT','DELETE','CLEAR','SNAPSHOT_RECONCILIATION'
  )),
  last_observed_at_us INTEGER NOT NULL CHECK(last_observed_at_us >= 0),
  last_source_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL,
  last_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  last_source_key TEXT NOT NULL,
  last_source_change_ordinal INTEGER CHECK(
    last_source_change_ordinal IS NULL OR last_source_change_ordinal >= 0
  ),
  last_observation_id INTEGER
    REFERENCES race_control_message_observations(id) ON DELETE SET NULL,
  removed_at_us INTEGER CHECK(removed_at_us IS NULL OR removed_at_us >= 0),
  removal_action TEXT CHECK(removal_action IS NULL OR removal_action IN (
    'DELETE','CLEAR','SNAPSHOT_RECONCILIATION'
  )),
  removed_source_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL,
  removed_source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  removed_source_key TEXT,
  removed_source_change_ordinal INTEGER CHECK(
    removed_source_change_ordinal IS NULL OR removed_source_change_ordinal >= 0
  ),
  removed_observation_id INTEGER
    REFERENCES race_control_message_observations(id) ON DELETE SET NULL,
  created_at_us INTEGER NOT NULL CHECK(created_at_us >= 0),
  updated_at_us INTEGER NOT NULL CHECK(updated_at_us >= 0),
  PRIMARY KEY(source_heat_id, message_id_raw),
  CHECK(
    (is_active = 1
      AND removed_at_us IS NULL
      AND removal_action IS NULL
      AND removed_source_frame_id IS NULL
      AND removed_source_message_id IS NULL
      AND removed_source_key IS NULL
      AND removed_source_change_ordinal IS NULL
      AND removed_observation_id IS NULL)
    OR
    (is_active = 0
      AND removed_at_us IS NOT NULL
      AND removal_action IS NOT NULL
      AND removed_source_key IS NOT NULL)
  )
);
CREATE INDEX race_control_messages_current_active
  ON race_control_messages_current(source_heat_id, line, first_observed_at_us, message_id_raw)
  WHERE is_active = 1;
CREATE INDEX race_control_messages_current_history
  ON race_control_messages_current(source_heat_id, first_observed_at_us, message_id_raw);
