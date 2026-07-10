-- Compact public dashboard anchors for deterministic archive playback. They
-- are emitted with metric materialization and intentionally outlive raw-frame
-- retention, so historical seek never needs to decode provider payloads.
CREATE TABLE playback_snapshots (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  observed_second INTEGER NOT NULL CHECK(observed_second >= 0),
  observed_at_us INTEGER NOT NULL CHECK(observed_at_us >= 0),
  source_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  projection_version INTEGER NOT NULL CHECK(projection_version = 1),
  metric_version INTEGER NOT NULL CHECK(metric_version > 0),
  is_event_boundary INTEGER NOT NULL DEFAULT 0 CHECK(is_event_boundary IN (0, 1)),
  payload_codec TEXT NOT NULL CHECK(payload_codec = 'gzip-json-v1'),
  payload BLOB NOT NULL,
  payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64),
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  PRIMARY KEY(source_heat_id, observed_second)
);
CREATE INDEX playback_snapshots_heat_time
  ON playback_snapshots(source_heat_id, observed_at_us);
