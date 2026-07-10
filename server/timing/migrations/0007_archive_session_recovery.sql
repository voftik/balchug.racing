-- Recovery provenance for a replacement archive assembled from an immutable
-- raw recorder capture. The original session stays addressable for audit and
-- direct archive links; only the archive listing suppresses it.
CREATE TABLE archive_session_replacements (
  superseded_session_id TEXT PRIMARY KEY
    REFERENCES analysis_sessions(id) ON DELETE RESTRICT,
  canonical_session_id TEXT NOT NULL
    REFERENCES analysis_sessions(id) ON DELETE RESTRICT,
  recording_sha256 TEXT NOT NULL UNIQUE CHECK(length(recording_sha256) = 64),
  frame_count INTEGER NOT NULL CHECK(frame_count > 0),
  capture_first_at_us INTEGER NOT NULL,
  capture_last_at_us INTEGER NOT NULL CHECK(capture_last_at_us >= capture_first_at_us),
  reason TEXT NOT NULL CHECK(reason = 'recovered_raw_capture'),
  created_at_us INTEGER NOT NULL,
  CHECK(superseded_session_id <> canonical_session_id)
);

CREATE INDEX archive_session_replacements_canonical
  ON archive_session_replacements(canonical_session_id);
