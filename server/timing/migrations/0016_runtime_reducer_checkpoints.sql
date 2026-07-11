-- The original table used UNIQUE(source_heat_id, observed_at_us). That is not
-- a valid runtime identity: two different SignalR frames can be received in
-- the same microsecond. Rebuild the small checkpoint table before any new
-- table references it, preserving IDs/provenance and keeping the legacy
-- timestamp uniqueness as a partial index below.
CREATE TABLE state_checkpoints_v2 (
  id INTEGER PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  source_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  state_hash TEXT NOT NULL,
  codec TEXT NOT NULL CHECK(codec IN ('gzip', 'identity')),
  payload BLOB NOT NULL,
  checkpoint_format TEXT NOT NULL DEFAULT 'legacy',
  checkpoint_format_version INTEGER NOT NULL DEFAULT 0,
  reducer_version TEXT NOT NULL DEFAULT 'legacy',
  created_at_us INTEGER NOT NULL
);
INSERT INTO state_checkpoints_v2(
  id,source_heat_id,source_frame_id,source_key,observed_at_us,state_hash,codec,payload,
  checkpoint_format,checkpoint_format_version,reducer_version,created_at_us
)
SELECT
  id,source_heat_id,source_frame_id,source_key,observed_at_us,state_hash,codec,payload,
  'legacy',0,'legacy',created_at_us
FROM state_checkpoints;
DROP TABLE state_checkpoints;
ALTER TABLE state_checkpoints_v2 RENAME TO state_checkpoints;

-- Existing generic callers retain their timestamp idempotency contract.
CREATE UNIQUE INDEX state_checkpoints_legacy_tick
  ON state_checkpoints(source_heat_id,observed_at_us,checkpoint_format,checkpoint_format_version)
  WHERE checkpoint_format <> 'timing-normalizer';
CREATE INDEX checkpoints_heat_time ON state_checkpoints(source_heat_id, observed_at_us DESC);

-- A runtime checkpoint is anchored to one immutable processed frame. Receive
-- timestamps alone are not unique across physical SignalR frames.
CREATE UNIQUE INDEX state_checkpoints_runtime_frame_anchor
  ON state_checkpoints(source_heat_id,source_frame_id)
  WHERE checkpoint_format = 'timing-normalizer'
    AND checkpoint_format_version = 1
    AND source_frame_id IS NOT NULL;
CREATE INDEX state_checkpoints_runtime_latest
  ON state_checkpoints(source_heat_id,checkpoint_format,reducer_version,source_frame_id DESC);

-- Once RAW timing frames have been pruned, a destructive full rebuild cannot
-- truthfully recreate the entire session. Keep an explicit floor rather than
-- silently treating the surviving tail as a complete recording.
CREATE TABLE timing_raw_retention_floors (
  analysis_session_id TEXT PRIMARY KEY REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  deleted_through_frame_id INTEGER NOT NULL CHECK(deleted_through_frame_id > 0),
  deleted_through_received_at_us INTEGER NOT NULL CHECK(deleted_through_received_at_us >= 0),
  checkpoint_id INTEGER REFERENCES state_checkpoints(id) ON DELETE SET NULL,
  created_at_us INTEGER NOT NULL CHECK(created_at_us >= 0),
  updated_at_us INTEGER NOT NULL CHECK(updated_at_us >= 0)
);

-- One reducer construction records how it became ready. The API process reads
-- this durable audit row instead of relying on in-process worker memory.
CREATE TABLE normalizer_restore_events (
  id INTEGER PRIMARY KEY,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  source_heat_id INTEGER REFERENCES source_heats(id) ON DELETE SET NULL,
  checkpoint_id INTEGER REFERENCES state_checkpoints(id) ON DELETE SET NULL,
  anchor_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL,
  outcome TEXT NOT NULL CHECK(outcome IN ('RESTORED','COLD_REPLAY','FALLBACK')),
  reason TEXT,
  replayed_tail_frames INTEGER NOT NULL CHECK(replayed_tail_frames >= 0),
  created_at_us INTEGER NOT NULL CHECK(created_at_us >= 0)
);
CREATE INDEX normalizer_restore_events_session_time
  ON normalizer_restore_events(analysis_session_id,created_at_us DESC);
