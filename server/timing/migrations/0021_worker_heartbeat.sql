-- One current, non-sensitive heartbeat per long-lived timing worker. Session
-- and source details remain in their normalized tables; this row only proves
-- process liveness and restart identity to the read-only health API.
CREATE TABLE timing_worker_heartbeats (
  worker_kind TEXT PRIMARY KEY,
  instance_id TEXT NOT NULL,
  pid INTEGER NOT NULL CHECK(pid > 0),
  state TEXT NOT NULL CHECK(state IN ('STARTING','READY','STOPPING','STOPPED','FAILED')),
  active_session_count INTEGER NOT NULL DEFAULT 0 CHECK(active_session_count >= 0),
  started_at_us INTEGER NOT NULL CHECK(started_at_us >= 0),
  ready_at_us INTEGER CHECK(ready_at_us IS NULL OR ready_at_us >= started_at_us),
  heartbeat_at_us INTEGER NOT NULL CHECK(heartbeat_at_us >= started_at_us),
  stopped_at_us INTEGER CHECK(stopped_at_us IS NULL OR stopped_at_us >= started_at_us),
  stop_reason TEXT,
  updated_at_us INTEGER NOT NULL CHECK(updated_at_us >= started_at_us)
);
