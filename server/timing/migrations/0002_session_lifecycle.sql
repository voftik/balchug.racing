-- Immutable control-plane additions for engineer analysis sessions.
-- Existing timing facts remain in 0001; this migration only records who
-- started/stopped a session and makes repeated write requests deterministic.

ALTER TABLE timing_sources ADD COLUMN display_name TEXT;
ALTER TABLE timing_sources ADD COLUMN timezone_name TEXT NOT NULL DEFAULT 'Europe/Moscow';

ALTER TABLE analysis_sessions ADD COLUMN timezone_name TEXT NOT NULL DEFAULT 'Europe/Moscow';
ALTER TABLE analysis_sessions ADD COLUMN stop_intent TEXT
  CHECK(stop_intent IS NULL OR stop_intent IN (
    'operator_stop',
    'operator_abort',
    'source_reset',
    'recovery_shutdown'
  ));

CREATE TABLE session_audit_events (
  id INTEGER PRIMARY KEY,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL CHECK(event_type IN ('created', 'started', 'stopped', 'aborted')),
  actor_kind TEXT NOT NULL,
  parameters_json TEXT NOT NULL,
  created_at_us INTEGER NOT NULL
);
CREATE INDEX session_audit_events_session_time
  ON session_audit_events(analysis_session_id, id);

CREATE TABLE session_idempotency_keys (
  idempotency_key TEXT PRIMARY KEY,
  operation TEXT NOT NULL CHECK(operation IN ('create', 'start', 'stop', 'abort')),
  request_hash TEXT NOT NULL,
  analysis_session_id TEXT NOT NULL REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  result_json TEXT NOT NULL,
  created_at_us INTEGER NOT NULL
);
CREATE INDEX session_idempotency_session
  ON session_idempotency_keys(analysis_session_id, created_at_us);
