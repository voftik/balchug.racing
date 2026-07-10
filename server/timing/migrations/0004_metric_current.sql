-- The dashboard needs the newest calculated state every second, while
-- metric_samples intentionally remains sparse for charts.  Keep this compact
-- materialization separate so current reads never need to reconstruct it from
-- the five-second/event history.
CREATE TABLE metric_current (
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  scope_kind TEXT NOT NULL CHECK(scope_kind IN ('participant', 'class', 'session')),
  scope_key TEXT NOT NULL,
  observed_at_us INTEGER NOT NULL,
  metric_version INTEGER NOT NULL CHECK(metric_version > 0),
  values_json TEXT NOT NULL,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  PRIMARY KEY(source_heat_id, scope_kind, scope_key)
);

-- This cursor is committed with metric_current/history. It is deliberately
-- separate from reducer checkpoints: it records the last *derived* boundary,
-- so retrying a normalized-but-unmaterialized frame after a worker restart
-- still emits its transition exactly once.
CREATE TABLE metric_runner_state (
  source_heat_id INTEGER PRIMARY KEY REFERENCES source_heats(id) ON DELETE CASCADE,
  observed_at_us INTEGER NOT NULL,
  source_frame_id INTEGER NOT NULL REFERENCES feed_frames(id) ON DELETE CASCADE,
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  metric_version INTEGER NOT NULL CHECK(metric_version > 0),
  boundary_state_json TEXT NOT NULL,
  updated_at_us INTEGER NOT NULL
);
