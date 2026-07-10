-- Durable cursors for replayable read-only SSE. Existing exceptional events
-- remain valid with NULL event_key/observed_at_us; new timing events use both.
ALTER TABLE stream_events ADD COLUMN event_key TEXT;
ALTER TABLE stream_events ADD COLUMN observed_at_us INTEGER;
ALTER TABLE stream_events ADD COLUMN source_frame_id INTEGER REFERENCES feed_frames(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX stream_events_session_event_key
  ON stream_events(analysis_session_id, event_key) WHERE event_key IS NOT NULL;
CREATE INDEX stream_events_session_cursor
  ON stream_events(analysis_session_id, id);

-- Retention records the highest cursor removed for each session. A reconnect
-- can then distinguish an expired Last-Event-ID from harmless global-id gaps
-- caused by another session's events.
CREATE TABLE stream_event_cursor_floors (
  analysis_session_id TEXT PRIMARY KEY REFERENCES analysis_sessions(id) ON DELETE CASCADE,
  deleted_through_id INTEGER NOT NULL CHECK(deleted_through_id >= 0),
  updated_at_us INTEGER NOT NULL
);

-- Detail endpoints constrain results by heat and time; these indexes keep
-- archived 24-hour sessions from scanning every crew's complete history.
CREATE INDEX laps_heat_time ON laps(source_heat_id, completed_at_us, participant_id);
CREATE INDEX pits_heat_time ON pit_stops(source_heat_id, entered_at_us, participant_id);
