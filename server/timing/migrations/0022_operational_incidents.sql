-- Durable transitions for non-sensitive timing infrastructure incidents.
-- Details are generated from an allowlisted operational health model; raw
-- provider payloads, tokens, team and driver fields never enter this table.
CREATE TABLE timing_operational_incidents (
  id INTEGER PRIMARY KEY,
  incident_code TEXT NOT NULL,
  scope_kind TEXT NOT NULL CHECK(scope_kind IN ('system','worker','session','source')),
  scope_key TEXT NOT NULL,
  severity TEXT NOT NULL CHECK(severity IN ('WARNING','CRITICAL')),
  status TEXT NOT NULL CHECK(status IN ('OPEN','RESOLVED')),
  details_json TEXT NOT NULL,
  opened_at_us INTEGER NOT NULL CHECK(opened_at_us >= 0),
  last_seen_at_us INTEGER NOT NULL CHECK(last_seen_at_us >= opened_at_us),
  resolved_at_us INTEGER CHECK(resolved_at_us IS NULL OR resolved_at_us >= opened_at_us),
  occurrence_count INTEGER NOT NULL DEFAULT 1 CHECK(occurrence_count > 0),
  created_at_us INTEGER NOT NULL CHECK(created_at_us >= 0),
  updated_at_us INTEGER NOT NULL CHECK(updated_at_us >= 0)
);
CREATE UNIQUE INDEX timing_operational_incidents_one_open
  ON timing_operational_incidents(incident_code,scope_kind,scope_key)
  WHERE status = 'OPEN';
CREATE INDEX timing_operational_incidents_status_time
  ON timing_operational_incidents(status,severity,opened_at_us DESC,id DESC);
