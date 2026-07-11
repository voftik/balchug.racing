-- A result-grid LAST cell is authoritative for the published lap duration,
-- while Tracker t_p events provide the exact physical chronology.  Keep a
-- separate canonical layer so a replay can repair derived lap numbering
-- without rewriting either immutable source stream or the legacy projection.

CREATE TABLE canonical_lap_boundaries (
  id TEXT PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  boundary_ordinal INTEGER NOT NULL CHECK(boundary_ordinal >= 0),
  boundary_kind TEXT NOT NULL CHECK(boundary_kind IN (
    'HEAT_START','COVERAGE_START','MAIN_FINISH','PIT_FINISH'
  )),
  source_kind TEXT NOT NULL CHECK(source_kind IN ('HEAT_GREEN','TRACKER_PASSING')),
  passing_observation_id INTEGER UNIQUE
    REFERENCES tracker_passing_observations(id) ON DELETE CASCADE,
  corroborating_passing_observation_id INTEGER
    REFERENCES tracker_passing_observations(id) ON DELETE SET NULL,
  provider_passed_at_raw TEXT NOT NULL,
  provider_passed_at_provider_us INTEGER NOT NULL,
  passed_at_us INTEGER,
  observed_at_us INTEGER NOT NULL,
  start_distance_mm INTEGER,
  stop_distance_mm INTEGER,
  sector_id INTEGER,
  is_in_pit INTEGER CHECK(is_in_pit IS NULL OR is_in_pit IN (0, 1)),
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, participant_id, boundary_ordinal),
  UNIQUE(source_heat_id, participant_id, provider_passed_at_provider_us, boundary_kind)
);
CREATE INDEX canonical_lap_boundaries_participant_time
  ON canonical_lap_boundaries(
    source_heat_id,participant_id,provider_passed_at_provider_us,boundary_ordinal
  );

CREATE TABLE canonical_laps (
  id TEXT PRIMARY KEY,
  source_heat_id INTEGER NOT NULL REFERENCES source_heats(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  lap_ordinal INTEGER NOT NULL CHECK(lap_ordinal >= 1),
  lap_number INTEGER CHECK(lap_number IS NULL OR lap_number >= 1),
  coverage_complete INTEGER NOT NULL CHECK(coverage_complete IN (0, 1)),
  start_boundary_id TEXT NOT NULL
    REFERENCES canonical_lap_boundaries(id) ON DELETE CASCADE,
  finish_boundary_id TEXT NOT NULL UNIQUE
    REFERENCES canonical_lap_boundaries(id) ON DELETE CASCADE,
  started_at_provider_us INTEGER NOT NULL,
  finished_at_provider_us INTEGER NOT NULL,
  started_at_us INTEGER,
  finished_at_us INTEGER,
  start_observed_at_us INTEGER NOT NULL,
  finish_observed_at_us INTEGER NOT NULL,
  tracker_duration_us INTEGER NOT NULL CHECK(tracker_duration_us > 0),
  tracker_duration_ms INTEGER NOT NULL CHECK(tracker_duration_ms > 0),
  source_last_cell_observation_id INTEGER UNIQUE
    REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL,
  source_duration_raw TEXT,
  source_duration_us INTEGER,
  source_duration_ms INTEGER,
  duration_reconciliation TEXT NOT NULL CHECK(duration_reconciliation IN (
    'PENDING','EXACT','MISMATCH','MISSING_SOURCE'
  )),
  is_pit_lap INTEGER NOT NULL CHECK(is_pit_lap IN (0, 1)),
  source_message_id INTEGER REFERENCES feed_messages(id) ON DELETE SET NULL,
  source_key TEXT NOT NULL,
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  UNIQUE(source_heat_id, participant_id, lap_ordinal),
  UNIQUE(source_heat_id, participant_id, lap_number)
);
CREATE INDEX canonical_laps_participant_finish
  ON canonical_laps(source_heat_id,participant_id,finished_at_provider_us,lap_ordinal);
CREATE INDEX canonical_laps_source_status
  ON canonical_laps(source_heat_id,duration_reconciliation,finish_observed_at_us);

CREATE TABLE canonical_lap_tracker_passings (
  canonical_lap_id TEXT NOT NULL REFERENCES canonical_laps(id) ON DELETE CASCADE,
  passing_observation_id INTEGER NOT NULL UNIQUE
    REFERENCES tracker_passing_observations(id) ON DELETE CASCADE,
  passing_ordinal INTEGER NOT NULL CHECK(passing_ordinal >= 1),
  role TEXT NOT NULL CHECK(role IN (
    'START_CORROBORATION','SECTOR_1_END','SECTOR_2_END','FINISH','PIT_PATH','OTHER'
  )),
  PRIMARY KEY(canonical_lap_id, passing_ordinal),
  UNIQUE(canonical_lap_id, passing_observation_id)
);

CREATE TABLE canonical_lap_sectors (
  canonical_lap_id TEXT NOT NULL REFERENCES canonical_laps(id) ON DELETE CASCADE,
  sector_number INTEGER NOT NULL CHECK(sector_number BETWEEN 1 AND 3),
  tracker_start_passing_observation_id INTEGER
    REFERENCES tracker_passing_observations(id) ON DELETE SET NULL,
  tracker_finish_passing_observation_id INTEGER
    REFERENCES tracker_passing_observations(id) ON DELETE SET NULL,
  tracker_started_at_provider_us INTEGER,
  tracker_finished_at_provider_us INTEGER,
  tracker_duration_us INTEGER CHECK(tracker_duration_us IS NULL OR tracker_duration_us > 0),
  tracker_duration_ms INTEGER CHECK(tracker_duration_ms IS NULL OR tracker_duration_ms > 0),
  source_cell_observation_id INTEGER UNIQUE
    REFERENCES participant_result_cell_observations(id) ON DELETE SET NULL,
  source_duration_raw TEXT,
  source_duration_us INTEGER,
  source_duration_ms INTEGER,
  duration_reconciliation TEXT NOT NULL CHECK(duration_reconciliation IN (
    'PENDING','EXACT','MISMATCH','MISSING_SOURCE','MISSING_TRACKER'
  )),
  created_at_us INTEGER NOT NULL,
  updated_at_us INTEGER NOT NULL,
  PRIMARY KEY(canonical_lap_id, sector_number)
);
CREATE INDEX canonical_lap_sectors_source_status
  ON canonical_lap_sectors(duration_reconciliation,canonical_lap_id,sector_number);

ALTER TABLE result_last_cell_ledger ADD COLUMN linked_canonical_lap_id TEXT
  REFERENCES canonical_laps(id) ON DELETE SET NULL;
CREATE INDEX result_last_cell_ledger_linked_canonical_lap
  ON result_last_cell_ledger(linked_canonical_lap_id)
  WHERE linked_canonical_lap_id IS NOT NULL;
