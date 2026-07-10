-- A clean lap cannot span a persisted pit in/out interval.  Older projections
-- predate this check, so repair the normalized boolean without touching raw
-- frames or their original duration facts.
WITH lap_intervals AS (
  SELECT id,source_heat_id,participant_id,completed_at_us,
         LAG(completed_at_us) OVER (
           PARTITION BY source_heat_id,participant_id ORDER BY lap_number
         ) AS lap_started_at_us
  FROM laps
  WHERE completed_at_us IS NOT NULL
), crossing_laps AS (
  SELECT lap_intervals.id
  FROM lap_intervals
  JOIN pit_stops AS pit
   ON pit.source_heat_id = lap_intervals.source_heat_id
   AND pit.participant_id = lap_intervals.participant_id
   AND pit.completed = 1
   AND pit.entered_at_us < lap_intervals.completed_at_us
   AND (pit.exited_at_us IS NULL OR pit.exited_at_us > lap_intervals.lap_started_at_us)
  WHERE lap_intervals.lap_started_at_us IS NOT NULL
)
UPDATE laps
SET is_clean = 0,
    crosses_pit = 1
WHERE id IN (SELECT id FROM crossing_laps);
