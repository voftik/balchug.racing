-- A long no-LAPS heat reads a bounded LAST/STATE tail per participant after
-- the durable metric cursor. The older column-time index is efficient for a
-- whole-grid audit, but makes SQLite scan every participant's STATE column for
-- each crew. Keep the source-order fields in the same participant-local index
-- so a 60-car tick remains bounded through a 24-hour race.
CREATE INDEX result_cell_observations_metric_tail
  ON participant_result_cell_observations(
    source_heat_id,participant_id,layout_version_id,column_index,
    source_message_id,source_change_ordinal,id
  );

CREATE INDEX participant_state_observations_metric_tail
  ON participant_state_observations(
    source_heat_id,participant_id,source_message_id,source_event_key
  )
  WHERE state_cell_observation_id IS NOT NULL;
