-- A raw result-grid TsTime remains source evidence even if it is malformed.
-- Only the derived calibrated instant is bounded to a 24-hour race plus a
-- two-hour transport/target reserve around the source observation.
--
-- Do not modify raw cells or typed provider TsTime fields here. A replay can
-- always reproduce them; this migration removes only unsafe derived UTC
-- values published by earlier normalizer versions.

UPDATE participant_state_observations
SET state_timer_target_at_us = NULL,
    state_timer_calibration_id = NULL
WHERE state_timer_target_at_us IS NOT NULL
  AND (
    state_timer_target_at_us < observed_at_us - 93600000000
    OR state_timer_target_at_us > observed_at_us + 93600000000
  );

UPDATE participant_state_observations
SET driver_stint_at_us = NULL,
    driver_stint_calibration_id = NULL
WHERE driver_stint_at_us IS NOT NULL
  AND (
    driver_stint_at_us < observed_at_us - 93600000000
    OR driver_stint_at_us > observed_at_us + 93600000000
  );

UPDATE participant_state_current
SET state_timer_target_at_us = NULL,
    state_timer_calibration_id = NULL
WHERE state_timer_target_at_us IS NOT NULL
  AND state_timer_observed_at_us IS NOT NULL
  AND (
    state_timer_target_at_us < state_timer_observed_at_us - 93600000000
    OR state_timer_target_at_us > state_timer_observed_at_us + 93600000000
  );

UPDATE participant_state_current
SET driver_stint_at_us = NULL,
    driver_stint_calibration_id = NULL
WHERE driver_stint_at_us IS NOT NULL
  AND driver_stint_observed_at_us IS NOT NULL
  AND (
    driver_stint_at_us < driver_stint_observed_at_us - 93600000000
    OR driver_stint_at_us > driver_stint_observed_at_us + 93600000000
  );

-- A source L-PIT=S<TsTime> is optional provenance for a state-confirmed pit
-- boundary. If its prior calibrated time is implausible, retain the state/PIT
-- evidence and rebind the entry to the durable source-frame receive time.
UPDATE pit_stops
SET entered_at_us = (
      SELECT frame.received_at_us
      FROM feed_messages AS message
      JOIN feed_frames AS frame ON frame.id = message.frame_id
      WHERE message.id = pit_stops.entered_at_source_message_id
    ),
    entered_at_source_cell_observation_id = NULL,
    entered_at_source_message_id = NULL,
    entered_at_source_key = NULL,
    entered_at_source_kind = NULL
WHERE entered_at_source_kind = 'RESULT_L_PIT_S'
  AND entered_at_source_message_id IS NOT NULL
  AND EXISTS (
    SELECT 1
    FROM feed_messages AS message
    JOIN feed_frames AS frame ON frame.id = message.frame_id
    WHERE message.id = pit_stops.entered_at_source_message_id
      AND (
        pit_stops.entered_at_us < frame.received_at_us - 93600000000
        OR pit_stops.entered_at_us > frame.received_at_us + 93600000000
      )
  );
