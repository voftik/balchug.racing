-- Race - REC 2026 exposed the provider literal `SFinshd` only after the
-- finish flag. Keep the source typo raw, but make the proven participant
-- state explicit and non-racing in current and append-only read models.
UPDATE participant_state_current
SET state = 'FINISHED', state_kind = 'FINISHED'
WHERE LOWER(TRIM(state_raw)) IN ('sfinshd','finshd','sfinished','finished');

UPDATE participant_state_observations
SET state_kind = 'FINISHED'
WHERE LOWER(TRIM(state_raw)) IN ('sfinshd','finshd','sfinished','finished');
