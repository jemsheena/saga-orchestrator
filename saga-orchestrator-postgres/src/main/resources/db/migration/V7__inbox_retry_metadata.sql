-- Add retry metadata columns to inbox table for Milestone 7
ALTER TABLE inbox
  ADD COLUMN retry_count integer NOT NULL DEFAULT 0,
  ADD COLUMN last_failure text NULL,
  ADD COLUMN last_attempt timestamp with time zone NULL,
  ADD COLUMN next_retry_time timestamp with time zone NULL,
  ADD COLUMN payload bytea NULL,
  ADD COLUMN correlation_id uuid NULL,
  ADD COLUMN causation_id uuid NULL;

-- Add index to support querying due-for-retry efficiently
CREATE INDEX IF NOT EXISTS idx_inbox_next_retry_time ON inbox (next_retry_time) WHERE next_retry_time IS NOT NULL;
