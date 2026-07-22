-- Milestone 5 schema: Outbox retry tracking and permanent failure marking.
-- Adds retry_count and failed_at to the outbox table so the publisher can
-- stop retrying records after a configured number of failed publish attempts.

ALTER TABLE outbox
    ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN failed_at TIMESTAMPTZ;

CREATE INDEX idx_outbox_failed_at_null ON outbox (created_at)
    WHERE dispatched_at IS NULL AND failed_at IS NULL;
