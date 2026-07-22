-- Migration to upgrade the inbox schema for consumer-aware deduplication and richer state tracking.
-- Existing rows are preserved and silently assigned the default consumer.

ALTER TABLE inbox ADD COLUMN consumer VARCHAR(200) NOT NULL DEFAULT 'default';
ALTER TABLE inbox ADD COLUMN topic VARCHAR(200) NOT NULL DEFAULT '';
ALTER TABLE inbox ADD COLUMN partition_key VARCHAR(200) NOT NULL DEFAULT '';
ALTER TABLE inbox ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'PROCESSED';
ALTER TABLE inbox ADD COLUMN received_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE inbox DROP CONSTRAINT IF EXISTS inbox_pkey;
ALTER TABLE inbox ADD PRIMARY KEY (message_id, consumer);

CREATE INDEX idx_inbox_processed_at ON inbox (processed_at);
CREATE INDEX idx_inbox_status ON inbox (status);
CREATE INDEX idx_inbox_received_at ON inbox (received_at);
