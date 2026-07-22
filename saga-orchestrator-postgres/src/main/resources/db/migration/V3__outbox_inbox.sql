-- Milestone 3, Phase 1: Outbox and Inbox pattern tables.
-- See Milestone 3 architecture review Sections 8-9, and messaging module's
-- OutboxStore/InboxStore javadoc, for the full reasoning.

-- Durable staging for messages that must eventually be published. A row's
-- existence here, once its enclosing business transaction commits, is a
-- durable promise that the message WILL be published (at-least-once) -
-- this is the entire Outbox guarantee.
CREATE TABLE outbox (
    outbox_id       UUID PRIMARY KEY,
    topic           VARCHAR(200) NOT NULL,
    message_key     VARCHAR(200) NOT NULL,
    message_type    VARCHAR(200) NOT NULL,
    payload         BYTEA NOT NULL,
    correlation_id  UUID NOT NULL,
    causation_id    UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    dispatched_at   TIMESTAMPTZ
);

-- Partial index: only undispatched rows are ever queried by the poller, and
-- there are always far fewer of those than the full historical table, so
-- indexing only that subset keeps the poller's claim query cheap regardless
-- of how large the dispatched-history portion of this table grows over time.
CREATE INDEX idx_outbox_undispatched ON outbox (created_at) WHERE dispatched_at IS NULL;

-- Deduplication for at-least-once message delivery. A row's existence means
-- "this message_id has already been processed" - see InboxStore.recordIfNew,
-- whose atomicity this table's PRIMARY KEY + ON CONFLICT DO NOTHING directly enables.
CREATE TABLE inbox (
    message_id      UUID PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
