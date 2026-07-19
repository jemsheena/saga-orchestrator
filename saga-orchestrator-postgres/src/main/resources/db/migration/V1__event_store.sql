-- Milestone 2 schema, part 1: the event-sourced write side.
-- See Milestone 2 architecture review, Section 8, for full ER reasoning.

-- The append-only event log. The single source of truth for a saga's history.
CREATE TABLE saga_event (
    event_id             UUID PRIMARY KEY,
    saga_id               UUID NOT NULL,
    sequence_no           BIGINT NOT NULL,
    global_sequence        BIGSERIAL,
    event_type            VARCHAR(100) NOT NULL,
    event_schema_version   INT NOT NULL,
    payload                JSONB NOT NULL,
    occurred_at            TIMESTAMPTZ NOT NULL,
    recorded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    correlation_id         UUID NOT NULL,
    causation_id           UUID,

    -- The absolute concurrency invariant: two events can never occupy the
    -- same position in the same saga's stream. saga_stream_head (below) is
    -- the fast, explicit conflict signal; this constraint is the backstop
    -- that makes a duplicate-position write structurally impossible even if
    -- the head-table logic ever has a bug. See architecture review Section 1.
    CONSTRAINT uq_saga_event_stream UNIQUE (saga_id, sequence_no)
);

-- Supports chronological cross-saga scans (projection rebuilds, future Kafka
-- publishing in ordered position) distinct from per-stream sequence_no.
CREATE INDEX idx_saga_event_global_sequence ON saga_event (global_sequence);

-- Supports the store's two loadEvents() query shapes directly.
CREATE INDEX idx_saga_event_saga_id_sequence ON saga_event (saga_id, sequence_no);

-- Append-only enforced at the permission level, not just application
-- discipline. Run once, against the actual application role, in a real
-- deployment (adjust role name accordingly):
-- REVOKE UPDATE, DELETE ON saga_event FROM application_role;

-- The one deliberately mutable table in this write-side schema. Drives
-- optimistic concurrency via a conditional UPDATE — see PostgresSagaEventStore
-- and architecture review Section 7 for the full mechanism and the two
-- alternatives (Axon's constraint-only approach, EventStoreDB's native
-- expectedVersion) it was chosen over.
CREATE TABLE saga_stream_head (
    saga_id               UUID PRIMARY KEY,
    current_sequence_no    BIGINT NOT NULL
);
