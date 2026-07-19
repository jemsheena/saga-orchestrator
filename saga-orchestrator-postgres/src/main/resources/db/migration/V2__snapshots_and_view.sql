-- Milestone 2 schema, part 2: snapshots (disposable performance cache) and
-- the CQRS read model. See architecture review Sections 4 and 8.

CREATE TABLE saga_snapshot (
    saga_id               UUID NOT NULL,
    sequence_no            BIGINT NOT NULL,
    saga_type              VARCHAR(100) NOT NULL,
    definition_version      INT NOT NULL,
    state                  VARCHAR(30) NOT NULL,
    current_step_index      INT NOT NULL,
    compensation_cursor      INT NOT NULL,
    schema_version           INT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (saga_id, sequence_no)
);

-- Supports "fetch the latest snapshot for this saga" cheaply.
CREATE INDEX idx_saga_snapshot_latest ON saga_snapshot (saga_id, sequence_no DESC);

-- The CQRS read model — see architecture review Section 2 for why this is
-- updated synchronously in the same transaction as the event append for
-- this milestone, and what changes (nothing in this table's shape) when
-- that becomes asynchronous later.
CREATE TABLE saga_instance_view (
    saga_id             UUID PRIMARY KEY,
    saga_type           VARCHAR(100) NOT NULL,
    state               VARCHAR(30) NOT NULL,
    current_step_index   INT NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at         TIMESTAMPTZ,
    duration_ms          BIGINT,
    last_error          TEXT
);

-- Supports "all currently-FAILED sagas" / dashboard-style queries — the
-- entire reason this table exists rather than querying the event store directly.
CREATE INDEX idx_saga_instance_view_state ON saga_instance_view (state);
