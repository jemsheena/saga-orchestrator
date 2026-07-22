-- Milestone 4 schema: Saga Timeout Handling
-- Adds timeout-related fields to the CQRS read model for efficient
-- scheduler queries. See Milestone 4 design: Phase 2 CQRS Projection.

-- Add new columns to saga_instance_view for timeout tracking
ALTER TABLE saga_instance_view
    ADD COLUMN last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN timeout_expired_at TIMESTAMPTZ;

-- Supports efficient scheduler queries: find expired non-terminal sagas
-- SELECT ... WHERE state NOT IN ('COMPLETED', 'FAILED')
--                AND timeout_expired_at IS NOT NULL
--                AND timeout_expired_at < now()
CREATE INDEX idx_saga_instance_view_expired_deadline ON saga_instance_view (timeout_expired_at, state)
    WHERE timeout_expired_at IS NOT NULL AND state NOT IN ('COMPLETED', 'FAILED');
