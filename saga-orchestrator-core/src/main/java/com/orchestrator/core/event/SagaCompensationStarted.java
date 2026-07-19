package com.orchestrator.core.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted when a step failure triggers compensation of previously-completed
 * steps (i.e. at least one prior step had succeeded). {@code compensationCursor}
 * records which step index compensation begins at, walking backward — this is
 * the value {@link com.orchestrator.core.engine.SagaInstance#compensationCursor()}
 * held immediately after this event. Not emitted when a saga fails on its very
 * first step, since there is nothing to compensate in that case (see
 * {@link SagaFailed} emitted directly instead).
 */
public record SagaCompensationStarted(
        UUID sagaId,
        int compensationCursor,
        Instant occurredAt
) implements SagaDomainEvent {
}
