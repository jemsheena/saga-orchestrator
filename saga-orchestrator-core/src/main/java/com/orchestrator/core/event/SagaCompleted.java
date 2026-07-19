package com.orchestrator.core.event;

import java.time.Instant;
import java.util.UUID;

/** Emitted once, when the final step of a saga completes successfully. Terminal. */
public record SagaCompleted(
        UUID sagaId,
        Instant occurredAt
) implements SagaDomainEvent {
}
