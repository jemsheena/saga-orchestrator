package com.orchestrator.core.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted each time one previously-completed step finishes being undone
 * during compensation. {@code compensationCursor} is the index of the step
 * that was JUST undone (not the next one to undo) — matching the semantics
 * of {@code SagaInstance.compensationCursor()} at the moment this event fires.
 */
public record CompensationStepCompleted(
        UUID sagaId,
        String stepName,
        int compensationCursor,
        Instant occurredAt
) implements SagaDomainEvent {
}
