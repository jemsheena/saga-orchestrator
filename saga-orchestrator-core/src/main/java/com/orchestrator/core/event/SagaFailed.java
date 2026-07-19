package com.orchestrator.core.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted once, terminally, when a saga ends in failure — either because its
 * very first step failed with nothing to compensate, or because compensation
 * of all previously-completed steps has finished. This event alone does not
 * distinguish those two cases; {@link StepFailed} and any
 * {@link CompensationStepCompleted} events earlier in the same saga's event
 * stream provide that detail. Keeping SagaFailed itself minimal mirrors
 * {@link SagaCompleted} — both are pure "the saga reached a terminal state"
 * markers, with the interesting detail living in the events that led there.
 */
public record SagaFailed(
        UUID sagaId,
        Instant occurredAt
) implements SagaDomainEvent {
}
