package com.orchestrator.core.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted when one specific step finishes successfully. Emitted for every
 * successful step, including the final one — {@link SagaCompleted} is a
 * separate, additional event fired only for the final step, since "this step
 * succeeded" and "the whole saga succeeded" are distinct business facts a
 * projection may care about independently (e.g. per-step success-rate
 * dashboards vs. overall saga completion-rate dashboards).
 */
public record StepCompleted(
        UUID sagaId,
        String stepName,
        int stepIndex,
        Instant occurredAt
) implements SagaDomainEvent {
}
