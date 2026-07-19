package com.orchestrator.core.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted when a step fails. {@code reason} is free-form and supplied by the
 * caller (ultimately, by whatever participant reported the failure) — the
 * aggregate does not interpret or validate its contents, only carries it.
 * This is what lets a support engineer later answer "why did this saga fail"
 * from the event log itself, rather than needing to correlate against
 * external service logs.
 */
public record StepFailed(
        UUID sagaId,
        String stepName,
        int stepIndex,
        String reason,
        Instant occurredAt
) implements SagaDomainEvent {
}
