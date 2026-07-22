package com.orchestrator.core.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted by the timeout scheduler when a saga has exceeded its configured
 * timeout policy deadline while in a non-terminal state. This event behaves
 * similarly to {@link StepFailed}: if compensating is needed, it is followed
 * by a separate compensation-triggering event; if not (first step timeout),
 * it is followed directly by {@link SagaFailed}.
 *
 * <p>This is a domain event, not a purely technical artifact — a saga timing
 * out is a meaningful business fact worth auditing in the event stream, same
 * as any explicit step failure. The scheduler is merely the delivery mechanism
 * for this timeout transition, not the source of truth.
 *
 * @param sagaId     the saga that timed out
 * @param occurredAt when the timeout was detected by the scheduler
 */
public record SagaTimedOut(
        UUID sagaId,
        Instant occurredAt
) implements SagaDomainEvent {
}
