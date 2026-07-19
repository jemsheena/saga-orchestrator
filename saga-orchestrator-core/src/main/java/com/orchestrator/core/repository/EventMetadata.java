package com.orchestrator.core.repository;

import java.util.Objects;
import java.util.UUID;

/**
 * Tracing metadata attached to a batch of events at the moment they're
 * appended — not part of {@code SagaDomainEvent} itself, because correlation
 * and causation are storage/observability concerns the aggregate has no
 * business deciding, only the application layer calling it does.
 *
 * <p>One {@code EventMetadata} applies to an entire batch from a single
 * business-method call (e.g. both {@code StepFailed} and
 * {@code SagaCompensationStarted} from one {@code failCurrentStep} call
 * share the same correlation/causation), because they are, by construction,
 * one causal unit — they happened as direct consequences of the same
 * decision at the same moment.
 *
 * @param correlationId ties this operation to the broader business activity
 *                       it's part of (e.g. the original HTTP request or Kafka
 *                       message that ultimately triggered it). Required —
 *                       every operation belongs to at least itself as a
 *                       correlation root if nothing else caused it.
 * @param causationId    the ID of the specific event/command that directly
 *                        caused this batch, distinct from mere chronological
 *                        ordering. Nullable — the very first event in a
 *                        causal chain (e.g. a saga's initiating
 *                        {@code SagaStarted}) has no prior domain event as
 *                        its cause, only an external trigger.
 */
public record EventMetadata(UUID correlationId, UUID causationId) {

    public EventMetadata {
        Objects.requireNonNull(correlationId, "correlationId must not be null");
    }

    /**
     * Convenience factory for the common case: a brand-new correlation root
     * with no prior cause (e.g. a saga's very first event).
     */
    public static EventMetadata newCorrelation() {
        return new EventMetadata(UUID.randomUUID(), null);
    }
}
