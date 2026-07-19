package com.orchestrator.core.event;

import java.time.Instant;
import java.util.UUID;

/**
 * The closed, exhaustive set of facts a {@code SagaInstance} can record about
 * its own execution. This is deliberately {@code sealed} — the same reasoning
 * as {@code SagaState} being a closed enum: the value of an event-sourced
 * system's event log depends entirely on the event vocabulary being fixed
 * and exhaustively known. An open/extensible event interface would let
 * arbitrary code introduce event types the event store, projections, and
 * replay logic don't know how to handle — silently, at runtime, instead of
 * being caught by the compiler's exhaustiveness checking on a {@code switch}.
 *
 * <p>Every event carries {@code sagaId} (which instance this happened to)
 * and {@code occurredAt} (when) as the two facts every consumer of the event
 * stream — the event store, a projection, a dashboard — needs regardless of
 * the specific event type. Everything else is event-specific.
 */
public sealed interface SagaDomainEvent
        permits SagaStarted, StepCompleted, SagaCompleted, StepFailed,
        SagaCompensationStarted, CompensationStepCompleted, SagaFailed {

    UUID sagaId();

    Instant occurredAt();
}
