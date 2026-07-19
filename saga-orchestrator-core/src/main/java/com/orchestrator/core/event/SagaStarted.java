package com.orchestrator.core.event;

import com.orchestrator.core.definition.SagaDefinitionReference;

import java.time.Instant;
import java.util.UUID;

/** Emitted exactly once, when a {@code SagaInstance} is first created. */
public record SagaStarted(
        UUID sagaId,
        SagaDefinitionReference definitionReference,
        Instant occurredAt
) implements SagaDomainEvent {
}
