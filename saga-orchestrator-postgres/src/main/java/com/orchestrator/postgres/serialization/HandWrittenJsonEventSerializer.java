package com.orchestrator.postgres.serialization;

import com.orchestrator.core.definition.SagaDefinitionReference;
import com.orchestrator.core.event.CompensationStepCompleted;
import com.orchestrator.core.event.SagaCompensationStarted;
import com.orchestrator.core.event.SagaCompleted;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.event.SagaFailed;
import com.orchestrator.core.event.SagaStarted;
import com.orchestrator.core.event.SagaTimedOut;
import com.orchestrator.core.event.StepCompleted;
import com.orchestrator.core.event.StepFailed;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * A hand-written, dependency-free JSON serializer for exactly the 8
 * {@link SagaDomainEvent} types.
 *
 * <p><b>Why this exists instead of using Jackson:</b> this implementation
 * was written in an environment with no access to Maven Central, and
 * Jackson is not on the classpath here. This is an environment constraint,
 * not a design preference — once Spring Boot enters the stack in a later
 * milestone, Jackson's {@code ObjectMapper} (with its Java-records module)
 * is the correct real replacement, and {@link SagaEventSerializer} exists
 * specifically so that swap touches exactly one class, never
 * {@code PostgresSagaEventStore} or anything that calls it.
 *
 * <p>All 7 event payloads are flat (no nesting) except {@link SagaStarted},
 * whose {@code SagaDefinitionReference} is flattened into two top-level
 * fields ({@code sagaType}, {@code definitionVersion}) here and reassembled
 * on deserialization — kept simple deliberately, since {@link SimpleJson} is
 * a flat-object-only parser by design (see its javadoc).
 */
public final class HandWrittenJsonEventSerializer implements SagaEventSerializer {

    private static final int CURRENT_SCHEMA_VERSION = 1;

    @Override
    public String eventType(SagaDomainEvent event) {
        return event.getClass().getSimpleName();
    }

    @Override
    public int schemaVersion(SagaDomainEvent event) {
        return CURRENT_SCHEMA_VERSION;
    }

    @Override
    public String serialize(SagaDomainEvent event) {
        SimpleJson.Writer w = new SimpleJson.Writer();
        w.field("sagaId", event.sagaId().toString());
        w.field("occurredAt", event.occurredAt().toString());

        switch (event) {
            case SagaStarted e -> w.field("sagaType", e.definitionReference().sagaType())
                    .field("definitionVersion", e.definitionReference().version());
            case StepCompleted e -> w.field("stepName", e.stepName()).field("stepIndex", e.stepIndex());
            case SagaCompleted e -> {
                // no additional fields
            }
            case StepFailed e -> w.field("stepName", e.stepName())
                    .field("stepIndex", e.stepIndex())
                    .field("reason", e.reason());
            case SagaCompensationStarted e -> w.field("compensationCursor", e.compensationCursor());
            case CompensationStepCompleted e -> w.field("stepName", e.stepName())
                    .field("compensationCursor", e.compensationCursor());
            case SagaFailed e -> {
                // no additional fields
            }
            case SagaTimedOut e -> {
                // no additional fields
            }
        }
        return w.build();
    }

    @Override
    public SagaDomainEvent deserialize(String eventType, int schemaVersion, String json) {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            // See SagaEventSerializer javadoc: a real multi-version implementation
            // would branch here to upcast an older shape. Nothing to upcast from yet.
            throw new IllegalArgumentException(
                    "Unsupported schema version " + schemaVersion + " for event type " + eventType
                            + " — no upcaster registered for this version.");
        }

        Map<String, String> fields = SimpleJson.parseFlatObject(json);
        UUID sagaId = UUID.fromString(fields.get("sagaId"));
        Instant occurredAt = Instant.parse(fields.get("occurredAt"));

        return switch (eventType) {
            case "SagaStarted" -> new SagaStarted(sagaId,
                    new SagaDefinitionReference(fields.get("sagaType"), Integer.parseInt(fields.get("definitionVersion"))),
                    occurredAt);
            case "StepCompleted" -> new StepCompleted(sagaId,
                    fields.get("stepName"), Integer.parseInt(fields.get("stepIndex")), occurredAt);
            case "SagaCompleted" -> new SagaCompleted(sagaId, occurredAt);
            case "StepFailed" -> new StepFailed(sagaId,
                    fields.get("stepName"), Integer.parseInt(fields.get("stepIndex")), fields.get("reason"), occurredAt);
            case "SagaCompensationStarted" -> new SagaCompensationStarted(sagaId,
                    Integer.parseInt(fields.get("compensationCursor")), occurredAt);
            case "CompensationStepCompleted" -> new CompensationStepCompleted(sagaId,
                    fields.get("stepName"), Integer.parseInt(fields.get("compensationCursor")), occurredAt);
            case "SagaFailed" -> new SagaFailed(sagaId, occurredAt);
            case "SagaTimedOut" -> new SagaTimedOut(sagaId, occurredAt);
            default -> throw new IllegalArgumentException(
                    "Unknown event type '" + eventType + "' encountered during deserialization — "
                            + "this indicates either data corruption or a new event type that was added to "
                            + "SagaDomainEvent without a corresponding case being added here.");
        };
    }
}
