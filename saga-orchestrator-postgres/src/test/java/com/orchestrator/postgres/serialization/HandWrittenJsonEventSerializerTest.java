package com.orchestrator.postgres.serialization;

import com.orchestrator.core.definition.SagaDefinitionReference;
import com.orchestrator.core.event.CompensationStepCompleted;
import com.orchestrator.core.event.SagaCompensationStarted;
import com.orchestrator.core.event.SagaCompleted;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.event.SagaFailed;
import com.orchestrator.core.event.SagaStarted;
import com.orchestrator.core.event.StepCompleted;
import com.orchestrator.core.event.StepFailed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HandWrittenJsonEventSerializerTest {

    private final SagaEventSerializer serializer = new HandWrittenJsonEventSerializer();
    private static final UUID SAGA_ID = UUID.randomUUID();
    private static final Instant NOW = Instant.parse("2026-01-01T12:34:56.789Z");

    static Stream<SagaDomainEvent> allEventTypes() {
        return Stream.of(
                new SagaStarted(SAGA_ID, new SagaDefinitionReference("OrderFulfillment", 1), NOW),
                new StepCompleted(SAGA_ID, "ChargePayment", 0, NOW),
                new SagaCompleted(SAGA_ID, NOW),
                new StepFailed(SAGA_ID, "CreateShippingLabel", 2, "carrier API down", NOW),
                new StepFailed(SAGA_ID, "CreateShippingLabel", 2, null, NOW), // null reason
                new SagaCompensationStarted(SAGA_ID, 1, NOW),
                new CompensationStepCompleted(SAGA_ID, "ReserveInventory", 1, NOW),
                new SagaFailed(SAGA_ID, NOW)
        );
    }

    @ParameterizedTest
    @MethodSource("allEventTypes")
    void everyEventType_roundTripsExactlyThroughJson(SagaDomainEvent original) {
        String eventType = serializer.eventType(original);
        int schemaVersion = serializer.schemaVersion(original);
        String json = serializer.serialize(original);

        SagaDomainEvent rebuilt = serializer.deserialize(eventType, schemaVersion, json);

        assertEquals(original, rebuilt); // records: structural equality
    }

    @Test
    void specialCharacters_quotesBackslashNewline_surviveRoundTrip() {
        StepFailed original = new StepFailed(SAGA_ID, "Step", 0,
                "reason with \"quotes\" and \\backslash\\ and\nnewline", NOW);

        String json = serializer.serialize(original);
        StepFailed rebuilt = (StepFailed) serializer.deserialize("StepFailed", 1, json);

        assertEquals(original.reason(), rebuilt.reason());
    }

    @Test
    void unknownEventType_throwsIllegalArgumentException() {
        String json = "{\"sagaId\":\"" + SAGA_ID + "\",\"occurredAt\":\"" + NOW + "\"}";
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize("SomeMadeUpEvent", 1, json));
    }

    @Test
    void unsupportedSchemaVersion_throwsIllegalArgumentException() {
        String json = "{\"sagaId\":\"" + SAGA_ID + "\",\"occurredAt\":\"" + NOW + "\"}";
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize("SagaCompleted", 99, json));
    }

    @Test
    void sagaStarted_flattensAndReassemblesDefinitionReferenceCorrectly() {
        SagaStarted original = new SagaStarted(SAGA_ID, new SagaDefinitionReference("QuickTask", 7), NOW);

        String json = serializer.serialize(original);
        SagaStarted rebuilt = (SagaStarted) serializer.deserialize("SagaStarted", 1, json);

        assertEquals("QuickTask", rebuilt.definitionReference().sagaType());
        assertEquals(7, rebuilt.definitionReference().version());
    }
}
