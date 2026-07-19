package com.orchestrator.postgres;

import com.orchestrator.core.definition.SagaDefinitionReference;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.event.SagaStarted;
import com.orchestrator.core.event.StepCompleted;
import com.orchestrator.core.exception.ConcurrencyConflictException;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.postgres.serialization.HandWrittenJsonEventSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link PostgresSagaEventStore} against a real,
 * ephemeral PostgreSQL instance. See {@link AbstractPostgresIntegrationTest}
 * — requires local Docker, not executed in the sandbox this was developed in.
 */
class PostgresSagaEventStoreIntegrationTest extends AbstractPostgresIntegrationTest {

    private PostgresSagaEventStore store;

    @BeforeEach
    void setUp() throws Exception {
        truncateAllTables();
        store = new PostgresSagaEventStore(dataSource, new HandWrittenJsonEventSerializer());
    }

    @Test
    void appendThenLoad_roundTripsEventsInOrder() {
        UUID sagaId = UUID.randomUUID();
        SagaStarted started = new SagaStarted(sagaId, new SagaDefinitionReference("OrderFulfillment", 1), Instant.now());
        StepCompleted stepCompleted = new StepCompleted(sagaId, "ChargePayment", 0, Instant.now());

        store.append(sagaId, 0, List.of(started), EventMetadata.newCorrelation());
        store.append(sagaId, 1, List.of(stepCompleted), EventMetadata.newCorrelation());

        List<SagaDomainEvent> loaded = store.loadEvents(sagaId);

        assertEquals(2, loaded.size());
        assertEquals(started, loaded.get(0));
        assertEquals(stepCompleted, loaded.get(1));
    }

    @Test
    void appendWithWrongExpectedVersion_throwsConcurrencyConflictException() {
        UUID sagaId = UUID.randomUUID();
        SagaStarted started = new SagaStarted(sagaId, new SagaDefinitionReference("OrderFulfillment", 1), Instant.now());
        store.append(sagaId, 0, List.of(started), EventMetadata.newCorrelation());

        StepCompleted stepCompleted = new StepCompleted(sagaId, "ChargePayment", 0, Instant.now());
        ConcurrencyConflictException ex = assertThrows(ConcurrencyConflictException.class,
                () -> store.append(sagaId, 0, List.of(stepCompleted), EventMetadata.newCorrelation())); // stale expectedVersion

        assertEquals(sagaId, ex.sagaId());
        assertEquals(0, ex.expectedVersion());
        assertEquals(1, ex.actualVersion());
    }

    @Test
    void loadEventsAfterSequence_returnsOnlyLaterEvents() {
        UUID sagaId = UUID.randomUUID();
        store.append(sagaId, 0, List.of(new SagaStarted(sagaId, new SagaDefinitionReference("OrderFulfillment", 1), Instant.now())),
                EventMetadata.newCorrelation());
        store.append(sagaId, 1, List.of(new StepCompleted(sagaId, "ChargePayment", 0, Instant.now())),
                EventMetadata.newCorrelation());
        store.append(sagaId, 2, List.of(new StepCompleted(sagaId, "ReserveInventory", 1, Instant.now())),
                EventMetadata.newCorrelation());

        List<SagaDomainEvent> afterFirst = store.loadEvents(sagaId, 1);

        assertEquals(2, afterFirst.size());
        assertTrue(afterFirst.get(0) instanceof StepCompleted);
    }

    @Test
    void appendingABatch_assignsConsecutiveSequenceNumbers_evenAcrossMultipleEventsInOneCall() {
        UUID sagaId = UUID.randomUUID();
        store.append(sagaId, 0, List.of(new SagaStarted(sagaId, new SagaDefinitionReference("OrderFulfillment", 1), Instant.now())),
                EventMetadata.newCorrelation());

        // Simulates failCurrentStep's two-event batch (StepFailed + SagaCompensationStarted).
        StepCompleted a = new StepCompleted(sagaId, "ChargePayment", 0, Instant.now());
        StepCompleted b = new StepCompleted(sagaId, "ReserveInventory", 1, Instant.now());
        store.append(sagaId, 1, List.of(a, b), EventMetadata.newCorrelation());

        List<SagaDomainEvent> loaded = store.loadEvents(sagaId);
        assertEquals(3, loaded.size());
    }
}
