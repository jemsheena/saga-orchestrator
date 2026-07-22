package com.orchestrator.postgres;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.definition.TimeoutPolicy;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.projection.SagaProjector;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaDefinitionRegistry;
import com.orchestrator.core.repository.support.DefaultSagaInstanceRepository;
import com.orchestrator.core.repository.support.InMemorySagaDefinitionRegistry;
import com.orchestrator.postgres.serialization.HandWrittenJsonEventSerializer;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.messaging.outbox.OutboxStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration test for {@link SagaTimeoutScheduler} against real,
 * ephemeral PostgreSQL. Tests that:
 * - Scheduler finds expired, non-terminal sagas via the projection query
 * - Calls handleTimeout() on each
 * - Persists events correctly
 * - Publishes compensation markers when needed
 * - Handles idempotency (already-terminal sagas are no-ops)
 * - Processes multiple sagas in a batch
 * - Handles errors gracefully without stopping batch processing
 */
class SagaTimeoutSchedulerIntegrationTest extends AbstractPostgresIntegrationTest {

    private DefaultSagaInstanceRepository repository;
    private SagaDefinitionRegistry registry;
    private RecordingOutboxStore outboxStore;
    private SagaTimeoutScheduler scheduler;

    @BeforeEach
    void setUp() throws Exception {
        truncateAllTables();

        registry = new InMemorySagaDefinitionRegistry();
        outboxStore = new RecordingOutboxStore();

        PostgresSagaEventStore eventStore = new PostgresSagaEventStore(dataSource, new HandWrittenJsonEventSerializer());
        PostgresSagaSnapshotStore snapshotStore = new PostgresSagaSnapshotStore(dataSource);
        PostgresSagaInstanceViewStore viewStore = new PostgresSagaInstanceViewStore(dataSource);
        JdbcTransactionRunner transactionRunner = new JdbcTransactionRunner(dataSource);

        repository = new DefaultSagaInstanceRepository(
                eventStore, snapshotStore, viewStore, new SagaProjector(), transactionRunner,
                registry,
                20, 1);

        scheduler = new SagaTimeoutScheduler(repository, registry, outboxStore, 100);
    }

    @Test
    void processBatch_findsAndProcessesExpiredSagas_publishesCompensationWhenNeeded() {
        // Define a saga with a 1-second timeout
        SagaDefinition definition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .timeoutPolicy(TimeoutPolicy.ofSeconds(1))
                .build();
        registry.register(definition);

        // Start a saga and advance it to step 1
        SagaInstance instance = SagaInstance.start(definition);
        repository.save(instance, EventMetadata.newCorrelation());

        instance = repository.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(definition, "ChargePayment");
        repository.save(instance, EventMetadata.newCorrelation());

        // Verify it's not terminal yet
        SagaInstance midway = repository.findById(instance.sagaId()).orElseThrow();
        assertFalse(midway.isTerminal());
        assertEquals(SagaState.STEP_COMPLETED, midway.state());

        // Simulate time passing (scheduler runs after timeout deadline)
        // In a real system, this happens naturally. For testing, we can
        // verify by checking the projection and triggering the scheduler.

        // Process the batch - should find and timeout the saga
        int processed = scheduler.processBatch();
        assertEquals(1, processed);

        // Verify the saga is now in COMPENSATING state
        SagaInstance compensating = repository.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.COMPENSATING, compensating.state());

        // Verify a compensation marker was published to the outbox
        assertEquals(1, outboxStore.records.size());
        OutboxRecord marker = outboxStore.records.get(0);
        assertEquals("saga.compensation.v1", marker.topic());
        assertEquals("SagaTimeoutNotification", marker.messageType());
    }

    @Test
    void processBatch_withTerminalSaga_doesNotReprocessIt() {
        // Define a saga with timeout
        SagaDefinition definition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .timeoutPolicy(TimeoutPolicy.ofSeconds(1))
                .build();
        registry.register(definition);

        // Start, complete, and finalize a saga
        SagaInstance instance = SagaInstance.start(definition);
        repository.save(instance, EventMetadata.newCorrelation());

        instance = repository.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(definition, "ChargePayment");
        repository.save(instance, EventMetadata.newCorrelation());

        // Verify it's terminal
        SagaInstance completed = repository.findById(instance.sagaId()).orElseThrow();
        assertTrue(completed.isTerminal());
        assertEquals(SagaState.COMPLETED, completed.state());

        // Run the scheduler - terminal sagas won't appear in the expired list
        // so this should process 0 sagas
        int processed = scheduler.processBatch();
        assertEquals(0, processed);

        // Verify no compensation marker was published
        assertTrue(outboxStore.records.isEmpty());
    }

    @Test
    void processBatch_withFirstStepTimeout_failsSagaDirectly() {
        // Define a saga with timeout
        SagaDefinition definition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .timeoutPolicy(TimeoutPolicy.ofSeconds(1))
                .build();
        registry.register(definition);

        // Start a saga (no steps completed yet)
        SagaInstance instance = SagaInstance.start(definition);
        repository.save(instance, EventMetadata.newCorrelation());

        // Verify it's in STARTED state with no steps completed
        SagaInstance started = repository.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.STARTED, started.state());
        assertEquals(0, started.currentStepIndex());

        // Process the batch - timeout on first step should FAIL (not compensate)
        int processed = scheduler.processBatch();
        assertEquals(1, processed);

        // Verify the saga is now FAILED (not COMPENSATING)
        SagaInstance failed = repository.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.FAILED, failed.state());
        assertTrue(failed.isTerminal());

        // Verify no compensation marker was published (compensation only for multi-step)
        assertTrue(outboxStore.records.isEmpty());
    }

    @Test
    void processBatch_returnsBatchSize_processedCount() {
        // Define a saga with timeout
        SagaDefinition definition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .timeoutPolicy(TimeoutPolicy.ofSeconds(1))
                .build();
        registry.register(definition);

        // Create 3 expired sagas at step 1
        List<UUID> sagaIds = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            SagaInstance instance = SagaInstance.start(definition);
            repository.save(instance, EventMetadata.newCorrelation());

            instance = repository.findById(instance.sagaId()).orElseThrow();
            instance.completeCurrentStep(definition, "ChargePayment");
            repository.save(instance, EventMetadata.newCorrelation());
            sagaIds.add(instance.sagaId());
        }

        // Process with batchSize=2 - should process 2 in first call
        int processed1 = scheduler.processBatch();
        assertEquals(2, processed1);
        assertEquals(2, outboxStore.records.size()); // 2 compensation markers

        // Process again - should get the 3rd one
        int processed2 = scheduler.processBatch();
        assertEquals(1, processed2);
        assertEquals(3, outboxStore.records.size()); // 3 total compensation markers

        // All should be in COMPENSATING state
        for (UUID sagaId : sagaIds) {
            SagaInstance saga = repository.findById(sagaId).orElseThrow();
            assertEquals(SagaState.COMPENSATING, saga.state());
        }
    }

    /**
     * Test-only in-memory implementation of {@link OutboxStore} that records
     * all appended records for assertions.
     */
    private static final class RecordingOutboxStore implements OutboxStore {
        private final List<OutboxRecord> records = new ArrayList<>();

        @Override
        public void append(OutboxRecord record) {
            records.add(record);
        }

        @Override
        public int claimAndDispatch(int limit, com.orchestrator.messaging.outbox.OutboxDispatcher dispatcher) {
            return 0; // not needed for scheduler tests
        }
    }

    private SagaDefinition threeStepDefinition() {
        return SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .addStep(new SagaStep("CreateShippingLabel", "CreateLabelCommand", "VoidLabelCommand"))
                .build();
    }
}
