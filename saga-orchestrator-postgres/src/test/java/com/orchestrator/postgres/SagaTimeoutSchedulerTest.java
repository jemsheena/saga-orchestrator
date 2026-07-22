package com.orchestrator.postgres;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.definition.TimeoutPolicy;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.projection.SagaInstanceView;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaDefinitionRegistry;
import com.orchestrator.core.repository.SagaInstanceRepository;
import com.orchestrator.core.repository.support.InMemorySagaDefinitionRegistry;
import com.orchestrator.core.repository.support.InMemorySagaEventStore;
import com.orchestrator.core.repository.support.InMemorySagaInstanceViewStore;
import com.orchestrator.core.repository.support.InMemorySagaSnapshotStore;
import com.orchestrator.core.repository.support.ImmediateTransactionRunner;
import com.orchestrator.core.projection.SagaProjector;
import com.orchestrator.core.repository.support.DefaultSagaInstanceRepository;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.messaging.outbox.OutboxStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SagaTimeoutScheduler} using in-memory repository fakes.
 * Tests the scheduler's orchestration logic without requiring a real database.
 */
class SagaTimeoutSchedulerTest {

    private SagaTimeoutScheduler scheduler;
    private TestInMemoryViewStore viewStore;
    private TestSagaInstanceRepository repository;
    private RecordingOutboxStore outboxStore;
    private SagaDefinitionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new InMemorySagaDefinitionRegistry();
        outboxStore = new RecordingOutboxStore();
        viewStore = new TestInMemoryViewStore();

        repository = new TestSagaInstanceRepository(
                new InMemorySagaEventStore(),
                new InMemorySagaSnapshotStore(),
                viewStore,
                new SagaProjector(),
                new ImmediateTransactionRunner(),
                20, 1);

        scheduler = new SagaTimeoutScheduler(repository, registry, outboxStore, 100);
    }

    @Test
    void processBatch_withNoExpiredSagas_returnsZero() {
        int processed = scheduler.processBatch();
        assertEquals(0, processed);
        assertTrue(outboxStore.records.isEmpty());
    }

    @Test
    void processBatch_findsExpiredSagaByProjectionQuery() {
        // Define a saga with timeout
        SagaDefinition definition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .timeoutPolicy(TimeoutPolicy.ofSeconds(1))
                .build();
        registry.register(definition);

        // Start and advance a saga
        SagaInstance instance = SagaInstance.start(definition);
        repository.save(instance, EventMetadata.newCorrelation());

        instance = repository.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(definition, "ChargePayment");
        repository.save(instance, EventMetadata.newCorrelation());

        // Manually add it to the expired view list (simulating time passing in real system)
        SagaInstanceView view = repository.loadView(instance.sagaId()).orElseThrow();
        viewStore.addExpiredView(view);

        // Process the batch
        int processed = scheduler.processBatch();
        assertEquals(1, processed);

        // Verify saga is now compensating
        SagaInstance compensating = repository.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.COMPENSATING, compensating.state());

        // Verify compensation marker was published
        assertEquals(1, outboxStore.records.size());
    }

    @Test
    void processBatch_withFirstStepTimeout_failsDirectly() {
        // Define a saga with timeout
        SagaDefinition definition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .timeoutPolicy(TimeoutPolicy.ofSeconds(1))
                .build();
        registry.register(definition);

        // Start a saga (no steps completed)
        SagaInstance instance = SagaInstance.start(definition);
        repository.save(instance, EventMetadata.newCorrelation());

        // Add to expired view
        SagaInstanceView view = repository.loadView(instance.sagaId()).orElseThrow();
        viewStore.addExpiredView(view);

        // Process
        int processed = scheduler.processBatch();
        assertEquals(1, processed);

        // Verify saga is FAILED (not COMPENSATING)
        SagaInstance failed = repository.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.FAILED, failed.state());
        assertTrue(failed.isTerminal());

        // No compensation marker (compensation only for multi-step)
        assertTrue(outboxStore.records.isEmpty());
    }

    @Test
    void processBatch_respectsBatchSize() {
        // Define a saga
        SagaDefinition definition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .timeoutPolicy(TimeoutPolicy.ofSeconds(1))
                .build();
        registry.register(definition);

        // Create 3 expired sagas
        List<UUID> sagaIds = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            SagaInstance instance = SagaInstance.start(definition);
            repository.save(instance, EventMetadata.newCorrelation());

            instance = repository.findById(instance.sagaId()).orElseThrow();
            instance.completeCurrentStep(definition, "ChargePayment");
            repository.save(instance, EventMetadata.newCorrelation());

            SagaInstanceView view = repository.loadView(instance.sagaId()).orElseThrow();
            viewStore.addExpiredView(view);
            sagaIds.add(instance.sagaId());
        }

        // Create scheduler with batch size 2
        SagaTimeoutScheduler batchedScheduler = new SagaTimeoutScheduler(repository, registry, outboxStore, 2);

        // First batch processes 2
        int processed1 = batchedScheduler.processBatch();
        assertEquals(2, processed1);
        assertEquals(2, outboxStore.records.size());

        // Second batch processes 1
        int processed2 = batchedScheduler.processBatch();
        assertEquals(1, processed2);
        assertEquals(3, outboxStore.records.size());
    }

    @Test
    void processBatch_ignoresTerminalSagas() {
        // Define a saga
        SagaDefinition definition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .timeoutPolicy(TimeoutPolicy.ofSeconds(1))
                .build();
        registry.register(definition);

        // Start and complete a saga
        SagaInstance instance = SagaInstance.start(definition);
        repository.save(instance, EventMetadata.newCorrelation());

        instance = repository.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(definition, "ChargePayment");
        repository.save(instance, EventMetadata.newCorrelation());

        // Terminal sagas won't be in the expired list, so nothing should be returned
        int processed = scheduler.processBatch();
        assertEquals(0, processed);
        assertTrue(outboxStore.records.isEmpty());
    }

    /**
     * Test-only {@link SagaInstanceRepository} that extends the default in-memory
     * implementation with a view-loading method for tests to inspect the projection.
     */
    private static class TestSagaInstanceRepository extends DefaultSagaInstanceRepository {
        public TestSagaInstanceRepository(com.orchestrator.core.repository.SagaEventStore eventStore,
                                          com.orchestrator.core.repository.SagaSnapshotStore snapshotStore,
                                          TestInMemoryViewStore viewStore,
                                          SagaProjector projector,
                                          com.orchestrator.core.repository.TransactionRunner transactionRunner,
                                          long snapshotIntervalEvents,
                                          int snapshotSchemaVersion) {
            super(eventStore, snapshotStore, viewStore, projector, transactionRunner, snapshotIntervalEvents, snapshotSchemaVersion);
            this.viewStore = viewStore;
        }

        private final TestInMemoryViewStore viewStore;

        Optional<SagaInstanceView> loadView(UUID sagaId) {
            return viewStore.findById(sagaId);
        }
    }

    /**
     * Test-only in-memory view store that can track expired views for scheduler testing.
     */
    private static class TestInMemoryViewStore extends InMemorySagaInstanceViewStore {
        private final List<SagaInstanceView> expiredViews = new ArrayList<>();

        void addExpiredView(SagaInstanceView view) {
            expiredViews.add(view);
        }

        @Override
        public List<SagaInstanceView> findExpiredNonTerminal(int limit, Instant deadlineNow) {
            return expiredViews.stream().limit(limit).toList();
        }
    }

    /**
     * Test-only outbox store that records all published records.
     */
    private static class RecordingOutboxStore implements OutboxStore {
        private final List<OutboxRecord> records = new ArrayList<>();

        @Override
        public void append(OutboxRecord record) {
            records.add(record);
        }

        @Override
        public int claimAndDispatch(int limit, com.orchestrator.messaging.outbox.OutboxDispatcher dispatcher) {
            return 0;
        }
    }
}
