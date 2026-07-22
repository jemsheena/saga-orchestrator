package com.orchestrator.core.repository.support;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.projection.SagaInstanceView;
import com.orchestrator.core.definition.TimeoutPolicy;
import com.orchestrator.core.projection.SagaProjector;
import com.orchestrator.core.repository.EventMetadata;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link DefaultSagaInstanceRepository} entirely against in-memory
 * fakes — no database of any kind. This is possible, and meaningful, only
 * because the class under test has zero Postgres/Spring/JPA dependencies;
 * see its class javadoc.
 */
class DefaultSagaInstanceRepositoryTest {

    @Test
    void findById_onUnknownSaga_returnsEmpty() {
        DefaultSagaInstanceRepository repo = newRepo(20);
        assertTrue(repo.findById(UUID.randomUUID()).isEmpty());
    }

    @Test
    void saveThenFindById_roundTripsMidSagaState_exactly() {
        DefaultSagaInstanceRepository repo = newRepo(20);
        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        instance.completeCurrentStep(def, "ChargePayment");
        repo.save(instance, EventMetadata.newCorrelation());

        SagaInstance reloaded = repo.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.STEP_COMPLETED, reloaded.state());
        assertEquals(1, reloaded.currentStepIndex());
        assertEquals(instance.version(), reloaded.version());
    }

    @Test
    void save_isNoOp_whenInstanceHasNoPendingEvents() {
        DefaultSagaInstanceRepository repo = newRepo(20);
        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        repo.save(instance, EventMetadata.newCorrelation());
        long versionAfterFirstSave = repo.findById(instance.sagaId()).orElseThrow().version();

        repo.save(instance, EventMetadata.newCorrelation()); // instance has zero pending events now
        long versionAfterSecondSave = repo.findById(instance.sagaId()).orElseThrow().version();

        assertEquals(versionAfterFirstSave, versionAfterSecondSave);
    }

    @Test
    void fullSaveReloadCycle_acrossHappyPath_reachesCompleted() {
        DefaultSagaInstanceRepository repo = newRepo(20);
        SagaDefinition def = threeStepDefinition();

        SagaInstance instance = SagaInstance.start(def);
        repo.save(instance, EventMetadata.newCorrelation());

        instance = repo.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(def, "ChargePayment");
        repo.save(instance, EventMetadata.newCorrelation());

        instance = repo.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(def, "ReserveInventory");
        repo.save(instance, EventMetadata.newCorrelation());

        instance = repo.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(def, "CreateShippingLabel");
        repo.save(instance, EventMetadata.newCorrelation());

        SagaInstance finalState = repo.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.COMPLETED, finalState.state());
        assertTrue(finalState.isTerminal());
    }

    @Test
    void readModel_reflectsSaves_viewRowMatchesFinalState() {
        InMemorySagaEventStore eventStore = new InMemorySagaEventStore();
        InMemorySagaSnapshotStore snapshotStore = new InMemorySagaSnapshotStore();
        InMemorySagaInstanceViewStore viewStore = new InMemorySagaInstanceViewStore();
        DefaultSagaInstanceRepository repo = new DefaultSagaInstanceRepository(
                eventStore, snapshotStore, viewStore, new SagaProjector(), new ImmediateTransactionRunner(),
                new InMemorySagaDefinitionRegistry(), 20, 1);

        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        instance.completeCurrentStep(def, "ChargePayment");
        instance.completeCurrentStep(def, "ReserveInventory");
        instance.completeCurrentStep(def, "CreateShippingLabel");
        repo.save(instance, EventMetadata.newCorrelation());

        Optional<SagaInstanceView> view = viewStore.findById(instance.sagaId());
        assertTrue(view.isPresent());
        assertEquals(SagaState.COMPLETED, view.get().state());
    }

    @Test
    void snapshotBoundaryCrossing_batchOfEventsJumpingOverBoundary_stillTriggersSnapshot() {
        InMemorySagaEventStore eventStore = new InMemorySagaEventStore();
        InMemorySagaSnapshotStore snapshotStore = new InMemorySagaSnapshotStore();
        InMemorySagaInstanceViewStore viewStore = new InMemorySagaInstanceViewStore();
        DefaultSagaInstanceRepository repo = new DefaultSagaInstanceRepository(
                eventStore, snapshotStore, viewStore, new SagaProjector(), new ImmediateTransactionRunner(),
                new InMemorySagaDefinitionRegistry(), 3, 1);

        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        repo.save(instance, EventMetadata.newCorrelation()); // version 1, no snapshot yet

        instance = repo.findById(instance.sagaId()).orElseThrow();
        // failCurrentStep on step 0 with nothing completed emits 2 events in ONE
        // save - version jumps 1 -> 3, exactly hitting (not landing exactly on, in
        // general - but here exactly on) the boundary in a single batch.
        instance.failCurrentStep(def, "ChargePayment", "boom");
        repo.save(instance, EventMetadata.newCorrelation());

        Optional<com.orchestrator.core.engine.SagaSnapshot> snapshot = snapshotStore.findLatest(instance.sagaId());
        assertTrue(snapshot.isPresent());
        assertEquals(3, snapshot.get().sequenceNo());
    }

    @Test
    void findById_viaSnapshotFastPath_producesIdenticalResultToFullReplay() {
        InMemorySagaEventStore eventStore = new InMemorySagaEventStore();
        InMemorySagaSnapshotStore snapshotStore = new InMemorySagaSnapshotStore();
        InMemorySagaInstanceViewStore viewStore = new InMemorySagaInstanceViewStore();
        DefaultSagaInstanceRepository snapshottingRepo = new DefaultSagaInstanceRepository(
                eventStore, snapshotStore, viewStore, new SagaProjector(), new ImmediateTransactionRunner(),
                new InMemorySagaDefinitionRegistry(), 2, 1);
        DefaultSagaInstanceRepository noSnapshotRepo = new DefaultSagaInstanceRepository(
                eventStore, new InMemorySagaSnapshotStore(), viewStore, new SagaProjector(), new ImmediateTransactionRunner(),
                new InMemorySagaDefinitionRegistry(), 999_999, 1);

        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        snapshottingRepo.save(instance, EventMetadata.newCorrelation());
        instance = snapshottingRepo.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(def, "ChargePayment");
        snapshottingRepo.save(instance, EventMetadata.newCorrelation()); // version=2, hits snapshot boundary

        assertTrue(snapshotStore.findLatest(instance.sagaId()).isPresent());

        SagaInstance viaSnapshot = snapshottingRepo.findById(instance.sagaId()).orElseThrow();
        SagaInstance viaFullReplay = noSnapshotRepo.findById(instance.sagaId()).orElseThrow();

        assertEquals(viaFullReplay.state(), viaSnapshot.state());
        assertEquals(viaFullReplay.currentStepIndex(), viaSnapshot.currentStepIndex());
        assertEquals(viaFullReplay.version(), viaSnapshot.version());
    }

    /**
     * The direct regression test for Milestone 2.5 Critical Finding #2:
     * a snapshot-store failure must NEVER propagate out of save() and must
     * NEVER prevent the already-successfully-appended events from being
     * durably persisted and visible on the very next findById().
     */
    @Test
    void snapshotStoreFailure_doesNotPropagate_andEventsRemainDurablyPersisted() {
        InMemorySagaEventStore eventStore = new InMemorySagaEventStore();
        InMemorySagaInstanceViewStore viewStore = new InMemorySagaInstanceViewStore();
        AlwaysThrowingSnapshotStore throwingSnapshotStore = new AlwaysThrowingSnapshotStore();
        DefaultSagaInstanceRepository repo = new DefaultSagaInstanceRepository(
                eventStore, throwingSnapshotStore, viewStore, new SagaProjector(),
                new ImmediateTransactionRunner(), new InMemorySagaDefinitionRegistry(), 1, 1); // interval=1: EVERY save crosses a snapshot boundary

        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);

        // Must NOT throw, despite the snapshot store always throwing internally.
        assertDoesNotThrow(() -> repo.save(instance, EventMetadata.newCorrelation()));

        // The event that was appended must be durably visible regardless.
        SagaInstance reloaded = repo.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.STARTED, reloaded.state());
        assertEquals(1, reloaded.version());
    }

    /** Test-only fake that always fails, to prove save() isolates snapshot failures. */
    private static final class AlwaysThrowingSnapshotStore implements com.orchestrator.core.repository.SagaSnapshotStore {
        @Override
        public void save(com.orchestrator.core.engine.SagaSnapshot snapshot) {
            throw new RuntimeException("simulated snapshot store failure");
        }

        @Override
        public java.util.Optional<com.orchestrator.core.engine.SagaSnapshot> findLatest(java.util.UUID sagaId) {
            return java.util.Optional.empty();
        }
    }

    private static DefaultSagaInstanceRepository newRepo(int snapshotInterval) {
        return new DefaultSagaInstanceRepository(
                new InMemorySagaEventStore(),
                new InMemorySagaSnapshotStore(),
                new InMemorySagaInstanceViewStore(),
                new SagaProjector(),
                new ImmediateTransactionRunner(),
                new InMemorySagaDefinitionRegistry(),
                snapshotInterval,
                1);
    }

    private static SagaDefinition threeStepDefinition() {
        return SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .addStep(new SagaStep("CreateShippingLabel", "CreateLabelCommand", "VoidLabelCommand"))
                .build();
    }
}
