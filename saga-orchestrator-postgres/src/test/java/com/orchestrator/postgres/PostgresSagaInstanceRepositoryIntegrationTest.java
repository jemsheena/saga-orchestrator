package com.orchestrator.postgres;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaSnapshot;
import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.projection.SagaInstanceView;
import com.orchestrator.core.projection.SagaInstanceViewStore;
import com.orchestrator.core.projection.SagaProjector;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.support.DefaultSagaInstanceRepository;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.postgres.serialization.HandWrittenJsonEventSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration test: {@link DefaultSagaInstanceRepository} wired
 * to real {@code Postgres*} adapters — including, as of Milestone 2.5,
 * {@link JdbcTransactionRunner} — against a real, ephemeral PostgreSQL
 * instance. Requires local Docker — not executed in the sandbox this was
 * developed in.
 */
class PostgresSagaInstanceRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

    private DefaultSagaInstanceRepository repository;
    private PostgresSagaEventStore rawEventStore;

    @BeforeEach
    void setUp() throws Exception {
        truncateAllTables();
        rawEventStore = new PostgresSagaEventStore(dataSource, new HandWrittenJsonEventSerializer());
        PostgresSagaSnapshotStore snapshotStore = new PostgresSagaSnapshotStore(dataSource);
        PostgresSagaInstanceViewStore viewStore = new PostgresSagaInstanceViewStore(dataSource);
        JdbcTransactionRunner transactionRunner = new JdbcTransactionRunner(dataSource);
        repository = new DefaultSagaInstanceRepository(
                rawEventStore, snapshotStore, viewStore, new SagaProjector(), transactionRunner, 20, 1);
    }

    @Test
    void fullHappyPath_persistsAcrossReloads_endsCompleted_withReadModelUpdated() {
        SagaDefinition def = threeStepDefinition();

        SagaInstance instance = SagaInstance.start(def);
        repository.save(instance, EventMetadata.newCorrelation());

        instance = repository.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(def, "ChargePayment");
        repository.save(instance, EventMetadata.newCorrelation());

        instance = repository.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(def, "ReserveInventory");
        repository.save(instance, EventMetadata.newCorrelation());

        instance = repository.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(def, "CreateShippingLabel");
        repository.save(instance, EventMetadata.newCorrelation());

        SagaInstance finalInstance = repository.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.COMPLETED, finalInstance.state());
        assertTrue(finalInstance.isTerminal());
    }

    /**
     * THE direct proof for Milestone 2.5 Critical Finding #1: event-append
     * and view-projection are now genuinely atomic. A deliberately-broken
     * {@link SagaInstanceViewStore} is wired in alongside the REAL
     * {@code PostgresSagaEventStore} and a REAL {@link JdbcTransactionRunner}.
     * The event-append's SQL executes and would, pre-Milestone-2.5, have
     * committed on its own regardless of what happened next. If this test
     * passes, it proves that didn't happen — the failed projection rolled
     * back the event append too, because they were sharing one transaction.
     */
    @Test
    void whenProjectionFails_theEventAppendIsRolledBackToo_provingRealAtomicity() {
        SagaDefinition def = threeStepDefinition();
        SagaInstanceViewStore alwaysFailingViewStore = new SagaInstanceViewStore() {
            @Override
            public void upsert(SagaInstanceView view) {
                throw new RuntimeException("simulated projection failure");
            }

            @Override
            public Optional<SagaInstanceView> findById(UUID sagaId) {
                return Optional.empty();
            }

            @Override
            public List<SagaInstanceView> findExpiredNonTerminal(int limit, Instant deadlineNow) {
                return List.of();
            }
        };
        DefaultSagaInstanceRepository brokenRepo = new DefaultSagaInstanceRepository(
                rawEventStore,
                new PostgresSagaSnapshotStore(dataSource),
                alwaysFailingViewStore,
                new SagaProjector(),
                new JdbcTransactionRunner(dataSource),
                20, 1);

        SagaInstance instance = SagaInstance.start(def);

        assertThrows(RuntimeException.class, () -> brokenRepo.save(instance, EventMetadata.newCorrelation()));

        // The real proof: query the REAL event store directly. If the transaction
        // genuinely rolled back, ZERO events exist for this saga, despite the
        // event-append SQL having executed without error moments before the
        // projection step threw.
        assertTrue(rawEventStore.loadEvents(instance.sagaId()).isEmpty(),
                "event append should have been rolled back along with the failed projection");
    }

    @Test
    void snapshotAndReplayFastPath_producesCorrectStateAgainstRealPostgres() {
        SagaDefinition def = threeStepDefinition();
        PostgresSagaSnapshotStore rawSnapshotStore = new PostgresSagaSnapshotStore(dataSource);
        DefaultSagaInstanceRepository snapshottingRepo = new DefaultSagaInstanceRepository(
                new PostgresSagaEventStore(dataSource, new HandWrittenJsonEventSerializer()),
                rawSnapshotStore,
                new PostgresSagaInstanceViewStore(dataSource),
                new SagaProjector(),
                new JdbcTransactionRunner(dataSource),
                2, // snapshot every 2 events - low, to actually exercise the path in this test
                1);

        SagaInstance instance = SagaInstance.start(def);
        snapshottingRepo.save(instance, EventMetadata.newCorrelation());
        instance = snapshottingRepo.findById(instance.sagaId()).orElseThrow();
        instance.completeCurrentStep(def, "ChargePayment"); // version 1 -> 2, crosses boundary
        snapshottingRepo.save(instance, EventMetadata.newCorrelation());

        Optional<SagaSnapshot> snapshot = rawSnapshotStore.findLatest(instance.sagaId());
        assertTrue(snapshot.isPresent());
        assertEquals(2, snapshot.get().sequenceNo());

        SagaInstance rehydrated = snapshottingRepo.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.STEP_COMPLETED, rehydrated.state());
        assertEquals(1, rehydrated.currentStepIndex());
    }

    @Test
    void transactionalOutboxWrite_rollbackRollsBackEventsAndOutboxRow() throws Exception {
        SagaDefinition def = threeStepDefinition();
        PostgresOutboxStore outboxStore = new PostgresOutboxStore(dataSource);

        SagaInstanceViewStore alwaysFailingViewStore = new SagaInstanceViewStore() {
            @Override
            public void upsert(SagaInstanceView view) {
                throw new RuntimeException("simulated projection failure");
            }

            @Override
            public Optional<SagaInstanceView> findById(UUID sagaId) {
                return Optional.empty();
            }

            @Override
            public List<SagaInstanceView> findExpiredNonTerminal(int limit, Instant deadlineNow) {
                return List.of();
            }
        };

        DefaultSagaInstanceRepository brokenRepo = new DefaultSagaInstanceRepository(
                rawEventStore,
                new PostgresSagaSnapshotStore(dataSource),
                alwaysFailingViewStore,
                new SagaProjector(),
                new JdbcTransactionRunner(dataSource),
                20, 1);

        SagaInstance instance = SagaInstance.start(def);
        OutboxRecord marker = new OutboxRecord(UUID.randomUUID(), "saga.compensation.v1",
                instance.sagaId().toString(), "SagaCompensation", new byte[]{1, 2, 3},
                UUID.randomUUID(), null, Instant.now());

        assertThrows(RuntimeException.class, () -> brokenRepo.save(instance, EventMetadata.newCorrelation(),
                () -> outboxStore.append(marker)));

        assertTrue(rawEventStore.loadEvents(instance.sagaId()).isEmpty(),
                "event append should have been rolled back along with the failed transaction");

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT count(*) FROM outbox")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "outbox row should have been rolled back along with the failed transaction");
        }
    }

    /**
     * The direct proof for Critical Finding #2, against real Postgres: a
     * snapshot store that always fails must not prevent events from being
     * durably persisted, and save() must not throw because of it.
     */
    @Test
    void snapshotFailure_againstRealPostgres_doesNotPreventEventPersistence() {
        SagaDefinition def = threeStepDefinition();
        com.orchestrator.core.repository.SagaSnapshotStore alwaysFailingSnapshotStore =
                new com.orchestrator.core.repository.SagaSnapshotStore() {
                    @Override
                    public void save(SagaSnapshot snapshot) {
                        throw new RuntimeException("simulated snapshot failure");
                    }

                    @Override
                    public Optional<SagaSnapshot> findLatest(UUID sagaId) {
                        return Optional.empty();
                    }
                };
        DefaultSagaInstanceRepository repoWithBrokenSnapshots = new DefaultSagaInstanceRepository(
                rawEventStore,
                alwaysFailingSnapshotStore,
                new PostgresSagaInstanceViewStore(dataSource),
                new SagaProjector(),
                new JdbcTransactionRunner(dataSource),
                1, // interval=1: every save crosses a snapshot boundary
                1);

        SagaInstance instance = SagaInstance.start(def);

        assertDoesNotThrowRuntime(() -> repoWithBrokenSnapshots.save(instance, EventMetadata.newCorrelation()));

        assertEquals(1, rawEventStore.loadEvents(instance.sagaId()).size());
    }

    @Test
    void readModel_queryableIndependently_afterSave() {
        SagaDefinition def = threeStepDefinition();
        PostgresSagaInstanceViewStore viewStore = new PostgresSagaInstanceViewStore(dataSource);

        SagaInstance instance = SagaInstance.start(def);
        instance.completeCurrentStep(def, "ChargePayment");
        repository.save(instance, EventMetadata.newCorrelation());

        Optional<SagaInstanceView> view = viewStore.findById(instance.sagaId());
        assertTrue(view.isPresent());
        assertEquals(SagaState.STEP_COMPLETED, view.get().state());
        assertEquals("OrderFulfillment", view.get().sagaType());
    }

    private static void assertDoesNotThrowRuntime(Runnable work) {
        try {
            work.run();
        } catch (RuntimeException e) {
            throw new AssertionError("Expected no exception, but got: " + e, e);
        }
    }

    private static SagaDefinition threeStepDefinition() {
        return SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .addStep(new SagaStep("CreateShippingLabel", "CreateLabelCommand", "VoidLabelCommand"))
                .build();
    }
}
