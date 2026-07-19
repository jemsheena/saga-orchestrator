package com.orchestrator.core.repository.support;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.exception.ConcurrencyConflictException;
import com.orchestrator.core.exception.StepMismatchException;
import com.orchestrator.core.repository.EventMetadata;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the optimistic-concurrency contract {@code SagaEventStore.append}
 * makes (see Milestone 2 architecture review Section 7) using
 * {@link InMemorySagaEventStore} — a faithful fake of that exact contract,
 * not a lesser substitute for {@code PostgresSagaEventStore}. See that
 * class's javadoc, and this milestone's Step 6 write-up, for why testing
 * the contract this way is valid.
 */
class ConcurrencyConflictTest {

    @Test
    void twoConcurrentWriters_exactlyOneSucceeds_theOtherGetsConcurrencyConflictException() {
        InMemorySagaEventStore store = new InMemorySagaEventStore();
        SagaDefinition def = threeStepDefinition();

        SagaInstance origin = SagaInstance.start(def);
        store.append(origin.sagaId(), 0, origin.pullDomainEvents(), EventMetadata.newCorrelation());

        SagaInstance writerA = SagaInstance.reconstruct(store.loadEvents(origin.sagaId()));
        SagaInstance writerB = SagaInstance.reconstruct(store.loadEvents(origin.sagaId()));

        writerA.completeCurrentStep(def, "ChargePayment");
        List<SagaDomainEvent> aEvents = writerA.pullDomainEvents();
        long aExpectedVersion = writerA.version() - aEvents.size();

        writerB.completeCurrentStep(def, "ChargePayment");
        List<SagaDomainEvent> bEvents = writerB.pullDomainEvents();
        long bExpectedVersion = writerB.version() - bEvents.size();

        store.append(origin.sagaId(), aExpectedVersion, aEvents, EventMetadata.newCorrelation());

        ConcurrencyConflictException ex = assertThrows(ConcurrencyConflictException.class,
                () -> store.append(origin.sagaId(), bExpectedVersion, bEvents, EventMetadata.newCorrelation()));

        assertEquals(origin.sagaId(), ex.sagaId());
        assertEquals(bExpectedVersion, ex.expectedVersion());
        assertEquals(aExpectedVersion + aEvents.size(), ex.actualVersion());
    }

    @Test
    void correctRecovery_reloadAfterConflict_retryThrowsStepMismatch_notACrash() {
        InMemorySagaEventStore store = new InMemorySagaEventStore();
        SagaDefinition def = threeStepDefinition();

        SagaInstance origin = SagaInstance.start(def);
        store.append(origin.sagaId(), 0, origin.pullDomainEvents(), EventMetadata.newCorrelation());

        SagaInstance writerA = SagaInstance.reconstruct(store.loadEvents(origin.sagaId()));
        SagaInstance writerB = SagaInstance.reconstruct(store.loadEvents(origin.sagaId()));

        writerA.completeCurrentStep(def, "ChargePayment");
        List<SagaDomainEvent> aEvents = writerA.pullDomainEvents();
        store.append(origin.sagaId(), writerA.version() - aEvents.size(), aEvents, EventMetadata.newCorrelation());

        writerB.completeCurrentStep(def, "ChargePayment");
        List<SagaDomainEvent> bEvents = writerB.pullDomainEvents();
        long bExpectedVersion = writerB.version() - bEvents.size();

        assertThrows(ConcurrencyConflictException.class,
                () -> store.append(origin.sagaId(), bExpectedVersion, bEvents, EventMetadata.newCorrelation()));

        // CORRECT RECOVERY: reload fresh and retry the ORIGINAL business intent -
        // NOT retry the same stale append.
        SagaInstance reloaded = SagaInstance.reconstruct(store.loadEvents(origin.sagaId()));
        StepMismatchException recognizedAsAlreadyHandled = assertThrows(StepMismatchException.class,
                () -> reloaded.completeCurrentStep(def, "ChargePayment"));

        assertEquals("ReserveInventory", recognizedAsAlreadyHandled.expectedStepName());
        assertEquals("ChargePayment", recognizedAsAlreadyHandled.actualStepName());
    }

    @Test
    void realConcurrentThreads_exactlyOneOfNRacingWritersSucceeds() throws InterruptedException {
        InMemorySagaEventStore store = new InMemorySagaEventStore();
        SagaDefinition def = threeStepDefinition();
        SagaInstance origin = SagaInstance.start(def);
        store.append(origin.sagaId(), 0, origin.pullDomainEvents(), EventMetadata.newCorrelation());

        int racerCount = 8;
        ExecutorService pool = Executors.newFixedThreadPool(racerCount);
        CountDownLatch startLine = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);

        List<Runnable> racers = new ArrayList<>();
        for (int i = 0; i < racerCount; i++) {
            racers.add(() -> {
                try {
                    startLine.await();
                    SagaInstance racer = SagaInstance.reconstruct(store.loadEvents(origin.sagaId()));
                    racer.completeCurrentStep(def, "ChargePayment");
                    List<SagaDomainEvent> events = racer.pullDomainEvents();
                    store.append(origin.sagaId(), racer.version() - events.size(), events, EventMetadata.newCorrelation());
                    successCount.incrementAndGet();
                } catch (ConcurrencyConflictException | StepMismatchException expectedForLosers) {
                    // Two independent, layered defenses can each catch a losing racer
                    // depending on timing - see Milestone 2 Step 6 write-up.
                } catch (InterruptedException ignored) {
                }
            });
        }
        racers.forEach(pool::execute);
        startLine.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "racers did not finish in time");

        assertEquals(1, successCount.get());
        assertEquals(2, store.loadEvents(origin.sagaId()).size()); // SagaStarted + exactly one StepCompleted
    }

    private static SagaDefinition threeStepDefinition() {
        return SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .addStep(new SagaStep("CreateShippingLabel", "CreateLabelCommand", "VoidLabelCommand"))
                .build();
    }
}
