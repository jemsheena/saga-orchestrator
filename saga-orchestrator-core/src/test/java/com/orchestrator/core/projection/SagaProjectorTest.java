package com.orchestrator.core.projection;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.event.SagaDomainEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaProjectorTest {

    private final SagaProjector projector = new SagaProjector();

    @Test
    void happyPath_viewEndsCompleted_withDurationAndNoLastError() {
        InMemorySagaInstanceViewStoreForTest store = new InMemorySagaInstanceViewStoreForTest();
        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        instance.completeCurrentStep(def, "ChargePayment");
        instance.completeCurrentStep(def, "ReserveInventory");
        instance.completeCurrentStep(def, "CreateShippingLabel");

        for (SagaDomainEvent e : instance.pullDomainEvents()) {
            projector.project(e, store);
        }

        SagaInstanceView view = store.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.COMPLETED, view.state());
        assertEquals("OrderFulfillment", view.sagaType());
        assertNotNull(view.durationMs());
        assertNotNull(view.completedAt());
        assertNull(view.lastError());
    }

    @Test
    void compensationPath_viewEndsFailed_withLastErrorCarriedFromStepFailed() {
        InMemorySagaInstanceViewStoreForTest store = new InMemorySagaInstanceViewStoreForTest();
        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        instance.completeCurrentStep(def, "ChargePayment");
        instance.completeCurrentStep(def, "ReserveInventory");
        instance.failCurrentStep(def, "CreateShippingLabel", "carrier API down");
        instance.completeCompensationStep(def, "ReserveInventory");
        instance.completeCompensationStep(def, "ChargePayment");

        for (SagaDomainEvent e : instance.pullDomainEvents()) {
            projector.project(e, store);
        }

        SagaInstanceView view = store.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.FAILED, view.state());
        assertEquals("carrier API down", view.lastError());
        assertNotNull(view.completedAt());
    }

    @Test
    void midSaga_viewReflectsStepCompletedAndCorrectStepIndex() {
        InMemorySagaInstanceViewStoreForTest store = new InMemorySagaInstanceViewStoreForTest();
        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        instance.completeCurrentStep(def, "ChargePayment");

        for (SagaDomainEvent e : instance.pullDomainEvents()) {
            projector.project(e, store);
        }

        SagaInstanceView view = store.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.STEP_COMPLETED, view.state());
        assertEquals(1, view.currentStepIndex());
    }

    @Test
    void projectingEventForUnknownSaga_throwsIllegalStateException() {
        InMemorySagaInstanceViewStoreForTest store = new InMemorySagaInstanceViewStoreForTest();
        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        instance.completeCurrentStep(def, "ChargePayment");
        List<SagaDomainEvent> events = instance.pullDomainEvents();

        // Deliberately skip projecting SagaStarted to simulate corrupted/out-of-order delivery.
        assertThrows(IllegalStateException.class, () -> projector.project(events.get(1), store));
    }

    @Test
    void compensationStepCompleted_doesNotChangeViewFields_stillShowsCompensating() {
        InMemorySagaInstanceViewStoreForTest store = new InMemorySagaInstanceViewStoreForTest();
        SagaDefinition def = threeStepDefinition();
        SagaInstance instance = SagaInstance.start(def);
        instance.completeCurrentStep(def, "ChargePayment");
        instance.completeCurrentStep(def, "ReserveInventory");
        instance.failCurrentStep(def, "CreateShippingLabel", "carrier down");
        for (SagaDomainEvent e : instance.pullDomainEvents()) {
            projector.project(e, store);
        }

        instance.completeCompensationStep(def, "ReserveInventory");
        for (SagaDomainEvent e : instance.pullDomainEvents()) {
            projector.project(e, store);
        }

        SagaInstanceView view = store.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.COMPENSATING, view.state());
        assertTrue(store.findById(instance.sagaId()).isPresent());
        assertFalse(view.state() == SagaState.FAILED); // not yet - only one of two compensations done
    }

    private static SagaDefinition threeStepDefinition() {
        return SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .addStep(new SagaStep("CreateShippingLabel", "CreateLabelCommand", "VoidLabelCommand"))
                .build();
    }

    /**
     * Minimal local in-memory {@link SagaInstanceViewStore} for this test class
     * specifically, kept separate from {@code InMemorySagaInstanceViewStore} in
     * {@code repository.support} to avoid a test-sources cross-package
     * dependency for what is a two-line fake.
     */
    private static final class InMemorySagaInstanceViewStoreForTest implements SagaInstanceViewStore {
        private final java.util.Map<java.util.UUID, SagaInstanceView> rows = new java.util.HashMap<>();

        @Override
        public void upsert(SagaInstanceView view) {
            rows.put(view.sagaId(), view);
        }

        @Override
        public java.util.Optional<SagaInstanceView> findById(java.util.UUID sagaId) {
            return java.util.Optional.ofNullable(rows.get(sagaId));
        }

        @Override
        public java.util.List<SagaInstanceView> findExpiredNonTerminal(int limit, Instant deadlineNow) {
            return rows.values().stream()
                    .filter(view -> view.state() != SagaState.COMPLETED && view.state() != SagaState.FAILED)
                    .filter(view -> view.timeoutExpiredAt() != null && view.timeoutExpiredAt().isBefore(deadlineNow))
                    .sorted((a, b) -> a.timeoutExpiredAt().compareTo(b.timeoutExpiredAt()))
                    .limit(limit)
                    .toList();
        }
    }
}
