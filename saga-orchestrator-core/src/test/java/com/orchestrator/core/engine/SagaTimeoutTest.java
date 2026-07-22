package com.orchestrator.core.engine;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.definition.TimeoutPolicy;
import com.orchestrator.core.event.SagaCompensationStarted;
import com.orchestrator.core.event.SagaCompleted;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.event.SagaFailed;
import com.orchestrator.core.event.SagaTimedOut;
import com.orchestrator.core.event.StepCompleted;
import com.orchestrator.core.exception.DefinitionMismatchException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SagaTimeoutTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    private SagaDefinition threeStepDefinitionWithTimeout;
    private SagaDefinition threeStepDefinitionNoTimeout;

    @BeforeEach
    void setUp() {
        threeStepDefinitionWithTimeout = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .addStep(new SagaStep("CreateShippingLabel", "CreateLabelCommand", "VoidLabelCommand"))
                .timeoutPolicy(TimeoutPolicy.ofMinutes(5))
                .build();

        threeStepDefinitionNoTimeout = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .addStep(new SagaStep("CreateShippingLabel", "CreateLabelCommand", "VoidLabelCommand"))
                .build();
    }

    // ---------- TimeoutPolicy configuration tests ----------

    @Test
    void definitionWithTimeoutPolicy_providesTimeoutPolicy() {
        assertEquals(TimeoutPolicy.ofMinutes(5), threeStepDefinitionWithTimeout.timeoutPolicy());
    }

    @Test
    void definitionWithoutTimeoutPolicy_hasNullPolicy() {
        assertEquals(null, threeStepDefinitionNoTimeout.timeoutPolicy());
    }

    // ---------- Timeout on first step (no compensation needed) ----------

    @Test
    void timeoutOnFirstStep_emitsSagaTimedOutThenSagaFailed_skipsCompensationEvents() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        instance.pullDomainEvents(); // discard SagaStarted

        instance.handleTimeout(threeStepDefinitionWithTimeout);

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(2, events.size());
        assertInstanceOf(SagaTimedOut.class, events.get(0));
        assertInstanceOf(SagaFailed.class, events.get(1));
        assertEquals(SagaState.FAILED, instance.state());
    }

    // ---------- Timeout after steps completed (compensation needed) ----------

    @Test
    void timeoutAfterOneStepCompleted_emitsSagaTimedOutThenSagaCompensationStarted() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinitionWithTimeout, "ChargePayment");
        instance.pullDomainEvents();

        instance.handleTimeout(threeStepDefinitionWithTimeout);

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(2, events.size());
        SagaTimedOut timedOut = assertInstanceOf(SagaTimedOut.class, events.get(0));
        assertEquals(instance.sagaId(), timedOut.sagaId());
        SagaCompensationStarted compensationStarted = assertInstanceOf(SagaCompensationStarted.class, events.get(1));
        assertEquals(0, compensationStarted.compensationCursor());
        assertEquals(SagaState.COMPENSATING, instance.state());
    }

    @Test
    void timeoutAfterTwoStepsCompleted_startsCompensationFromCurrentStepMinus1() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinitionWithTimeout, "ChargePayment");
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinitionWithTimeout, "ReserveInventory");
        instance.pullDomainEvents();

        instance.handleTimeout(threeStepDefinitionWithTimeout);

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(2, events.size());
        SagaCompensationStarted compensationStarted = assertInstanceOf(SagaCompensationStarted.class, events.get(1));
        assertEquals(1, compensationStarted.compensationCursor());
        assertEquals(SagaState.COMPENSATING, instance.state());
    }

    // ---------- Timeout on terminal sagas (idempotency) ----------

    @Test
    void timeoutOnCompletedSaga_isNoOp() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinitionWithTimeout, "ChargePayment");
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinitionWithTimeout, "ReserveInventory");
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinitionWithTimeout, "CreateShippingLabel");
        instance.pullDomainEvents();

        assertEquals(SagaState.COMPLETED, instance.state());

        instance.handleTimeout(threeStepDefinitionWithTimeout);

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(0, events.size()); // No new events
        assertEquals(SagaState.COMPLETED, instance.state());
    }

    @Test
    void timeoutOnFailedSaga_isNoOp() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.failCurrentStep(threeStepDefinitionWithTimeout, "ChargePayment", "card declined");
        instance.pullDomainEvents();

        assertEquals(SagaState.FAILED, instance.state());

        instance.handleTimeout(threeStepDefinitionWithTimeout);

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(0, events.size()); // No new events
        assertEquals(SagaState.FAILED, instance.state());
    }

    // ---------- Event replay (apply) tests ----------

    @Test
    void applySagaTimedOut_doesnNotChangeState() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinitionWithTimeout, "ChargePayment");
        instance.pullDomainEvents();

        SagaTimedOut timedOutEvent = new SagaTimedOut(instance.sagaId(), FIXED_CLOCK.instant());
        long versionBefore = instance.version();

        instance.apply(timedOutEvent);

        // State should still be STEP_COMPLETED because SagaTimedOut doesn't change state;
        // the actual state transition comes from the following SagaCompensationStarted
        assertEquals(SagaState.STEP_COMPLETED, instance.state());
        assertEquals(versionBefore + 1, instance.version());
    }

    // ---------- Version tracking ----------

    @Test
    void timeoutEmitsTwoEvents_incrementsVersionByTwo() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinitionWithTimeout, "ChargePayment");
        instance.pullDomainEvents();

        long versionBefore = instance.version();

        instance.handleTimeout(threeStepDefinitionWithTimeout);

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(2, events.size());
        assertEquals(versionBefore + 2, instance.version());
    }

    @Test
    void timeoutOnFirstStepEmitsTwoEvents_incrementsVersionByTwo() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        long versionAfterStart = instance.version();
        instance.pullDomainEvents();

        instance.handleTimeout(threeStepDefinitionWithTimeout);

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(2, events.size());
        assertEquals(versionAfterStart + 2, instance.version());
    }

    // ---------- State transition validation ----------

    @Test
    void handleTimeoutPreservesDefinitionValidation() {
        SagaInstance instance = SagaInstance.start(threeStepDefinitionWithTimeout, FIXED_CLOCK);
        instance.pullDomainEvents();

        SagaDefinition wrongDefinition = SagaDefinition.builder("WrongSaga")
                .addStep(new SagaStep("SomeStep", "SomeCommand", "SomeCompensation"))
                .build();

        assertThrows(DefinitionMismatchException.class, () -> instance.handleTimeout(wrongDefinition));
    }
}
