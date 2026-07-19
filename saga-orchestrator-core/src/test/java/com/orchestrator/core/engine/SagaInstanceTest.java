package com.orchestrator.core.engine;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.event.CompensationStepCompleted;
import com.orchestrator.core.event.SagaCompensationStarted;
import com.orchestrator.core.event.SagaCompleted;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.event.SagaFailed;
import com.orchestrator.core.event.SagaStarted;
import com.orchestrator.core.event.StepCompleted;
import com.orchestrator.core.event.StepFailed;
import com.orchestrator.core.exception.DefinitionMismatchException;
import com.orchestrator.core.exception.InvalidStateTransitionException;
import com.orchestrator.core.exception.StepMismatchException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaInstanceTest {

    /** Fixed clock so emitted-event timestamps are deterministic and assertable. */
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    private SagaDefinition threeStepDefinition;

    @BeforeEach
    void setUp() {
        threeStepDefinition = SagaDefinition.builder("OrderFulfillment")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("ReserveInventory", "ReserveInventoryCommand", "ReleaseInventoryCommand"))
                .addStep(new SagaStep("CreateShippingLabel", "CreateLabelCommand", "VoidLabelCommand"))
                .build();
    }

    // ---------- Domain event generation ----------

    @Test
    void start_emitsExactlyOneSagaStartedEvent() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);

        List<SagaDomainEvent> events = instance.pullDomainEvents();

        assertEquals(1, events.size());
        SagaStarted started = assertInstanceOf(SagaStarted.class, events.get(0));
        assertEquals(instance.sagaId(), started.sagaId());
        assertEquals(threeStepDefinition.reference(), started.definitionReference());
        assertEquals(FIXED_CLOCK.instant(), started.occurredAt());
    }

    @Test
    void completingAStep_emitsStepCompleted_onlyForThatStep() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.pullDomainEvents(); // discard SagaStarted

        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(1, events.size());
        StepCompleted event = assertInstanceOf(StepCompleted.class, events.get(0));
        assertEquals("ChargePayment", event.stepName());
        assertEquals(0, event.stepIndex());
    }

    @Test
    void completingFinalStep_emitsBothStepCompletedAndSagaCompleted_inOrder() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        instance.pullDomainEvents();

        instance.completeCurrentStep(threeStepDefinition, "CreateShippingLabel");
        List<SagaDomainEvent> events = instance.pullDomainEvents();

        assertEquals(2, events.size());
        assertInstanceOf(StepCompleted.class, events.get(0));
        assertInstanceOf(SagaCompleted.class, events.get(1));
    }

    @Test
    void failingFirstStep_emitsStepFailedThenSagaFailed_skipsCompensationEvents() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.pullDomainEvents();

        instance.failCurrentStep(threeStepDefinition, "ChargePayment", "card declined");

        List<SagaDomainEvent> events = instance.pullDomainEvents();
        assertEquals(2, events.size());
        StepFailed failed = assertInstanceOf(StepFailed.class, events.get(0));
        assertEquals("card declined", failed.reason());
        assertInstanceOf(SagaFailed.class, events.get(1));
    }

    @Test
    void failingLaterStep_emitsStepFailedThenSagaCompensationStarted() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        instance.pullDomainEvents();

        instance.failCurrentStep(threeStepDefinition, "CreateShippingLabel", "carrier API down");
        List<SagaDomainEvent> events = instance.pullDomainEvents();

        assertEquals(2, events.size());
        assertInstanceOf(StepFailed.class, events.get(0));
        SagaCompensationStarted compensationStarted = assertInstanceOf(SagaCompensationStarted.class, events.get(1));
        assertEquals(1, compensationStarted.compensationCursor());
    }

    @Test
    void fullCompensationWalk_emitsCompensationStepCompletedPerStep_thenSagaFailedAtEnd() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");
        instance.pullDomainEvents();
        instance.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        instance.pullDomainEvents();
        instance.failCurrentStep(threeStepDefinition, "CreateShippingLabel", "carrier API down");
        instance.pullDomainEvents();

        instance.completeCompensationStep(threeStepDefinition, "ReserveInventory"); // undo step index 1
        List<SagaDomainEvent> midEvents = instance.pullDomainEvents();
        assertEquals(1, midEvents.size());
        assertInstanceOf(CompensationStepCompleted.class, midEvents.get(0));
        assertEquals(SagaState.COMPENSATING, instance.state());

        instance.completeCompensationStep(threeStepDefinition, "ChargePayment"); // undo step index 0
        List<SagaDomainEvent> finalEvents = instance.pullDomainEvents();
        assertEquals(2, finalEvents.size());
        assertInstanceOf(CompensationStepCompleted.class, finalEvents.get(0));
        assertInstanceOf(SagaFailed.class, finalEvents.get(1));
        assertEquals(SagaState.FAILED, instance.state());
    }

    // ---------- Pending event queue semantics ----------

    @Test
    void pullDomainEvents_clearsInternalQueue_secondPullIsEmpty() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);

        instance.pullDomainEvents();
        List<SagaDomainEvent> secondPull = instance.pullDomainEvents();

        assertTrue(secondPull.isEmpty());
    }

    @Test
    void events_accumulateAcrossMultipleCalls_ifNotPulledBetween() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        // deliberately NOT pulling after start
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");
        // deliberately NOT pulling after this either

        List<SagaDomainEvent> events = instance.pullDomainEvents();

        assertEquals(2, events.size());
        assertInstanceOf(SagaStarted.class, events.get(0));
        assertInstanceOf(StepCompleted.class, events.get(1));
    }

    // ---------- Step correlation validation ----------

    @Test
    void completingWrongStepName_throwsStepMismatchException_notSilentlyAccepted() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);

        StepMismatchException ex = assertThrows(StepMismatchException.class,
                () -> instance.completeCurrentStep(threeStepDefinition, "ReserveInventory"));

        assertEquals("ChargePayment", ex.expectedStepName());
        assertEquals("ReserveInventory", ex.actualStepName());
    }

    @Test
    void duplicateStepCompletion_secondReportForAlreadyAdvancedStep_throwsStepMismatch() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment"); // advances to step index 1

        // Simulates a duplicate/replayed Kafka message reporting "ChargePayment"
        // completed again, after the instance has already moved on.
        assertThrows(StepMismatchException.class,
                () -> instance.completeCurrentStep(threeStepDefinition, "ChargePayment"));
    }

    @Test
    void compensatingWrongStepName_throwsStepMismatchException() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");
        instance.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        instance.failCurrentStep(threeStepDefinition, "CreateShippingLabel", "carrier API down");
        // COMPENSATING, cursor points at "ReserveInventory" (index 1) first

        assertThrows(StepMismatchException.class,
                () -> instance.completeCompensationStep(threeStepDefinition, "ChargePayment"));
    }

    // ---------- Definition pinning validation ----------

    @Test
    void invokingWithADifferentDefinitionVersion_throwsDefinitionMismatchException() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);

        SagaDefinition differentVersion = SagaDefinition.builder("OrderFulfillment")
                .version(2)
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .build();

        DefinitionMismatchException ex = assertThrows(DefinitionMismatchException.class,
                () -> instance.completeCurrentStep(differentVersion, "ChargePayment"));

        assertEquals(threeStepDefinition.reference(), ex.expected());
        assertEquals(differentVersion.reference(), ex.actual());
    }

    // ---------- Invalid state transitions (unchanged behavior from Milestone 1) ----------

    @Test
    void completingAStep_onATerminalCompletedInstance_throwsInvalidStateTransition() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");
        instance.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        instance.completeCurrentStep(threeStepDefinition, "CreateShippingLabel"); // COMPLETED

        assertThrows(InvalidStateTransitionException.class,
                () -> instance.completeCurrentStep(threeStepDefinition, "CreateShippingLabel"));
    }

    @Test
    void failingAStep_onATerminalFailedInstance_throwsInvalidStateTransition() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        instance.failCurrentStep(threeStepDefinition, "ChargePayment", "declined"); // FAILED

        assertThrows(InvalidStateTransitionException.class,
                () -> instance.failCurrentStep(threeStepDefinition, "ChargePayment", "declined again"));
    }

    // ---------- Identity-based equality ----------

    @Test
    void twoInstances_withDifferentSagaIds_areNotEqual_evenWithIdenticalState() {
        SagaInstance a = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        SagaInstance b = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);

        assertNotEquals(a, b);
    }

    @Test
    void equality_dependsOnlyOnSagaId_ignoresMutableState() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        SagaInstance sameIdentityDifferentState = sameIdentityAs(instance);

        // Advance ONE of them so their mutable state (state, currentStepIndex,
        // pending events) diverges completely, while sagaId stays identical.
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");

        assertEquals(instance, sameIdentityDifferentState);
        assertEquals(instance.hashCode(), sameIdentityDifferentState.hashCode());
        assertNotEquals(instance.state(), sameIdentityDifferentState.state(),
                "sanity check: mutable state must actually differ for this test to be meaningful");
    }

    /**
     * Test helper simulating "the same saga, loaded twice" (e.g. once by a web
     * request thread, once by a Kafka consumer thread, both from persistence)
     * — same sagaId, independently constructed objects. Full rehydration-from-
     * events doesn't exist until Milestone 2; this stands in for it using the
     * package-private, identity-preserving factory.
     */
    private SagaInstance sameIdentityAs(SagaInstance original) {
        return SagaInstance.startWithId(original.sagaId(), threeStepDefinition, FIXED_CLOCK);
    }

    // ---------- Determinism ("replay-shaped" check) ----------

    @Test
    void twoInstancesDrivenThroughIdenticalEventScripts_produceIdenticalFinalState_andIdenticalEventSequence() {
        SagaInstance first = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        first.pullDomainEvents();
        SagaInstance second = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        second.pullDomainEvents();

        List<SagaDomainEvent> firstEvents = driveHappyPath(first);
        List<SagaDomainEvent> secondEvents = driveHappyPath(second);

        assertEquals(first.state(), second.state());
        assertEquals(first.currentStepIndex(), second.currentStepIndex());
        assertEquals(firstEvents.size(), secondEvents.size());
        for (int i = 0; i < firstEvents.size(); i++) {
            assertEquals(firstEvents.get(i).getClass(), secondEvents.get(i).getClass(),
                    "event #" + i + " type diverged between two identically-driven instances");
        }
    }

    private List<SagaDomainEvent> driveHappyPath(SagaInstance instance) {
        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");
        instance.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        instance.completeCurrentStep(threeStepDefinition, "CreateShippingLabel");
        return instance.pullDomainEvents();
    }

    // ---------- Edge cases ----------

    @Test
    void singleStepSaga_failingItsOnlyStep_goesStraightToFailed_withNoCompensationEvents() {
        SagaDefinition singleStep = SagaDefinition.builder("QuickTask")
                .addStep(new SagaStep("DoThing", "DoThingCommand", null))
                .build();
        SagaInstance instance = SagaInstance.start(singleStep, FIXED_CLOCK);
        instance.pullDomainEvents();

        instance.failCurrentStep(singleStep, "DoThing", "boom");
        List<SagaDomainEvent> events = instance.pullDomainEvents();

        assertEquals(2, events.size());
        assertInstanceOf(StepFailed.class, events.get(0));
        assertInstanceOf(SagaFailed.class, events.get(1));
        assertFalse(events.stream().anyMatch(e -> e instanceof SagaCompensationStarted));
    }

    // ==================== Milestone 2, Step 1: apply() / reconstruction ====================

    @Test
    void version_incrementsOncePerEvent_notOncePerBusinessMethodCall() {
        SagaInstance instance = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        assertEquals(1, instance.version()); // SagaStarted already recorded by start()

        instance.completeCurrentStep(threeStepDefinition, "ChargePayment");
        assertEquals(2, instance.version());

        // failCurrentStep on a later step (with prior completion) emits TWO events
        // (StepFailed + SagaCompensationStarted) - version must advance by 2, not 1.
        instance.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        instance.failCurrentStep(threeStepDefinition, "CreateShippingLabel", "carrier down");
        assertEquals(5, instance.version());
    }

    @Test
    void reconstruct_fromFullEventHistory_matchesLiveInstanceExactly() {
        SagaInstance live = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        live.completeCurrentStep(threeStepDefinition, "ChargePayment");
        live.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        live.failCurrentStep(threeStepDefinition, "CreateShippingLabel", "carrier down");
        live.completeCompensationStep(threeStepDefinition, "ReserveInventory");
        live.completeCompensationStep(threeStepDefinition, "ChargePayment");
        List<SagaDomainEvent> allEvents = live.pullDomainEvents();

        SagaInstance rebuilt = SagaInstance.reconstruct(allEvents, FIXED_CLOCK);

        assertEquals(live.sagaId(), rebuilt.sagaId());
        assertEquals(live.state(), rebuilt.state());
        assertEquals(SagaState.FAILED, rebuilt.state());
        assertEquals(live.currentStepIndex(), rebuilt.currentStepIndex());
        assertEquals(live.compensationCursor(), rebuilt.compensationCursor());
        assertEquals(live.version(), rebuilt.version());
        assertEquals(live.definitionReference(), rebuilt.definitionReference());
    }

    @Test
    void reconstruct_rejectsEmptyEventList() {
        assertThrows(IllegalArgumentException.class, () -> SagaInstance.reconstruct(List.of(), FIXED_CLOCK));
    }

    @Test
    void reconstruct_rejectsStreamNotStartingWithSagaStarted() {
        SagaInstance live = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        live.completeCurrentStep(threeStepDefinition, "ChargePayment");
        List<SagaDomainEvent> events = live.pullDomainEvents();
        List<SagaDomainEvent> corrupted = events.subList(1, events.size()); // drop SagaStarted

        assertThrows(IllegalArgumentException.class, () -> SagaInstance.reconstruct(corrupted, FIXED_CLOCK));
    }

    @Test
    void toSnapshot_thenReconstructFromSnapshot_withNoSubsequentEvents_matchesSource() {
        SagaInstance live = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        live.completeCurrentStep(threeStepDefinition, "ChargePayment");
        live.pullDomainEvents();

        SagaSnapshot snapshot = live.toSnapshot(1);
        SagaInstance rebuilt = SagaInstance.reconstructFromSnapshot(snapshot, List.of(), FIXED_CLOCK);

        assertEquals(live.state(), rebuilt.state());
        assertEquals(live.currentStepIndex(), rebuilt.currentStepIndex());
        assertEquals(live.version(), rebuilt.version());
        assertEquals(live.sagaId(), rebuilt.sagaId());
    }

    @Test
    void reconstructFromSnapshot_plusSubsequentEvents_matchesFullReplayResult_midSaga() {
        SagaInstance live = SagaInstance.start(threeStepDefinition, FIXED_CLOCK);
        live.completeCurrentStep(threeStepDefinition, "ChargePayment"); // snapshot taken here
        SagaSnapshot snapshot = live.toSnapshot(1);
        live.pullDomainEvents();

        live.completeCurrentStep(threeStepDefinition, "ReserveInventory");
        live.completeCurrentStep(threeStepDefinition, "CreateShippingLabel");
        List<SagaDomainEvent> remaining = live.pullDomainEvents();

        SagaInstance fastPath = SagaInstance.reconstructFromSnapshot(snapshot, remaining, FIXED_CLOCK);

        assertEquals(SagaState.COMPLETED, fastPath.state());
        assertEquals(live.state(), fastPath.state());
        assertEquals(live.version(), fastPath.version());
    }

    @Test
    void apply_onFinalStep_leavesCurrentStepIndexOnePastLast_documentedQuirk_stateCompleted() {
        SagaDefinition singleStep = SagaDefinition.builder("QuickTask")
                .addStep(new SagaStep("DoThing", "DoThingCommand", null))
                .build();
        SagaInstance live = SagaInstance.start(singleStep, FIXED_CLOCK);
        live.completeCurrentStep(singleStep, "DoThing");
        List<SagaDomainEvent> events = live.pullDomainEvents();

        SagaInstance rebuilt = SagaInstance.reconstruct(events, FIXED_CLOCK);

        assertEquals(SagaState.COMPLETED, rebuilt.state());
        assertEquals(1, rebuilt.currentStepIndex()); // one past index 0 - see apply() javadoc
    }
}
