package com.orchestrator.core.engine;

import java.util.EnumSet;
import java.util.Set;

/**
 * The finite set of states a {@link SagaInstance} can be in, and — critically —
 * the legal transitions between them.
 *
 * <p><b>Design decision worth defending in an interview:</b> the transition
 * graph lives ON the enum itself ({@link #legalNextStates()}), not in a
 * separate "SagaStateMachine" class with a big switch statement. Putting the
 * legality rule next to the state it governs means the two can never drift
 * out of sync, and it makes SagaState a genuinely self-contained, testable
 * unit — you can unit test "what can STARTED transition to" without
 * constructing a SagaInstance at all. This is the State pattern applied in
 * its data-driven form rather than its textbook one-class-per-state form
 * (see Milestone 1 class design notes for why we rejected the latter here).
 *
 * <pre>
 *              ┌───────────┐
 *   (start) -> │  STARTED   │
 *              └─────┬──────┘
 *                    │ step succeeds          │ step fails, nothing to compensate yet
 *                    ▼                        ▼
 *          ┌──────────────────┐        ┌──────────────┐
 *          │  STEP_COMPLETED   │──────▶│ COMPENSATING  │
 *          │ (self-loop while  │ later  └──────┬───────┘
 *          │  more steps exist)│ step           │ (self-loop while more
 *          └─────────┬─────────┘ fails          │  compensations remain)
 *                    │ last step done            │
 *                    ▼                           ▼
 *            ┌───────────────┐           ┌───────────────┐
 *            │   COMPLETED    │           │     FAILED     │
 *            │  (terminal)    │           │   (terminal)   │
 *            └───────────────┘           └───────────────┘
 * </pre>
 */
public enum SagaState {

    /** Saga instance created; no step has completed yet. */
    STARTED {
        @Override
        public Set<SagaState> legalNextStates() {
            return EnumSet.of(STEP_COMPLETED, COMPENSATING, COMPLETED, FAILED);
            // COMPLETED is reachable directly from STARTED only for the edge case
            // of a single-step saga definition — step 1 completing IS the last step.
            //
            // FAILED is reachable directly from STARTED for the edge case where
            // step 0 itself fails: nothing has completed yet, so there's nothing
            // to compensate, so SagaInstance.failCurrentStep() skips COMPENSATING
            // entirely and goes straight to FAILED. This is a distinct edge from
            // reaching FAILED via a normal compensation walk (COMPENSATING -> FAILED).
            // Caught by SagaInstanceTest — see its javadoc for the full story.
        }
    },

    /**
     * The most recently attempted step succeeded. This state is intentionally
     * reused via a self-transition as the saga advances through step 2, 3, ... N —
     * the enum tracks COARSE saga status; fine-grained progress (which step index
     * we're on) is tracked by {@link SagaInstance#currentStepIndex()}, not by
     * proliferating one enum value per step. Seeing that separation is exactly
     * what tells you whether an engineer understands state machine design or is
     * just pattern-matching "add an enum value per thing that can happen."
     */
    STEP_COMPLETED {
        @Override
        public Set<SagaState> legalNextStates() {
            return EnumSet.of(STEP_COMPLETED, COMPENSATING, COMPLETED);
        }
    },

    /** A step failed after at least one prior step succeeded; undoing in reverse order. */
    COMPENSATING {
        @Override
        public Set<SagaState> legalNextStates() {
            return EnumSet.of(COMPENSATING, FAILED);
        }
    },

    /** Terminal: every step completed successfully. */
    COMPLETED {
        @Override
        public Set<SagaState> legalNextStates() {
            return EnumSet.noneOf(SagaState.class);
        }
    },

    /** Terminal: the saga did not complete; compensation (if any) has finished. */
    FAILED {
        @Override
        public Set<SagaState> legalNextStates() {
            return EnumSet.noneOf(SagaState.class);
        }
    };

    /**
     * @return the set of states this state is allowed to transition into.
     *         An empty set means this state is terminal.
     */
    public abstract Set<SagaState> legalNextStates();

    public boolean isTerminal() {
        return legalNextStates().isEmpty();
    }

    public boolean canTransitionTo(SagaState target) {
        return legalNextStates().contains(target);
    }
}
