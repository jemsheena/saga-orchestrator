package com.orchestrator.core.exception;

import java.util.UUID;

/**
 * Thrown when a caller reports an outcome (completion, failure, compensation)
 * for a step that is NOT the step the {@code SagaInstance} is currently
 * expecting — e.g. a "step 1 completed" message arrives after the instance
 * has already moved on to step 2.
 *
 * <p><b>Why this is a distinct exception from {@code InvalidStateTransitionException}:</b>
 * the two failure modes have different likely causes and, critically, different
 * correct responses once Kafka is in the picture (Milestone 3):
 * <ul>
 *   <li>{@code InvalidStateTransitionException} — the instance is in a state
 *       (e.g. already terminal) that cannot legally accept ANY step outcome
 *       right now. This is most often a genuine duplicate/replayed message —
 *       safe to log and acknowledge without further action.</li>
 *   <li>{@code StepMismatchException} — the instance IS in a state that can
 *       accept a step outcome, but the WRONG step's outcome arrived. This is
 *       more likely to indicate real out-of-order delivery or a routing bug
 *       upstream, and may warrant an alert rather than a silent ack — you
 *       don't want to silently swallow "the orchestrator and a participant
 *       have disagreed about which step is in flight."</li>
 * </ul>
 * Collapsing these into one exception type would force every caller to
 * re-derive which case occurred by string-parsing a message — exactly the
 * anti-pattern typed exceptions exist to avoid.
 *
 * <p><b>Why this validation belongs inside the aggregate, not the caller:</b>
 * the expected current step is exactly the kind of fact only the aggregate
 * itself can authoritatively know (it's derived from {@code currentStepIndex}
 * or {@code compensationCursor}, both private). Pushing this check out to an
 * application service would mean either duplicating that derivation logic
 * outside the aggregate (a DRY and encapsulation violation) or exposing the
 * cursor fields as public mutable state to make the check possible elsewhere
 * — both worse than keeping the invariant where the state actually lives.
 */
public class StepMismatchException extends RuntimeException {

    private final UUID sagaId;
    private final String expectedStepName;
    private final String actualStepName;

    public StepMismatchException(UUID sagaId, String expectedStepName, String actualStepName) {
        super("Saga " + sagaId + " expected an outcome for step '" + expectedStepName
                + "' but received one for step '" + actualStepName
                + "'. This usually indicates an out-of-order or misrouted message.");
        this.sagaId = sagaId;
        this.expectedStepName = expectedStepName;
        this.actualStepName = actualStepName;
    }

    public UUID sagaId() {
        return sagaId;
    }

    public String expectedStepName() {
        return expectedStepName;
    }

    public String actualStepName() {
        return actualStepName;
    }
}
