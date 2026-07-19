package com.orchestrator.core.exception;

import com.orchestrator.core.engine.SagaState;

/**
 * Thrown when code attempts to move a {@code SagaInstance} into a state that
 * is not reachable from its current state.
 *
 * <p><b>Why a dedicated exception type instead of IllegalStateException:</b>
 * We deliberately do NOT use the generic {@code java.lang.IllegalStateException}.
 * A dedicated, domain-specific exception type lets calling code (later: the
 * Kafka consumer in Milestone 3, the REST controller in Milestone 6) catch
 * this specific failure and react correctly — e.g. treat it as a signal that
 * a duplicate/out-of-order event arrived, rather than as an unexpected bug.
 * Generic exceptions force callers into fragile string-matching or blanket
 * catch-alls; a typed exception makes the failure mode part of the API contract.
 */
public class InvalidStateTransitionException extends RuntimeException {

    private final SagaState fromState;
    private final SagaState attemptedState;

    public InvalidStateTransitionException(SagaState fromState, SagaState attemptedState) {
        super("Cannot transition saga from " + fromState + " to " + attemptedState
                + ". Legal next states from " + fromState + " are: " + fromState.legalNextStates());
        this.fromState = fromState;
        this.attemptedState = attemptedState;
    }

    public SagaState fromState() {
        return fromState;
    }

    public SagaState attemptedState() {
        return attemptedState;
    }
}
