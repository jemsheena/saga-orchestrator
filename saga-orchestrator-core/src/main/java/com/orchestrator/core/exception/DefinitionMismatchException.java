package com.orchestrator.core.exception;

import com.orchestrator.core.definition.SagaDefinitionReference;

import java.util.UUID;

/**
 * Thrown when a caller passes a {@code SagaDefinition} into a business method
 * whose {@code (sagaType, version)} does not match the {@code SagaDefinitionReference}
 * the {@code SagaInstance} was pinned to at creation time.
 *
 * <p><b>This exception exists because of the Goal #1 refactor, not as an
 * independent addition:</b> once {@code SagaInstance} stops holding a live
 * {@code SagaDefinition} and instead requires callers to resolve and pass one
 * in on every business method call (see {@code SagaInstance} javadoc), a new
 * failure mode becomes possible that literally could not happen before —
 * the caller resolving and passing in the WRONG version. Concretely: if
 * definition v2 is deployed while a saga instance started against v1 is
 * still mid-flight, an application service that naively resolves "the
 * latest definition for this sagaType" instead of "the definition pinned to
 * this specific instance" would silently start evaluating v1's in-flight
 * saga against v2's step list — index 2 might mean something completely
 * different in the two versions. That is a data-corrupting bug, and one
 * that would be very difficult to diagnose after the fact, since nothing
 * about it looks obviously wrong until step indices stop lining up with
 * reality. This validation converts that from a silent corruption into a
 * loud, immediate, unmissable failure at the exact call site that caused it.
 */
public class DefinitionMismatchException extends RuntimeException {

    private final UUID sagaId;
    private final SagaDefinitionReference expected;
    private final SagaDefinitionReference actual;

    public DefinitionMismatchException(UUID sagaId, SagaDefinitionReference expected, SagaDefinitionReference actual) {
        super("Saga " + sagaId + " is pinned to definition " + expected
                + " but was invoked with definition " + actual
                + ". A saga instance must always be operated against the exact "
                + "definition version it started with.");
        this.sagaId = sagaId;
        this.expected = expected;
        this.actual = actual;
    }

    public UUID sagaId() {
        return sagaId;
    }

    public SagaDefinitionReference expected() {
        return expected;
    }

    public SagaDefinitionReference actual() {
        return actual;
    }
}
