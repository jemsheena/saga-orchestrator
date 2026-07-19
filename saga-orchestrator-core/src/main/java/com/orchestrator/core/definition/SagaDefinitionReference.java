package com.orchestrator.core.definition;

import java.util.Objects;

/**
 * A lightweight, immutable pointer to a specific version of a {@link SagaDefinition} —
 * identified by {@code (sagaType, version)}, never by holding the object itself.
 *
 * <p><b>Why aggregates reference other aggregates/shared reference-data by identity,
 * not by object reference (the DDD rule this class exists to satisfy):</b>
 * A {@code SagaInstance} that stores a live {@code SagaDefinition} object couples
 * the instance's entire lifetime to one specific in-memory object graph. That
 * breaks in two concrete ways once persistence enters the picture (Milestone 2):
 *
 * <ol>
 *   <li><b>Rehydration has no object to hand back.</b> When you reconstruct a
 *       {@code SagaInstance} by replaying its {@code saga_event} rows, what
 *       you have on disk is {@code sagaType} and {@code version} columns —
 *       never a serialized Java object graph. If the instance's identity is
 *       "whichever SagaDefinition object I was built with," rehydration has
 *       no way to satisfy that contract without a completely different
 *       construction path than the live one uses. A reference type makes
 *       both paths identical: resolve the definition by
 *       {@code (sagaType, version)} — whether "now" during live execution or
 *       "after replaying 40 events" during rehydration doesn't matter.</li>
 *   <li><b>Version pinning becomes explicit and enforceable.</b> A saga that
 *       started against v1 of a definition must keep executing against v1
 *       even if v2 is deployed while it's mid-flight (see {@link SagaDefinition}'s
 *       own javadoc on why definitions are versioned). Storing only the
 *       reference — not the object — makes "which version am I pinned to"
 *       a first-class, inspectable fact about the instance, and lets us
 *       validate every incoming operation against it (see
 *       {@code SagaInstance.completeCurrentStep}, which now rejects a
 *       caller-supplied definition that doesn't match this reference).</li>
 * </ol>
 *
 * <p>This is a record specifically so it gets free, correct structural
 * equality — two references to the same {@code (sagaType, version)} pair
 * must be interchangeable, which matters both for the validation above and
 * for using this type as a cache/registry lookup key later.
 */
public record SagaDefinitionReference(String sagaType, int version) {

    public SagaDefinitionReference {
        Objects.requireNonNull(sagaType, "sagaType must not be null");
        if (sagaType.isBlank()) {
            throw new IllegalArgumentException("sagaType must not be blank");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
    }
}
