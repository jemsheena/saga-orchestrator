package com.orchestrator.core.repository;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaDefinitionReference;

import java.util.Optional;

/**
 * Resolves {@link SagaDefinitionReference}s back into usable
 * {@link SagaDefinition} instances — the lookup both live execution and
 * replay depend on (see Milestone 1.5 javadoc on why {@code SagaInstance}
 * holds a reference rather than a live object, and Milestone 2 architecture
 * review Section 5's rehydration sequence).
 *
 * <p>An implementation might back this with an in-memory map (definitions
 * are typically registered once at startup and rarely change), a database
 * table, or both — this interface makes no assumption either way.
 */
public interface SagaDefinitionRegistry {

    /**
     * Registers a definition so it can later be resolved by its reference.
     * Registering two definitions with the same {@code (sagaType, version)}
     * is a configuration error — implementations should reject the second
     * registration rather than silently overwrite the first, since a saga
     * instance already pinned to that reference must never see its
     * definition's step list change underneath it.
     */
    void register(SagaDefinition definition);

    /** Resolves an exact, versioned reference — used during rehydration to honor version pinning. */
    Optional<SagaDefinition> resolve(SagaDefinitionReference reference);

    /**
     * Resolves the highest-versioned registered definition for a given
     * {@code sagaType} — used only when *starting* a brand-new saga
     * instance, which should always begin against the latest workflow
     * version. Never used during rehydration, where {@link #resolve} against
     * the instance's pinned reference is mandatory instead.
     */
    Optional<SagaDefinition> resolveLatest(String sagaType);
}
