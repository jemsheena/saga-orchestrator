package com.orchestrator.core.repository.support;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaDefinitionReference;
import com.orchestrator.core.repository.SagaDefinitionRegistry;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple in-memory {@link SagaDefinitionRegistry}, backed by a
 * {@code ConcurrentHashMap}. Unlike the other {@code InMemory*} classes in
 * this codebase, this one is a legitimate default production implementation,
 * not a test fake — see Milestone 2 architecture review Section 6:
 * definitions are typically registered once at application startup and
 * rarely change, which is exactly the shape an in-memory registry serves
 * well. A database-backed registry remains a reasonable future addition if
 * hot-reloadable, undeployed-code workflow definitions ever become a
 * requirement — not needed for anything this system does today.
 */
public final class InMemorySagaDefinitionRegistry implements SagaDefinitionRegistry {

    private final ConcurrentHashMap<SagaDefinitionReference, SagaDefinition> byReference = new ConcurrentHashMap<>();

    @Override
    public void register(SagaDefinition definition) {
        Objects.requireNonNull(definition, "definition must not be null");
        SagaDefinition existing = byReference.putIfAbsent(definition.reference(), definition);
        if (existing != null) {
            throw new IllegalStateException(
                    "A definition is already registered for " + definition.reference()
                            + " — a saga instance already pinned to this reference must never see "
                            + "its step list change underneath it. Register a new version instead.");
        }
    }

    @Override
    public Optional<SagaDefinition> resolve(SagaDefinitionReference reference) {
        Objects.requireNonNull(reference, "reference must not be null");
        return Optional.ofNullable(byReference.get(reference));
    }

    @Override
    public Optional<SagaDefinition> resolveLatest(String sagaType) {
        Objects.requireNonNull(sagaType, "sagaType must not be null");
        return byReference.values().stream()
                .filter(d -> d.sagaType().equals(sagaType))
                .max(Comparator.comparingInt(SagaDefinition::version));
    }
}
