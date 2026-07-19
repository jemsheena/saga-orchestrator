package com.orchestrator.core.repository;

import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.exception.ConcurrencyConflictException;

import java.util.Optional;
import java.util.UUID;

/**
 * The single interface application code (a REST controller, a Kafka
 * consumer — none of which exist yet) actually depends on to load and
 * persist sagas. Composes {@link SagaEventStore}, {@link SagaSnapshotStore},
 * and {@link SagaDefinitionRegistry} internally so callers never need to
 * know a snapshot or a raw event stream exists at all — see Milestone 2
 * architecture review, Section 6.
 */
public interface SagaInstanceRepository {

    /**
     * Loads and fully reconstructs a saga instance, transparently using the
     * latest snapshot plus subsequent events when available, or full replay
     * from event 0 otherwise (see {@code SagaInstance.reconstructFromSnapshot}
     * vs. {@code SagaInstance.reconstruct}, and architecture review Section 5's
     * sequence diagram — this method is what performs exactly that sequence).
     *
     * @return the reconstructed instance, or empty if no saga with this ID
     *         has ever been started
     */
    Optional<SagaInstance> findById(UUID sagaId);

    /**
     * Persists whatever pending domain events {@code instance} is currently
     * holding: pulls them via {@code instance.pullDomainEvents()}, appends
     * them to the event store under optimistic concurrency control, updates
     * the {@code saga_instance_view} read-model projection in the same
     * transaction (see architecture review Section 2 — synchronous CQRS
     * projection for this milestone), and persists a new snapshot if the
     * configured event-count threshold has been crossed.
     *
     * <p>A no-op if {@code instance} has no pending events — calling this
     * after loading an instance but making no business-method calls on it
     * is always safe and cheap.
     *
     * @throws ConcurrencyConflictException if another writer appended to
     *         this saga since {@code instance} was loaded — see that
     *         exception's javadoc for the correct recovery (reload and retry,
     *         not blindly retry the same call)
     */
    void save(SagaInstance instance, EventMetadata metadata);
}
