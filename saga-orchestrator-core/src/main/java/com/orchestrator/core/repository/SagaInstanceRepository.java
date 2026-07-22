package com.orchestrator.core.repository;

import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.exception.ConcurrencyConflictException;
import com.orchestrator.core.projection.SagaInstanceView;

import java.time.Instant;
import java.util.List;
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

    /**
     * Finds all non-terminal saga instances that are eligible for timeout
     * processing. A saga is eligible if:
     * - its state is not terminal (not COMPLETED or FAILED)
     * - its lastActivityAt timestamp + timeout duration is strictly before now()
     *
     * <p>This method is intended for use by the timeout scheduler to batch-query
     * candidates for timeout handling. The scheduler is responsible for:
     * - loading each returned saga via {@code findById}
     * - calling {@code handleTimeout()} on it
     * - persisting the result via {@code save()}
     *
     * <p>The query uses {@code SELECT ... FOR UPDATE SKIP LOCKED} at the
     * database level to ensure multiple scheduler instances processing in
     * parallel can divide the work without coordination.
     *
     * @param limit      the maximum number of expired sagas to return in one batch
     * @param deadlineNow the cutoff instant — sagas with lastActivityAt < (deadlineNow - timeoutDuration)
     *                    will be returned
     * @return a list of view rows for expired, non-terminal sagas; empty if none
     */
    List<SagaInstanceView> findExpiredNonTerminalSagas(int limit, Instant deadlineNow);
}
