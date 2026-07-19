package com.orchestrator.core.repository.support;

import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaSnapshot;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.projection.SagaInstanceViewStore;
import com.orchestrator.core.projection.SagaProjector;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaEventStore;
import com.orchestrator.core.repository.SagaInstanceRepository;
import com.orchestrator.core.repository.SagaSnapshotStore;
import com.orchestrator.core.repository.TransactionRunner;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The default, framework-free implementation of {@link SagaInstanceRepository},
 * composing {@link SagaEventStore}, {@link SagaSnapshotStore},
 * {@link SagaInstanceViewStore}, {@link SagaProjector}, and (as of Milestone
 * 2.5) {@link TransactionRunner} — see Milestone 2 architecture review
 * Section 6, and the Milestone 2.5 write-up for why {@code TransactionRunner}
 * was added and why {@code SagaDefinitionRegistry} deliberately still isn't
 * one of this class's dependencies.
 *
 * <p><b>Milestone 2.5 correction:</b> a Milestone 2 code review found that
 * event-append and read-model projection were NOT actually atomic — each
 * opened its own database connection and committed independently, directly
 * contradicting this codebase's own documented "same-transaction" CQRS
 * design decision. {@link #save} now wraps both inside a single
 * {@link TransactionRunner#runInTransaction} call. Snapshot persistence
 * remains deliberately OUTSIDE that transaction and independently
 * exception-guarded — see {@link #maybeSnapshot} — because a snapshot is
 * purely a performance optimization and must never be able to invalidate
 * events that were already durably, successfully persisted.
 *
 * <p>Still contains zero Postgres/Spring/JPA references — {@code TransactionRunner}
 * is as framework-agnostic as every other dependency here. This class remains
 * fully unit-testable using purely in-memory fakes; see
 * {@code ImmediateTransactionRunner} for why a real transactional
 * implementation isn't needed to test this class's own logic.
 */
public final class DefaultSagaInstanceRepository implements SagaInstanceRepository {

    private final SagaEventStore eventStore;
    private final SagaSnapshotStore snapshotStore;
    private final SagaInstanceViewStore viewStore;
    private final SagaProjector projector;
    private final TransactionRunner transactionRunner;
    private final long snapshotIntervalEvents;
    private final int snapshotSchemaVersion;

    public DefaultSagaInstanceRepository(SagaEventStore eventStore,
                                          SagaSnapshotStore snapshotStore,
                                          SagaInstanceViewStore viewStore,
                                          SagaProjector projector,
                                          TransactionRunner transactionRunner,
                                          long snapshotIntervalEvents,
                                          int snapshotSchemaVersion) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.snapshotStore = Objects.requireNonNull(snapshotStore, "snapshotStore must not be null");
        this.viewStore = Objects.requireNonNull(viewStore, "viewStore must not be null");
        this.projector = Objects.requireNonNull(projector, "projector must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        if (snapshotIntervalEvents < 1) {
            throw new IllegalArgumentException("snapshotIntervalEvents must be >= 1");
        }
        this.snapshotIntervalEvents = snapshotIntervalEvents;
        this.snapshotSchemaVersion = snapshotSchemaVersion;
    }

    /**
     * Implements exactly the sequence from Milestone 2 architecture review
     * Section 5: snapshot lookup, snapshot-schema-version validation
     * (discard-and-fall-back-to-full-replay if incompatible — see
     * {@code SagaSnapshot} javadoc on snapshot invalidation), then either the
     * fast path ({@code reconstructFromSnapshot}) or the full-replay path
     * ({@code reconstruct}).
     */
    @Override
    public Optional<SagaInstance> findById(UUID sagaId) {
        Objects.requireNonNull(sagaId, "sagaId must not be null");

        Optional<SagaSnapshot> snapshotOpt = snapshotStore.findLatest(sagaId);
        if (snapshotOpt.isPresent() && snapshotOpt.get().schemaVersion() == snapshotSchemaVersion) {
            SagaSnapshot snapshot = snapshotOpt.get();
            List<SagaDomainEvent> eventsSinceSnapshot = eventStore.loadEvents(sagaId, snapshot.sequenceNo());
            return Optional.of(SagaInstance.reconstructFromSnapshot(snapshot, eventsSinceSnapshot));
        }
        // No snapshot, OR a snapshot whose schema version this code no longer
        // understands — either way, full replay from event 0 is always correct,
        // only ever slower. See architecture review Section 9 on snapshot corruption.

        List<SagaDomainEvent> allEvents = eventStore.loadEvents(sagaId);
        if (allEvents.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(SagaInstance.reconstruct(allEvents));
    }

    /**
     * Pulls {@code instance}'s pending events and, as ONE atomic transaction
     * (Milestone 2.5 fix — previously two independently-committed operations,
     * see class javadoc), appends them under optimistic concurrency control
     * and synchronously projects each into the read model. Snapshotting, if
     * a threshold boundary was crossed, happens AFTER that transaction
     * commits and is independently failure-isolated — see {@link #maybeSnapshot}.
     *
     * <p>The {@code expectedVersion} arithmetic is unchanged from Milestone 2
     * — see the original javadoc reasoning, preserved below.
     *
     * <p><b>The {@code expectedVersion} arithmetic, spelled out because it's
     * easy to get subtly wrong:</b> by the time this method runs,
     * {@code instance.version()} already reflects the NEW events — each
     * {@code recordEvent()} call inside a business method increments
     * {@code version} immediately, live, before persistence ever happens.
     * So the version the event store needs — what the stream looked like
     * BEFORE this batch — is {@code instance.version() - newEvents.size()},
     * not {@code instance.version()} itself.
     */
    @Override
    public void save(SagaInstance instance, EventMetadata metadata) {
        Objects.requireNonNull(instance, "instance must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");

        List<SagaDomainEvent> newEvents = instance.pullDomainEvents();
        if (newEvents.isEmpty()) {
            return; // no-op, per interface contract
        }

        long versionBeforeThisBatch = instance.version() - newEvents.size();

        // ATOMIC: event append and read-model projection succeed or fail together.
        // This is the actual fix for the Milestone 2 review's Critical Finding #1 -
        // previously these were two separately-committed operations. See class
        // javadoc for the consistency/availability trade-off this now enforces.
        transactionRunner.runInTransaction(() -> {
            eventStore.append(instance.sagaId(), versionBeforeThisBatch, newEvents, metadata);
            for (SagaDomainEvent event : newEvents) {
                projector.project(event, viewStore);
            }
        });

        // NOT part of the transaction above, and deliberately independently
        // guarded - see maybeSnapshot(). Fix for Critical Finding #2.
        maybeSnapshot(instance, versionBeforeThisBatch);
    }

    /**
     * Snapshots when this save crosses a multiple-of-{@code snapshotIntervalEvents}
     * boundary (integer-division comparison, correctly handles a batch that
     * jumps OVER a boundary in one call — see Milestone 2 Step 7 write-up).
     *
     * <p><b>Milestone 2.5 fix:</b> the snapshot save is now wrapped in a
     * try/catch that swallows (after reporting) any failure. This is not a
     * style preference — it's the literal contract both
     * {@code SagaSnapshotStore} and {@code PostgresSagaSnapshotStore}'s own
     * javadoc already promised ("a failure here must never fail the business
     * operation that triggered it") and which the Milestone 2 code review
     * found was NOT actually being honored by this call site. The events
     * this save() call persisted are already durably committed by the time
     * this method runs; nothing here can or should undo that.
     */
    private void maybeSnapshot(SagaInstance instance, long versionBeforeThisBatch) {
        long newVersion = instance.version();
        if (newVersion / snapshotIntervalEvents <= versionBeforeThisBatch / snapshotIntervalEvents) {
            return;
        }
        try {
            SagaSnapshot snapshot = instance.toSnapshot(snapshotSchemaVersion);
            snapshotStore.save(snapshot);
        } catch (RuntimeException e) {
            reportSnapshotFailure(instance.sagaId(), e);
        }
    }

    /**
     * Reports (without rethrowing) a snapshot persistence failure. Uses
     * {@code System.err} rather than a real logging framework because no
     * logging infrastructure has been introduced into this project yet —
     * this is an honest placeholder for the OUTPUT DESTINATION only; the
     * swallow-and-continue BEHAVIOR itself is complete and correct as
     * written, not a stand-in for missing logic. Route this to a real
     * structured logger (SLF4J/Logback, likely arriving with Spring Boot in
     * a later milestone) without changing the calling contract at all.
     */
    private void reportSnapshotFailure(UUID sagaId, RuntimeException e) {
        System.err.println("[WARN] Snapshot persistence failed for saga " + sagaId
                + " - already-committed events are unaffected; replay will simply be "
                + "slower for this saga until a future snapshot succeeds. Cause: " + e);
    }
}
