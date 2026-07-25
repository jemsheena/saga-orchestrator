package com.orchestrator.core.repository.support;

import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaSnapshot;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.projection.SagaInstanceView;
import com.orchestrator.core.projection.SagaInstanceViewStore;
import com.orchestrator.core.projection.SagaProjector;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaEventStore;
import com.orchestrator.core.repository.SagaInstanceRepository;
import com.orchestrator.core.repository.SagaSnapshotStore;
import com.orchestrator.core.snapshot.Snapshot;
import com.orchestrator.core.snapshot.SnapshotSerializer;
import com.orchestrator.core.snapshot.SnapshotStore;
import com.orchestrator.core.repository.TransactionRunner;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import com.orchestrator.core.snapshot.SnapshotStrategy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

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
    private final SagaSnapshotStore sagaSnapshotStore;
    private final SnapshotStore snapshotStore;
    private final SnapshotSerializer snapshotSerializer;
    private final SagaInstanceViewStore viewStore;
    private final SagaProjector projector;
    private final TransactionRunner transactionRunner;
    private final long snapshotIntervalEvents;
    private final int snapshotSchemaVersion;
    private final SnapshotStrategy snapshotStrategy;
    private final MeterRegistry meterRegistry;
    private final Counter snapshotCreatedCounter;
    private final Counter snapshotLoadedCounter;
    private final Counter snapshotCleanupCounter;
    private final Counter snapshotReplaySavedEventsCounter;
    private final Timer snapshotSaveTimer;
    private final Timer snapshotLoadTimer;

    public DefaultSagaInstanceRepository(SagaEventStore eventStore,
                                          SagaSnapshotStore snapshotStore,
                                          SagaInstanceViewStore viewStore,
                                          SagaProjector projector,
                                          TransactionRunner transactionRunner,
                                          long snapshotIntervalEvents,
                                          int snapshotSchemaVersion) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.sagaSnapshotStore = Objects.requireNonNull(snapshotStore, "snapshotStore must not be null");
        this.snapshotStore = null;
        this.snapshotSerializer = null;
        this.viewStore = Objects.requireNonNull(viewStore, "viewStore must not be null");
        this.projector = Objects.requireNonNull(projector, "projector must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        if (snapshotIntervalEvents < 1) {
            throw new IllegalArgumentException("snapshotIntervalEvents must be >= 1");
        }
        this.snapshotIntervalEvents = snapshotIntervalEvents;
        this.snapshotSchemaVersion = snapshotSchemaVersion;
        this.snapshotStrategy = SnapshotStrategy.defaultStrategy();
        this.meterRegistry = null;
        this.snapshotCreatedCounter = null;
        this.snapshotLoadedCounter = null;
        this.snapshotCleanupCounter = null;
        this.snapshotReplaySavedEventsCounter = null;
        this.snapshotSaveTimer = null;
        this.snapshotLoadTimer = null;
    }

    /**
     * New constructor accepting a SnapshotStrategy and a MeterRegistry for metrics.
     */
    public DefaultSagaInstanceRepository(SagaEventStore eventStore,
                                          SagaSnapshotStore snapshotStore,
                                          SagaInstanceViewStore viewStore,
                                          SagaProjector projector,
                                          TransactionRunner transactionRunner,
                                          SnapshotStrategy snapshotStrategy,
                                          int snapshotSchemaVersion,
                                          MeterRegistry meterRegistry) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.sagaSnapshotStore = Objects.requireNonNull(snapshotStore, "snapshotStore must not be null");
        this.snapshotStore = null;
        this.snapshotSerializer = null;
        this.viewStore = Objects.requireNonNull(viewStore, "viewStore must not be null");
        this.projector = Objects.requireNonNull(projector, "projector must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.snapshotStrategy = Objects.requireNonNull(snapshotStrategy, "snapshotStrategy must not be null");
        this.snapshotIntervalEvents = snapshotStrategy.snapshotEveryEvents();
        this.snapshotSchemaVersion = snapshotSchemaVersion;
        this.meterRegistry = meterRegistry;
        if (meterRegistry != null) {
            this.snapshotCreatedCounter = meterRegistry.counter("snapshot.created");
            this.snapshotLoadedCounter = meterRegistry.counter("snapshot.loaded");
            this.snapshotCleanupCounter = meterRegistry.counter("snapshot.cleanup.count");
            this.snapshotReplaySavedEventsCounter = meterRegistry.counter("snapshot.replay.saved.events");
            this.snapshotSaveTimer = meterRegistry.timer("snapshot.save.time");
            this.snapshotLoadTimer = meterRegistry.timer("snapshot.load.time");
        } else {
            this.snapshotCreatedCounter = null;
            this.snapshotLoadedCounter = null;
            this.snapshotCleanupCounter = null;
            this.snapshotReplaySavedEventsCounter = null;
            this.snapshotSaveTimer = null;
            this.snapshotLoadTimer = null;
        }
    }

    /**
     * New constructor accepting the generic SnapshotStore and a SnapshotSerializer.
     */
    public DefaultSagaInstanceRepository(SagaEventStore eventStore,
                                          SnapshotStore snapshotStore,
                                          SnapshotSerializer snapshotSerializer,
                                          SagaInstanceViewStore viewStore,
                                          SagaProjector projector,
                                          TransactionRunner transactionRunner,
                                          SnapshotStrategy snapshotStrategy,
                                          int snapshotSchemaVersion,
                                          MeterRegistry meterRegistry) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.snapshotStore = Objects.requireNonNull(snapshotStore, "snapshotStore must not be null");
        this.snapshotSerializer = Objects.requireNonNull(snapshotSerializer, "snapshotSerializer must not be null");
        this.sagaSnapshotStore = null;
        this.viewStore = Objects.requireNonNull(viewStore, "viewStore must not be null");
        this.projector = Objects.requireNonNull(projector, "projector must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.snapshotStrategy = Objects.requireNonNull(snapshotStrategy, "snapshotStrategy must not be null");
        this.snapshotIntervalEvents = snapshotStrategy.snapshotEveryEvents();
        this.snapshotSchemaVersion = snapshotSchemaVersion;
        this.meterRegistry = meterRegistry;
        if (meterRegistry != null) {
            this.snapshotCreatedCounter = meterRegistry.counter("snapshot.created");
            this.snapshotLoadedCounter = meterRegistry.counter("snapshot.loaded");
            this.snapshotCleanupCounter = meterRegistry.counter("snapshot.cleanup.count");
            this.snapshotReplaySavedEventsCounter = meterRegistry.counter("snapshot.replay.saved.events");
            this.snapshotSaveTimer = meterRegistry.timer("snapshot.save.time");
            this.snapshotLoadTimer = meterRegistry.timer("snapshot.load.time");
        } else {
            this.snapshotCreatedCounter = null;
            this.snapshotLoadedCounter = null;
            this.snapshotCleanupCounter = null;
            this.snapshotReplaySavedEventsCounter = null;
            this.snapshotSaveTimer = null;
            this.snapshotLoadTimer = null;
        }
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

        Optional<SagaSnapshot> snapshotOpt = Optional.empty();
        // Try generic SnapshotStore first when available
        if (snapshotStore != null && snapshotSerializer != null) {
            if (snapshotLoadTimer != null) {
                java.util.Optional<com.orchestrator.core.snapshot.Snapshot> gen = snapshotLoadTimer.record(() -> snapshotStore.loadLatest("saga", sagaId.toString()));
                if (gen != null && gen.isPresent()) {
                    try {
                        SagaSnapshot s = snapshotSerializer.deserialize(gen.get().payload(), SagaSnapshot.class);
                        snapshotOpt = Optional.of(s);
                    } catch (RuntimeException ignore) {
                        // fall through to full replay
                    }
                }
            } else {
                java.util.Optional<com.orchestrator.core.snapshot.Snapshot> gen = snapshotStore.loadLatest("saga", sagaId.toString());
                if (gen != null && gen.isPresent()) {
                    try {
                        SagaSnapshot s = snapshotSerializer.deserialize(gen.get().payload(), SagaSnapshot.class);
                        snapshotOpt = Optional.of(s);
                    } catch (RuntimeException ignore) {
                        // fall through to full replay
                    }
                }
            }
        } else if (snapshotLoadTimer != null) {
            snapshotOpt = snapshotLoadTimer.record(() -> sagaSnapshotStore.findLatest(sagaId));
        } else {
            snapshotOpt = sagaSnapshotStore.findLatest(sagaId);
        }

        if (snapshotOpt.isPresent() && snapshotOpt.get().schemaVersion() == snapshotSchemaVersion) {
            SagaSnapshot snapshot = snapshotOpt.get();
            List<SagaDomainEvent> eventsSinceSnapshot = eventStore.loadEvents(sagaId, snapshot.sequenceNo());
            if (snapshotLoadedCounter != null) snapshotLoadedCounter.increment();
            if (snapshotReplaySavedEventsCounter != null) snapshotReplaySavedEventsCounter.increment(eventsSinceSnapshot.size());
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
        save(instance, metadata, () -> {
        });
    }

    @Override
    public void save(SagaInstance instance, EventMetadata metadata, Runnable additionalTransactionalWork) {
        Objects.requireNonNull(instance, "instance must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
        Objects.requireNonNull(additionalTransactionalWork, "additionalTransactionalWork must not be null");

        List<SagaDomainEvent> newEvents = instance.pullDomainEvents();
        long versionBeforeThisBatch = instance.version() - newEvents.size();

        // ATOMIC: event append, read-model projection, and any additional
        // transactional work (e.g. outbox writes) succeed or fail together.
        transactionRunner.runInTransaction(() -> {
            if (!newEvents.isEmpty()) {
                eventStore.append(instance.sagaId(), versionBeforeThisBatch, newEvents, metadata);
                for (SagaDomainEvent event : newEvents) {
                    projector.project(event, viewStore);
                }
            }
            additionalTransactionalWork.run();
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
            if (snapshotSaveTimer != null) {
                if (snapshotStore != null && snapshotSerializer != null) {
                    snapshotSaveTimer.record(() -> snapshotStore.save(new Snapshot(
                            java.util.UUID.randomUUID(), snapshot.sagaId().toString(), "saga", snapshot.sequenceNo(), snapshotSchemaVersion,
                            snapshotSerializer.serialize(snapshot), java.time.Instant.now()
                    )));
                } else {
                    snapshotSaveTimer.record(() -> sagaSnapshotStore.save(snapshot));
                }
            } else {
                if (snapshotStore != null && snapshotSerializer != null) {
                    snapshotStore.save(new Snapshot(
                            java.util.UUID.randomUUID(), snapshot.sagaId().toString(), "saga", snapshot.sequenceNo(), snapshotSchemaVersion,
                            snapshotSerializer.serialize(snapshot), java.time.Instant.now()
                    ));
                } else {
                    sagaSnapshotStore.save(snapshot);
                }
            }
            if (snapshotCreatedCounter != null) snapshotCreatedCounter.increment();

            // cleanup according to strategy if supported
            if (snapshotStrategy != null && snapshotStrategy.keepLatest() > 0) {
                try {
                    int deleted = 0;
                    if (snapshotStore != null) {
                        deleted = snapshotStore.purgeExceptLatest("saga", snapshot.sagaId().toString(), snapshotStrategy.keepLatest());
                    } else {
                        deleted = sagaSnapshotStore.purgeExceptLatest(snapshot.sagaId(), snapshotStrategy.keepLatest());
                    }
                    if (snapshotCleanupCounter != null && deleted > 0) snapshotCleanupCounter.increment(deleted);
                } catch (RuntimeException ignore) {
                    // best-effort cleanup; do not fail the caller
                }
            }
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

    @Override
    public List<SagaInstanceView> findExpiredNonTerminalSagas(int limit, Instant deadlineNow) {
        return viewStore.findExpiredNonTerminal(limit, deadlineNow);
    }
}
