package com.orchestrator.postgres;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.projection.SagaInstanceView;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaDefinitionRegistry;
import com.orchestrator.core.repository.SagaInstanceRepository;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.messaging.outbox.OutboxStore;
import com.orchestrator.messaging.proto.SagaReply;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Periodically scans for sagas whose timeout deadline has passed and triggers
 * compensation via the aggregate's {@code handleTimeout()} method, exactly
 * mirroring the {@link com.orchestrator.core.engine.SagaOrchestrator}'s
 * pattern of load → advance → save → publish-compensation-if-needed.
 *
 * <p><b>Design: SELECT FOR UPDATE SKIP LOCKED</b> — the underlying repository
 * query uses database-level row locking to allow multiple scheduler instances
 * to safely divide the expired-saga workload without coordination. Each
 * scheduler replica claims a batch atomically, processes it, and releases the
 * locks when committed — other replicas then see those rows as no longer
 * claimed and can process them if needed. This is why {@code findExpiredNonTerminalSagas}
 * uses {@code FOR UPDATE SKIP LOCKED} in its SQL: it ensures parallel
 * schedulers never duplicate effort on the same saga.
 *
 * <p><b>Idempotency via terminal check:</b> the aggregate's
 * {@code handleTimeout()} method returns immediately without side effects if
 * the saga is already in a terminal state (COMPLETED or FAILED). This makes
 * the scheduler safe to orchestrator restart — if a scheduler crashes after
 * processing a timeout but before committing, another scheduler replica can
 * re-process it and get the same result.
 *
 * <p><b>Compensation flow:</b> when a timeout triggers compensation (via
 * {@code handleTimeout()} emitting SagaCompensationStarted), this scheduler
 * publishes a compensation marker to the outbox. The marker tells the
 * orchestrator (via Kafka, or whatever transport) that compensation is ready.
 * The orchestrator then processes compensation steps via its normal reply-
 * handling loop — the scheduler itself does not drive step-by-step
 * compensation, only the initial "saga has timed out" transition.
 */
public final class SagaTimeoutScheduler {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaDefinitionRegistry sagaDefinitionRegistry;
    private final OutboxStore outboxStore;
    private final int batchSize;

    public SagaTimeoutScheduler(SagaInstanceRepository sagaInstanceRepository,
                                SagaDefinitionRegistry sagaDefinitionRegistry,
                                OutboxStore outboxStore,
                                int batchSize) {
        this.sagaInstanceRepository = Objects.requireNonNull(sagaInstanceRepository, "sagaInstanceRepository must not be null");
        this.sagaDefinitionRegistry = Objects.requireNonNull(sagaDefinitionRegistry, "sagaDefinitionRegistry must not be null");
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.batchSize = batchSize;
    }

    /**
     * Scans the CQRS projection for expired, non-terminal sagas and processes
     * each one via {@code handleTimeout()}. Called periodically by a scheduler
     * (Spring's {@code @Scheduled}, a timer thread, etc.) — this class itself
     * is agnostic to how/when it's invoked.
     *
     * <p>Returns the number of sagas successfully processed in this batch. A
     * saga may fail (e.g., definition not found) and be skipped — those
     * failures are logged but do not prevent other sagas in the batch from
     * being processed.
     *
     * <p>Safe to call concurrently from multiple scheduler replicas — the
     * database-level {@code FOR UPDATE SKIP LOCKED} ensures each saga is
     * claimed by exactly one replica per invocation.
     */
    public int processBatch() {
        Instant now = Instant.now();
        List<SagaInstanceView> expiredViews = sagaInstanceRepository.findExpiredNonTerminalSagas(batchSize, now);

        int processed = 0;
        for (SagaInstanceView view : expiredViews) {
            try {
                if (processExpiredSaga(view.sagaId())) {
                    processed++;
                }
            } catch (Exception e) {
                // Per-saga failure isolation: one saga's error doesn't prevent
                // the next saga in the batch from being processed. A real production
                // system would route this to structured logging and alerting.
                System.err.println("[ERROR] Timeout processing failed for saga " + view.sagaId() + ": " + e);
            }
        }
        return processed;
    }

    /**
     * Loads a specific saga, calls {@code handleTimeout()}, saves it, and
     * publishes a compensation marker if compensation was triggered.
     *
     * @return true if the saga was successfully processed, false if it could
     *         not be loaded or the definition could not be resolved (logged
     *         but not rethrown)
     */
    private boolean processExpiredSaga(UUID sagaId) {
        SagaInstance instance = sagaInstanceRepository.findById(sagaId)
                .orElseThrow(() -> new IllegalStateException(
                        "Expired saga view exists but saga instance cannot be loaded: " + sagaId));

        SagaDefinition definition = sagaDefinitionRegistry
                .resolve(instance.definitionReference())
                .orElseThrow(() -> new IllegalStateException(
                        "Saga definition not found for saga " + sagaId
                                + " with reference " + instance.definitionReference()));

        // Call the aggregate's business method. If the saga is already terminal,
        // this is a no-op (idempotent). If it's not terminal, it emits events
        // that move the saga toward a terminal state.
        instance.handleTimeout(definition);

        // Persist the events emitted by handleTimeout(). Uses optimistic
        // concurrency — if another writer appended to this saga since we loaded
        // it, this will throw ConcurrencyConflictException, and the scheduler
        // will retry on the next polling cycle (the saga will still be in the
        // expired list until it reaches a terminal state or the timeout policy
        // changes).
        sagaInstanceRepository.save(instance, new EventMetadata(UUID.randomUUID(), null));

        // If timeout triggered compensation, publish a marker to the outbox
        // so the orchestrator/Kafka consumer knows to start compensation steps.
        // This is identical to SagaOrchestrator.publishCompensationIfNeeded().
        if (instance.state() == SagaState.COMPENSATING) {
            publishCompensationMarker(instance);
        }

        return true;
    }

    /**
     * Publishes a compensation marker to the outbox, following the same
     * pattern as {@code SagaOrchestrator.publishCompensationIfNeeded()}.
     *
     * <p>The marker is a minimal record — the orchestrator and participants
     * don't need the timeout details, only the fact that compensation is
     * required. A real implementation might include the timeout deadline,
     * step index, etc., for observability, but the minimum viable payload
     * is sufficient for orchestration to proceed.
     */
    private void publishCompensationMarker(SagaInstance instance) {
        // Construct a minimal marker. In a production system, you might want
        // to include more context (timeout deadline, which step timed out, etc.)
        // for observability. For now, an empty byte[] is sufficient for the
        // orchestrator to know "compensation is required for this saga."
        SagaReply marker = SagaReply.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSagaId(instance.sagaId().toString())
                .setOutcome(SagaReply.Outcome.FAILURE)
                .setReason("Saga execution exceeded timeout deadline")
                .build();

        outboxStore.append(new OutboxRecord(
                UUID.randomUUID(),
                "saga.compensation.v1",
                instance.sagaId().toString(),
                "SagaTimeoutNotification",
                marker.toByteArray(),
                UUID.randomUUID(), // new correlation ID for timeout event
                null, // no causation ID (this is spontaneous, not driven by a request)
                Instant.now()));
    }
}
