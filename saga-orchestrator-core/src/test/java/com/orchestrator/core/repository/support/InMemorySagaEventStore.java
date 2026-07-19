package com.orchestrator.core.repository.support;

import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.exception.ConcurrencyConflictException;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaEventStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory {@link SagaEventStore} that upholds the exact same
 * concurrency contract as {@code PostgresSagaEventStore} — an append whose
 * {@code expectedVersion} doesn't match the stream's actual current length
 * throws {@link ConcurrencyConflictException}. This exists specifically to
 * test application-level logic built on top of {@code SagaEventStore}
 * (retry flows, {@code DefaultSagaInstanceRepository}) without requiring a
 * real Postgres instance — see Milestone 2, Step 6, for why testing the
 * contract this way is valid rather than a lesser substitute.
 *
 * <p>Test-only. Production code always uses {@code PostgresSagaEventStore}.
 */
public final class InMemorySagaEventStore implements SagaEventStore {

    private final Map<UUID, List<SagaDomainEvent>> streams = new ConcurrentHashMap<>();

    /**
     * Synchronized on the whole store, not per-saga — deliberately simple
     * for a test fake. This is a real, if coarse-grained, correctness
     * mechanism (not a no-op): it's what makes the two-writer race in
     * {@code VerifyStep6}/the equivalent JUnit test deterministic and
     * reproducible rather than flaky.
     */
    @Override
    public synchronized void append(UUID sagaId, long expectedVersion, List<SagaDomainEvent> newEvents, EventMetadata metadata) {
        if (newEvents == null || newEvents.isEmpty()) {
            throw new IllegalArgumentException("newEvents must not be empty");
        }
        List<SagaDomainEvent> existing = streams.computeIfAbsent(sagaId, id -> new ArrayList<>());
        long actualVersion = existing.size();
        if (actualVersion != expectedVersion) {
            throw new ConcurrencyConflictException(sagaId, expectedVersion, actualVersion);
        }
        existing.addAll(newEvents);
    }

    @Override
    public synchronized List<SagaDomainEvent> loadEvents(UUID sagaId) {
        return loadEvents(sagaId, 0L);
    }

    @Override
    public synchronized List<SagaDomainEvent> loadEvents(UUID sagaId, long afterSequenceNo) {
        List<SagaDomainEvent> existing = streams.getOrDefault(sagaId, List.of());
        if (afterSequenceNo >= existing.size()) {
            return List.of();
        }
        return List.copyOf(existing.subList((int) afterSequenceNo, existing.size()));
    }
}
