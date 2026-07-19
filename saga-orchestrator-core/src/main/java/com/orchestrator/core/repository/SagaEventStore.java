package com.orchestrator.core.repository;

import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.exception.ConcurrencyConflictException;

import java.util.List;
import java.util.UUID;

/**
 * The append-only event log — the single source of truth for a saga's
 * entire history. See Milestone 2 architecture review, Sections 3 and 7,
 * for the full storage and concurrency design this interface abstracts.
 *
 * <p>Deliberately minimal: this interface knows nothing about
 * {@code SagaInstance}, snapshots, or definitions — only about durably,
 * atomically, and order-preservingly storing and retrieving batches of
 * domain events per saga. Composition into a full aggregate happens one
 * layer up, in {@code SagaInstanceRepository}.
 */
public interface SagaEventStore {

    /**
     * Atomically appends {@code newEvents} to {@code sagaId}'s stream,
     * assigning them consecutive sequence numbers immediately after
     * {@code expectedVersion}.
     *
     * @param sagaId          the saga whose stream is being appended to
     * @param expectedVersion the number of events the caller believes are
     *                        already in this stream (i.e. the aggregate's
     *                        {@code version} at the moment it was loaded,
     *                        before any of {@code newEvents} were recorded)
     * @param newEvents       the events to append, in the order they occurred;
     *                        must not be empty
     * @param metadata        correlation/causation tracing data shared by this batch
     * @throws ConcurrencyConflictException if the stream's actual current
     *         version does not match {@code expectedVersion} — another
     *         writer appended to this saga first
     * @throws IllegalArgumentException if {@code newEvents} is empty
     */
    void append(UUID sagaId, long expectedVersion, List<SagaDomainEvent> newEvents, EventMetadata metadata);

    /**
     * Loads every event ever recorded for {@code sagaId}, in ascending
     * sequence order. Returns an empty list if no such saga exists — this
     * is not distinguished from "exists but has been fully compensated";
     * callers determine existence by whether the returned list is empty.
     */
    List<SagaDomainEvent> loadEvents(UUID sagaId);

    /**
     * Loads only the events recorded strictly after {@code afterSequenceNo},
     * in ascending sequence order — the fast path used alongside a snapshot
     * (see {@code SagaSnapshotStore}) to avoid replaying a stream's entire
     * history just to reconstruct its current state.
     */
    List<SagaDomainEvent> loadEvents(UUID sagaId, long afterSequenceNo);
}
