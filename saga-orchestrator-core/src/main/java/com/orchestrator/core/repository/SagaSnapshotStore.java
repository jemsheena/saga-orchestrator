package com.orchestrator.core.repository;

import com.orchestrator.core.engine.SagaSnapshot;

import java.util.Optional;
import java.util.UUID;

/**
 * Persists and retrieves {@link SagaSnapshot}s — a strictly disposable
 * performance cache, never a second source of truth (see
 * {@code SagaSnapshot} javadoc and Milestone 2 architecture review Sections
 * 4/9/10). An implementation is free to store only the single latest
 * snapshot per saga and discard older ones; nothing in this system ever
 * needs more than the latest.
 */
public interface SagaSnapshotStore {

    /**
     * Persists {@code snapshot}, replacing any previous snapshot for the
     * same {@code sagaId}. Implementations should treat this as best-effort
     * from the caller's perspective — a failure here should not fail the
     * business operation that triggered it (the event append is what must
     * be durable; the snapshot is only ever an optimization).
     */
    void save(SagaSnapshot snapshot);

    /**
     * @return the most recent snapshot for {@code sagaId}, or empty if none
     *         exists (or if the caller has chosen to discard an
     *         incompatible-schema-version snapshot before it reaches here —
     *         see {@code SagaSnapshot.schemaVersion()}).
     */
    Optional<SagaSnapshot> findLatest(UUID sagaId);
}
