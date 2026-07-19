package com.orchestrator.core.repository.support;

import com.orchestrator.core.engine.SagaSnapshot;
import com.orchestrator.core.repository.SagaSnapshotStore;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Test-only in-memory {@link SagaSnapshotStore} — keeps only the latest per saga. */
public final class InMemorySagaSnapshotStore implements SagaSnapshotStore {

    private final Map<UUID, SagaSnapshot> latestBySagaId = new ConcurrentHashMap<>();

    @Override
    public void save(SagaSnapshot snapshot) {
        latestBySagaId.merge(snapshot.sagaId(), snapshot,
                (existing, incoming) -> incoming.sequenceNo() >= existing.sequenceNo() ? incoming : existing);
    }

    @Override
    public Optional<SagaSnapshot> findLatest(UUID sagaId) {
        return Optional.ofNullable(latestBySagaId.get(sagaId));
    }
}
