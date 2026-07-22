package com.orchestrator.core.repository.support;

import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.projection.SagaInstanceView;
import com.orchestrator.core.projection.SagaInstanceViewStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Test-only in-memory {@link SagaInstanceViewStore}. */
public final class InMemorySagaInstanceViewStore implements SagaInstanceViewStore {

    private final Map<UUID, SagaInstanceView> rows = new ConcurrentHashMap<>();

    @Override
    public void upsert(SagaInstanceView view) {
        rows.put(view.sagaId(), view);
    }

    @Override
    public Optional<SagaInstanceView> findById(UUID sagaId) {
        return Optional.ofNullable(rows.get(sagaId));
    }

    @Override
    public List<SagaInstanceView> findExpiredNonTerminal(int limit, Instant deadlineNow) {
        return rows.values().stream()
                .filter(view -> view.state() != SagaState.COMPLETED && view.state() != SagaState.FAILED)
                .filter(view -> view.timeoutExpiredAt() != null && view.timeoutExpiredAt().isBefore(deadlineNow))
                .sorted((a, b) -> a.timeoutExpiredAt().compareTo(b.timeoutExpiredAt()))
                .limit(limit)
                .toList();
    }
}

