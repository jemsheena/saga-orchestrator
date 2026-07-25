package com.orchestrator.core.repository.support;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.projection.SagaInstanceViewStore;
import com.orchestrator.core.projection.SagaProjector;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaEventStore;
import com.orchestrator.core.repository.SagaSnapshotStore;
import com.orchestrator.core.repository.TransactionRunner;
import com.orchestrator.core.snapshot.SnapshotStrategy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultSagaInstanceRepositorySnapshotTest {

    static final SagaDefinition DEF = SagaDefinition.builder("TestSaga")
            .addStep(new SagaStep("s1", "c1", "r1"))
            .addStep(new SagaStep("s2", "c2", "r2"))
            .addStep(new SagaStep("s3", "c3", "r3"))
            .build();

    @Test
    public void snapshotCreatedAndUsedForFastPath() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        InMemorySagaSnapshotStore snapshotStore = new InMemorySagaSnapshotStore();
        SagaInstanceViewStore viewStore = new com.orchestrator.core.repository.support.InMemorySagaInstanceViewStore();
        SagaProjector projector = new com.orchestrator.core.projection.SagaProjector();
        TransactionRunner tx = new com.orchestrator.core.repository.support.ImmediateTransactionRunner();

        var meter = new SimpleMeterRegistry();
        var repo = new DefaultSagaInstanceRepository(eventStore, snapshotStore, viewStore, projector, tx,
                new SnapshotStrategy(3,1), 1, meter);

        SagaInstance instance = SagaInstance.start(DEF);
        // initial save (contains SagaStarted)
        repo.save(instance, new EventMetadata(UUID.randomUUID(), UUID.randomUUID()));

        // perform two more step completions and saves to reach 3 events
        instance.completeCurrentStep(DEF, "s1");
        repo.save(instance, new EventMetadata(UUID.randomUUID(), UUID.randomUUID()));
        instance.completeCurrentStep(DEF, "s2");
        repo.save(instance, new EventMetadata(UUID.randomUUID(), UUID.randomUUID()));

        // snapshot should exist
        var maybe = snapshotStore.findLatest(instance.sagaId());
        assertTrue(maybe.isPresent(), "snapshot should have been created");
        long seq = maybe.get().sequenceNo();
        assertTrue(seq >= 3, "snapshot sequence should be at or beyond 3");

        // Now load via repository and ensure EventStore.loadEvents was called with afterSequenceNo == snapshot.sequenceNo()
        eventStore.resetLastLoad();
        var loaded = repo.findById(instance.sagaId());
        assertTrue(loaded.isPresent());
        assertEquals(seq, eventStore.getLastLoadAfter());
    }

    // --- simple in-memory test doubles ---
    static class InMemoryEventStore implements SagaEventStore {
        private final Map<UUID, List<SagaDomainEvent>> store = new HashMap<>();
        private final Map<UUID, Long> versions = new HashMap<>();
        private long lastLoadAfter = -1;

        @Override
        public synchronized void append(UUID sagaId, long expectedVersion, List<SagaDomainEvent> newEvents, EventMetadata metadata) {
            Objects.requireNonNull(newEvents);
            if (newEvents.isEmpty()) throw new IllegalArgumentException("newEvents empty");
            List<SagaDomainEvent> list = store.computeIfAbsent(sagaId, k -> new ArrayList<>());
            long current = list.size();
            if (current != expectedVersion) throw new RuntimeException("concurrency");
            list.addAll(newEvents);
            versions.put(sagaId, (long) list.size());
        }

        @Override
        public synchronized List<SagaDomainEvent> loadEvents(UUID sagaId) {
            return List.copyOf(store.getOrDefault(sagaId, Collections.emptyList()));
        }

        @Override
        public synchronized List<SagaDomainEvent> loadEvents(UUID sagaId, long afterSequenceNo) {
            this.lastLoadAfter = afterSequenceNo;
            List<SagaDomainEvent> all = store.getOrDefault(sagaId, Collections.emptyList());
            if (afterSequenceNo >= all.size()) return Collections.emptyList();
            return List.copyOf(all.subList((int) afterSequenceNo, all.size()));
        }

        void resetLastLoad() { lastLoadAfter = -1; }
        long getLastLoadAfter() { return lastLoadAfter; }
    }

    static class InMemorySagaSnapshotStore implements SagaSnapshotStore {
        private final Map<UUID, com.orchestrator.core.engine.SagaSnapshot> map = new HashMap<>();

        @Override
        public synchronized void save(com.orchestrator.core.engine.SagaSnapshot snapshot) {
            map.put(snapshot.sagaId(), snapshot);
        }

        @Override
        public synchronized Optional<com.orchestrator.core.engine.SagaSnapshot> findLatest(UUID sagaId) {
            return Optional.ofNullable(map.get(sagaId));
        }

        @Override
        public int purgeExceptLatest(UUID sagaId, int keepLatest) {
            // in-memory store only keeps latest
            return 0;
        }
    }
}
