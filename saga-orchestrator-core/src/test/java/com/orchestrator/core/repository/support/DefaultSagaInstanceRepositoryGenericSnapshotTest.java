package com.orchestrator.core.repository.support;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.engine.SagaInstance;
import com.orchestrator.core.engine.SagaSnapshot;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaEventStore;
import com.orchestrator.core.repository.TransactionRunner;
import com.orchestrator.core.snapshot.Snapshot;
import com.orchestrator.core.snapshot.SnapshotSerializer;
import com.orchestrator.core.snapshot.SnapshotStore;
import com.orchestrator.core.snapshot.SnapshotStrategy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultSagaInstanceRepositoryGenericSnapshotTest {

    static final SagaDefinition DEF = SagaDefinition.builder("TestSaga")
            .addStep(new SagaStep("s1", "c1", "r1"))
            .addStep(new SagaStep("s2", "c2", "r2"))
            .addStep(new SagaStep("s3", "c3", "r3"))
            .build();

    @Test
    public void serializerRoundTrip_and_genericSnapshotLoad() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        TestSnapshotSerializer serializer = new TestSnapshotSerializer();

        SagaInstanceViewStoreStub viewStore = new SagaInstanceViewStoreStub();
        TransactionRunner tx = work -> work.run();

        var meter = new SimpleMeterRegistry();
        var repo = new DefaultSagaInstanceRepository(eventStore, snapshotStore, serializer, viewStore,
                new com.orchestrator.core.projection.SagaProjector(), tx,
                new SnapshotStrategy(3, 1), 1, meter);

        // create an instance and persist several events
        SagaInstance instance = SagaInstance.start(DEF);
        repo.save(instance, new EventMetadata(UUID.randomUUID(), UUID.randomUUID()));
        instance.completeCurrentStep(DEF, "s1");
        repo.save(instance, new EventMetadata(UUID.randomUUID(), UUID.randomUUID()));
        instance.completeCurrentStep(DEF, "s2");
        repo.save(instance, new EventMetadata(UUID.randomUUID(), UUID.randomUUID()));

        // ensure generic snapshot exists
        Optional<Snapshot> maybe = snapshotStore.loadLatest("saga", instance.sagaId().toString());
        assertTrue(maybe.isPresent());

        // now load via repository and ensure only events after snapshot are loaded
        eventStore.resetLastLoad();
        var loaded = repo.findById(instance.sagaId());
        assertTrue(loaded.isPresent());
        long after = eventStore.getLastLoadAfter();
        assertTrue(after >= 3);
    }

    @Test
    public void snapshotCleanup_keepsLastN() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        // insert 5 snapshots for same aggregate
        String aggId = "agg-1";
        for (int i = 1; i <= 5; i++) {
            store.save(new Snapshot(UUID.randomUUID(), aggId, "sometype", i, 1, ("v"+i).getBytes(StandardCharsets.UTF_8), Instant.now().plusSeconds(i)));
        }
        int deleted = store.purgeExceptLatest("sometype", aggId, 2);
        assertEquals(3, deleted);
        Optional<Snapshot> latest = store.loadLatest("sometype", aggId);
        assertTrue(latest.isPresent());
        assertEquals(5, latest.get().aggregateVersion());
    }

    @Test
    public void multiAggregateTypes_areIsolated() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        store.save(new Snapshot(UUID.randomUUID(), "1", "typeA", 1,1, "a".getBytes(), Instant.now()));
        store.save(new Snapshot(UUID.randomUUID(), "1", "typeB", 2,1, "b".getBytes(), Instant.now()));

        Optional<Snapshot> a = store.loadLatest("typeA", "1");
        Optional<Snapshot> b = store.loadLatest("typeB", "1");
        assertTrue(a.isPresent());
        assertTrue(b.isPresent());
        assertEquals(1, a.get().aggregateVersion());
        assertEquals(2, b.get().aggregateVersion());
    }

    @Disabled("Benchmark-like test; disabled in normal CI")
    @Test
    public void benchmark_replay_with_and_without_snapshot() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        // create many events
        UUID id = UUID.randomUUID();
        for (int i = 0; i < 500; i++) {
            eventStore.append(id, i, List.of(new com.orchestrator.core.event.StepCompleted(id, "step", 0, Instant.now())), new EventMetadata(UUID.randomUUID(), UUID.randomUUID()));
        }

        // measure full replay
        long t0 = System.nanoTime();
        List<SagaDomainEvent> full = eventStore.loadEvents(id);
        long fullTime = System.nanoTime() - t0;

        // create snapshot at 480
        TestSnapshotSerializer serializer = new TestSnapshotSerializer();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        SagaSnapshot snap = new SagaSnapshot(id, new com.orchestrator.core.definition.SagaDefinitionReference("TestSaga", 1), 480, com.orchestrator.core.engine.SagaState.STARTED, 0, -1, 1, Instant.now());
        snapshotStore.save(new Snapshot(UUID.randomUUID(), id.toString(), "saga", 480, 1, serializer.serialize(snap), Instant.now()));

        // measure replay after snapshot
        long t1 = System.nanoTime();
        List<SagaDomainEvent> partial = eventStore.loadEvents(id, 480);
        long partialTime = System.nanoTime() - t1;

        System.out.println("full ns=" + fullTime + " partial ns=" + partialTime);
        assertTrue(partialTime < fullTime);
    }

    // --- in-memory helpers ---
    static class InMemoryEventStore implements SagaEventStore {
        private final Map<UUID, List<SagaDomainEvent>> store = new HashMap<>();
        private long lastLoadAfter = -1;

        @Override
        public synchronized void append(UUID sagaId, long expectedVersion, List<SagaDomainEvent> newEvents, EventMetadata metadata) {
            List<SagaDomainEvent> list = store.computeIfAbsent(sagaId, k -> new ArrayList<>());
            list.addAll(newEvents);
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

        long getLastLoadAfter() { return lastLoadAfter; }
        void resetLastLoad() { lastLoadAfter = -1; }
    }

    static class InMemorySnapshotStore implements SnapshotStore {
        private final Map<String, List<Snapshot>> map = new HashMap<>();

        private String key(String type, String id) { return type + ":" + id; }

        @Override
        public synchronized void save(Snapshot snapshot) {
            map.computeIfAbsent(key(snapshot.aggregateType(), snapshot.aggregateId()), k -> new ArrayList<>()).add(snapshot);
        }

        @Override
        public synchronized Optional<Snapshot> loadLatest(String aggregateType, String aggregateId) {
            List<Snapshot> list = map.getOrDefault(key(aggregateType, aggregateId), Collections.emptyList());
            if (list.isEmpty()) return Optional.empty();
            list.sort(Comparator.comparing(Snapshot::createdAt));
            return Optional.of(list.get(list.size()-1));
        }

        @Override
        public synchronized void delete(String aggregateType, String aggregateId) { map.remove(key(aggregateType, aggregateId)); }

        @Override
        public synchronized int deleteOlderThan(String aggregateType, Instant olderThan) {
            List<Snapshot> list = map.getOrDefault(key(aggregateType, ""), Collections.emptyList());
            int before = list.size();
            list.removeIf(s -> s.createdAt().isBefore(olderThan));
            return before - list.size();
        }

        @Override
        public synchronized boolean exists(String aggregateType, String aggregateId) { return map.containsKey(key(aggregateType, aggregateId)); }

        @Override
        public synchronized int purgeExceptLatest(String aggregateType, String aggregateId, int keepLatest) {
            List<Snapshot> list = map.getOrDefault(key(aggregateType, aggregateId), new ArrayList<>());
            if (list.size() <= keepLatest) return 0;
            list.sort(Comparator.comparing(Snapshot::createdAt));
            int toDelete = list.size() - keepLatest;
            for (int i = 0; i < toDelete; i++) list.remove(0);
            return toDelete;
        }
    }

    static class TestSnapshotSerializer implements SnapshotSerializer {
        private final Map<UUID, Object> registry = new HashMap<>();

        @Override
        public byte[] serialize(Object payload) {
            UUID id = UUID.randomUUID();
            registry.put(id, payload);
            return id.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T deserialize(byte[] data, Class<T> target) {
            String s = new String(data, StandardCharsets.UTF_8);
            UUID id = UUID.fromString(s);
            return (T) registry.get(id);
        }
    }

    // minimal stubs
    static class SagaInstanceViewStoreStub implements com.orchestrator.core.projection.SagaInstanceViewStore {
        private final Map<UUID, com.orchestrator.core.projection.SagaInstanceView> map = new HashMap<>();
        @Override public void upsert(com.orchestrator.core.projection.SagaInstanceView view) { map.put(view.sagaId(), view); }
        @Override public Optional<com.orchestrator.core.projection.SagaInstanceView> findById(UUID sagaId) { return Optional.ofNullable(map.get(sagaId)); }
        @Override public List<com.orchestrator.core.projection.SagaInstanceView> findExpiredNonTerminal(int limit, Instant deadlineNow) { return Collections.emptyList(); }
    }
}
