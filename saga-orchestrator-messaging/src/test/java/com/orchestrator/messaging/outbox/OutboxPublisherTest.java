package com.orchestrator.messaging.outbox;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxPublisherTest {

    @Test
    void pollOnce_dispatchesAllUndispatchedRecords_upToBatchSize() {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
        OutboxPublisher outboxPublisher = new OutboxPublisher(store, publisher, 10);

        store.append(record("topic-a", "key-1"));
        store.append(record("topic-a", "key-2"));

        int dispatchedCount = outboxPublisher.pollOnce();

        assertEquals(2, dispatchedCount);
        assertEquals(2, publisher.published().size());
        assertEquals(0, store.undispatchedCount());
    }

    @Test
    void pollOnce_respectsBatchSize_leavesRestForNextPoll() {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
        OutboxPublisher outboxPublisher = new OutboxPublisher(store, publisher, 1);

        store.append(record("topic-a", "key-1"));
        store.append(record("topic-a", "key-2"));

        int firstPoll = outboxPublisher.pollOnce();
        assertEquals(1, firstPoll);
        assertEquals(1, store.undispatchedCount());

        int secondPoll = outboxPublisher.pollOnce();
        assertEquals(1, secondPoll);
        assertEquals(0, store.undispatchedCount());
    }

    @Test
    void pollOnce_headersDerivedFromRecordCorrelationCausation() {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
        OutboxPublisher outboxPublisher = new OutboxPublisher(store, publisher, 10);

        UUID correlationId = UUID.randomUUID();
        UUID causationId = UUID.randomUUID();
        store.append(new OutboxRecord(UUID.randomUUID(), "topic-a", "key-1", "SagaCommand",
                new byte[]{1, 2, 3}, correlationId, causationId, Instant.now()));

        outboxPublisher.pollOnce();

        InMemoryMessagePublisher.PublishedMessage published = publisher.published().get(0);
        assertEquals(correlationId, published.headers().correlationId());
        assertEquals(causationId, published.headers().causationId());
    }

    @Test
    void pollOnce_oneRecordFailingPublish_doesNotBlockOthersInSameBatch() {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
        publisher.failForKeys(Set.of("key-fails"));
        OutboxPublisher outboxPublisher = new OutboxPublisher(store, publisher, 10);

        store.append(record("topic-a", "key-ok-1"));
        store.append(record("topic-a", "key-fails"));
        store.append(record("topic-a", "key-ok-2"));

        int dispatchedCount = outboxPublisher.pollOnce();

        assertEquals(2, dispatchedCount);
        assertEquals(1, store.undispatchedCount()); // the failed one remains, claimable again
        List<String> publishedKeys = publisher.published().stream()
                .map(InMemoryMessagePublisher.PublishedMessage::key).toList();
        assertTrue(publishedKeys.contains("key-ok-1"));
        assertTrue(publishedKeys.contains("key-ok-2"));
        assertTrue(!publishedKeys.contains("key-fails"));
    }

    @Test
    void pollOnce_failedRecord_isRetriedAndSucceedsOnceUnblocked() {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
        publisher.failForKeys(Set.of("key-temp-fail"));
        OutboxPublisher outboxPublisher = new OutboxPublisher(store, publisher, 10);
        store.append(record("topic-a", "key-temp-fail"));

        int firstAttempt = outboxPublisher.pollOnce();
        assertEquals(0, firstAttempt);
        assertEquals(1, store.undispatchedCount());

        publisher.failForKeys(Set.of()); // simulate the transient problem resolving
        int secondAttempt = outboxPublisher.pollOnce();
        assertEquals(1, secondAttempt);
        assertEquals(0, store.undispatchedCount());
    }

    @Test
    void pollOnce_emptyOutbox_dispatchesNothing_doesNotThrow() {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
        OutboxPublisher outboxPublisher = new OutboxPublisher(store, publisher, 10);

        assertEquals(0, outboxPublisher.pollOnce());
    }

    @Test
    void start_usesConfiguredInterval_andStopsCleanly() throws InterruptedException {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
        OutboxPublisherConfig config = new OutboxPublisherConfig(10, 10, 20);
        OutboxPublisher outboxPublisher = new OutboxPublisher(store, publisher, config);

        try {
            outboxPublisher.start();
            Thread.sleep(50);
        } finally {
            outboxPublisher.stop();
        }
    }

    private static OutboxRecord record(String topic, String key) {
        return new OutboxRecord(UUID.randomUUID(), topic, key, "SagaCommand",
                new byte[]{1, 2, 3}, UUID.randomUUID(), null, Instant.now());
    }
}
