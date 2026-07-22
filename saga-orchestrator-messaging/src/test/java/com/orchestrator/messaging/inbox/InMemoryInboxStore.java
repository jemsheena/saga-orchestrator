package com.orchestrator.messaging.inbox;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * In-memory {@link InboxStore} upholding the same atomicity contract as
 * {@code PostgresInboxStore}. Test-only.
 */
public final class InMemoryInboxStore implements InboxStore {

    private final Map<String, InboxRecord> records = new HashMap<>();

    @Override
    public synchronized boolean recordIfNew(UUID messageId) {
        return recordIfNew(messageId, "default", "", "");
    }

    @Override
    public synchronized boolean recordIfNew(UUID messageId, String consumer, String topic, String partitionKey) {
        String key = compositeKey(messageId, consumer);
        if (records.containsKey(key)) {
            return false;
        }
        records.put(key, new InboxRecord(messageId, consumer, topic, partitionKey,
                Instant.now(), null, InboxStatus.RECEIVED));
        return true;
    }

    @Override
    public synchronized boolean exists(UUID messageId, String consumer) {
        return records.containsKey(compositeKey(messageId, consumer));
    }

    @Override
    public synchronized void save(InboxRecord record) {
        records.putIfAbsent(compositeKey(record.messageId(), record.consumer()), record);
    }

    @Override
    public synchronized void markProcessed(UUID messageId, String consumer) {
        String key = compositeKey(messageId, consumer);
        InboxRecord record = records.get(key);
        if (record != null) {
            records.put(key, new InboxRecord(record.messageId(), record.consumer(), record.topic(), record.partitionKey(),
                    record.receivedAt(), Instant.now(), InboxStatus.PROCESSED));
        }
    }

    @Override
    public synchronized void markFailed(UUID messageId, String consumer) {
        String key = compositeKey(messageId, consumer);
        InboxRecord record = records.get(key);
        if (record != null) {
            records.put(key, new InboxRecord(record.messageId(), record.consumer(), record.topic(), record.partitionKey(),
                    record.receivedAt(), null, InboxStatus.FAILED));
        }
    }

    @Override
    public synchronized Optional<InboxRecord> find(UUID messageId, String consumer) {
        return Optional.ofNullable(records.get(compositeKey(messageId, consumer)));
    }

    @Override
    public synchronized int cleanup(Instant olderThan, int limit) {
        return records.values().stream()
                .filter(record -> record.status() == InboxStatus.PROCESSED && record.processedAt() != null
                        && record.processedAt().isBefore(olderThan))
                .limit(limit)
                .map(record -> compositeKey(record.messageId(), record.consumer()))
                .map(records::remove)
                .map(removed -> 1)
                .reduce(0, Integer::sum);
    }

    private static String compositeKey(UUID messageId, String consumer) {
        return messageId + "@" + consumer;
    }
}
