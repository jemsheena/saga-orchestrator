package com.orchestrator.messaging.inbox;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * In-memory {@link InboxStore} upholding the same atomicity contract as
 * {@code PostgresInboxStore} — {@link #recordIfNew} is synchronized so
 * "check and record" is genuinely one indivisible operation, mirroring what
 * Postgres's {@code INSERT ... ON CONFLICT DO NOTHING} + update-count check
 * achieves via the database's own primary-key uniqueness. Test-only.
 */
public final class InMemoryInboxStore implements InboxStore {

    private final Set<UUID> seen = new HashSet<>();

    @Override
    public synchronized boolean recordIfNew(UUID messageId) {
        return seen.add(messageId);
    }

    @Override
    public synchronized boolean recordIfNew(UUID messageId, String consumer, String topic, String partitionKey) {
        return seen.add(messageId);
    }

    @Override
    public synchronized boolean exists(UUID messageId, String consumer) {
        return seen.contains(messageId);
    }

    @Override
    public synchronized void save(InboxRecord record) {
        seen.add(record.messageId());
    }

    @Override
    public synchronized void markProcessed(UUID messageId, String consumer) {
        // no-op for in-memory test store
    }

    @Override
    public synchronized void markFailed(UUID messageId, String consumer) {
        // no-op for in-memory test store
    }

    @Override
    public synchronized java.util.Optional<InboxRecord> find(UUID messageId, String consumer) {
        return seen.contains(messageId)
                ? java.util.Optional.of(new InboxRecord(messageId, consumer, "", "", java.time.Instant.now(), java.time.Instant.now(), InboxStatus.PROCESSED))
                : java.util.Optional.empty();
    }

    @Override
    public synchronized int cleanup(java.time.Instant olderThan, int limit) {
        return 0;
    }
}
