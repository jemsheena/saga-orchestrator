package com.orchestrator.messaging.inbox;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Deduplication and state tracking for at-least-once message delivery.
 */
public interface InboxStore {

    default boolean recordIfNew(UUID messageId) {
        return recordIfNew(messageId, "default", "", "");
    }

    default boolean recordIfNew(UUID messageId, String consumer) {
        return recordIfNew(messageId, consumer, "", "");
    }

    boolean recordIfNew(UUID messageId, String consumer, String topic, String partitionKey);

    boolean exists(UUID messageId, String consumer);

    void save(InboxRecord record);

    void markProcessed(UUID messageId, String consumer);

    void markFailed(UUID messageId, String consumer);

    Optional<InboxRecord> find(UUID messageId, String consumer);

    int cleanup(Instant olderThan, int limit);
}
