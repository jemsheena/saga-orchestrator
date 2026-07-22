package com.orchestrator.messaging.inbox;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Deduplication and state tracking for at-least-once message delivery.
 */
/**
 * Deduplication and state tracking for messages that may be delivered more than once.
 *
 * <p>Implementations must guarantee {@link #recordIfNew(UUID, String, String, String)} is
 * atomic and safe under concurrent delivery/redelivery races. The inbox is the
 * application-side idempotence guard for at-least-once transports such as Kafka.
 */
public interface InboxStore {

    /**
     * Record the given message id if it has not been seen by the default consumer.
     *
     * @return {@code true} if this is the first time the message id has been recorded
     */
    default boolean recordIfNew(UUID messageId) {
        return recordIfNew(messageId, "default", "", "");
    }

    /**
     * Record the given message id if it has not been seen by the named consumer.
     *
     * @return {@code true} if this is the first time the message id has been recorded
     */
    default boolean recordIfNew(UUID messageId, String consumer) {
        return recordIfNew(messageId, consumer, "", "");
    }

    /**
     * Atomically records an inbox entry for the supplied message id and consumer.
     *
     * @param messageId the unique business message id used for deduplication
     * @param consumer the logical consumer identity, allowing separate inbox semantics per consumer
     * @param topic the topic or stream name the message arrived on
     * @param partitionKey the partition key used for the message
     * @return {@code true} if this is the first time the message was seen for this consumer;
     *         {@code false} if a previous delivery has already been recorded
     */
    boolean recordIfNew(UUID messageId, String consumer, String topic, String partitionKey);

    /**
     * Returns {@code true} if an inbox record exists for the specified message id and consumer.
     */
    boolean exists(UUID messageId, String consumer);

    /**
     * Persist a complete inbox record.
     */
    void save(InboxRecord record);

    /**
     * Mark the inbox entry as successfully processed.
     */
    void markProcessed(UUID messageId, String consumer);

    /**
     * Mark the inbox entry as failed.
     */
    void markFailed(UUID messageId, String consumer);

    /**
     * Find the inbox record for the specified message id and consumer.
     */
    Optional<InboxRecord> find(UUID messageId, String consumer);

    /**
     * Remove processed inbox records older than the supplied cutoff time.
     *
     * @return the number of records deleted
     */
    int cleanup(Instant olderThan, int limit);
}
