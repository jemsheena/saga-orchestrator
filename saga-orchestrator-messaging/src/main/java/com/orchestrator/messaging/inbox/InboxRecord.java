package com.orchestrator.messaging.inbox;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Durable inbox metadata for an incoming message.
 */
public final class InboxRecord {

    private final UUID messageId;
    private final String consumer;
    private final String topic;
    private final String partitionKey;
    private final Instant receivedAt;
    private final Instant processedAt;
    private final InboxStatus status;
    private final int retryCount;
    private final String lastFailure;
    private final Instant lastAttempt;
    private final Instant nextRetryTime;
    private final byte[] payload;
    private final com.orchestrator.messaging.MessageHeaders headers;

    public InboxRecord(UUID messageId,
                       String consumer,
                       String topic,
                       String partitionKey,
                       Instant receivedAt,
                       Instant processedAt,
                       InboxStatus status,
                       int retryCount,
                       String lastFailure,
                       Instant lastAttempt,
                       Instant nextRetryTime,
                       byte[] payload,
                       com.orchestrator.messaging.MessageHeaders headers) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.partitionKey = Objects.requireNonNull(partitionKey, "partitionKey must not be null");
        this.receivedAt = Objects.requireNonNull(receivedAt, "receivedAt must not be null");
        this.processedAt = processedAt;
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.retryCount = retryCount;
        this.lastFailure = lastFailure;
        this.lastAttempt = lastAttempt;
        this.nextRetryTime = nextRetryTime;
        this.payload = payload;
        this.headers = headers;
    }

    public UUID messageId() {
        return messageId;
    }

    public String consumer() {
        return consumer;
    }

    public String topic() {
        return topic;
    }

    public String partitionKey() {
        return partitionKey;
    }

    public Instant receivedAt() {
        return receivedAt;
    }

    public Instant processedAt() {
        return processedAt;
    }

    public InboxStatus status() {
        return status;
    }

    public int retryCount() {
        return retryCount;
    }

    public String lastFailure() { return lastFailure; }

    public Instant lastAttempt() { return lastAttempt; }

    public Instant nextRetryTime() { return nextRetryTime; }

    public byte[] payload() { return payload; }

    public com.orchestrator.messaging.MessageHeaders headers() { return headers; }
}
