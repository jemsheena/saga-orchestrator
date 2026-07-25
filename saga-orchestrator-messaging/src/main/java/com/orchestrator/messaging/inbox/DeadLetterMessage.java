package com.orchestrator.messaging.inbox;

import com.orchestrator.messaging.MessageHeaders;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Represents a permanently failed message moved to a Dead Letter Queue.
 */
public final class DeadLetterMessage {

    private final UUID messageId;
    private final Optional<String> sagaId;
    private final Optional<String> aggregateId;
    private final byte[] payload;
    private final String topic;
    private final String exceptionClass;
    private final String exceptionMessage;
    private final String stackTrace;
    private final Instant failedAt;
    private final int retryCount;
    private final MessageHeaders headers;

    public DeadLetterMessage(UUID messageId,
                             Optional<String> sagaId,
                             Optional<String> aggregateId,
                             byte[] payload,
                             String topic,
                             String exceptionClass,
                             String exceptionMessage,
                             String stackTrace,
                             Instant failedAt,
                             int retryCount,
                             MessageHeaders headers) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.sagaId = Objects.requireNonNull(sagaId, "sagaId must not be null");
        this.aggregateId = Objects.requireNonNull(aggregateId, "aggregateId must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.exceptionClass = Objects.requireNonNull(exceptionClass, "exceptionClass must not be null");
        this.exceptionMessage = exceptionMessage;
        this.stackTrace = stackTrace;
        this.failedAt = Objects.requireNonNull(failedAt, "failedAt must not be null");
        this.retryCount = retryCount;
        this.headers = Objects.requireNonNull(headers, "headers must not be null");
    }

    public UUID messageId() { return messageId; }
    public Optional<String> sagaId() { return sagaId; }
    public Optional<String> aggregateId() { return aggregateId; }
    public byte[] payload() { return payload; }
    public String topic() { return topic; }
    public String exceptionClass() { return exceptionClass; }
    public String exceptionMessage() { return exceptionMessage; }
    public String stackTrace() { return stackTrace; }
    public Instant failedAt() { return failedAt; }
    public int retryCount() { return retryCount; }
    public MessageHeaders headers() { return headers; }
}
