package com.orchestrator.messaging.outbox;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A durable, not-yet-confirmed-published message, captured atomically
 * alongside whatever business write required it — this is the Outbox
 * pattern's entire mechanism (see Milestone 3 architecture review Section 8,
 * and the Milestone 2 architecture review's original "there's no second
 * system yet" reasoning for why Outbox was deliberately deferred until now).
 *
 * <p><b>Known caveat, stated rather than hidden:</b> this is a record with a
 * {@code byte[]} field, so the compiler-generated {@code equals}/{@code hashCode}
 * use reference equality for {@code payload}, not content equality
 * ({@code Arrays.equals}). Not overridden here because nothing in this
 * codebase compares {@code OutboxRecord} instances for equality or uses them
 * as hash keys — if a future use case needs that, override both explicitly
 * at that point rather than guessing at the right semantics now.
 *
 * @param outboxId      identity of this outbox row
 * @param topic         destination Kafka topic
 * @param messageKey    partition key (the saga ID, per architecture review Section 5)
 * @param messageType   human-readable type name (e.g. "SagaCommand") - observability only, not parsed
 * @param payload       serialized message bytes (protobuf {@code toByteArray()} output)
 * @param correlationId propagated into {@link com.orchestrator.messaging.MessageHeaders} at publish time
 * @param causationId   propagated into {@link com.orchestrator.messaging.MessageHeaders} at publish time; nullable
 * @param createdAt     when this row was written
 */
public record OutboxRecord(
        UUID outboxId,
        String topic,
        String messageKey,
        String messageType,
        byte[] payload,
        UUID correlationId,
        UUID causationId,
        Instant createdAt
) {
    public OutboxRecord {
        Objects.requireNonNull(outboxId, "outboxId must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(messageKey, "messageKey must not be null");
        Objects.requireNonNull(messageType, "messageType must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
    }
}
