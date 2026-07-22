package com.orchestrator.messaging;

/**
 * A transport-agnostic port for publishing an already-serialized message.
 *
 * <p><b>Module-level design note, stated here since this is the module's
 * first interface:</b> {@code saga-orchestrator-messaging} has zero
 * dependency on {@code saga-orchestrator-core}. This is deliberate, not an
 * oversight — this module doesn't know what a saga, a step, or a
 * {@code SagaDomainEvent} is. It knows only "publish these bytes, with this
 * key, to this topic, with this tracing metadata" and "here is a batch of
 * bytes that arrived — hand them to a handler." That makes this module
 * genuinely reusable messaging infrastructure, not saga-specific plumbing —
 * a property worth having even though, in practice, this project only ever
 * uses it for saga commands/replies. Translating between
 * {@code SagaDomainEvent}/{@code SagaCommand} and raw bytes is an
 * application-layer concern (Phase 2+, the future {@code saga-orchestrator-api}
 * module), not this module's job.
 *
 * <p>Takes {@code byte[]} rather than a protobuf {@code Message} type
 * directly, for the same reason: this interface shouldn't need to know
 * Protobuf exists either. A caller serializes its protobuf message to bytes
 * ({@code someMessage.toByteArray()}) before calling {@link #publish}.
 */
public interface MessagePublisher {

    /**
     * Publishes {@code payload} to {@code topic}, partitioned by {@code key}
     * (see Milestone 3 architecture review Section 5 — callers should pass
     * the saga's ID as the key, to keep per-saga ordering intact).
     *
     * @throws MessagingException if the publish could not be confirmed —
     *         see that exception's javadoc for what "confirmed" means and
     *         why this method is synchronous
     */
    void publish(String topic, String key, byte[] payload, MessageHeaders headers);
}
