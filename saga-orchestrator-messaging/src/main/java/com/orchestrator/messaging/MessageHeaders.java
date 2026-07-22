package com.orchestrator.messaging;

import java.util.Objects;
import java.util.UUID;

/**
 * Tracing metadata carried alongside a message, independent of whatever
 * transport (Kafka, or anything else) actually delivers it. Mirrors
 * {@code core.repository.EventMetadata}'s shape deliberately — same
 * correlation/causation concept, applied at the messaging boundary instead
 * of the persistence boundary. Kept as a separate type rather than reusing
 * {@code EventMetadata} directly because this module has zero dependency on
 * {@code core} (see module-level design note in {@link MessagePublisher}) —
 * duplicating this tiny shape is a smaller cost than introducing a
 * cross-module dependency for it.
 *
 * <p>Per Milestone 3 architecture review Section 16: these travel as
 * transport-level headers (Kafka record headers), never as fields inside
 * the protobuf payload itself — correlation/causation is a cross-cutting
 * concern every message has regardless of its business shape, and belongs
 * at the envelope level, not mixed into business data.
 */
public record MessageHeaders(UUID correlationId, UUID causationId) {

    public MessageHeaders {
        Objects.requireNonNull(correlationId, "correlationId must not be null");
    }

    /** Convenience factory for a brand-new correlation root with no prior cause. */
    public static MessageHeaders newCorrelation() {
        return new MessageHeaders(UUID.randomUUID(), null);
    }
}
