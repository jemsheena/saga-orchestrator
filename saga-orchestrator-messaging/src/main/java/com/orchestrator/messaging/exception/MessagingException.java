package com.orchestrator.messaging.exception;

/**
 * Wraps an underlying transport-level failure (a Kafka publish that
 * couldn't be confirmed, a broker connection problem, etc.). Deliberately
 * transport-neutral in name — mirrors the same naming lesson already
 * learned once in this project (see {@code PostgresAdapterException},
 * renamed in Milestone 2.5 after {@code PostgresEventStoreException} turned
 * out to be misleading once thrown from non-event-store classes). Applying
 * that lesson proactively here rather than waiting to rename it later.
 */
public class MessagingException extends RuntimeException {
    public MessagingException(String message, Throwable cause) {
        super(message, cause);
    }
}
