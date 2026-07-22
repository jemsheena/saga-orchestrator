package com.orchestrator.messaging;

/**
 * A transport-agnostic, lifecycle-managed message subscription.
 *
 * <p><b>At-least-once delivery contract:</b> an implementation must only
 * commit/acknowledge a message's position AFTER {@link MessageHandler#handle}
 * returns successfully. If the handler throws, the message must remain
 * redeliverable — this is what makes at-least-once (not at-most-once) the
 * guarantee, per Milestone 3 architecture review Section 9's reasoning:
 * duplicate delivery is an accepted, designed-for possibility (handled by
 * the Inbox pattern), but silently losing a message is not acceptable under
 * any circumstance this system tolerates.
 */
public interface MessageConsumer extends AutoCloseable {

    /** Begins consuming on a dedicated thread (or thread pool); returns immediately. */
    void start();

    /** Stops consuming and releases underlying resources. Safe to call more than once. */
    @Override
    void close();
}
