package com.orchestrator.messaging.outbox;

/**
 * Called by {@link OutboxStore#claimAndDispatch} once per claimed record.
 * Implementations perform the actual publish (typically delegating to a
 * {@code MessagePublisher}). Throwing leaves that specific record
 * undispatched for a future poll to retry — see
 * {@link OutboxStore#claimAndDispatch} javadoc for the full per-record
 * failure-isolation contract this establishes.
 */
@FunctionalInterface
public interface OutboxDispatcher {
    void dispatch(OutboxRecord record) throws Exception;
}
