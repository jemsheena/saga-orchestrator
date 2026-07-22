package com.orchestrator.messaging.inbox;

import com.orchestrator.messaging.MessageHeaders;

/**
 * Business logic handler for an inboxed message.
 *
 * @param <T> the message payload type
 */
public interface InboxMessageHandler<T> {

    /**
     * Processes a single inbound message.
     *
     * @throws Exception if processing failed; the calling inbox processor will
     *                   mark the message failed and prevent offset commit until
     *                   the batch is retried or handled by the transport layer.
     */
    void handle(T payload, MessageHeaders headers) throws Exception;
}
