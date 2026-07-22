package com.orchestrator.messaging.inbox;

import com.orchestrator.messaging.MessageHeaders;

/**
 * Business logic handler for an inboxed message.
 *
 * @param <T> the message payload type
 */
public interface InboxMessageHandler<T> {

    void handle(T payload, MessageHeaders headers) throws Exception;
}
