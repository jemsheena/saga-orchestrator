package com.orchestrator.messaging.inbox;

import com.orchestrator.messaging.MessageHeaders;

/**
 * Publishes permanently failed messages to a Dead Letter Queue.
 */
public interface DeadLetterPublisher {

    void publish(DeadLetterMessage message, MessageHeaders headers);

}
