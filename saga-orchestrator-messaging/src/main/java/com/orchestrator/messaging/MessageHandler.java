package com.orchestrator.messaging;

/**
 * Handles one received message. Implementations are business logic —
 * "what to actually do with a SagaCommand/SagaReply" — supplied by whatever
 * wires a {@link MessageConsumer} up (Phase 2+'s application layer, not
 * this module).
 */
public interface MessageHandler {

    /**
     * @throws Exception to signal this message was NOT successfully
     *         processed. A {@link MessageConsumer} implementation must not
     *         commit the offset for a record whose handler threw — see
     *         {@link MessageConsumer} javadoc for the full at-least-once
     *         contract this establishes.
     */
    void handle(byte[] payload, MessageHeaders headers) throws Exception;
}
