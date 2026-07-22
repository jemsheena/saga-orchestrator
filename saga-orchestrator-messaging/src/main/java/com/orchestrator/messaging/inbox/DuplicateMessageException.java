package com.orchestrator.messaging.inbox;

/**
 * Thrown when a message is detected as a duplicate and should not be processed again.
 */
public final class DuplicateMessageException extends RuntimeException {

    public DuplicateMessageException(String message) {
        super(message);
    }

    public DuplicateMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
