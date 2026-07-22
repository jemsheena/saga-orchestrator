package com.orchestrator.messaging.inbox;

/**
 * The lifecycle state of an inbox record.
 */
public enum InboxStatus {
    RECEIVED,
    PROCESSED,
    FAILED
}
