package com.orchestrator.postgres.serialization;

import com.orchestrator.core.event.SagaDomainEvent;

/**
 * Converts an older stored event shape into the current runtime event model.
 */
public interface EventUpcaster {

    boolean supports(String eventType, int schemaVersion, String contentType);

    SagaDomainEvent upcast(String eventType, int schemaVersion, String contentType, byte[] payload);
}
