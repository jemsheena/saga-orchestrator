package com.orchestrator.postgres.serialization;

import com.orchestrator.core.event.SagaDomainEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class EventUpcasterRegistry {

    private final List<EventUpcaster> upcasters = new ArrayList<>();

    public void register(EventUpcaster upcaster) {
        this.upcasters.add(Objects.requireNonNull(upcaster, "upcaster must not be null"));
    }

    public SagaDomainEvent upcast(String eventType, int schemaVersion, String contentType, byte[] payload) {
        for (EventUpcaster upcaster : upcasters) {
            if (upcaster.supports(eventType, schemaVersion, contentType)) {
                return upcaster.upcast(eventType, schemaVersion, contentType, payload);
            }
        }
        throw new IllegalArgumentException("No upcaster registered for event type=" + eventType
                + ", schemaVersion=" + schemaVersion + ", contentType=" + contentType);
    }
}
