package com.orchestrator.postgres.serialization;

import com.orchestrator.core.event.SagaDomainEvent;

/**
 * Converts a {@link SagaDomainEvent} to/from the JSON payload stored in
 * {@code saga_event.payload}.
 *
 * <p><b>Why this is its own interface, not inlined into
 * {@code PostgresSagaEventStore}:</b> the event store's actual job (append-
 * only storage, ordering, concurrency control) has nothing to do with the
 * specific JSON library used. Isolating serialization here means swapping
 * the implementation — e.g. to a Jackson-backed one once Spring is
 * introduced in a later milestone — never touches the store's SQL or
 * transaction logic. See {@link HandWrittenJsonEventSerializer} for the
 * current implementation and why it exists in this specific form.
 */
public interface SagaEventSerializer {

    /** The stable, storage-facing name for this event's type — e.g. {@code "StepCompleted"}. */
    String eventType(SagaDomainEvent event);

    /**
     * The serialization schema version for this event's shape — see
     * Milestone 2 architecture review Section 3 on why this is tracked
     * separately from {@code SagaDefinition} versioning. Fixed at {@code 1}
     * for every event type until a shape actually changes.
     */
    int schemaVersion(SagaDomainEvent event);

    String serialize(SagaDomainEvent event);

    /**
     * @param eventType     as returned by {@link #eventType}, read back from storage
     * @param schemaVersion as returned by {@link #schemaVersion}, read back from storage —
     *                      an implementation supporting multiple historical shapes for the
     *                      same eventType would branch on this to "upcast" older payloads;
     *                      this milestone's implementation only ever has one shape per type
     * @param json          the raw stored payload
     */
    SagaDomainEvent deserialize(String eventType, int schemaVersion, String json);
}
