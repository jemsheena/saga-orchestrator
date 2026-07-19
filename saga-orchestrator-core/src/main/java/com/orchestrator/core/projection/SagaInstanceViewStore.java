package com.orchestrator.core.projection;

import java.util.Optional;
import java.util.UUID;

/**
 * The storage port {@link SagaProjector} writes read-model rows through.
 * Deliberately a separate interface from anything in
 * {@code com.orchestrator.core.repository} — the write-side repositories
 * exist to serve the event-sourced aggregate; this one exists to serve
 * queries, and conflating the two interfaces would blur exactly the
 * boundary CQRS exists to draw.
 */
public interface SagaInstanceViewStore {

    /** Inserts or replaces the row for {@code view.sagaId()}. */
    void upsert(SagaInstanceView view);

    Optional<SagaInstanceView> findById(UUID sagaId);
}
