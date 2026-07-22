package com.orchestrator.core.projection;

import java.time.Instant;
import java.util.List;
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

    /**
     * Finds all non-terminal saga instances eligible for timeout processing.
     * Returns a list of view rows for sagas where:
     * - state is not COMPLETED or FAILED (i.e., currently STARTED, STEP_COMPLETED, or COMPENSATING)
     * - timeoutExpiredAt is not null AND is strictly before {@code deadlineNow}
     *
     * <p>Results are ordered by timeoutExpiredAt ascending (soonest-expired first),
     * and limited to the specified {@code limit} to support batched processing.
     *
     * <p>Database implementations should use {@code SELECT ... FOR UPDATE SKIP LOCKED}
     * to allow multiple scheduler instances to divide the work without coordination.
     *
     * @param limit the maximum number of rows to return
     * @param deadlineNow the cutoff instant; only sagas with timeoutExpiredAt < deadlineNow are returned
     * @return a list of expired, non-terminal saga views, ordered by timeoutExpiredAt ascending;
     *         empty list if none found
     */
    List<SagaInstanceView> findExpiredNonTerminal(int limit, Instant deadlineNow);
}
