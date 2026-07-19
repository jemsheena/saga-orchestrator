package com.orchestrator.core.projection;

import com.orchestrator.core.engine.SagaState;

import java.time.Instant;
import java.util.UUID;

/**
 * The denormalized, query-optimized read-model shape for one saga instance —
 * what {@code saga_instance_view} holds a row of. Exists specifically to
 * answer queries the write-side event store cannot answer efficiently
 * ("all currently-FAILED sagas," dashboards, P95 duration) without replaying
 * every saga's full history on every query — see Milestone 2 architecture
 * review, Section 2, for why CQRS is justified here at all.
 *
 * <p>This is a plain value object, not an entity — a fresh instance is
 * constructed for every projection update rather than mutating one in place,
 * which keeps {@link SagaProjector} straightforwardly a pure function
 * (event + current view in, new view out).
 */
public record SagaInstanceView(
        UUID sagaId,
        String sagaType,
        SagaState state,
        int currentStepIndex,
        Instant startedAt,
        Instant completedAt,
        Long durationMs,
        String lastError
) {
}
