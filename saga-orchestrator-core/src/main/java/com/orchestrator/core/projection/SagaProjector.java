package com.orchestrator.core.projection;

import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.event.CompensationStepCompleted;
import com.orchestrator.core.event.SagaCompensationStarted;
import com.orchestrator.core.event.SagaCompleted;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.event.SagaFailed;
import com.orchestrator.core.event.SagaStarted;
import com.orchestrator.core.event.SagaTimedOut;
import com.orchestrator.core.event.StepCompleted;
import com.orchestrator.core.event.StepFailed;

import java.time.Duration;
import java.util.UUID;

/**
 * Translates a single {@link SagaDomainEvent} into a mutation of the
 * {@link SagaInstanceViewStore} read model.
 *
 * <p><b>Deliberately transaction-boundary-agnostic — this is the whole
 * point of this class, per Milestone 2 architecture review Section 2:</b>
 * this class has no opinion about, and no dependency on, whether it's being
 * called inside the same database transaction as an event append (this
 * milestone's synchronous CQRS choice) or by an independent Kafka consumer
 * processing events minutes later (Milestone 3's likely eventual evolution).
 * It only needs a {@code SagaDomainEvent} and a place to write the resulting
 * view. Migrating from synchronous to asynchronous projection later is a
 * change to <i>who calls this class and when</i> — never to this class
 * itself.
 *
 * <p>Stateless and trivially safe to share across threads — holds no fields.
 */
public final class SagaProjector {

    public void project(SagaDomainEvent event, SagaInstanceViewStore store) {
        switch (event) {
            case SagaStarted e -> store.upsert(new SagaInstanceView(
                    e.sagaId(), e.definitionReference().sagaType(), SagaState.STARTED,
                    0, e.occurredAt(), null, null, null, e.occurredAt(), null));

            case StepCompleted e -> {
                SagaInstanceView current = requireView(store, e.sagaId());
                store.upsert(new SagaInstanceView(
                        current.sagaId(), current.sagaType(), SagaState.STEP_COMPLETED,
                        e.stepIndex() + 1, current.startedAt(), null, null, current.lastError(),
                        e.occurredAt(), current.timeoutExpiredAt()));
            }

            case SagaCompleted e -> {
                SagaInstanceView current = requireView(store, e.sagaId());
                store.upsert(new SagaInstanceView(
                        current.sagaId(), current.sagaType(), SagaState.COMPLETED,
                        current.currentStepIndex(), current.startedAt(), e.occurredAt(),
                        durationMillis(current.startedAt(), e.occurredAt()), null,
                        e.occurredAt(), null));
            }

            case StepFailed e -> {
                // Informational only, same as SagaInstance.apply()'s treatment — carries the
                // failure reason forward into the view, but the state itself doesn't change
                // here; whichever event follows (SagaFailed or SagaCompensationStarted) does that.
                SagaInstanceView current = requireView(store, e.sagaId());
                store.upsert(new SagaInstanceView(
                        current.sagaId(), current.sagaType(), current.state(),
                        current.currentStepIndex(), current.startedAt(), current.completedAt(),
                        current.durationMs(), e.reason(), e.occurredAt(), current.timeoutExpiredAt()));
            }

            case SagaCompensationStarted e -> {
                SagaInstanceView current = requireView(store, e.sagaId());
                store.upsert(new SagaInstanceView(
                        current.sagaId(), current.sagaType(), SagaState.COMPENSATING,
                        current.currentStepIndex(), current.startedAt(), null, null, current.lastError(),
                        e.occurredAt(), current.timeoutExpiredAt()));
            }

            case CompensationStepCompleted e -> {
                // No dashboard-visible field changes as each individual compensation
                // step completes — the saga is already shown as COMPENSATING, and stays
                // that way until the terminal SagaFailed event. Explicitly a no-op branch
                // rather than an omitted case, so the exhaustive switch documents that
                // this was a deliberate choice, not a missed event type.
                SagaInstanceView current = requireView(store, e.sagaId());
                store.upsert(new SagaInstanceView(
                        current.sagaId(), current.sagaType(), current.state(),
                        current.currentStepIndex(), current.startedAt(), current.completedAt(),
                        current.durationMs(), current.lastError(), e.occurredAt(), current.timeoutExpiredAt()));
            }

            case SagaFailed e -> {
                SagaInstanceView current = requireView(store, e.sagaId());
                store.upsert(new SagaInstanceView(
                        current.sagaId(), current.sagaType(), SagaState.FAILED,
                        current.currentStepIndex(), current.startedAt(), e.occurredAt(),
                        durationMillis(current.startedAt(), e.occurredAt()), current.lastError(),
                        e.occurredAt(), null));
            }

            case SagaTimedOut e -> {
                // Informational only — timeout itself doesn't change the projection state.
                // The actual state transition comes from the following event
                // (SagaFailed or SagaCompensationStarted), just like StepFailed.
                SagaInstanceView current = requireView(store, e.sagaId());
                store.upsert(new SagaInstanceView(
                        current.sagaId(), current.sagaType(), current.state(),
                        current.currentStepIndex(), current.startedAt(), current.completedAt(),
                        current.durationMs(), current.lastError(), e.occurredAt(), current.timeoutExpiredAt()));
            }
        }
    }

    private SagaInstanceView requireView(SagaInstanceViewStore store, UUID sagaId) {
        return store.findById(sagaId).orElseThrow(() -> new IllegalStateException(
                "Received a projection event for saga " + sagaId + " with no existing view row. "
                        + "SagaStarted must always be the first event projected for any saga — "
                        + "this indicates events are being projected out of order, or the view was "
                        + "corrupted/partially deleted."));
    }

    private long durationMillis(java.time.Instant startedAt, java.time.Instant endedAt) {
        return Duration.between(startedAt, endedAt).toMillis();
    }
}
