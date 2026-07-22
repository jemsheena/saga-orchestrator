package com.orchestrator.core.engine;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaDefinitionReference;
import com.orchestrator.core.event.CompensationStepCompleted;
import com.orchestrator.core.event.SagaCompensationStarted;
import com.orchestrator.core.event.SagaCompleted;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.event.SagaFailed;
import com.orchestrator.core.event.SagaStarted;
import com.orchestrator.core.event.SagaTimedOut;
import com.orchestrator.core.event.StepCompleted;
import com.orchestrator.core.event.StepFailed;
import com.orchestrator.core.exception.DefinitionMismatchException;
import com.orchestrator.core.exception.InvalidStateTransitionException;
import com.orchestrator.core.exception.StepMismatchException;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * The aggregate root for a single, in-flight execution of a saga.
 *
 * <p><b>Milestone 1.5 change — definition by reference, not by object:</b>
 * this class no longer stores a live {@code SagaDefinition}. It stores only
 * a {@link SagaDefinitionReference} (its permanent identity pointer to a
 * specific version). Every business method that needs to know the step list
 * — {@link #completeCurrentStep}, {@link #failCurrentStep},
 * {@link #completeCompensationStep} — requires the caller to resolve and
 * pass in the {@code SagaDefinition} for that call, and validates it against
 * the pinned reference before doing anything else (see
 * {@link DefinitionMismatchException}). This mirrors exactly how Milestone 2's
 * rehydration will work: replay events, resolve the pinned definition version
 * from a registry, apply. There is no second code path for "live" vs.
 * "rehydrated" execution — both go through the same validated entry points.
 *
 * <p><b>Milestone 1.5 change — domain events:</b> every business method now
 * appends one or more {@link SagaDomainEvent}s to an internal pending queue
 * as it mutates state, rather than leaving callers to infer what happened
 * from field diffs. Call {@link #pullDomainEvents()} after invoking a
 * business method to retrieve (and clear) whatever was just recorded — this
 * is the exact shape Milestone 2's event store persists.
 *
 * <p><b>Milestone 1.5 change — step correlation:</b> {@link #completeCurrentStep}
 * and {@link #failCurrentStep} now require the caller to state which step
 * they're reporting on, and reject the call with {@link StepMismatchException}
 * if it doesn't match what the instance is actually expecting.
 *
 * <p><b>Milestone 2 change — {@code version} redefined, and corrected from
 * Milestone 1's framing:</b> Milestone 1 described this counter as "the
 * future JPA {@code @Version} column." That doesn't map cleanly onto a pure
 * event store — there's no single row to attach a version column to, only a
 * stream of events. The event-sourcing-idiomatic definition (matching how
 * EventStoreDB's and Axon's "expected version" checks work) is: <b>an
 * aggregate's version is the number of events that have been applied to
 * it</b> — i.e. its current stream length. This class now increments
 * {@code version} exactly once per event, in {@link #recordEvent} during
 * live execution and in {@link #apply} during replay — never once per
 * business-method call, since a single call can emit more than one event
 * (e.g. {@link #failCurrentStep} on the first step emits both
 * {@code StepFailed} and {@code SagaFailed}). This is stricter than
 * Milestone 1's per-call increment and fixes a latent inconsistency: under
 * the old scheme, a two-event method call only advanced the counter once,
 * silently under-counting relative to the aggregate's real event history.
 *
 * <p><b>Milestone 2 change — decide vs. apply:</b> the existing business
 * methods ({@code completeCurrentStep}, etc.) are the "decide" side —
 * they validate business rules against current state and, if legal, compute
 * and record new events. {@link #apply(SagaDomainEvent)} is the "apply" (or
 * "evolve") side — it takes an already-historical event as unquestionable
 * ground truth and mutates state to match, with <b>no validation and no new
 * event emission</b>. Replay must never re-run business rules against
 * history (today's rules might differ from what was true when the event
 * actually happened) and must never re-emit events that already happened —
 * conflating decide and apply is one of the most common event-sourcing
 * design mistakes, and keeping them as two distinct methods on this class,
 * rather than routing replay through the business methods, is what prevents
 * it here. See {@link #reconstruct(List)} and
 * {@link #reconstructFromSnapshot(SagaSnapshot, List)} for the two entry
 * points that drive {@code apply} to rebuild an instance.
 */
public final class SagaInstance {

    private final UUID sagaId;
    private final SagaDefinitionReference definitionReference;
    private final Clock clock;

    private SagaState state;
    private int currentStepIndex;
    private int compensationCursor;

    /**
     * The number of events applied to this instance so far — see the
     * Milestone 2 class-javadoc note above for why this replaced Milestone 1's
     * {@code instanceVersion} framing. This is exactly the value a repository
     * passes as {@code expectedVersion} to {@code SagaEventStore.append(...)}.
     */
    private long version;

    /**
     * Events recorded by the most recent sequence of business-method calls,
     * not yet claimed by {@link #pullDomainEvents()}. Deliberately private
     * and reachable only through that method — nothing outside this class
     * can inspect or mutate this list directly, which is what guarantees
     * "every event that happened is exactly what pullDomainEvents() returns,
     * no more, no less."
     */
    private final List<SagaDomainEvent> pendingEvents = new ArrayList<>();

    private SagaInstance(UUID sagaId, SagaDefinitionReference definitionReference, Clock clock) {
        this(sagaId, definitionReference, clock, SagaState.STARTED, 0, -1, 0L);
    }

    /**
     * Full-state constructor used exclusively by {@link #reconstructFromSnapshot}
     * to restore an instance directly from a {@link SagaSnapshot} without
     * replaying every event since the beginning of time. Private: the only
     * legal ways to obtain a {@code SagaInstance} from outside this class are
     * {@link #start}, {@link #reconstruct}, and {@link #reconstructFromSnapshot}.
     */
    private SagaInstance(UUID sagaId, SagaDefinitionReference definitionReference, Clock clock,
                          SagaState state, int currentStepIndex, int compensationCursor, long version) {
        this.sagaId = sagaId;
        this.definitionReference = definitionReference;
        this.clock = clock;
        this.state = state;
        this.currentStepIndex = currentStepIndex;
        this.compensationCursor = compensationCursor;
        this.version = version;
    }

    /**
     * Starts a new saga instance against a given definition. The instance
     * pins itself to {@code definition.reference()} — the exact version
     * passed here — for its entire lifetime; see class javadoc.
     */
    public static SagaInstance start(SagaDefinition definition) {
        return start(definition, Clock.systemUTC());
    }

    /**
     * Test/deterministic-clock seam. Package-private: application code should
     * never need to supply a clock explicitly outside of tests — production
     * code always uses wall-clock time via {@link #start(SagaDefinition)}.
     */
    static SagaInstance start(SagaDefinition definition, Clock clock) {
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(clock, "clock must not be null");

        SagaInstance instance = new SagaInstance(UUID.randomUUID(), definition.reference(), clock);
        instance.recordEvent(new SagaStarted(instance.sagaId, instance.definitionReference,
                definition.timeoutPolicy() == null ? null : definition.timeoutPolicy().timeoutDuration(),
                instance.now()));
        return instance;
    }

    /**
     * Test-only factory that preserves a caller-supplied {@code sagaId} instead
     * of generating one. Exists so tests can construct two independently-built
     * {@code SagaInstance} objects that share an identity — simulating "the
     * same saga, loaded by two different threads/requests" — without needing
     * Milestone 2's real repository/rehydration machinery to exist yet.
     * Package-private: no production code should ever choose its own sagaId.
     */
    static SagaInstance startWithId(UUID sagaId, SagaDefinition definition, Clock clock) {
        Objects.requireNonNull(sagaId, "sagaId must not be null");
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(clock, "clock must not be null");

        SagaInstance instance = new SagaInstance(sagaId, definition.reference(), clock);
        instance.recordEvent(new SagaStarted(instance.sagaId, instance.definitionReference,
                definition.timeoutPolicy() == null ? null : definition.timeoutPolicy().timeoutDuration(),
                instance.now()));
        return instance;
    }

    /**
     * Rebuilds a {@code SagaInstance} entirely from its event history, from
     * the beginning of time. {@code events} must be non-empty and its first
     * element must be a {@link SagaStarted} — that event is what supplies the
     * {@code sagaId} and {@code definitionReference} the reconstructed
     * instance is identified by, so there is no separate "which saga am I
     * rebuilding" parameter to accidentally get out of sync with the events
     * themselves.
     *
     * <p>Production code should generally prefer
     * {@link #reconstructFromSnapshot} when a snapshot is available, since
     * this method's cost is O(total events ever recorded for this saga).
     * This method remains the correct (and only) path when no snapshot
     * exists yet, and is what {@code reconstructFromSnapshot} itself falls
     * back to internally.
     *
     * @throws IllegalArgumentException if {@code events} is empty or its
     *         first element is not a {@code SagaStarted}
     */
    public static SagaInstance reconstruct(List<SagaDomainEvent> events) {
        return reconstruct(events, Clock.systemUTC());
    }

    /** Test/deterministic-clock seam — see {@link #start(SagaDefinition, Clock)}. */
    static SagaInstance reconstruct(List<SagaDomainEvent> events, Clock clock) {
        Objects.requireNonNull(events, "events must not be null");
        Objects.requireNonNull(clock, "clock must not be null");
        if (events.isEmpty()) {
            throw new IllegalArgumentException("Cannot reconstruct a SagaInstance from an empty event list");
        }
        if (!(events.get(0) instanceof SagaStarted first)) {
            throw new IllegalArgumentException(
                    "First event in a saga's history must be SagaStarted, but was: "
                            + events.get(0).getClass().getSimpleName()
                            + ". This indicates a corrupted or incorrectly-loaded event stream.");
        }

        SagaInstance instance = new SagaInstance(first.sagaId(), first.definitionReference(), clock);
        for (SagaDomainEvent event : events) {
            instance.apply(event);
        }
        return instance;
    }

    /**
     * Rebuilds a {@code SagaInstance} from a {@link SagaSnapshot} plus
     * whatever events were recorded after that snapshot was taken — the
     * fast path, bounded to O(events since the snapshot) rather than
     * O(total events ever recorded). See Milestone 2 architecture review,
     * Section 4, for snapshot cadence and invalidation reasoning.
     *
     * @param snapshot         a previously-persisted snapshot for this saga
     * @param eventsAfterSnapshot events with {@code sequence} strictly greater
     *         than {@code snapshot.sequenceNo()}, in ascending order. Passing
     *         events out of order, or events at or before the snapshot's
     *         sequence number, produces an incorrectly-reconstructed instance
     *         — this method does not re-derive or verify ordering itself; see
     *         {@code SagaInstanceRepository} for where that guarantee is enforced.
     */
    public static SagaInstance reconstructFromSnapshot(SagaSnapshot snapshot, List<SagaDomainEvent> eventsAfterSnapshot) {
        return reconstructFromSnapshot(snapshot, eventsAfterSnapshot, Clock.systemUTC());
    }

    /** Test/deterministic-clock seam — see {@link #start(SagaDefinition, Clock)}. */
    static SagaInstance reconstructFromSnapshot(SagaSnapshot snapshot, List<SagaDomainEvent> eventsAfterSnapshot, Clock clock) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        Objects.requireNonNull(eventsAfterSnapshot, "eventsAfterSnapshot must not be null");
        Objects.requireNonNull(clock, "clock must not be null");

        SagaInstance instance = new SagaInstance(
                snapshot.sagaId(), snapshot.definitionReference(), clock,
                snapshot.state(), snapshot.currentStepIndex(), snapshot.compensationCursor(), snapshot.sequenceNo());

        for (SagaDomainEvent event : eventsAfterSnapshot) {
            instance.apply(event);
        }
        return instance;
    }

    /**
     * Captures this instance's current state as a {@link SagaSnapshot},
     * suitable for persistence by a repository once the configured
     * event-count threshold is crossed. Pure — has no side effect on this
     * instance itself.
     */
    public SagaSnapshot toSnapshot(int schemaVersion) {
        return new SagaSnapshot(sagaId, definitionReference, version, state,
                currentStepIndex, compensationCursor, schemaVersion, now());
    }

    /**
     * Mutates this instance to reflect an already-historical event — the
     * "apply"/"evolve" side of decide-vs-apply (see class javadoc). Unlike
     * the business methods, this performs <b>no legality validation and
     * emits no new events</b>: the event given here is ground truth by
     * definition, because it already happened.
     *
     * <p>Deliberately takes no {@code SagaDefinition} parameter. Every event
     * carries everything needed to replay its own effect — {@link StepCompleted}
     * carries its own {@code stepIndex}, {@link SagaCompensationStarted}
     * carries the cursor it establishes, and so on. This is a stronger
     * event-sourcing property than merely "enough to replay": it means
     * replay never needs to consult business configuration that might
     * itself change over time, only the immutable facts already recorded.
     *
     * <p>One accepted quirk, documented rather than hidden: applying a
     * {@link StepCompleted} always advances {@code currentStepIndex} to
     * {@code stepIndex + 1}, even when that step was the saga's last one
     * (in which case a {@link SagaCompleted} always follows immediately and
     * sets {@code state} to {@code COMPLETED}). This leaves
     * {@code currentStepIndex} one past the final valid step index on a
     * completed saga. That's harmless — {@code COMPLETED} is terminal, and
     * no code path reads {@code currentStepIndex} meaningfully once a saga
     * is terminal — but it's a real asymmetry worth knowing about rather
     * than discovering by surprise later.
     *
     * @throws IllegalStateException if {@code event} is a type this method
     *         does not know how to apply — see the exhaustive {@code switch}
     *         below. Because {@link SagaDomainEvent} is {@code sealed}, this
     *         can only happen if a new event type is added to the interface's
     *         {@code permits} clause without a corresponding case here — a
     *         compile-time reminder, not a runtime one, is the actual first
     *         line of defense (see the switch's exhaustiveness), but this
     *         default branch exists as defense-in-depth against reflection-
     *         based deserialization producing an unexpected instance.
     */
    public void apply(SagaDomainEvent event) {
        switch (event) {
            case SagaStarted e -> {
                // The blank instance this event is applied to (see reconstruct())
                // was already constructed with this event's sagaId/definitionReference,
                // so there is no further field mutation to perform here. Applying it
                // is still meaningful: it is what makes version accounting exact
                // (see class javadoc) — the first event in any stream still counts.
            }
            case StepCompleted e -> {
                state = SagaState.STEP_COMPLETED;
                currentStepIndex = e.stepIndex() + 1;
            }
            case SagaCompleted e -> state = SagaState.COMPLETED;
            case StepFailed e -> {
                // Purely informational/audit — see StepFailed javadoc. The actual
                // state transition is carried by whichever event follows it
                // (SagaFailed or SagaCompensationStarted).
            }
            case SagaCompensationStarted e -> {
                state = SagaState.COMPENSATING;
                compensationCursor = e.compensationCursor();
            }
            case CompensationStepCompleted e -> compensationCursor = e.compensationCursor() - 1;
            case SagaFailed e -> state = SagaState.FAILED;
            case SagaTimedOut e -> {
                // Purely informational/audit — timeout itself doesn't change state.
                // The actual state transition is carried by whichever event follows it
                // (SagaFailed or SagaCompensationStarted), exactly like StepFailed.
            }
        }
        version++;
    }

    /**
     * Records that the given step finished successfully.
     *
     * @param definition the saga definition to evaluate this step against —
     *                    must match this instance's pinned {@link #definitionReference()}
     * @param stepName    the step the caller is reporting on — must match the
     *                    step this instance is actually currently expecting
     * @throws DefinitionMismatchException     if {@code definition} is not the pinned version
     * @throws StepMismatchException           if {@code stepName} is not the expected current step
     * @throws InvalidStateTransitionException if the instance cannot accept a step completion right now
     */
    public void completeCurrentStep(SagaDefinition definition, String stepName) {
        validateDefinition(definition);
        String expectedStepName = definition.stepAt(currentStepIndex).stepName();
        validateStepName(expectedStepName, stepName);

        boolean isLast = definition.isLastStep(currentStepIndex);
        SagaState target = isLast ? SagaState.COMPLETED : SagaState.STEP_COMPLETED;

        transitionTo(target);
        recordEvent(new StepCompleted(sagaId, stepName, currentStepIndex, now()));

        if (isLast) {
            recordEvent(new SagaCompleted(sagaId, now()));
        } else {
            currentStepIndex++;
        }
    }

    /**
     * Records that the given step failed. This is the entry point into the
     * compensation path — see Milestone 1's class javadoc for the two
     * distinct outcomes (straight-to-FAILED vs. enters COMPENSATING), which
     * are unchanged by this refactor.
     *
     * @param definition the saga definition to evaluate this step against —
     *                    must match this instance's pinned {@link #definitionReference()}
     * @param stepName    the step the caller is reporting on — must match the
     *                    step this instance is actually currently expecting
     * @param reason      free-form failure reason, carried into the emitted
     *                    {@link StepFailed} event for later diagnosis; may be null
     */
    public void failCurrentStep(SagaDefinition definition, String stepName, String reason) {
        validateDefinition(definition);
        String expectedStepName = definition.stepAt(currentStepIndex).stepName();
        validateStepName(expectedStepName, stepName);

        boolean anyStepCompleted = currentStepIndex > 0;

        if (!anyStepCompleted) {
            transitionTo(SagaState.FAILED);
            recordEvent(new StepFailed(sagaId, stepName, currentStepIndex, reason, now()));
            recordEvent(new SagaFailed(sagaId, now()));
            return;
        }

        transitionTo(SagaState.COMPENSATING);
        this.compensationCursor = currentStepIndex - 1;
        recordEvent(new StepFailed(sagaId, stepName, currentStepIndex, reason, now()));
        recordEvent(new SagaCompensationStarted(sagaId, compensationCursor, now()));
    }

    /**
     * Records that the compensation for the step the instance is currently
     * undoing has finished.
     *
     * @param definition the saga definition to evaluate this step against —
     *                    must match this instance's pinned {@link #definitionReference()}
     * @param stepName    the step whose compensation the caller is reporting on
     *                    — must match the step at {@link #compensationCursor()}
     */
    public void completeCompensationStep(SagaDefinition definition, String stepName) {
        validateDefinition(definition);

        if (state != SagaState.COMPENSATING) {
            throw new InvalidStateTransitionException(state, SagaState.COMPENSATING);
        }

        String expectedStepName = definition.stepAt(compensationCursor).stepName();
        validateStepName(expectedStepName, stepName);

        if (compensationCursor == 0) {
            transitionTo(SagaState.FAILED);
            recordEvent(new CompensationStepCompleted(sagaId, stepName, compensationCursor, now()));
            recordEvent(new SagaFailed(sagaId, now()));
            compensationCursor = -1;
            return;
        }

        transitionTo(SagaState.COMPENSATING);
        recordEvent(new CompensationStepCompleted(sagaId, stepName, compensationCursor, now()));
        compensationCursor--;
    }

    /**
     * Records that this saga has exceeded its timeout deadline. This is the
     * entry point for timeout-driven compensation, with the same two outcomes
     * as {@link #failCurrentStep}: either straight-to-FAILED if no steps have
     * completed yet, or entering COMPENSATING to undo already-completed steps.
     *
     * <p>This method is called by the scheduler/timeout processor after
     * detecting an expired saga in the CQRS projection. It remains validation-free
     * and state-transition logic-free — just like all business methods, it records
     * domain events; the scheduler is responsible for loading, calling this,
     * and persisting the result.
     *
     * @param definition the saga definition to evaluate against — must match
     *                    this instance's pinned {@link #definitionReference()}
     * @throws DefinitionMismatchException if {@code definition} is not the pinned version
     * @throws InvalidStateTransitionException if the instance is already in a terminal state
     */
    public void handleTimeout(SagaDefinition definition) {
        validateDefinition(definition);

        if (isTerminal()) {
            // Saga already finished, no timeout needed — scheduler should have
            // filtered this out using the CQRS projection, but be defensive.
            return;
        }

        boolean anyStepCompleted = currentStepIndex > 0;

        if (!anyStepCompleted) {
            // No steps completed yet, go straight to FAILED
            transitionTo(SagaState.FAILED);
            recordEvent(new SagaTimedOut(sagaId, now()));
            recordEvent(new SagaFailed(sagaId, now()));
            return;
        }

        // At least one step completed, enter compensation
        transitionTo(SagaState.COMPENSATING);
        this.compensationCursor = currentStepIndex - 1;
        recordEvent(new SagaTimedOut(sagaId, now()));
        recordEvent(new SagaCompensationStarted(sagaId, compensationCursor, now()));
    }

    /**
     * Returns every domain event recorded since the last call to this method,
     * and clears the internal queue. Intended to be called by the application
     * layer immediately after invoking a business method, so events are never
     * left un-persisted for long — but the aggregate itself does not enforce
     * that timing; it only guarantees the events returned are exactly (and
     * only) the ones recorded since the last pull.
     *
     * @return an immutable snapshot of the events that were pending; empty
     *         (never null) if nothing was pending
     */
    public List<SagaDomainEvent> pullDomainEvents() {
        List<SagaDomainEvent> snapshot = List.copyOf(pendingEvents);
        pendingEvents.clear();
        return snapshot;
    }

    private void recordEvent(SagaDomainEvent event) {
        pendingEvents.add(event);
        version++;
    }

    private void validateDefinition(SagaDefinition definition) {
        Objects.requireNonNull(definition, "definition must not be null");
        SagaDefinitionReference providedReference = definition.reference();
        if (!providedReference.equals(this.definitionReference)) {
            throw new DefinitionMismatchException(sagaId, this.definitionReference, providedReference);
        }
    }

    private void validateStepName(String expectedStepName, String actualStepName) {
        Objects.requireNonNull(actualStepName, "stepName must not be null");
        if (!expectedStepName.equals(actualStepName)) {
            throw new StepMismatchException(sagaId, expectedStepName, actualStepName);
        }
    }

    private void transitionTo(SagaState target) {
        // See Milestone 1 design-review history: no "state == target" bypass here.
        // Legal self-loops are encoded explicitly in SagaState.legalNextStates()
        // for STEP_COMPLETED and COMPENSATING only; terminal states correctly
        // reject re-entry into themselves. Note: this method no longer touches
        // `version` — see class javadoc on the Milestone 2 version redefinition;
        // version now increments exactly once per event, in recordEvent()/apply(),
        // not once per transition (a single business-method call may legally
        // cause one transition but record two events).
        if (!state.canTransitionTo(target)) {
            throw new InvalidStateTransitionException(state, target);
        }
        this.state = target;
    }

    private Instant now() {
        return clock.instant();
    }

    // ---- Read-only accessors. No public setters exist anywhere on this
    // class — every mutation goes through a named business method above. ----

    public UUID sagaId() {
        return sagaId;
    }

    public SagaDefinitionReference definitionReference() {
        return definitionReference;
    }

    public SagaState state() {
        return state;
    }

    public int currentStepIndex() {
        return currentStepIndex;
    }

    public int compensationCursor() {
        return compensationCursor;
    }

    public long version() {
        return version;
    }

    public boolean isTerminal() {
        return state.isTerminal();
    }

    /**
     * Milestone 1.5 addition: identity-based equality, as required for any
     * DDD Entity/Aggregate Root. Two {@code SagaInstance} objects represent
     * the "same" saga if and only if they share a {@code sagaId} — their
     * current {@code state}, step index, or any other mutable field is
     * irrelevant to identity. This matters the moment a repository or cache
     * is introduced (Milestone 2): comparing entities by their full field
     * set (structural equality) would mean an instance stops being "equal to
     * itself" the instant any field changes — exactly backwards for identity
     * semantics, and a well-known source of subtle bugs in sets/maps keyed
     * by entities that mutate after insertion.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SagaInstance other)) {
            return false;
        }
        return sagaId.equals(other.sagaId);
    }

    @Override
    public int hashCode() {
        return sagaId.hashCode();
    }

    @Override
    public String toString() {
        return "SagaInstance{sagaId=" + sagaId
                + ", definitionReference=" + definitionReference
                + ", state=" + state
                + ", currentStepIndex=" + currentStepIndex
                + ", compensationCursor=" + compensationCursor
                + ", version=" + version
                + '}';
    }
}
