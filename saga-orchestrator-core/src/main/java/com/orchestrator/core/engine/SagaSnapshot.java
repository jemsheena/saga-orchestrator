package com.orchestrator.core.engine;

import com.orchestrator.core.definition.SagaDefinitionReference;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * An immutable point-in-time capture of a {@link SagaInstance}'s internal
 * state, taken at a specific {@code sequenceNo} (event count) in its stream.
 * This is the Memento pattern applied directly: {@code SagaInstance} is the
 * originator, this is the memento, and a {@code SagaSnapshotStore} is the
 * caretaker that persists and retrieves it without needing to understand
 * its internals.
 *
 * <p><b>Snapshots are strictly a performance optimization, never a second
 * source of truth</b> — see Milestone 2 architecture review, Section 4/9.
 * The event stream in {@code saga_event} always remains authoritative;
 * losing every snapshot ever taken degrades reconstruction cost back to
 * full replay but never produces an incorrect result. {@code schemaVersion}
 * exists specifically so a consumer can detect a snapshot written by
 * older/incompatible code and safely discard it in favor of full replay,
 * per that same reasoning.
 *
 * @param sagaId             identity of the saga this is a snapshot of
 * @param definitionReference the pinned definition version, carried here so
 *                            {@link SagaInstance#reconstructFromSnapshot} does
 *                            not need a separate lookup just to restore this field
 * @param sequenceNo         the event count at the moment this snapshot was taken —
 *                            equivalently, {@link SagaInstance#version()} at that moment.
 *                            Events with a sequence number at or below this value are
 *                            already folded into {@code state}/{@code currentStepIndex}/
 *                            {@code compensationCursor} below; only later events need replaying.
 * @param state               the saga's {@link SagaState} at the time of the snapshot
 * @param currentStepIndex    forward-progress cursor at the time of the snapshot
 * @param compensationCursor  backward-undo cursor at the time of the snapshot (-1 if not compensating)
 * @param schemaVersion       version of this snapshot's own shape/semantics — see class javadoc
 * @param createdAt           when this snapshot was taken
 */
public record SagaSnapshot(
        UUID sagaId,
        SagaDefinitionReference definitionReference,
        long sequenceNo,
        SagaState state,
        int currentStepIndex,
        int compensationCursor,
        int schemaVersion,
        Instant createdAt
) {
    public SagaSnapshot {
        Objects.requireNonNull(sagaId, "sagaId must not be null");
        Objects.requireNonNull(definitionReference, "definitionReference must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        if (sequenceNo < 0) {
            throw new IllegalArgumentException("sequenceNo must not be negative");
        }
    }
}
