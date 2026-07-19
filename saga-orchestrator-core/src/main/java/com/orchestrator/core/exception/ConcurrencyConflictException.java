package com.orchestrator.core.exception;

import java.util.UUID;

/**
 * Thrown by {@code SagaEventStore.append(...)} when the caller's
 * {@code expectedVersion} does not match the stream's actual current
 * version — i.e. someone else appended events to this saga since the
 * caller last loaded it. See Milestone 2 architecture review, Section 7,
 * for the full mechanism ({@code saga_stream_head} conditional update)
 * this exception surfaces the failure of.
 *
 * <p><b>Correct response to this exception is NOT "retry the same append."</b>
 * The right recovery is: reload the aggregate fresh (which now reflects
 * whatever the other writer committed), and re-run the original business
 * method against that fresh state. Thanks to Milestone 1.5's step
 * correlation validation, that retry will very often legitimately throw
 * {@code StepMismatchException} instead — correctly recognized as "someone
 * else already handled this," not as a bug.
 */
public class ConcurrencyConflictException extends RuntimeException {

    private final UUID sagaId;
    private final long expectedVersion;
    private final long actualVersion;

    public ConcurrencyConflictException(UUID sagaId, long expectedVersion, long actualVersion) {
        super("Concurrency conflict appending to saga " + sagaId
                + ": expected version " + expectedVersion + " but stream is actually at version "
                + actualVersion + ". Reload the aggregate and retry the operation against fresh state.");
        this.sagaId = sagaId;
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    public UUID sagaId() {
        return sagaId;
    }

    public long expectedVersion() {
        return expectedVersion;
    }

    public long actualVersion() {
        return actualVersion;
    }
}
