package com.orchestrator.core.definition;

import java.time.Duration;
import java.util.Objects;

/**
 * Defines the timeout behavior for a saga execution. This is an operational
 * policy, not a business-domain concern — kept separate from {@link SagaStep}
 * to enforce a clean separation between workflow definition and execution behavior.
 *
 * <p><b>Current scope:</b> a single fixed timeout per saga. Future features
 * (retry policies, backoff, deadline extensions) can extend this without
 * redesigning the step model or the event stream.
 *
 * <p>Immutable and designed to be cached and shared across every instance of
 * a saga definition — a single accidental mutation would corrupt every in-flight saga.
 *
 * @param timeoutDuration the maximum duration a saga may spend in any non-terminal state
 *                         before timing out and triggering automatic compensation.
 *                         Must be positive.
 */
public record TimeoutPolicy(
        Duration timeoutDuration
) {

    /**
     * Compact record constructor: enforces invariants at construction time.
     */
    public TimeoutPolicy {
        Objects.requireNonNull(timeoutDuration, "timeoutDuration must not be null");
        if (timeoutDuration.isNegative() || timeoutDuration.isZero()) {
            throw new IllegalArgumentException("timeoutDuration must be positive, but was: " + timeoutDuration);
        }
    }

    /**
     * Convenience factory for common timeout durations, eliminating the need
     * to import Duration and type out Duration.ofSeconds(...) etc. in every
     * saga definition builder call.
     */
    public static TimeoutPolicy ofSeconds(long seconds) {
        return new TimeoutPolicy(Duration.ofSeconds(seconds));
    }

    /**
     * Convenience factory for minute-scale timeouts.
     */
    public static TimeoutPolicy ofMinutes(long minutes) {
        return new TimeoutPolicy(Duration.ofMinutes(minutes));
    }

    /**
     * Convenience factory for hour-scale timeouts.
     */
    public static TimeoutPolicy ofHours(long hours) {
        return new TimeoutPolicy(Duration.ofHours(hours));
    }
}
