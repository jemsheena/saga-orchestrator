package com.orchestrator.messaging.inbox;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Configurable retry policy for transient message handling failures.
 */
public final class RetryPolicy {

    private final int maxRetries;
    private final long initialDelayMillis;
    private final double backoffMultiplier;
    private final Predicate<Exception> retryablePredicate;

    private RetryPolicy(int maxRetries, long initialDelayMillis, double backoffMultiplier,
                        Predicate<Exception> retryablePredicate) {
        this.maxRetries = maxRetries;
        this.initialDelayMillis = initialDelayMillis;
        this.backoffMultiplier = backoffMultiplier;
        this.retryablePredicate = Objects.requireNonNull(retryablePredicate, "retryablePredicate must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public int maxRetries() {
        return maxRetries;
    }

    public long delayMillisForAttempt(int attempt) {
        if (attempt <= 1) return initialDelayMillis;
        return (long) (initialDelayMillis * Math.pow(backoffMultiplier, attempt - 1));
    }

    public boolean isRetryable(Exception e) {
        return retryablePredicate.test(e);
    }

    public boolean canRetry(Exception e, int attempt) {
        return attempt <= maxRetries && isRetryable(e);
    }

    public static RetryPolicy noRetries() {
        return builder().maxRetries(0).initialDelay(Duration.ofMillis(0)).build();
    }

    public static final class Builder {
        private int maxRetries = 0;
        private long initialDelayMillis = 0L;
        private double backoffMultiplier = 1.0;
        private Predicate<Exception> retryablePredicate = e -> true;

        private Builder() {}

        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder initialDelay(Duration delay) {
            this.initialDelayMillis = Objects.requireNonNull(delay, "delay must not be null").toMillis();
            return this;
        }

        public Builder backoffMultiplier(double multiplier) {
            if (multiplier < 1.0) throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
            this.backoffMultiplier = multiplier;
            return this;
        }

        public Builder retryablePredicate(Predicate<Exception> predicate) {
            this.retryablePredicate = Objects.requireNonNull(predicate, "predicate must not be null");
            return this;
        }

        public RetryPolicy build() {
            return new RetryPolicy(maxRetries, initialDelayMillis, backoffMultiplier, retryablePredicate);
        }
    }
}
