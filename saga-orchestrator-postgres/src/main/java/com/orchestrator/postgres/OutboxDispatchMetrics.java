package com.orchestrator.postgres;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Objects;

public final class OutboxDispatchMetrics {

    private final Counter retryCounter;
    private final Counter deadLetterCounter;

    public OutboxDispatchMetrics(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.retryCounter = Counter.builder("outbox.retry.count")
                .description("Number of outbox retries")
                .register(meterRegistry);
        this.deadLetterCounter = Counter.builder("outbox.deadletter.count")
                .description("Number of outbox records permanently failed")
                .register(meterRegistry);
    }

    public void incrementRetry() {
        retryCounter.increment();
    }

    public void incrementDeadLetter() {
        deadLetterCounter.increment();
    }
}
