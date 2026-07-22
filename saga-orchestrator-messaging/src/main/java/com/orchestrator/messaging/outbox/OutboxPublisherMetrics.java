package com.orchestrator.messaging.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Objects;

public final class OutboxPublisherMetrics {

    private final Counter publishedCounter;
    private final Counter publishFailedCounter;
    private final Counter retryCounter;
    private final Counter deadLetterCounter;

    public OutboxPublisherMetrics(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.publishedCounter = Counter.builder("outbox.messages.published")
                .description("Number of outbox messages successfully published")
                .register(meterRegistry);
        this.publishFailedCounter = Counter.builder("outbox.publish.failed")
                .description("Number of failed outbox publish attempts")
                .register(meterRegistry);
        this.retryCounter = Counter.builder("outbox.retry.count")
                .description("Number of outbox retries")
                .register(meterRegistry);
        this.deadLetterCounter = Counter.builder("outbox.deadletter.count")
                .description("Number of outbox records permanently failed")
                .register(meterRegistry);
    }

    public void incrementPublished() {
        publishedCounter.increment();
    }

    public void incrementPublishFailed() {
        publishFailedCounter.increment();
    }

    public void incrementRetry() {
        retryCounter.increment();
    }

    public void incrementDeadLetter() {
        deadLetterCounter.increment();
    }
}
