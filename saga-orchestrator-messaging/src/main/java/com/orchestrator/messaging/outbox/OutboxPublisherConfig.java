package com.orchestrator.messaging.outbox;

import java.util.Objects;

public final class OutboxPublisherConfig {

    public static final OutboxPublisherConfig DEFAULT = new OutboxPublisherConfig(10, 1000, 1000);

    private final int batchSize;
    private final long pollIntervalMillis;
    private final long errorBackoffMillis;

    public OutboxPublisherConfig(int batchSize, long pollIntervalMillis, long errorBackoffMillis) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        if (pollIntervalMillis < 1) {
            throw new IllegalArgumentException("pollIntervalMillis must be >= 1");
        }
        if (errorBackoffMillis < 1) {
            throw new IllegalArgumentException("errorBackoffMillis must be >= 1");
        }
        this.batchSize = batchSize;
        this.pollIntervalMillis = pollIntervalMillis;
        this.errorBackoffMillis = errorBackoffMillis;
    }

    public int batchSize() {
        return batchSize;
    }

    public long pollIntervalMillis() {
        return pollIntervalMillis;
    }

    public long errorBackoffMillis() {
        return errorBackoffMillis;
    }
}
