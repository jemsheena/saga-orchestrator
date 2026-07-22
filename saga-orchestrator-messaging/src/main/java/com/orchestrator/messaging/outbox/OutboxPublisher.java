package com.orchestrator.messaging.outbox;

import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.MessagePublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls an {@link OutboxStore} on a fixed interval and dispatches claimed
 * records through a {@link MessagePublisher}.
 *
 * <p><b>Deliberately framework-free — no Spring {@code @Scheduled}, per
 * this project's standing "no Spring until it earns its place" discipline.</b>
 * Uses a plain JDK {@link ScheduledExecutorService}. {@link #pollOnce} is
 * the real unit of testable logic (and is exactly what a future Spring
 * {@code @Scheduled} method would call, if/when this project introduces
 * Spring Boot); {@link #start}/{@link #stop} are a minimal, complete,
 * already-runnable scheduling wrapper around it so this class is a fully
 * working component on its own, not something that only becomes useful once
 * a framework is layered on top.
 */
public final class OutboxPublisher {

    private final OutboxStore outboxStore;
    private final MessagePublisher messagePublisher;
    private final int batchSize;
    private final OutboxPublisherConfig config;
    private final OutboxPublisherMetrics metrics;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "outbox-publisher");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean running = new AtomicBoolean(false);

    public OutboxPublisher(OutboxStore outboxStore,
                           MessagePublisher messagePublisher,
                           OutboxPublisherConfig config,
                           OutboxPublisherMetrics metrics) {
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
        this.messagePublisher = Objects.requireNonNull(messagePublisher, "messagePublisher must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.batchSize = config.batchSize();
    }

    public OutboxPublisher(OutboxStore outboxStore,
                           MessagePublisher messagePublisher,
                           int batchSize,
                           OutboxPublisherMetrics metrics) {
        this(outboxStore, messagePublisher, new OutboxPublisherConfig(batchSize, 1000, 1000), metrics);
    }

    public OutboxPublisher(OutboxStore outboxStore,
                           MessagePublisher messagePublisher,
                           OutboxPublisherConfig config) {
        this(outboxStore, messagePublisher, config, new OutboxPublisherMetrics(new SimpleMeterRegistry()));
    }

    public OutboxPublisher(OutboxStore outboxStore, MessagePublisher messagePublisher, int batchSize) {
        this(outboxStore, messagePublisher, batchSize,
                new OutboxPublisherMetrics(new SimpleMeterRegistry()));
    }

    /**
     * Claims and dispatches one batch. This is the actual unit of logic —
     * see the accompanying test suite, which exercises this method directly
     * against in-memory fakes, with no scheduler involved at all.
     *
     * @return the number of records successfully dispatched
     */
    public int pollOnce() {
        return outboxStore.claimAndDispatch(batchSize, record -> {
            try {
                messagePublisher.publish(
                        record.topic(),
                        record.messageKey(),
                        record.payload(),
                        new MessageHeaders(record.correlationId(), record.causationId()));
                metrics.incrementPublished();
            } catch (RuntimeException e) {
                metrics.incrementPublishFailed();
                throw e;
            }
        });
    }

    /** Begins polling using the configured interval and backoff. Returns immediately. */
    public void start() {
        start(config.pollIntervalMillis());
    }

    public void start(long intervalMillis) {
        if (intervalMillis < 1) {
            throw new IllegalArgumentException("intervalMillis must be >= 1");
        }
        if (!running.compareAndSet(false, true)) {
            return; // already started
        }
        scheduleNext(0, intervalMillis);
    }

    private void scheduleNext(long delayMillis, long intervalMillis) {
        if (!running.get()) {
            return;
        }
        scheduler.schedule(() -> runPollCycle(intervalMillis), delayMillis, TimeUnit.MILLISECONDS);
    }

    private void runPollCycle(long intervalMillis) {
        long nextDelayMillis = intervalMillis;
        try {
            pollOnce();
        } catch (RuntimeException e) {
            System.err.println("[WARN] Outbox poll cycle failed, will retry after backoff: " + e);
            nextDelayMillis = config.errorBackoffMillis();
        }
        if (running.get()) {
            scheduleNext(nextDelayMillis, intervalMillis);
        }
    }

    /** Stops polling and releases the scheduler thread. */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        scheduler.shutdownNow();
    }
}
