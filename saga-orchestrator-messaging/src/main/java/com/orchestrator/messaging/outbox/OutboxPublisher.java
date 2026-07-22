package com.orchestrator.messaging.outbox;

import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.MessagePublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final OutboxPublisherMetrics metrics;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "outbox-publisher");
        t.setDaemon(true);
        return t;
    });

    public OutboxPublisher(OutboxStore outboxStore,
                           MessagePublisher messagePublisher,
                           int batchSize,
                           OutboxPublisherMetrics metrics) {
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
        this.messagePublisher = Objects.requireNonNull(messagePublisher, "messagePublisher must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.batchSize = batchSize;
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

    /** Begins polling every {@code intervalMillis}. Returns immediately. */
    public void start(long intervalMillis) {
        scheduler.scheduleWithFixedDelay(this::pollOnceSwallowingErrors, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void pollOnceSwallowingErrors() {
        try {
            pollOnce();
        } catch (RuntimeException e) {
            // A poll cycle failing outright (e.g. the DB is briefly unreachable) must
            // not kill the scheduled task permanently - ScheduledExecutorService
            // silently stops future executions if a scheduled Runnable throws, which
            // would be a much worse failure mode than logging and trying again next cycle.
            System.err.println("[WARN] Outbox poll cycle failed, will retry next cycle: " + e);
        }
    }

    /** Stops polling and releases the scheduler thread. */
    public void stop() {
        scheduler.shutdown();
    }
}
