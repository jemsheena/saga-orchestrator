package com.orchestrator.messaging.inbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import com.orchestrator.messaging.transaction.TransactionRunner;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background scheduler that scans the inbox store for messages due for retry
 * and attempts to reprocess them using the provided handler.
 */
public final class InboxRetryScheduler {

    private final InboxStore inboxStore;
    private final InboxMessageHandler<byte[]> handler;
    private final TransactionRunner transactionRunner;
    private final RetryPolicy retryPolicy;
    private final DeadLetterPublisher deadLetterPublisher;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final Counter retryStarted;
    private final Counter retryCompleted;
    private final Counter retryFailed;
    private final Counter dlqPublished;
    private final Counter dlqPublishFailed;

    public InboxRetryScheduler(InboxStore inboxStore,
                              InboxMessageHandler<byte[]> handler,
                              TransactionRunner transactionRunner,
                              RetryPolicy retryPolicy,
                              DeadLetterPublisher deadLetterPublisher,
                              MeterRegistry meterRegistry) {
        this.inboxStore = Objects.requireNonNull(inboxStore);
        this.handler = Objects.requireNonNull(handler);
        this.transactionRunner = Objects.requireNonNull(transactionRunner);
        this.retryPolicy = Objects.requireNonNull(retryPolicy);
        this.deadLetterPublisher = deadLetterPublisher;
        Objects.requireNonNull(meterRegistry);
        this.retryStarted = meterRegistry.counter("inbox.retry.started");
        this.retryCompleted = meterRegistry.counter("inbox.retry.completed");
        this.retryFailed = meterRegistry.counter("inbox.retry.failed");
        this.dlqPublished = meterRegistry.counter("inbox.dlq.published");
        this.dlqPublishFailed = meterRegistry.counter("inbox.dlq.publish.failed");
    }

    public void start(long initialDelaySeconds, long intervalSeconds) {
        executor.scheduleWithFixedDelay(this::scanAndProcess, initialDelaySeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdownNow();
    }

    private void scanAndProcess() {
        try {
            List<InboxRecord> due = inboxStore.findDueForRetry(Instant.now(), 100);
            for (InboxRecord record : due) {
                processRetry(record);
            }
        } catch (Exception e) {
            // swallow, scheduler will try again
        }
    }

    private void processRetry(InboxRecord record) {
        retryStarted.increment();
        try {
            transactionRunner.runInTransaction(() -> {
                try {
                    handler.handle(record.payload(), record.headers());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            inboxStore.updateRetryMetadata(record.messageId(), record.consumer(), 0, null, Instant.now(), null);
            retryCompleted.increment();
        } catch (Exception e) {
            retryFailed.increment();
            int nextRetryCount = record.retryCount() + 1;
            Instant now = Instant.now();
            long delayMillis = retryPolicy.delayMillisForAttempt(nextRetryCount);
            Instant nextRetry = now.plusMillis(delayMillis);
            inboxStore.updateRetryMetadata(record.messageId(), record.consumer(), nextRetryCount, e.getClass().getName() + ": " + e.getMessage(), now, nextRetry);
            if (nextRetryCount > retryPolicy.maxRetries()) {
                if (deadLetterPublisher != null) {
                    try {
                        deadLetterPublisher.publish(new DeadLetterMessage(record.messageId(), java.util.Optional.empty(), java.util.Optional.empty(), new byte[0], record.topic(), e.getClass().getName(), e.getMessage(), getStackTrace(e), now, nextRetryCount, null), null);
                        dlqPublished.increment();
                    } catch (Exception dlqEx) {
                        dlqPublishFailed.increment();
                    }
                }
            }
        }
    }

    private static String getStackTrace(Exception e) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
