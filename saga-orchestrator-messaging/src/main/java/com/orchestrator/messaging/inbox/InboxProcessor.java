package com.orchestrator.messaging.inbox;

import com.orchestrator.messaging.MessageHandler;
import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.transaction.TransactionRunner;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

/**
 * Orchestrates duplicate detection, business handling, and inbox state updates
 * for exactly-once processing from the application's point of view.
 */
public final class InboxProcessor implements MessageHandler {

    private static final String DEFAULT_TOPIC = "";
    private static final String DEFAULT_PARTITION_KEY = "";

    private final InboxStore inboxStore;
    private final String consumer;
    private final Function<byte[], UUID> messageIdExtractor;
    private final InboxMessageHandler<byte[]> handler;
    private final TransactionRunner transactionRunner;
    private final Counter receivedCounter;
    private final Counter duplicateCounter;
    private final Counter processedCounter;
    private final Counter failedCounter;
    private final Counter dlqPublishedCounter;
    private final Counter dlqPublishFailedCounter;
    private final RetryPolicy retryPolicy;
    private final DeadLetterPublisher deadLetterPublisher;

    public InboxProcessor(InboxStore inboxStore,
                          String consumer,
                          Function<byte[], UUID> messageIdExtractor,
                          InboxMessageHandler<byte[]> handler,
                          TransactionRunner transactionRunner,
                          MeterRegistry meterRegistry) {
        this(inboxStore, consumer, messageIdExtractor, handler, transactionRunner, meterRegistry, RetryPolicy.noRetries(), null);
    }

    public InboxProcessor(InboxStore inboxStore,
                          String consumer,
                          Function<byte[], UUID> messageIdExtractor,
                          InboxMessageHandler<byte[]> handler,
                          TransactionRunner transactionRunner,
                          MeterRegistry meterRegistry,
                          RetryPolicy retryPolicy,
                          DeadLetterPublisher deadLetterPublisher) {
        this.inboxStore = Objects.requireNonNull(inboxStore, "inboxStore must not be null");
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        this.messageIdExtractor = Objects.requireNonNull(messageIdExtractor, "messageIdExtractor must not be null");
        this.handler = Objects.requireNonNull(handler, "handler must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
                        this.receivedCounter = meterRegistry.counter("inbox.messages.received");
                        this.dlqPublishedCounter = meterRegistry.counter("inbox.dlq.published");
                        this.dlqPublishFailedCounter = meterRegistry.counter("inbox.dlq.publish.failed");
        this.duplicateCounter = meterRegistry.counter("inbox.messages.duplicates");
        this.processedCounter = meterRegistry.counter("inbox.messages.processed");
        this.failedCounter = meterRegistry.counter("inbox.messages.failed");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        this.deadLetterPublisher = deadLetterPublisher; // may be null when no DLQ configured
    }

    @Override
    public void handle(byte[] payload, MessageHeaders headers) throws Exception {
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(headers, "headers must not be null");

        UUID messageId = Objects.requireNonNull(messageIdExtractor.apply(payload), "messageIdExtractor returned null");
        receivedCounter.increment();

        // Ensure we record the inbox entry exactly once and detect duplicates
        final boolean[] firstSeenHolder = new boolean[1];
        transactionRunner.runInTransaction(() -> {
            boolean firstSeen = inboxStore.recordIfNew(messageId, consumer, DEFAULT_TOPIC, DEFAULT_PARTITION_KEY);
            firstSeenHolder[0] = firstSeen;
            if (!firstSeen) {
                duplicateCounter.increment();
            }
        });

        // If duplicate, bail out
        if (!firstSeenHolder[0]) {
            return;
        }

        // persist payload and headers for retryability
        try {
            InboxRecord record = new InboxRecord(messageId, consumer, DEFAULT_TOPIC, DEFAULT_PARTITION_KEY, Instant.now(), null, InboxStatus.RECEIVED, 0, null, null, null, payload, headers);
            inboxStore.save(record);
        } catch (Exception ignored) {
        }

        try {
            transactionRunner.runInTransaction(() -> {
                try {
                    handler.handle(payload, headers);
                    inboxStore.markProcessed(messageId, consumer);
                    processedCounter.increment();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            return;
        } catch (Exception e) {
            failedCounter.increment();
            // load current record to get retryCount
            InboxRecord current = inboxStore.find(messageId, consumer).orElse(null);
            int currentCount = current != null ? current.retryCount() : 0;
            int nextCount = currentCount + 1;
            Instant now = Instant.now();
            Instant nextRetry = now.plusMillis(retryPolicy.delayMillisForAttempt(nextCount));
            try {
                inboxStore.updateRetryMetadata(messageId, consumer, nextCount, e.getClass().getName() + ": " + e.getMessage(), now, nextRetry);
            } catch (Exception ignored) {
            }

            if (nextCount > retryPolicy.maxRetries()) {
                try {
                    inboxStore.markFailed(messageId, consumer);
                } catch (Exception ignored) {
                }
                if (deadLetterPublisher != null) {
                    DeadLetterMessage dlq = new DeadLetterMessage(messageId,
                            java.util.Optional.empty(), java.util.Optional.empty(), payload,
                            DEFAULT_TOPIC, e.getClass().getName(), e.getMessage(), getStackTrace(e), now, nextCount, headers);
                    try {
                        deadLetterPublisher.publish(dlq, headers);
                        dlqPublishedCounter.increment();
                    } catch (Exception dlqEx) {
                        dlqPublishFailedCounter.increment();
                    }
                }
            }
            return;
        }
    }

    private static String getStackTrace(Exception e) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
