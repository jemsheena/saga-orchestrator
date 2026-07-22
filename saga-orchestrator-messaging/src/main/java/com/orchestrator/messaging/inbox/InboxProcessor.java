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

    public InboxProcessor(InboxStore inboxStore,
                          String consumer,
                          Function<byte[], UUID> messageIdExtractor,
                          InboxMessageHandler<byte[]> handler,
                          TransactionRunner transactionRunner,
                          MeterRegistry meterRegistry) {
        this.inboxStore = Objects.requireNonNull(inboxStore, "inboxStore must not be null");
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        this.messageIdExtractor = Objects.requireNonNull(messageIdExtractor, "messageIdExtractor must not be null");
        this.handler = Objects.requireNonNull(handler, "handler must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.receivedCounter = meterRegistry.counter("inbox.messages.received");
        this.duplicateCounter = meterRegistry.counter("inbox.messages.duplicates");
        this.processedCounter = meterRegistry.counter("inbox.messages.processed");
        this.failedCounter = meterRegistry.counter("inbox.messages.failed");
    }

    @Override
    public void handle(byte[] payload, MessageHeaders headers) throws Exception {
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(headers, "headers must not be null");

        UUID messageId = Objects.requireNonNull(messageIdExtractor.apply(payload), "messageIdExtractor returned null");
        receivedCounter.increment();

        transactionRunner.runInTransaction(() -> {
            boolean firstSeen = inboxStore.recordIfNew(messageId, consumer, DEFAULT_TOPIC, DEFAULT_PARTITION_KEY);
            if (!firstSeen) {
                duplicateCounter.increment();
                return;
            }

            try {
                handler.handle(payload, headers);
                inboxStore.markProcessed(messageId, consumer);
                processedCounter.increment();
            } catch (Exception e) {
                inboxStore.markFailed(messageId, consumer);
                failedCounter.increment();
                throw new RuntimeException("Inbox message handling failed", e);
            }
        });
    }
}
