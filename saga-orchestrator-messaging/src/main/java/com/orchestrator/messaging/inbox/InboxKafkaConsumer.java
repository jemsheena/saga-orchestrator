package com.orchestrator.messaging.inbox;

import com.orchestrator.messaging.MessageConsumer;
import com.orchestrator.messaging.kafka.KafkaMessageConsumer;
import com.orchestrator.messaging.transaction.TransactionRunner;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

/**
 * Kafka-backed consumer that delegates inbound messages through an {@link InboxProcessor}.
 *
 * <p>Offsets are committed only after the inbox processor completes successfully,
 * making the transport at-least-once while the inbox layer provides application-level
 * idempotence.
 */
public final class InboxKafkaConsumer implements MessageConsumer, AutoCloseable {

    private final KafkaMessageConsumer consumer;

    /**
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId Kafka consumer group id
     * @param topics topics to subscribe to
     * @param inboxStore durable inbox persistence
     * @param consumerId logical consumer identity used for inbox deduplication
     * @param messageIdExtractor function that extracts a stable deduplication id from payload bytes
     * @param handler business logic for the inbound message
     * @param transactionRunner transactional boundary around inbox state updates and handler execution
     * @param meterRegistry metrics registry for inbox counters
     */
    public InboxKafkaConsumer(String bootstrapServers,
                              String groupId,
                              List<String> topics,
                              InboxStore inboxStore,
                              String consumerId,
                              Function<byte[], UUID> messageIdExtractor,
                              InboxMessageHandler<byte[]> handler,
                              TransactionRunner transactionRunner,
                              MeterRegistry meterRegistry) {
        this.consumer = new KafkaMessageConsumer(bootstrapServers, groupId, topics,
                new InboxProcessor(inboxStore, consumerId, messageIdExtractor, handler, transactionRunner, meterRegistry));
    }

    @Override
    public void start() {
        consumer.start();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
