package com.orchestrator.messaging.inbox;

import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.kafka.KafkaMessagePublisher;
import com.orchestrator.messaging.transaction.TransactionRunner;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class InboxKafkaConsumerIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.0");

    private static final String TOPIC = "test.inbox.v1";

    @Test
    void inboxKafkaConsumer_processesMessageAndIgnoresDuplicate() throws InterruptedException {
        UUID payloadId = UUID.randomUUID();
        byte[] payload = String.format("{\"id\":\"%s\",\"value\":\"hello\"}", payloadId)
                .getBytes(StandardCharsets.UTF_8);
        UUID expectedMessageId = UUID.nameUUIDFromBytes(payload);

        InMemoryInboxStore inboxStore = new InMemoryInboxStore();
        AtomicInteger handledCount = new AtomicInteger(0);
        CountDownLatch processed = new CountDownLatch(1);

        TransactionRunner transactionRunner = work -> work.run();
        InboxMessageHandler<byte[]> handler = (msgPayload, headers) -> {
            handledCount.incrementAndGet();
            processed.countDown();
        };

        try (InboxKafkaConsumer consumer = new InboxKafkaConsumer(
                KAFKA.getBootstrapServers(),
                "inbox-test-group",
                List.of(TOPIC),
                inboxStore,
                "inbox-consumer",
                bytes -> UUID.nameUUIDFromBytes(bytes),
                handler,
                transactionRunner,
                new SimpleMeterRegistry())) {

            consumer.start();

            try (KafkaMessagePublisher publisher = new KafkaMessagePublisher(KAFKA.getBootstrapServers())) {
                publisher.publish(TOPIC, "test-key", payload,
                        new MessageHeaders(UUID.randomUUID(), null));
            }

            assertTrue(processed.await(30, TimeUnit.SECONDS), "first message was not processed");

            try (KafkaMessagePublisher publisher = new KafkaMessagePublisher(KAFKA.getBootstrapServers())) {
                publisher.publish(TOPIC, "test-key", payload,
                        new MessageHeaders(UUID.randomUUID(), null));
            }

            // allow the duplicate to be consumed and detected by the inbox processor
            Thread.sleep(1500);
        }

        assertEquals(1, handledCount.get(), "Duplicate message should not be reprocessed");
        assertTrue(inboxStore.find(expectedMessageId, "inbox-consumer").isPresent());
        assertEquals(InboxStatus.PROCESSED, inboxStore.find(expectedMessageId, "inbox-consumer").get().status());
    }
}
