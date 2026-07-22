package com.orchestrator.messaging.kafka;

import com.orchestrator.messaging.MessageHeaders;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof that {@link KafkaMessagePublisher} and
 * {@link KafkaMessageConsumer} actually interoperate against a real,
 * ephemeral Kafka broker: publish a message with headers, consume it back,
 * and verify both the payload bytes and the correlation/causation headers
 * survive the round trip. Requires local Docker — not executed in the
 * sandbox this was developed in (no Docker there, and {@code kafka-clients}
 * itself isn't compilable in that sandbox either — see
 * {@code KafkaProducerConfig}'s sandbox note).
 */
@Testcontainers
class KafkaPublishConsumeIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.0");

    private static final String TOPIC = "test.messages.v1";

    @Test
    void publishedMessage_isConsumedWithPayloadAndHeadersIntact() throws InterruptedException {
        UUID correlationId = UUID.randomUUID();
        UUID causationId = UUID.randomUUID();
        byte[] payload = "hello saga orchestrator".getBytes(StandardCharsets.UTF_8);

        AtomicReference<byte[]> receivedPayload = new AtomicReference<>();
        AtomicReference<MessageHeaders> receivedHeaders = new AtomicReference<>();
        CountDownLatch received = new CountDownLatch(1);

        try (KafkaMessageConsumer consumer = new KafkaMessageConsumer(
                KAFKA.getBootstrapServers(), "test-group", List.of(TOPIC),
                (msgPayload, headers) -> {
                    receivedPayload.set(msgPayload);
                    receivedHeaders.set(headers);
                    received.countDown();
                })) {

            consumer.start();

            try (KafkaMessagePublisher publisher = new KafkaMessagePublisher(KAFKA.getBootstrapServers())) {
                publisher.publish(TOPIC, "test-key", payload, new MessageHeaders(correlationId, causationId));
            }

            assertTrue(received.await(30, TimeUnit.SECONDS), "message was not consumed in time");
        }

        assertEquals(new String(payload, StandardCharsets.UTF_8), new String(receivedPayload.get(), StandardCharsets.UTF_8));
        assertEquals(correlationId, receivedHeaders.get().correlationId());
        assertEquals(causationId, receivedHeaders.get().causationId());
    }

    @Test
    void publishedMessage_withNoCausationId_consumesWithNullCausationId() throws InterruptedException {
        UUID correlationId = UUID.randomUUID();
        AtomicReference<MessageHeaders> receivedHeaders = new AtomicReference<>();
        CountDownLatch received = new CountDownLatch(1);

        try (KafkaMessageConsumer consumer = new KafkaMessageConsumer(
                KAFKA.getBootstrapServers(), "test-group-2", List.of(TOPIC),
                (msgPayload, headers) -> {
                    receivedHeaders.set(headers);
                    received.countDown();
                })) {
            consumer.start();

            try (KafkaMessagePublisher publisher = new KafkaMessagePublisher(KAFKA.getBootstrapServers())) {
                publisher.publish(TOPIC, "test-key-2", "payload".getBytes(StandardCharsets.UTF_8),
                        new MessageHeaders(correlationId, null));
            }

            assertTrue(received.await(30, TimeUnit.SECONDS));
        }

        assertEquals(correlationId, receivedHeaders.get().correlationId());
        assertNull(receivedHeaders.get().causationId());
    }
}
