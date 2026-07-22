package com.orchestrator.messaging.kafka;

import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.outbox.InMemoryOutboxStore;
import com.orchestrator.messaging.outbox.OutboxPublisher;
import com.orchestrator.messaging.outbox.OutboxRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class KafkaOutboxPublisherIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.0");

    private static final String TOPIC = "test.outbox.publish.v1";

    @Test
    void outboxPublisher_publishesPendingRecords_toKafka() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        try (KafkaMessagePublisher publisher = new KafkaMessagePublisher(KAFKA.getBootstrapServers());
             KafkaMessageConsumer consumer = new KafkaMessageConsumer(
                     KAFKA.getBootstrapServers(), "test-group-outbox", java.util.List.of(TOPIC),
                     (payload, headers) -> {
                         // no-op handler; we only need the message body and headers to arrive
                     })) {
            AtomicReference<byte[]> payloadRef = new AtomicReference<>();
            AtomicReference<MessageHeaders> headersRef = new AtomicReference<>();
            CountDownLatch received = new CountDownLatch(1);

            try (KafkaMessageConsumer recordingConsumer = new KafkaMessageConsumer(
                    KAFKA.getBootstrapServers(), "test-group-outbox-2", java.util.List.of(TOPIC),
                    (payload, headers) -> {
                        payloadRef.set(payload);
                        headersRef.set(headers);
                        received.countDown();
                    })) {
                recordingConsumer.start();

                UUID correlationId = UUID.randomUUID();
                UUID causationId = UUID.randomUUID();
                store.append(new OutboxRecord(
                        UUID.randomUUID(),
                        TOPIC,
                        "outbox-key",
                        "SagaCommand",
                        "hello Kafka outbox".getBytes(StandardCharsets.UTF_8),
                        correlationId,
                        causationId,
                        Instant.now()));

                OutboxPublisher outboxPublisher = new OutboxPublisher(store, publisher, 10);
                int dispatched = outboxPublisher.pollOnce();

                assertEquals(1, dispatched);
                assertTrue(received.await(20, TimeUnit.SECONDS), "message should be consumed from Kafka");
                assertEquals("hello Kafka outbox", new String(payloadRef.get(), StandardCharsets.UTF_8));
                assertEquals(correlationId, headersRef.get().correlationId());
                assertEquals(causationId, headersRef.get().causationId());
            }
        }
    }
}
