package com.orchestrator.messaging.kafka;

import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.MessagePublisher;
import com.orchestrator.messaging.exception.MessagingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka-backed {@link MessagePublisher}. See {@link KafkaProducerConfig}'s
 * sandbox note.
 *
 * <p><b>Why {@link #publish} blocks (calls {@code .get()} on the send
 * future) rather than firing-and-forgetting asynchronously:</b> this
 * publisher's primary caller, in Phase 1's design, is
 * {@code OutboxPublisher} — which needs to know definitively whether a
 * publish succeeded before deciding whether to mark the corresponding
 * outbox row dispatched (see {@code PostgresOutboxStore.claimAndDispatch}).
 * An async fire-and-forget publish would make that decision impossible to
 * get right without inventing a separate confirmation-callback mechanism.
 * Blocking here trades a small amount of publisher throughput for a much
 * simpler, more obviously correct Outbox implementation — the right trade
 * for this system's realistic message volume (see Milestone 3 architecture
 * review Section 19, Scalability Analysis).
 */
public final class KafkaMessagePublisher implements MessagePublisher, AutoCloseable {

    private static final String HEADER_CORRELATION_ID = "correlationId";
    private static final String HEADER_CAUSATION_ID = "causationId";

    private final Producer<String, byte[]> producer;
    private final long publishTimeoutSeconds;

    public KafkaMessagePublisher(String bootstrapServers) {
        this(new KafkaProducer<>(KafkaProducerConfig.build(bootstrapServers)), 15L);
    }

    /** Test/DI seam — accepts a pre-built {@code Producer}, e.g. a mock or a Testcontainers-backed real one. */
    public KafkaMessagePublisher(Producer<String, byte[]> producer, long publishTimeoutSeconds) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
        this.publishTimeoutSeconds = publishTimeoutSeconds;
    }

    @Override
    public void publish(String topic, String key, byte[] payload, MessageHeaders headers) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(headers, "headers must not be null");

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, payload);
        record.headers().add(HEADER_CORRELATION_ID, headers.correlationId().toString().getBytes(StandardCharsets.UTF_8));
        if (headers.causationId() != null) {
            record.headers().add(HEADER_CAUSATION_ID, headers.causationId().toString().getBytes(StandardCharsets.UTF_8));
        }

        try {
            producer.send(record).get(publishTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingException("Interrupted while publishing to topic " + topic, e);
        } catch (ExecutionException e) {
            throw new MessagingException("Failed to publish to topic " + topic, e.getCause() != null ? e.getCause() : e);
        } catch (TimeoutException e) {
            throw new MessagingException("Timed out publishing to topic " + topic
                    + " after " + publishTimeoutSeconds + "s", e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
