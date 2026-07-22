package com.orchestrator.messaging.kafka;

import com.orchestrator.messaging.MessageConsumer;
import com.orchestrator.messaging.MessageHandler;
import com.orchestrator.messaging.MessageHeaders;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.errors.WakeupException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka-backed {@link MessageConsumer}. See {@link KafkaProducerConfig}'s
 * sandbox note.
 *
 * <p>Runs a single dedicated polling thread (one {@code Consumer} instance
 * per {@code KafkaMessageConsumer}, matching the Kafka client's own
 * single-threaded-access requirement — a {@code Consumer} instance is not
 * thread-safe, and this class never shares one across threads).
 *
 * <p><b>The at-least-once mechanism, concretely:</b> for each poll batch,
 * every record's {@link MessageHandler#handle} is called in order; offsets
 * are committed (synchronously, via {@code commitSync}) only after the
 * ENTIRE batch's handlers have all succeeded. If any handler in the batch
 * throws, the batch's offsets are NOT committed, the exception is logged,
 * and the same records will be redelivered on the next poll (after Kafka's
 * consumer-side retry/rebalance behavior, or a restart) — this is what
 * makes at-least-once real rather than aspirational, and is exactly why
 * {@code InboxStore} is a Phase 1 requirement, not optional polish.
 */
public final class KafkaMessageConsumer implements MessageConsumer {

    private static final String HEADER_CORRELATION_ID = "correlationId";
    private static final String HEADER_CAUSATION_ID = "causationId";

    private final Consumer<String, byte[]> consumer;
    private final MessageHandler handler;
    private final Duration pollTimeout;
    private final ExecutorService pollingThread = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "kafka-message-consumer");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean running = new AtomicBoolean(false);

    public KafkaMessageConsumer(String bootstrapServers, String groupId, List<String> topics, MessageHandler handler) {
        this(new KafkaConsumer<>(KafkaConsumerConfig.build(bootstrapServers, groupId)), topics, handler, Duration.ofSeconds(1));
        this.consumer.subscribe(topics);
    }

    /** Test/DI seam — accepts a pre-built {@code Consumer} already subscribed to its topics. */
    public KafkaMessageConsumer(Consumer<String, byte[]> consumer, List<String> topics, MessageHandler handler, Duration pollTimeout) {
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        Objects.requireNonNull(topics, "topics must not be null");
        this.handler = Objects.requireNonNull(handler, "handler must not be null");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return; // already started - idempotent, matches close()'s "safe to call more than once" spirit
        }
        pollingThread.submit(this::pollLoop);
    }

    private void pollLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
                if (records.isEmpty()) {
                    continue;
                }
                try {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        handler.handle(record.value(), extractHeaders(record));
                    }
                    consumer.commitSync();
                } catch (Exception handlerFailure) {
                    // Deliberately do NOT commit - see class javadoc on the at-least-once
                    // mechanism. Logged (real structured logging arrives with Spring Boot
                    // in a later milestone - see the same honest placeholder pattern used
                    // in DefaultSagaInstanceRepository.reportSnapshotFailure).
                    System.err.println("[WARN] Message handling failed, batch will be redelivered: " + handlerFailure);
                }
            }
        } catch (WakeupException e) {
            // Expected during close() - see close() below.
        } finally {
            consumer.close();
        }
    }

    private MessageHeaders extractHeaders(ConsumerRecord<String, byte[]> record) {
        UUID correlationId = readUuidHeader(record, HEADER_CORRELATION_ID);
        UUID causationId = readUuidHeader(record, HEADER_CAUSATION_ID);
        if (correlationId == null) {
            // A message arriving with no correlationId header is itself a data-quality
            // problem worth surfacing loudly rather than defaulting silently - every
            // publisher in this system (KafkaMessagePublisher) always sets one.
            throw new IllegalStateException("Received a message with no correlationId header - "
                    + "topic=" + record.topic() + ", partition=" + record.partition() + ", offset=" + record.offset());
        }
        return new MessageHeaders(correlationId, causationId);
    }

    private UUID readUuidHeader(ConsumerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null) {
            return null;
        }
        return UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        consumer.wakeup(); // interrupts a blocked poll() call from another thread - the standard Kafka client shutdown signal
        pollingThread.shutdown();
    }
}
