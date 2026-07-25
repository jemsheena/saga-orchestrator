package com.orchestrator.messaging.kafka;

import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.MessagePublisher;
import com.orchestrator.messaging.inbox.DeadLetterMessage;
import com.orchestrator.messaging.inbox.DeadLetterPublisher;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Simple DLQ publisher that serializes a DeadLetterMessage into a UTF-8 text
 * representation and publishes it using a provided {@link MessagePublisher}.
 */
public final class KafkaDeadLetterPublisher implements DeadLetterPublisher {

    private final MessagePublisher publisher;
    private final String topic;

    public KafkaDeadLetterPublisher(MessagePublisher publisher, String topic) {
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
    }

    @Override
    public void publish(DeadLetterMessage message, MessageHeaders headers) {
        // Lightweight serialization: human-readable fields + base64 payload would be nicer,
        // but keeping this dependency-free and simple for now.
        StringBuilder sb = new StringBuilder();
        sb.append("messageId=").append(message.messageId()).append('\n');
        sb.append("sagaId=").append(message.sagaId().orElse("")).append('\n');
        sb.append("topic=").append(message.topic()).append('\n');
        sb.append("failedAt=").append(DateTimeFormatter.ISO_INSTANT.format(message.failedAt())).append('\n');
        sb.append("retryCount=").append(message.retryCount()).append('\n');
        sb.append("exceptionClass=").append(message.exceptionClass()).append('\n');
        sb.append("exceptionMessage=").append(message.exceptionMessage()).append('\n');
        sb.append("payload=").append(new String(message.payload(), StandardCharsets.UTF_8)).append('\n');

        publisher.publish(topic, message.messageId().toString(), sb.toString().getBytes(StandardCharsets.UTF_8), headers);
    }
}
