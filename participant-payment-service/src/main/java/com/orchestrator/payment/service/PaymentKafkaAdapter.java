package com.orchestrator.payment.service;

import com.orchestrator.messaging.MessageConsumer;
import com.orchestrator.messaging.MessagePublisher;
import com.orchestrator.messaging.kafka.KafkaMessageConsumer;
import com.orchestrator.messaging.kafka.KafkaMessagePublisher;
import com.orchestrator.messaging.inbox.InboxStore;
import com.orchestrator.messaging.outbox.OutboxStore;
import com.orchestrator.payment.domain.PaymentRepository;

import java.util.List;

/**
 * Thin infrastructure adapter that wires the transport-agnostic payment handler into Kafka.
 */
public final class PaymentKafkaAdapter {

    private final MessageConsumer consumer;
    private final MessagePublisher publisher;

    public PaymentKafkaAdapter(String bootstrapServers, String groupId, List<String> topics,
                               PaymentRepository paymentRepository, InboxStore inboxStore, OutboxStore outboxStore) {
        this.publisher = new KafkaMessagePublisher(bootstrapServers);
        this.consumer = new KafkaMessageConsumer(bootstrapServers, groupId, topics,
                new PaymentCommandHandler(paymentRepository, inboxStore, outboxStore));
    }

    public void start() {
        consumer.start();
    }

    public void close() throws Exception {
        consumer.close();
        if (publisher instanceof AutoCloseable autoCloseable) {
            autoCloseable.close();
        }
    }
}
