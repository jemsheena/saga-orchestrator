package com.orchestrator.payment.service;

import com.google.protobuf.ByteString;
import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.inbox.InboxStore;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.messaging.outbox.OutboxStore;
import com.orchestrator.messaging.proto.SagaCommand;
import com.orchestrator.payment.domain.PaymentRepository;
import com.orchestrator.payment.infrastructure.InMemoryPaymentRepository;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaymentOutboxIntegrationTest {

    @Test
    void chargeCommand_writesReplyToOutbox() throws Exception {
        PaymentRepository repository = new InMemoryPaymentRepository();
        RecordingOutboxStore outboxStore = new RecordingOutboxStore();
        InboxStore inboxStore = new InboxStore() {
            @Override
            public boolean recordIfNew(UUID messageId, String consumer, String topic, String partitionKey) {
                return true;
            }

            @Override
            public boolean exists(UUID messageId, String consumer) {
                return false;
            }

            @Override
            public void save(com.orchestrator.messaging.inbox.InboxRecord record) {
            }

            @Override
            public void markProcessed(UUID messageId, String consumer) {
            }

            @Override
            public void markFailed(UUID messageId, String consumer) {
            }

            @Override
            public Optional<com.orchestrator.messaging.inbox.InboxRecord> find(UUID messageId, String consumer) {
                return Optional.empty();
            }

            @Override
            public int cleanup(java.time.Instant olderThan, int limit) {
                return 0;
            }
        };
        PaymentCommandHandler handler = new PaymentCommandHandler(repository, inboxStore, outboxStore);

        SagaCommand command = SagaCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setSagaId("saga-outbox")
                .setStepName("ChargePayment")
                .setCommandType("ChargePaymentCommand")
                .setPayload(ByteString.copyFromUtf8("50.00"))
                .build();

        handler.handle(command.toByteArray(), new MessageHeaders(UUID.randomUUID(), null));

        assertEquals(1, outboxStore.records.size());
        OutboxRecord record = outboxStore.records.get(0);
        assertEquals("payment.events.v1", record.topic());
        assertEquals("saga-outbox", record.messageKey());
    }

    private static final class RecordingOutboxStore implements OutboxStore {
        private final List<OutboxRecord> records = new ArrayList<>();

        @Override
        public void append(OutboxRecord record) {
            records.add(record);
        }

        @Override
        public int claimAndDispatch(int limit, com.orchestrator.messaging.outbox.OutboxDispatcher dispatcher) {
            return 0;
        }
    }
}
