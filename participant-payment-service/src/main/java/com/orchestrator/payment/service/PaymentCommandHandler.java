package com.orchestrator.payment.service;

import com.google.protobuf.ByteString;
import com.orchestrator.messaging.MessageHandler;
import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.inbox.InboxStore;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.messaging.outbox.OutboxStore;
import com.orchestrator.messaging.proto.SagaCommand;
import com.orchestrator.messaging.proto.SagaReply;
import com.orchestrator.payment.domain.Payment;
import com.orchestrator.payment.domain.PaymentRepository;
import com.orchestrator.payment.domain.PaymentStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Transport-agnostic payment command handler. It uses the existing inbox and outbox abstractions
 * to remain resilient under at-least-once delivery and to preserve the repository's reliability model.
 */
public final class PaymentCommandHandler implements MessageHandler {

    private final PaymentRepository paymentRepository;
    private final InboxStore inboxStore;
    private final OutboxStore outboxStore;

    public PaymentCommandHandler(PaymentRepository paymentRepository, InboxStore inboxStore, OutboxStore outboxStore) {
        this.paymentRepository = Objects.requireNonNull(paymentRepository, "paymentRepository must not be null");
        this.inboxStore = Objects.requireNonNull(inboxStore, "inboxStore must not be null");
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
    }

    @Override
    public void handle(byte[] payload, MessageHeaders headers) throws Exception {
        SagaCommand command = SagaCommand.parseFrom(payload);
        UUID messageId = UUID.fromString(command.getCommandId());
        if (!inboxStore.recordIfNew(messageId)) {
            return;
        }

        Payment payment = paymentRepository.findBySagaId(command.getSagaId()).orElseGet(() ->
                Payment.pending(command.getSagaId(), parseAmount(command)));

        if ("ChargePaymentCommand".equals(command.getCommandType())) {
            handleCharge(payment, command, headers);
        } else if ("RefundPaymentCommand".equals(command.getCommandType())) {
            handleRefund(payment, command, headers);
        }
    }

    private void handleCharge(Payment payment, SagaCommand command, MessageHeaders headers) throws Exception {
        if (payment.status() == PaymentStatus.CHARGED) {
            publishReply(command, headers, true, "");
            return;
        }
        try {
            payment.charge(parseAmount(command));
            paymentRepository.save(payment);
            publishReply(command, headers, true, "");
        } catch (RuntimeException e) {
            payment.fail();
            paymentRepository.save(payment);
            publishReply(command, headers, false, e.getMessage());
        }
    }

    private void handleRefund(Payment payment, SagaCommand command, MessageHeaders headers) throws Exception {
        try {
            payment.refund();
            paymentRepository.save(payment);
            publishReply(command, headers, true, "");
        } catch (RuntimeException e) {
            payment.fail();
            paymentRepository.save(payment);
            publishReply(command, headers, false, e.getMessage());
        }
    }

    private void publishReply(SagaCommand command, MessageHeaders headers, boolean success, String reason) {
        SagaReply reply = SagaReply.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSagaId(command.getSagaId())
                .setStepName(command.getStepName())
                .setOutcome(success ? SagaReply.Outcome.SUCCESS : SagaReply.Outcome.FAILURE)
                .setReason(reason)
                .setPayload(ByteString.copyFromUtf8(success ? "payment-accepted" : "payment-rejected"))
                .build();

        outboxStore.append(new OutboxRecord(
                UUID.randomUUID(),
                "payment.events.v1",
                command.getSagaId(),
                "SagaReply",
                reply.toByteArray(),
                headers.correlationId(),
                headers.causationId(),
                Instant.now()));
    }

    private BigDecimal parseAmount(SagaCommand command) {
        return new BigDecimal(command.getPayload().toStringUtf8());
    }
}
