package com.orchestrator.payment.domain;

import java.math.BigDecimal;
import java.util.Objects;

public final class Payment {

    private final String sagaId;
    private final BigDecimal amount;
    private PaymentStatus status;

    private Payment(String sagaId, BigDecimal amount, PaymentStatus status) {
        this.sagaId = Objects.requireNonNull(sagaId, "sagaId must not be null");
        this.amount = Objects.requireNonNull(amount, "amount must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
    }

    public static Payment pending(String sagaId, BigDecimal amount) {
        return new Payment(sagaId, amount, PaymentStatus.PENDING);
    }

    public void charge(BigDecimal amount) {
        if (!PaymentStatus.PENDING.equals(status)) {
            throw new IllegalStateException("Cannot charge a payment that is not pending");
        }
        if (amount.compareTo(this.amount) != 0) {
            throw new IllegalArgumentException("Charge amount must match the original amount");
        }
        this.status = PaymentStatus.CHARGED;
    }

    public void refund() {
        if (!PaymentStatus.CHARGED.equals(status)) {
            throw new IllegalStateException("Cannot refund a payment that is not charged");
        }
        this.status = PaymentStatus.REFUNDED;
    }

    public void fail() {
        if (PaymentStatus.PENDING.equals(status)) {
            this.status = PaymentStatus.FAILED;
        }
    }

    public String sagaId() {
        return sagaId;
    }

    public BigDecimal amount() {
        return amount;
    }

    public PaymentStatus status() {
        return status;
    }
}
