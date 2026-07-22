package com.orchestrator.payment.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaymentTest {

    @Test
    void chargeTransitionsPendingPaymentToCharged() {
        Payment payment = Payment.pending("saga-1", new BigDecimal("100.00"));

        payment.charge(new BigDecimal("100.00"));

        assertEquals(PaymentStatus.CHARGED, payment.status());
        assertEquals(new BigDecimal("100.00"), payment.amount());
    }

    @Test
    void refundTransitionsChargedPaymentToRefunded() {
        Payment payment = Payment.pending("saga-1", new BigDecimal("100.00"));
        payment.charge(new BigDecimal("100.00"));

        payment.refund();

        assertEquals(PaymentStatus.REFUNDED, payment.status());
    }

    @Test
    void refundingPendingPaymentFails() {
        Payment payment = Payment.pending("saga-1", new BigDecimal("100.00"));

        assertThrows(IllegalStateException.class, payment::refund);
    }
}
