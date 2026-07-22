package com.orchestrator.payment.infrastructure;

import com.orchestrator.payment.domain.Payment;
import com.orchestrator.payment.domain.PaymentRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryPaymentRepositoryTest {

    @Test
    void saveAndFindRoundTrip() {
        PaymentRepository repository = new InMemoryPaymentRepository();
        Payment payment = Payment.pending("saga-2", new BigDecimal("50.00"));

        repository.save(payment);
        Optional<Payment> loaded = repository.findBySagaId("saga-2");

        assertTrue(loaded.isPresent());
        assertEquals("saga-2", loaded.get().sagaId());
        assertEquals(new BigDecimal("50.00"), loaded.get().amount());
    }
}
