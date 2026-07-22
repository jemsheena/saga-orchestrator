package com.orchestrator.payment.domain;

import java.util.Optional;

public interface PaymentRepository {

    void save(Payment payment);

    Optional<Payment> findBySagaId(String sagaId);
}
