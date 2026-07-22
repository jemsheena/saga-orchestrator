package com.orchestrator.payment.infrastructure;

import com.orchestrator.payment.domain.Payment;
import com.orchestrator.payment.domain.PaymentRepository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryPaymentRepository implements PaymentRepository {

    private final Map<String, Payment> payments = new ConcurrentHashMap<>();

    @Override
    public void save(Payment payment) {
        payments.put(payment.sagaId(), payment);
    }

    @Override
    public Optional<Payment> findBySagaId(String sagaId) {
        return Optional.ofNullable(payments.get(sagaId));
    }
}
