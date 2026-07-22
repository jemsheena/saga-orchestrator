package com.orchestrator.messaging;

import java.util.UUID;

/**
 * Domain-facing saga event publisher. Hides transport details behind a simple
 * saga-oriented publishing contract.
 */
public interface SagaEventPublisher {

    void publishSagaStarted(UUID sagaId, byte[] payload, MessageHeaders headers);

    void publishStepCompleted(UUID sagaId, byte[] payload, MessageHeaders headers);

    void publishSagaFailed(UUID sagaId, byte[] payload, MessageHeaders headers);
}
