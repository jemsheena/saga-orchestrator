package com.orchestrator.messaging.kafka;

import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.SagaEventPublisher;
import com.orchestrator.messaging.MessagePublisher;

import java.util.Objects;
import java.util.UUID;

/**
 * Thin saga-specific adapter over {@link MessagePublisher}.
 */
public final class KafkaSagaEventPublisher implements SagaEventPublisher {

    private final MessagePublisher delegate;
    private final String startedTopic;
    private final String stepCompletedTopic;
    private final String failedTopic;

    public KafkaSagaEventPublisher(MessagePublisher delegate,
                                   String startedTopic,
                                   String stepCompletedTopic,
                                   String failedTopic) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.startedTopic = Objects.requireNonNull(startedTopic, "startedTopic must not be null");
        this.stepCompletedTopic = Objects.requireNonNull(stepCompletedTopic, "stepCompletedTopic must not be null");
        this.failedTopic = Objects.requireNonNull(failedTopic, "failedTopic must not be null");
    }

    @Override
    public void publishSagaStarted(UUID sagaId, byte[] payload, MessageHeaders headers) {
        delegate.publish(startedTopic, sagaId.toString(), payload, headers);
    }

    @Override
    public void publishStepCompleted(UUID sagaId, byte[] payload, MessageHeaders headers) {
        delegate.publish(stepCompletedTopic, sagaId.toString(), payload, headers);
    }

    @Override
    public void publishSagaFailed(UUID sagaId, byte[] payload, MessageHeaders headers) {
        delegate.publish(failedTopic, sagaId.toString(), payload, headers);
    }
}
