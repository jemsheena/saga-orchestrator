package com.orchestrator.core.engine;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaDefinitionRegistry;
import com.orchestrator.core.repository.SagaInstanceRepository;
import com.orchestrator.messaging.MessageHandler;
import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.inbox.InboxStore;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.messaging.outbox.OutboxStore;
import com.orchestrator.messaging.proto.SagaReply;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Thin orchestrator boundary that consumes participant replies and advances the saga aggregate.
 * It stays transport-agnostic and reuses the existing saga engine and messaging abstractions.
 */
public final class SagaOrchestrator implements MessageHandler {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaDefinitionRegistry sagaDefinitionRegistry;
    private final InboxStore inboxStore;
    private final OutboxStore outboxStore;

    public SagaOrchestrator(SagaInstanceRepository sagaInstanceRepository,
                            SagaDefinitionRegistry sagaDefinitionRegistry,
                            InboxStore inboxStore,
                            OutboxStore outboxStore) {
        this.sagaInstanceRepository = Objects.requireNonNull(sagaInstanceRepository, "sagaInstanceRepository must not be null");
        this.sagaDefinitionRegistry = Objects.requireNonNull(sagaDefinitionRegistry, "sagaDefinitionRegistry must not be null");
        this.inboxStore = Objects.requireNonNull(inboxStore, "inboxStore must not be null");
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
    }

    @Override
    public void handle(byte[] payload, MessageHeaders headers) throws Exception {
        SagaReply reply = SagaReply.parseFrom(payload);
        UUID messageId = UUID.fromString(reply.getEventId());
        if (!inboxStore.recordIfNew(messageId)) {
            return;
        }

        SagaInstance instance = sagaInstanceRepository.findById(UUID.fromString(reply.getSagaId())).orElseThrow(
                () -> new IllegalStateException("Saga instance not found: " + reply.getSagaId()));
        SagaDefinition definition = sagaDefinitionRegistry.resolve(instance.definitionReference()).orElseThrow(
                () -> new IllegalStateException("Saga definition not found for saga " + reply.getSagaId()));

        if (reply.getOutcome() == SagaReply.Outcome.SUCCESS) {
            instance.completeCurrentStep(definition, reply.getStepName());
        } else {
            instance.failCurrentStep(definition, reply.getStepName(), reply.getReason());
        }

        sagaInstanceRepository.save(instance, new EventMetadata(headers.correlationId(), headers.causationId()));
        publishCompensationIfNeeded(instance, definition, reply, headers);
    }

    private void publishCompensationIfNeeded(SagaInstance instance, SagaDefinition definition, SagaReply reply, MessageHeaders headers) {
        if (instance.state() == SagaState.COMPENSATING) {
            outboxStore.append(new OutboxRecord(
                    UUID.randomUUID(),
                    "saga.compensation.v1",
                    instance.sagaId().toString(),
                    "SagaCompensation",
                    reply.toByteArray(),
                    headers.correlationId(),
                    headers.causationId(),
                    Instant.now()));
        }
    }
}
