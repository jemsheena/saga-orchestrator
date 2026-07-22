package com.orchestrator.core.engine;

import com.orchestrator.core.definition.SagaDefinition;
import com.orchestrator.core.definition.SagaDefinitionReference;
import com.orchestrator.core.definition.SagaStep;
import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaDefinitionRegistry;
import com.orchestrator.core.repository.SagaInstanceRepository;
import com.orchestrator.core.repository.support.InMemorySagaDefinitionRegistry;
import com.orchestrator.core.repository.support.InMemorySagaEventStore;
import com.orchestrator.core.repository.support.InMemorySagaInstanceViewStore;
import com.orchestrator.core.repository.support.InMemorySagaSnapshotStore;
import com.orchestrator.core.repository.support.ImmediateTransactionRunner;
import com.orchestrator.core.projection.SagaProjector;
import com.orchestrator.core.repository.support.DefaultSagaInstanceRepository;
import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.inbox.InboxStore;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.messaging.outbox.OutboxStore;
import com.orchestrator.messaging.proto.SagaReply;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaOrchestratorTest {

    @Test
    void successReply_advancesSagaToNextStep() throws Exception {
        SagaDefinition definition = SagaDefinition.builder("PaymentSaga")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("RefundPayment", "RefundPaymentCommand", null))
                .build();
        SagaDefinitionRegistry registry = new InMemorySagaDefinitionRegistry();
        registry.register(definition);

        SagaInstanceRepository repository = new DefaultSagaInstanceRepository(
                new InMemorySagaEventStore(),
                new InMemorySagaSnapshotStore(),
                new InMemorySagaInstanceViewStore(),
                new SagaProjector(),
                new ImmediateTransactionRunner(),
                registry,
                10,
                1);

        SagaInstance instance = SagaInstance.start(definition);
        repository.save(instance, new EventMetadata(UUID.randomUUID(), null));

        RecordingOutboxStore outboxStore = new RecordingOutboxStore();
        InboxStore inboxStore = new InboxStore() {
            @Override
            public boolean recordIfNew(UUID messageId) {
                return true;
            }
        };
        SagaOrchestrator orchestrator = new SagaOrchestrator(repository, registry, inboxStore, outboxStore);

        SagaReply reply = SagaReply.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSagaId(instance.sagaId().toString())
                .setStepName("ChargePayment")
                .setOutcome(SagaReply.Outcome.SUCCESS)
                .build();

        orchestrator.handle(reply.toByteArray(), new MessageHeaders(UUID.randomUUID(), null));

        SagaInstance reloaded = repository.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.STEP_COMPLETED, reloaded.state());
        assertEquals(1, reloaded.currentStepIndex());
        assertTrue(outboxStore.records.isEmpty());
    }

    @Test
    void failureReply_triggersCompensationPath() throws Exception {
        SagaDefinition definition = SagaDefinition.builder("PaymentSaga")
                .addStep(new SagaStep("ChargePayment", "ChargePaymentCommand", "RefundPaymentCommand"))
                .addStep(new SagaStep("RefundPayment", "RefundPaymentCommand", null))
                .build();
        SagaDefinitionRegistry registry = new InMemorySagaDefinitionRegistry();
        registry.register(definition);

        SagaInstanceRepository repository = new DefaultSagaInstanceRepository(
                new InMemorySagaEventStore(),
                new InMemorySagaSnapshotStore(),
                new InMemorySagaInstanceViewStore(),
                new SagaProjector(),
                new ImmediateTransactionRunner(),
                registry,
                10,
                1);

        SagaInstance instance = SagaInstance.start(definition);
        repository.save(instance, new EventMetadata(UUID.randomUUID(), null));
        instance.completeCurrentStep(definition, "ChargePayment");
        repository.save(instance, new EventMetadata(UUID.randomUUID(), null));

        RecordingOutboxStore outboxStore = new RecordingOutboxStore();
        InboxStore inboxStore = new InboxStore() {
            @Override
            public boolean recordIfNew(UUID messageId) {
                return true;
            }
        };
        SagaOrchestrator orchestrator = new SagaOrchestrator(repository, registry, inboxStore, outboxStore);

        SagaReply reply = SagaReply.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSagaId(instance.sagaId().toString())
                .setStepName("RefundPayment")
                .setOutcome(SagaReply.Outcome.FAILURE)
                .setReason("payment failed")
                .build();

        orchestrator.handle(reply.toByteArray(), new MessageHeaders(UUID.randomUUID(), null));

        SagaInstance reloaded = repository.findById(instance.sagaId()).orElseThrow();
        assertEquals(SagaState.COMPENSATING, reloaded.state());
    }

    private static final class RecordingOutboxStore implements OutboxStore {
        private final List<OutboxRecord> records = new java.util.ArrayList<>();

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
