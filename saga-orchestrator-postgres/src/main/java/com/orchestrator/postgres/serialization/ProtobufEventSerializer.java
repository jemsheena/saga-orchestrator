package com.orchestrator.postgres.serialization;

import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.postgres.serialization.proto.SagaEventProto;
import com.orchestrator.postgres.serialization.proto.SagaEventProto.CompensationStepCompleted;
import com.orchestrator.postgres.serialization.proto.SagaEventProto.SagaCompensationStarted;
import com.orchestrator.postgres.serialization.proto.SagaEventProto.SagaCompleted;
import com.orchestrator.postgres.serialization.proto.SagaEventProto.SagaFailed;
import com.orchestrator.postgres.serialization.proto.SagaEventProto.SagaStarted;
import com.orchestrator.postgres.serialization.proto.SagaEventProto.SagaTimedOut;
import com.orchestrator.postgres.serialization.proto.SagaEventProto.StepCompleted;
import com.orchestrator.postgres.serialization.proto.SagaEventProto.StepFailed;

import java.util.Objects;

public final class ProtobufEventSerializer implements SagaEventSerializer {

    private static final String CONTENT_TYPE = "application/vnd.com.orchestrator.event+proto";

    @Override
    public String eventType(SagaDomainEvent event) {
        return event.getClass().getSimpleName();
    }

    @Override
    public int schemaVersion(SagaDomainEvent event) {
        return 1;
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public byte[] serialize(SagaDomainEvent event) {
        SagaEventProto.SagaEvent.Builder builder = SagaEventProto.SagaEvent.newBuilder();
        switch (event) {
            case com.orchestrator.core.event.SagaStarted e -> builder.setSagaStarted(SagaStarted.newBuilder()
                    .setSagaId(e.sagaId().toString())
                    .setSagaType(e.definitionReference().sagaType())
                    .setDefinitionVersion(e.definitionReference().version())
                    .setTimeoutDuration(e.timeoutDuration() == null ? "" : e.timeoutDuration().toString())
                    .setOccurredAt(e.occurredAt().toString())
                    .build());
            case com.orchestrator.core.event.StepCompleted e -> builder.setStepCompleted(StepCompleted.newBuilder()
                    .setSagaId(e.sagaId().toString())
                    .setStepName(e.stepName())
                    .setStepIndex(e.stepIndex())
                    .setOccurredAt(e.occurredAt().toString())
                    .build());
            case com.orchestrator.core.event.SagaCompleted e -> builder.setSagaCompleted(SagaCompleted.newBuilder()
                    .setSagaId(e.sagaId().toString())
                    .setOccurredAt(e.occurredAt().toString())
                    .build());
            case com.orchestrator.core.event.StepFailed e -> builder.setStepFailed(StepFailed.newBuilder()
                    .setSagaId(e.sagaId().toString())
                    .setStepName(e.stepName())
                    .setStepIndex(e.stepIndex())
                    .setReason(e.reason() == null ? "" : e.reason())
                    .setOccurredAt(e.occurredAt().toString())
                    .build());
            case com.orchestrator.core.event.SagaCompensationStarted e -> builder.setSagaCompensationStarted(SagaCompensationStarted.newBuilder()
                    .setSagaId(e.sagaId().toString())
                    .setCompensationCursor(e.compensationCursor())
                    .setOccurredAt(e.occurredAt().toString())
                    .build());
            case com.orchestrator.core.event.CompensationStepCompleted e -> builder.setCompensationStepCompleted(CompensationStepCompleted.newBuilder()
                    .setSagaId(e.sagaId().toString())
                    .setStepName(e.stepName())
                    .setCompensationCursor(e.compensationCursor())
                    .setOccurredAt(e.occurredAt().toString())
                    .build());
            case com.orchestrator.core.event.SagaFailed e -> builder.setSagaFailed(SagaFailed.newBuilder()
                    .setSagaId(e.sagaId().toString())
                    .setOccurredAt(e.occurredAt().toString())
                    .build());
            case com.orchestrator.core.event.SagaTimedOut e -> builder.setSagaTimedOut(SagaTimedOut.newBuilder()
                    .setSagaId(e.sagaId().toString())
                    .setOccurredAt(e.occurredAt().toString())
                    .build());
            default -> throw new IllegalArgumentException("Unsupported event type " + event.getClass().getName());
        }
        return builder.build().toByteArray();
    }

    @Override
    public SagaDomainEvent deserialize(String eventType, int schemaVersion, String contentType, byte[] payload) {
        if (!CONTENT_TYPE.equals(contentType)) {
            throw new IllegalArgumentException("Unsupported content type: " + contentType);
        }
        try {
            SagaEventProto.SagaEvent proto = SagaEventProto.SagaEvent.parseFrom(payload);
            return switch (eventType) {
                case "SagaStarted" -> {
                    SagaStarted message = proto.getSagaStarted();
                    yield new com.orchestrator.core.event.SagaStarted(
                            java.util.UUID.fromString(message.getSagaId()),
                            new com.orchestrator.core.definition.SagaDefinitionReference(message.getSagaType(), message.getDefinitionVersion()),
                            message.getTimeoutDuration().isEmpty() ? null : java.time.Duration.parse(message.getTimeoutDuration()),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "StepCompleted" -> {
                    StepCompleted message = proto.getStepCompleted();
                    yield new com.orchestrator.core.event.StepCompleted(
                            java.util.UUID.fromString(message.getSagaId()),
                            message.getStepName(),
                            message.getStepIndex(),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "SagaCompleted" -> {
                    SagaCompleted message = proto.getSagaCompleted();
                    yield new com.orchestrator.core.event.SagaCompleted(
                            java.util.UUID.fromString(message.getSagaId()),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "StepFailed" -> {
                    StepFailed message = proto.getStepFailed();
                    yield new com.orchestrator.core.event.StepFailed(
                            java.util.UUID.fromString(message.getSagaId()),
                            message.getStepName(),
                            message.getStepIndex(),
                            message.getReason().isEmpty() ? null : message.getReason(),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "SagaCompensationStarted" -> {
                    SagaCompensationStarted message = proto.getSagaCompensationStarted();
                    yield new com.orchestrator.core.event.SagaCompensationStarted(
                            java.util.UUID.fromString(message.getSagaId()),
                            message.getCompensationCursor(),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "CompensationStepCompleted" -> {
                    CompensationStepCompleted message = proto.getCompensationStepCompleted();
                    yield new com.orchestrator.core.event.CompensationStepCompleted(
                            java.util.UUID.fromString(message.getSagaId()),
                            message.getStepName(),
                            message.getCompensationCursor(),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "SagaFailed" -> {
                    SagaFailed message = proto.getSagaFailed();
                    yield new com.orchestrator.core.event.SagaFailed(
                            java.util.UUID.fromString(message.getSagaId()),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "SagaTimedOut" -> {
                    SagaTimedOut message = proto.getSagaTimedOut();
                    yield new com.orchestrator.core.event.SagaTimedOut(
                            java.util.UUID.fromString(message.getSagaId()),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                default -> throw new IllegalArgumentException("Unknown event type '" + eventType + "' for protobuf deserialization");
            };
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize protobuf event payload to " + eventType, e);
        }
    }
}
