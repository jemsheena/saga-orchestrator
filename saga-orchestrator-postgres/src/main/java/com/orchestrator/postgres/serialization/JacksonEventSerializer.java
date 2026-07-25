package com.orchestrator.postgres.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

public final class JacksonEventSerializer implements SagaEventSerializer {

    private static final String JSON_CONTENT_TYPE = "application/vnd.com.orchestrator.event+json";
    private static final String PROTOBUF_CONTENT_TYPE = "application/vnd.com.orchestrator.event+proto";
    private final ObjectMapper mapper;
    private final EventUpcasterRegistry upcasterRegistry;

    public JacksonEventSerializer() {
        this(new ObjectMapper(), new EventUpcasterRegistry());
    }

    public JacksonEventSerializer(ObjectMapper mapper, EventUpcasterRegistry upcasterRegistry) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.upcasterRegistry = Objects.requireNonNull(upcasterRegistry, "upcasterRegistry must not be null");
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

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
        return JSON_CONTENT_TYPE;
    }

    @Override
    public byte[] serialize(SagaDomainEvent event) {
        try {
            return mapper.writeValueAsBytes(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event to JSON", e);
        }
    }

    @Override
    public SagaDomainEvent deserialize(String eventType, int schemaVersion, String contentType, byte[] payload) {
        if (contentType == null || JSON_CONTENT_TYPE.equals(contentType)) {
            return deserializeJson(eventType, payload);
        }
        if (PROTOBUF_CONTENT_TYPE.equals(contentType)) {
            return deserializeProto(eventType, payload);
        }
        return upcasterRegistry.upcast(eventType, schemaVersion, contentType, payload);
    }

    private SagaDomainEvent deserializeJson(String eventType, byte[] payload) {
        try {
            Class<? extends SagaDomainEvent> eventClass = eventClassForType(eventType);
            return mapper.readValue(payload, eventClass);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize JSON event payload to " + eventType, e);
        }
    }

    private SagaDomainEvent deserializeProto(String eventType, byte[] payload) {
        try {
            SagaEventProto.SagaEvent proto = SagaEventProto.SagaEvent.parseFrom(payload);
            switch (eventType) {
                case "SagaStarted" -> {
                    SagaStarted message = proto.getSagaStarted();
                    return new com.orchestrator.core.event.SagaStarted(
                            java.util.UUID.fromString(message.getSagaId()),
                            new com.orchestrator.core.definition.SagaDefinitionReference(message.getSagaType(), message.getDefinitionVersion()),
                            message.getTimeoutDuration().isEmpty() ? null : java.time.Duration.parse(message.getTimeoutDuration()),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "StepCompleted" -> {
                    StepCompleted message = proto.getStepCompleted();
                    return new com.orchestrator.core.event.StepCompleted(
                            java.util.UUID.fromString(message.getSagaId()),
                            message.getStepName(),
                            message.getStepIndex(),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "SagaCompleted" -> {
                    SagaCompleted message = proto.getSagaCompleted();
                    return new com.orchestrator.core.event.SagaCompleted(
                            java.util.UUID.fromString(message.getSagaId()),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "StepFailed" -> {
                    StepFailed message = proto.getStepFailed();
                    return new com.orchestrator.core.event.StepFailed(
                            java.util.UUID.fromString(message.getSagaId()),
                            message.getStepName(),
                            message.getStepIndex(),
                            message.getReason().isEmpty() ? null : message.getReason(),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "SagaCompensationStarted" -> {
                    SagaCompensationStarted message = proto.getSagaCompensationStarted();
                    return new com.orchestrator.core.event.SagaCompensationStarted(
                            java.util.UUID.fromString(message.getSagaId()),
                            message.getCompensationCursor(),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "CompensationStepCompleted" -> {
                    CompensationStepCompleted message = proto.getCompensationStepCompleted();
                    return new com.orchestrator.core.event.CompensationStepCompleted(
                            java.util.UUID.fromString(message.getSagaId()),
                            message.getStepName(),
                            message.getCompensationCursor(),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "SagaFailed" -> {
                    SagaFailed message = proto.getSagaFailed();
                    return new com.orchestrator.core.event.SagaFailed(
                            java.util.UUID.fromString(message.getSagaId()),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                case "SagaTimedOut" -> {
                    SagaTimedOut message = proto.getSagaTimedOut();
                    return new com.orchestrator.core.event.SagaTimedOut(
                            java.util.UUID.fromString(message.getSagaId()),
                            java.time.Instant.parse(message.getOccurredAt()));
                }
                default -> throw new IllegalArgumentException("Unknown event type '" + eventType + "' for protobuf deserialization");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize protobuf event payload to " + eventType, e);
        }
    }

    private static Class<? extends SagaDomainEvent> eventClassForType(String eventType) {
        return switch (eventType) {
            case "SagaStarted" -> com.orchestrator.core.event.SagaStarted.class;
            case "StepCompleted" -> com.orchestrator.core.event.StepCompleted.class;
            case "SagaCompleted" -> com.orchestrator.core.event.SagaCompleted.class;
            case "StepFailed" -> com.orchestrator.core.event.StepFailed.class;
            case "SagaCompensationStarted" -> com.orchestrator.core.event.SagaCompensationStarted.class;
            case "CompensationStepCompleted" -> com.orchestrator.core.event.CompensationStepCompleted.class;
            case "SagaFailed" -> com.orchestrator.core.event.SagaFailed.class;
            case "SagaTimedOut" -> com.orchestrator.core.event.SagaTimedOut.class;
            default -> throw new IllegalArgumentException("Unknown event type '" + eventType + "' encountered during deserialization");
        };
    }
}
