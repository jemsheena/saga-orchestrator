package com.orchestrator.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestrator.core.snapshot.SnapshotSerializer;

import java.util.Objects;

public final class JacksonSnapshotSerializer implements SnapshotSerializer {

    private final ObjectMapper mapper;

    public JacksonSnapshotSerializer(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    }

    @Override
    public byte[] serialize(Object payload) {
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize snapshot payload", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> target) {
        try {
            return mapper.readValue(data, target);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize snapshot payload", e);
        }
    }
}
