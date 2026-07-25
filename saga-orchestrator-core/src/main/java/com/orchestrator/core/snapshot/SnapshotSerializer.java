package com.orchestrator.core.snapshot;

/**
 * Serializes/deserializes snapshot payloads.
 */
public interface SnapshotSerializer {

    byte[] serialize(Object payload);

    <T> T deserialize(byte[] data, Class<T> target);
}
