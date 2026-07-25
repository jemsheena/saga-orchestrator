package com.orchestrator.core.snapshot;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Generic aggregate snapshot model for performance-optimized rehydration.
 */
public final class Snapshot {

    private final UUID snapshotId;
    private final String aggregateId;
    private final String aggregateType;
    private final long aggregateVersion;
    private final int snapshotVersion;
    private final byte[] payload;
    private final Instant createdAt;

    public Snapshot(UUID snapshotId, String aggregateId, String aggregateType, long aggregateVersion, int snapshotVersion, byte[] payload, Instant createdAt) {
        this.snapshotId = Objects.requireNonNull(snapshotId, "snapshotId must not be null");
        this.aggregateId = Objects.requireNonNull(aggregateId, "aggregateId must not be null");
        this.aggregateType = Objects.requireNonNull(aggregateType, "aggregateType must not be null");
        if (aggregateVersion < 0) throw new IllegalArgumentException("aggregateVersion must not be negative");
        this.aggregateVersion = aggregateVersion;
        this.snapshotVersion = snapshotVersion;
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
    }

    public UUID snapshotId() { return snapshotId; }
    public String aggregateId() { return aggregateId; }
    public String aggregateType() { return aggregateType; }
    public long aggregateVersion() { return aggregateVersion; }
    public int snapshotVersion() { return snapshotVersion; }
    public byte[] payload() { return payload; }
    public Instant createdAt() { return createdAt; }
}
