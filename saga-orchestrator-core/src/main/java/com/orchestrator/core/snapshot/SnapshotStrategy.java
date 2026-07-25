package com.orchestrator.core.snapshot;

import java.util.Objects;

/**
 * Strategy for when to take snapshots and how many to retain.
 */
public final class SnapshotStrategy {

    private final long snapshotEveryEvents;
    private final int keepLatest;

    public SnapshotStrategy(long snapshotEveryEvents, int keepLatest) {
        if (snapshotEveryEvents < 1) throw new IllegalArgumentException("snapshotEveryEvents must be >=1");
        if (keepLatest < 1) throw new IllegalArgumentException("keepLatest must be >=1");
        this.snapshotEveryEvents = snapshotEveryEvents;
        this.keepLatest = keepLatest;
    }

    public long snapshotEveryEvents() { return snapshotEveryEvents; }
    public int keepLatest() { return keepLatest; }

    public static SnapshotStrategy defaultStrategy() {
        return new SnapshotStrategy(20, 1);
    }
}
