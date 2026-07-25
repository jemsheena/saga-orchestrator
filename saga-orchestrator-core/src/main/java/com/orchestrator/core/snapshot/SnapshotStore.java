package com.orchestrator.core.snapshot;

import java.time.Instant;
import java.util.Optional;

/**
 * Generic snapshot persistence API.
 */
public interface SnapshotStore {

    void save(Snapshot snapshot);

    Optional<Snapshot> loadLatest(String aggregateType, String aggregateId);

    void delete(String aggregateType, String aggregateId);

    int deleteOlderThan(String aggregateType, Instant olderThan);

    boolean exists(String aggregateType, String aggregateId);

    /**
     * Purge snapshots for the given aggregate, keeping only the latest N entries.
     * @return number of rows deleted
     */
    int purgeExceptLatest(String aggregateType, String aggregateId, int keepLatest);
}
