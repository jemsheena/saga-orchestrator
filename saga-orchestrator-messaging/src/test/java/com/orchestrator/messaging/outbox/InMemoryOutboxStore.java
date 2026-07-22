package com.orchestrator.messaging.outbox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * In-memory {@link OutboxStore} upholding the same per-record failure
 * isolation contract as {@code PostgresOutboxStore} — a record whose
 * dispatch throws stays undispatched and is claimable again on the next
 * call; records that succeed are marked dispatched regardless of what
 * happens to other records in the same batch. Test-only.
 */
public final class InMemoryOutboxStore implements OutboxStore {

    private final Map<UUID, OutboxRecord> undispatched = new LinkedHashMap<>();
    private final List<OutboxRecord> dispatched = new ArrayList<>();

    @Override
    public synchronized void append(OutboxRecord record) {
        undispatched.put(record.outboxId(), record);
    }

    @Override
    public synchronized int claimAndDispatch(int limit, OutboxDispatcher dispatcher) {
        List<OutboxRecord> batch = undispatched.values().stream().limit(limit).toList();
        int dispatchedCount = 0;
        for (OutboxRecord record : batch) {
            try {
                dispatcher.dispatch(record);
                undispatched.remove(record.outboxId());
                dispatched.add(record);
                dispatchedCount++;
            } catch (Exception e) {
                // Left in `undispatched` - claimable again next call, matching
                // PostgresOutboxStore's per-record failure isolation contract.
            }
        }
        return dispatchedCount;
    }

    public synchronized List<OutboxRecord> dispatchedRecords() {
        return List.copyOf(dispatched);
    }

    public synchronized int undispatchedCount() {
        return undispatched.size();
    }
}
