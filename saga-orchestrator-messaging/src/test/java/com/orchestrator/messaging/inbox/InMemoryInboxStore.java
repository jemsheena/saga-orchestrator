package com.orchestrator.messaging.inbox;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * In-memory {@link InboxStore} upholding the same atomicity contract as
 * {@code PostgresInboxStore} — {@link #recordIfNew} is synchronized so
 * "check and record" is genuinely one indivisible operation, mirroring what
 * Postgres's {@code INSERT ... ON CONFLICT DO NOTHING} + update-count check
 * achieves via the database's own primary-key uniqueness. Test-only.
 */
public final class InMemoryInboxStore implements InboxStore {

    private final Set<UUID> seen = new HashSet<>();

    @Override
    public synchronized boolean recordIfNew(UUID messageId) {
        return seen.add(messageId);
    }
}
