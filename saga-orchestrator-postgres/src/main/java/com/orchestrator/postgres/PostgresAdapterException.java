package com.orchestrator.postgres;

/**
 * Wraps an underlying {@code SQLException} or other infrastructure-level
 * failure from any {@code Postgres*} adapter class (event store, snapshot
 * store, view store, transaction runner) — deliberately named
 * adapter-scope-neutral rather than {@code PostgresEventStoreException}
 * (its Milestone 2 name), which was misleading once thrown from the
 * snapshot and view stores too. Renamed in Milestone 2.5 per code review
 * Important Finding #4.
 *
 * <p>Deliberately NOT placed in {@code com.orchestrator.core.exception} —
 * that package holds only pure domain exceptions that make sense regardless
 * of storage technology. A JDBC failure is specifically a Postgres-adapter
 * concern.
 */
public class PostgresAdapterException extends RuntimeException {
    public PostgresAdapterException(String message, Throwable cause) {
        super(message, cause);
    }
}
