package com.orchestrator.core.repository;

/**
 * A framework-agnostic transaction boundary. Exists specifically so
 * {@code DefaultSagaInstanceRepository} can require that event-append and
 * read-model projection happen atomically, without core knowing anything
 * about JDBC, connections, or any specific database technology — see
 * Milestone 2.5 write-up, "Alternative Designs Considered," Option A vs. B,
 * for why this exists instead of threading a {@code java.sql.Connection}
 * through the repository interfaces.
 *
 * <p>Production code uses a JDBC-backed implementation from the
 * {@code saga-orchestrator-postgres} module. Tests use a trivial
 * pass-through implementation, since in-memory fakes have no real
 * transactional semantics to coordinate.
 */
public interface TransactionRunner {

    /**
     * Runs {@code work} as a single atomic unit. If {@code work} throws any
     * {@code RuntimeException}, everything it did must be rolled back as a
     * whole. Implementations are not required to support nested/re-entrant
     * calls on the same thread — see {@code JdbcTransactionRunner}, which
     * deliberately fails loudly rather than silently guessing at
     * untested nested-transaction semantics.
     */
    void runInTransaction(Runnable work);
}
