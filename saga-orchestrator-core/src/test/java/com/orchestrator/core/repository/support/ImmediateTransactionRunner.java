package com.orchestrator.core.repository.support;

import com.orchestrator.core.repository.TransactionRunner;

/**
 * Test-only {@link TransactionRunner} that simply runs the work with no
 * transactional coordination at all. Appropriate specifically because the
 * in-memory fakes it's paired with ({@code InMemorySagaEventStore},
 * {@code InMemorySagaInstanceViewStore}) have no real transaction to
 * coordinate in the first place — there is nothing for a "real" transaction
 * boundary to do here. Production code always uses
 * {@code JdbcTransactionRunner} instead.
 */
public final class ImmediateTransactionRunner implements TransactionRunner {
    @Override
    public void runInTransaction(Runnable work) {
        work.run();
    }
}
