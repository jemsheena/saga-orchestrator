package com.orchestrator.messaging.transaction;

/**
 * Simple transaction boundary abstraction for messaging-side components.
 */
public interface TransactionRunner {

    /**
     * Executes the provided work inside a transaction boundary.
     */
    void runInTransaction(Runnable work);
}
