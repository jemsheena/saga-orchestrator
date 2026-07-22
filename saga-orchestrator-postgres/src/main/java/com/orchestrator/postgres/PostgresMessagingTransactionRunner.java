package com.orchestrator.postgres;

import com.orchestrator.messaging.transaction.TransactionRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Messaging-side transaction runner adapter that delegates to Postgres JDBC.
 */
public final class PostgresMessagingTransactionRunner implements TransactionRunner {

    private final DataSource dataSource;

    public PostgresMessagingTransactionRunner(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void runInTransaction(Runnable work) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            ManagedConnection.bind(connection);
            try {
                work.run();
                connection.commit();
            } catch (RuntimeException e) {
                safeRollback(connection);
                throw e;
            } catch (Exception e) {
                safeRollback(connection);
                throw new RuntimeException(e);
            } finally {
                ManagedConnection.clear();
                closeQuietlyRestoringAutoCommit(connection);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to start messaging transaction", e);
        }
    }

    private void safeRollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            System.err.println("[WARN] Rollback failed after a messaging transaction error: " + rollbackFailure);
        }
    }

    private void closeQuietlyRestoringAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException closeFailure) {
            System.err.println("[WARN] Failed to cleanly close/reset connection after messaging transaction: " + closeFailure);
        }
    }
}
