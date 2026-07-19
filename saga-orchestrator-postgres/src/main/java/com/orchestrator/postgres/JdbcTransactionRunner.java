package com.orchestrator.postgres;

import com.orchestrator.core.repository.TransactionRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * JDBC-backed {@link TransactionRunner}. Opens exactly one {@link Connection}
 * per call, binds it to the current thread via {@link ManagedConnection} so
 * that any {@code Postgres*} store method invoked (directly or transitively)
 * during {@code work.run()} joins the same transaction instead of opening
 * its own, commits on success, rolls back on any {@code RuntimeException},
 * and always unbinds — see {@link ManagedConnection#clear} on why that must
 * be unconditional.
 *
 * <p><b>Does not support nested/re-entrant calls on the same thread.</b> If
 * {@code runInTransaction} is invoked while a transaction is already bound
 * to the calling thread, this throws {@link IllegalStateException} rather
 * than silently reusing the outer transaction or silently starting an
 * unrelated inner one — both of those behaviors are plausible-sounding but
 * neither is implemented or tested here, and Milestone 2.5's scope is
 * fixing the two Critical findings, not building untested support for a
 * scenario nothing in this codebase currently needs. Revisit only if a
 * future milestone actually introduces a nested-call use case.
 */
public final class JdbcTransactionRunner implements TransactionRunner {

    private final DataSource dataSource;

    public JdbcTransactionRunner(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void runInTransaction(Runnable work) {
        if (ManagedConnection.currentlyBound().isPresent()) {
            throw new IllegalStateException(
                    "runInTransaction() called while a transaction is already bound to this thread. "
                            + "Nested/re-entrant transactions are not supported - see class javadoc.");
        }

        Connection connection;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to open a connection to start a transaction", e);
        }

        ManagedConnection.bind(connection);
        try {
            work.run();
            connection.commit();
        } catch (RuntimeException e) {
            safeRollback(connection);
            throw e;
        } catch (SQLException e) {
            safeRollback(connection);
            throw new PostgresAdapterException("Failed to commit transaction", e);
        } finally {
            ManagedConnection.clear();
            closeQuietlyRestoringAutoCommit(connection);
        }
    }

    private void safeRollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            // The original exception is what the caller needs to see and already
            // will, via the rethrow at each call site above; a rollback failure on
            // top of that is logged, not allowed to mask the real cause.
            System.err.println("[WARN] Rollback failed after a transaction error: " + rollbackFailure);
        }
    }

    private void closeQuietlyRestoringAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException closeFailure) {
            System.err.println("[WARN] Failed to cleanly close/reset connection after transaction: " + closeFailure);
        }
    }
}
