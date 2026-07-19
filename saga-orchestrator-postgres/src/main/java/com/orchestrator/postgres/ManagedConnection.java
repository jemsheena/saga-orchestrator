package com.orchestrator.postgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Wraps a {@link Connection} together with whether the CURRENT call site
 * "owns" it (opened it, and is therefore responsible for commit/rollback/
 * restoring autocommit/closing it) or is "borrowing" it from an
 * already-active transaction bound by {@link JdbcTransactionRunner} (in
 * which case none of that lifecycle management belongs to this call site —
 * the outer transaction owns it).
 *
 * <p>This is the single place that branching logic lives. Before Milestone
 * 2.5, each {@code Postgres*} store class always opened, committed, and
 * closed its own connection unconditionally — which is exactly why
 * event-append and view-projection weren't actually atomic (Milestone 2
 * code review, Critical Finding #1). Centralizing the branch here, rather
 * than duplicating an {@code if (ownsConnection) ...} check in every store
 * method, is what keeps that fix from having to be repeated (and potentially
 * gotten subtly wrong in only one of the places) across the module.
 *
 * <p>Package-private — this is an internal mechanism of the Postgres
 * adapter module, never part of its public API.
 */
final class ManagedConnection implements AutoCloseable {

    private static final ThreadLocal<Connection> BOUND_CONNECTION = new ThreadLocal<>();

    private final Connection connection;
    private final boolean owned;

    private ManagedConnection(Connection connection, boolean owned) {
        this.connection = connection;
        this.owned = owned;
    }

    /**
     * If a transaction is currently bound to this thread (via
     * {@link JdbcTransactionRunner}), returns a borrowed, non-owning wrapper
     * around it. Otherwise opens a brand-new connection from
     * {@code dataSource}, sets it to manual-commit mode, and returns an
     * owning wrapper responsible for its own lifecycle.
     */
    static ManagedConnection obtain(DataSource dataSource) throws SQLException {
        Connection bound = BOUND_CONNECTION.get();
        if (bound != null) {
            return new ManagedConnection(bound, false);
        }
        Connection fresh = dataSource.getConnection();
        fresh.setAutoCommit(false);
        return new ManagedConnection(fresh, true);
    }

    /** Used only by {@link JdbcTransactionRunner} to publish the connection it owns for this thread. */
    static void bind(Connection connection) {
        BOUND_CONNECTION.set(connection);
    }

    /**
     * Used only by {@link JdbcTransactionRunner}. Always called in a
     * {@code finally} block by that class — an unbind that's skipped due to
     * an exception path would leak a stale bound connection into whatever
     * this pooled thread handles next, a real and known risk of any
     * ThreadLocal-based mechanism. Centralizing bind/clear to exactly one
     * caller (the transaction runner) is what keeps that discipline
     * enforceable in one place instead of trusted at every call site.
     */
    static void clear() {
        BOUND_CONNECTION.remove();
    }

    static Optional<Connection> currentlyBound() {
        return Optional.ofNullable(BOUND_CONNECTION.get());
    }

    Connection connection() {
        return connection;
    }

    /** Commits only if this call site owns the connection; a no-op when borrowed. */
    void commitIfOwned() throws SQLException {
        if (owned) {
            connection.commit();
        }
    }

    /** Rolls back only if this call site owns the connection; a no-op when borrowed. */
    void rollbackIfOwned() throws SQLException {
        if (owned) {
            connection.rollback();
        }
    }

    /**
     * Closes and restores {@code autoCommit(true)} only if this call site
     * owns the connection — fixing Milestone 2 Important Finding #3
     * (autoCommit was previously never explicitly restored, correctness
     * borrowed from HikariCP's connection-reset behavior rather than being
     * guaranteed by this code). A borrowed connection is left completely
     * untouched; the transaction that owns it is responsible for all of this.
     */
    @Override
    public void close() throws SQLException {
        if (owned) {
            connection.setAutoCommit(true);
            connection.close();
        }
    }
}
