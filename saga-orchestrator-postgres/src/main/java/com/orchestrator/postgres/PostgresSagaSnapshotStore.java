package com.orchestrator.postgres;

import com.orchestrator.core.definition.SagaDefinitionReference;
import com.orchestrator.core.engine.SagaSnapshot;
import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.repository.SagaSnapshotStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Plain-JDBC implementation of {@link SagaSnapshotStore}. Every
 * {@link #save} call inserts a new row rather than updating in place —
 * simplest-correct choice for a table whose only query is "give me the
 * latest" ({@link #findLatest}, backed by the
 * {@code idx_saga_snapshot_latest} index). A follow-up optimization
 * (periodically purging superseded snapshot rows for a saga) is legitimate
 * deferred cleanup, not required for correctness — see {@link SagaSnapshot}
 *
 * <p><b>Milestone 2.5 note — deliberately NOT using {@link ManagedConnection}:</b>
 * unlike {@code PostgresSagaEventStore} and {@code PostgresSagaInstanceViewStore},
 * this class always opens and manages its own independent connection, even
 * when called from within an active {@code JdbcTransactionRunner} transaction.
 * This is intentional, not an oversight: {@code DefaultSagaInstanceRepository}
 * calls this store AFTER the event-append/projection transaction has already
 * committed, specifically so a snapshot failure can never roll back or block
 * already-successfully-persisted events — see Milestone 2.5 Critical Finding #2.
 * Joining the caller's transaction here would silently reintroduce that exact bug.
 *
 * <p>See {@link SagaSnapshot} javadoc on snapshot invalidation and on snapshots
 * being strictly disposable regardless of how many historical rows happen to
 * still exist, for the related reasoning behind this class's design overall.</p>
 */
public final class PostgresSagaSnapshotStore implements SagaSnapshotStore {

    private final DataSource dataSource;

    public PostgresSagaSnapshotStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void save(SagaSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        String sql = "INSERT INTO saga_snapshot "
                + "(saga_id, sequence_no, saga_type, definition_version, state, "
                + " current_step_index, compensation_cursor, schema_version, created_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (saga_id, sequence_no) DO NOTHING"; // idempotent under retried saves

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, snapshot.sagaId());
            stmt.setLong(2, snapshot.sequenceNo());
            stmt.setString(3, snapshot.definitionReference().sagaType());
            stmt.setInt(4, snapshot.definitionReference().version());
            stmt.setString(5, snapshot.state().name());
            stmt.setInt(6, snapshot.currentStepIndex());
            stmt.setInt(7, snapshot.compensationCursor());
            stmt.setInt(8, snapshot.schemaVersion());
            stmt.setTimestamp(9, Timestamp.from(snapshot.createdAt()));
            stmt.executeUpdate();
        } catch (SQLException e) {
            // Deliberately NOT rethrown as a failure that would abort the caller's
            // broader operation — see SagaSnapshotStore javadoc: a snapshot write
            // failure must never fail the business operation that triggered it.
            // Logged (via a real logger once one is wired up in a later milestone)
            // rather than silently swallowed with no trace at all.
            throw new PostgresAdapterException(
                    "Failed to save snapshot for saga " + snapshot.sagaId()
                            + " — this does not affect correctness, only replay performance, "
                            + "but should be investigated if it recurs.", e);
        }
    }

    @Override
    public Optional<SagaSnapshot> findLatest(UUID sagaId) {
        Objects.requireNonNull(sagaId, "sagaId must not be null");
        String sql = "SELECT sequence_no, saga_type, definition_version, state, "
                + "current_step_index, compensation_cursor, schema_version, created_at "
                + "FROM saga_snapshot WHERE saga_id = ? ORDER BY sequence_no DESC LIMIT 1";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, sagaId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                SagaSnapshot snapshot = new SagaSnapshot(
                        sagaId,
                        new SagaDefinitionReference(rs.getString("saga_type"), rs.getInt("definition_version")),
                        rs.getLong("sequence_no"),
                        SagaState.valueOf(rs.getString("state")),
                        rs.getInt("current_step_index"),
                        rs.getInt("compensation_cursor"),
                        rs.getInt("schema_version"),
                        rs.getTimestamp("created_at").toInstant());
                return Optional.of(snapshot);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to load latest snapshot for saga " + sagaId, e);
        }
    }
}
