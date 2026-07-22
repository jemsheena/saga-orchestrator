package com.orchestrator.postgres;

import com.orchestrator.messaging.outbox.OutboxDispatcher;
import com.orchestrator.messaging.outbox.OutboxRecord;
import com.orchestrator.messaging.outbox.OutboxStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Plain-JDBC implementation of {@link OutboxStore} against the {@code outbox}
 * table (schema in {@code V3__outbox_inbox.sql}).
 *
 * <p><b>Joins the caller's transaction via {@link ManagedConnection},</b>
 * exactly like {@code PostgresSagaEventStore} and
 * {@code PostgresSagaInstanceViewStore} do — this is what will let Phase 2
 * wire {@link #append} into the SAME transaction as
 * {@code DefaultSagaInstanceRepository.save}'s event-append and
 * view-projection (once that wiring exists), giving the full three-way
 * atomicity ("event append + view projection + outbox write, all-or-nothing")
 * that a correct Outbox implementation requires. Phase 1 does not perform
 * that wiring yet — see Phase 1 scope note — but the mechanism is already
 * transaction-aware and ready for it, at zero extra cost, because it reuses
 * infrastructure Milestone 2.5 already built and proved correct.
 *
 * <p><b>{@link #claimAndDispatch}'s {@code SELECT ... FOR UPDATE SKIP LOCKED}</b>
 * is the same technique named (but not yet built) in the Milestone 3
 * architecture review's timeout-mechanism discussion — multiple concurrent
 * pollers (multiple orchestrator replicas, each running their own
 * {@code OutboxPublisher}) atomically claim disjoint batches of undispatched
 * rows with zero coordination service required.
 */
public final class PostgresOutboxStore implements OutboxStore {

    private final DataSource dataSource;

    public PostgresOutboxStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void append(OutboxRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        String sql = "INSERT INTO outbox "
                + "(outbox_id, topic, message_key, message_type, payload, correlation_id, causation_id, created_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, record.outboxId());
                stmt.setString(2, record.topic());
                stmt.setString(3, record.messageKey());
                stmt.setString(4, record.messageType());
                stmt.setBytes(5, record.payload());
                stmt.setObject(6, record.correlationId());
                if (record.causationId() != null) {
                    stmt.setObject(7, record.causationId());
                } else {
                    stmt.setNull(7, java.sql.Types.OTHER);
                }
                stmt.setTimestamp(8, Timestamp.from(record.createdAt()));
                stmt.executeUpdate();
                managed.commitIfOwned();
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to append outbox record " + record.outboxId(), e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to append outbox record", e);
        }
    }

    @Override
    public int claimAndDispatch(int limit, OutboxDispatcher dispatcher) {
        Objects.requireNonNull(dispatcher, "dispatcher must not be null");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1");
        }

        String selectSql = "SELECT outbox_id, topic, message_key, message_type, payload, "
                + "correlation_id, causation_id, created_at "
                + "FROM outbox WHERE dispatched_at IS NULL "
                + "ORDER BY created_at ASC LIMIT ? FOR UPDATE SKIP LOCKED";
        String markDispatchedSql = "UPDATE outbox SET dispatched_at = ? WHERE outbox_id = ?";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try {
                List<OutboxRecord> claimed = selectClaimed(connection, selectSql, limit);
                int dispatchedCount = 0;

                for (OutboxRecord record : claimed) {
                    try {
                        dispatcher.dispatch(record);
                        try (PreparedStatement markStmt = connection.prepareStatement(markDispatchedSql)) {
                            markStmt.setTimestamp(1, Timestamp.from(Instant.now()));
                            markStmt.setObject(2, record.outboxId());
                            markStmt.executeUpdate();
                        }
                        dispatchedCount++;
                    } catch (Exception dispatchFailure) {
                        // Per OutboxStore.claimAndDispatch javadoc: this record simply
                        // stays undispatched for a future poll. The transaction is NOT
                        // aborted - other already-successful records in this batch must
                        // still commit.
                        System.err.println("[WARN] Failed to dispatch outbox record "
                                + record.outboxId() + ", will retry next poll: " + dispatchFailure);
                    }
                }

                managed.commitIfOwned();
                return dispatchedCount;
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to claim outbox batch", e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to claim outbox batch", e);
        }
    }

    private List<OutboxRecord> selectClaimed(Connection connection, String sql, int limit) throws SQLException {
        List<OutboxRecord> claimed = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, limit);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    UUID causationId = rs.getObject("causation_id", UUID.class); // null-safe: getObject returns null if column was SQL NULL
                    claimed.add(new OutboxRecord(
                            (UUID) rs.getObject("outbox_id"),
                            rs.getString("topic"),
                            rs.getString("message_key"),
                            rs.getString("message_type"),
                            rs.getBytes("payload"),
                            (UUID) rs.getObject("correlation_id"),
                            causationId,
                            rs.getTimestamp("created_at").toInstant()));
                }
            }
        }
        return claimed;
    }
}
