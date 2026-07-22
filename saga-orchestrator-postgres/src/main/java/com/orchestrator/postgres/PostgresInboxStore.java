package com.orchestrator.postgres;

import com.orchestrator.messaging.inbox.InboxRecord;
import com.orchestrator.messaging.inbox.InboxStatus;
import com.orchestrator.messaging.inbox.InboxStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Plain-JDBC implementation of {@link InboxStore} against the {@code inbox}
 * table (schema in {@code V3__outbox_inbox.sql}).
 */
public final class PostgresInboxStore implements InboxStore {

    private static final String DEFAULT_CONSUMER = "default";

    private final DataSource dataSource;

    public PostgresInboxStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public boolean recordIfNew(UUID messageId, String consumer, String topic, String partitionKey) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(partitionKey, "partitionKey must not be null");

        String sql = "INSERT INTO inbox (message_id, consumer, topic, partition_key, received_at, status) "
                + "VALUES (?, ?, ?, ?, now(), ?) ON CONFLICT (message_id, consumer) DO NOTHING";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, messageId);
                stmt.setString(2, consumer);
                stmt.setString(3, topic);
                stmt.setString(4, partitionKey);
                stmt.setString(5, InboxStatus.RECEIVED.name());
                int updated = stmt.executeUpdate();
                managed.commitIfOwned();
                return updated == 1;
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to record inbox message " + messageId, e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to record inbox message " + messageId, e);
        }
    }

    @Override
    public boolean exists(UUID messageId, String consumer) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");

        String sql = "SELECT 1 FROM inbox WHERE message_id = ? AND consumer = ?";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, messageId);
                stmt.setString(2, consumer);
                try (ResultSet rs = stmt.executeQuery()) {
                    return rs.next();
                }
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to check inbox existence for message " + messageId, e);
        }
    }

    @Override
    public void save(InboxRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        String sql = "INSERT INTO inbox (message_id, consumer, topic, partition_key, received_at, processed_at, status) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (message_id, consumer) DO NOTHING";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, record.messageId());
                stmt.setString(2, record.consumer());
                stmt.setString(3, record.topic());
                stmt.setString(4, record.partitionKey());
                stmt.setTimestamp(5, Timestamp.from(record.receivedAt()));
                if (record.processedAt() != null) {
                    stmt.setTimestamp(6, Timestamp.from(record.processedAt()));
                } else {
                    stmt.setNull(6, java.sql.Types.TIMESTAMP);
                }
                stmt.setString(7, record.status().name());
                stmt.executeUpdate();
                managed.commitIfOwned();
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to save inbox record " + record.messageId(), e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to save inbox record " + record.messageId(), e);
        }
    }

    @Override
    public void markProcessed(UUID messageId, String consumer) {
        updateStatus(messageId, consumer, InboxStatus.PROCESSED, true);
    }

    @Override
    public void markFailed(UUID messageId, String consumer) {
        updateStatus(messageId, consumer, InboxStatus.FAILED, false);
    }

    @Override
    public Optional<InboxRecord> find(UUID messageId, String consumer) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");

        String sql = "SELECT message_id, consumer, topic, partition_key, received_at, processed_at, status "
                + "FROM inbox WHERE message_id = ? AND consumer = ?";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, messageId);
                stmt.setString(2, consumer);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new InboxRecord(
                            (UUID) rs.getObject("message_id"),
                            rs.getString("consumer"),
                            rs.getString("topic"),
                            rs.getString("partition_key"),
                            rs.getTimestamp("received_at").toInstant(),
                            rs.getTimestamp("processed_at") != null ? rs.getTimestamp("processed_at").toInstant() : null,
                            InboxStatus.valueOf(rs.getString("status"))));
                }
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to find inbox record " + messageId, e);
        }
    }

    @Override
    public int cleanup(Instant olderThan, int limit) {
        Objects.requireNonNull(olderThan, "olderThan must not be null");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1");
        }

        String sql = "DELETE FROM inbox WHERE processed_at < ? AND status = ? ORDER BY processed_at ASC LIMIT ?";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setTimestamp(1, Timestamp.from(olderThan));
                stmt.setString(2, InboxStatus.PROCESSED.name());
                stmt.setInt(3, limit);
                int deleted = stmt.executeUpdate();
                managed.commitIfOwned();
                return deleted;
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to cleanup inbox records", e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to cleanup inbox records", e);
        }
    }

    private void updateStatus(UUID messageId, String consumer, InboxStatus status, boolean setProcessedAt) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        Objects.requireNonNull(status, "status must not be null");

        String sql = "UPDATE inbox SET status = ?, processed_at = ? WHERE message_id = ? AND consumer = ?";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, status.name());
                if (setProcessedAt) {
                    stmt.setTimestamp(2, Timestamp.from(Instant.now()));
                } else {
                    stmt.setNull(2, java.sql.Types.TIMESTAMP);
                }
                stmt.setObject(3, messageId);
                stmt.setString(4, consumer);
                int updated = stmt.executeUpdate();
                if (updated == 0) {
                    throw new PostgresAdapterException("No inbox record found to update for message " + messageId, null);
                }
                managed.commitIfOwned();
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to update inbox status for message " + messageId, e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to update inbox status for message " + messageId, e);
        }
    }
}
