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

        String sql = "INSERT INTO inbox (message_id, consumer, topic, partition_key, received_at, status, retry_count, last_failure, last_attempt, next_retry_time, payload, correlation_id, causation_id) "
            + "VALUES (?, ?, ?, ?, now(), ?, 0, NULL, NULL, NULL, NULL, NULL, NULL) ON CONFLICT (message_id, consumer) DO NOTHING";

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
        String sql = "INSERT INTO inbox (message_id, consumer, topic, partition_key, received_at, processed_at, status, retry_count, last_failure, last_attempt, next_retry_time, payload, correlation_id, causation_id) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (message_id, consumer) DO UPDATE SET processed_at = EXCLUDED.processed_at, status = EXCLUDED.status, retry_count = EXCLUDED.retry_count, last_failure = EXCLUDED.last_failure, last_attempt = EXCLUDED.last_attempt, next_retry_time = EXCLUDED.next_retry_time, payload = COALESCE(EXCLUDED.payload, inbox.payload), correlation_id = COALESCE(EXCLUDED.correlation_id, inbox.correlation_id), causation_id = COALESCE(EXCLUDED.causation_id, inbox.causation_id)";

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
                stmt.setInt(8, record.retryCount());
                if (record.lastFailure() != null) {
                    stmt.setString(9, record.lastFailure());
                } else {
                    stmt.setNull(9, java.sql.Types.VARCHAR);
                }
                if (record.lastAttempt() != null) {
                    stmt.setTimestamp(10, Timestamp.from(record.lastAttempt()));
                } else {
                    stmt.setNull(10, java.sql.Types.TIMESTAMP);
                }
                if (record.nextRetryTime() != null) {
                    stmt.setTimestamp(11, Timestamp.from(record.nextRetryTime()));
                } else {
                    stmt.setNull(11, java.sql.Types.TIMESTAMP);
                }
                if (record.payload() != null) {
                    stmt.setBytes(12, record.payload());
                } else {
                    stmt.setNull(12, java.sql.Types.BINARY);
                }
                if (record.headers() != null) {
                    stmt.setObject(13, record.headers().correlationId());
                    if (record.headers().causationId() != null) stmt.setObject(14, record.headers().causationId()); else stmt.setNull(14, java.sql.Types.OTHER);
                } else {
                    stmt.setNull(13, java.sql.Types.OTHER);
                    stmt.setNull(14, java.sql.Types.OTHER);
                }
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

        String sql = "SELECT message_id, consumer, topic, partition_key, received_at, processed_at, status, retry_count, last_failure, last_attempt, next_retry_time "
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
                        java.util.UUID correlation = null;
                        java.util.UUID causation = null;
                        try {
                            correlation = (java.util.UUID) rs.getObject("correlation_id");
                        } catch (Exception ignore) {}
                        try {
                            causation = (java.util.UUID) rs.getObject("causation_id");
                        } catch (Exception ignore) {}
                        com.orchestrator.messaging.MessageHeaders headers = null;
                        if (correlation != null) headers = new com.orchestrator.messaging.MessageHeaders(correlation, causation);
                        return Optional.of(new InboxRecord(
                                (UUID) rs.getObject("message_id"),
                                rs.getString("consumer"),
                                rs.getString("topic"),
                                rs.getString("partition_key"),
                                rs.getTimestamp("received_at").toInstant(),
                                rs.getTimestamp("processed_at") != null ? rs.getTimestamp("processed_at").toInstant() : null,
                                InboxStatus.valueOf(rs.getString("status")),
                                rs.getInt("retry_count"),
                                rs.getString("last_failure"),
                                rs.getTimestamp("last_attempt") != null ? rs.getTimestamp("last_attempt").toInstant() : null,
                                rs.getTimestamp("next_retry_time") != null ? rs.getTimestamp("next_retry_time").toInstant() : null,
                                rs.getBytes("payload"), headers));
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

    @Override
    public void updateRetryMetadata(UUID messageId, String consumer, int retryCount, String lastFailure, Instant lastAttempt, Instant nextRetryTime) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");

        String sql = "UPDATE inbox SET retry_count = ?, last_failure = ?, last_attempt = ?, next_retry_time = ? WHERE message_id = ? AND consumer = ?";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setInt(1, retryCount);
                if (lastFailure != null) stmt.setString(2, lastFailure); else stmt.setNull(2, java.sql.Types.VARCHAR);
                if (lastAttempt != null) stmt.setTimestamp(3, Timestamp.from(lastAttempt)); else stmt.setNull(3, java.sql.Types.TIMESTAMP);
                if (nextRetryTime != null) stmt.setTimestamp(4, Timestamp.from(nextRetryTime)); else stmt.setNull(4, java.sql.Types.TIMESTAMP);
                stmt.setObject(5, messageId);
                stmt.setString(6, consumer);
                int updated = stmt.executeUpdate();
                if (updated == 0) {
                    throw new PostgresAdapterException("No inbox record found to update retry metadata for message " + messageId, null);
                }
                managed.commitIfOwned();
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to update retry metadata for message " + messageId, e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to update retry metadata for message " + messageId, e);
        }
    }

    @Override
    public java.util.List<InboxRecord> findDueForRetry(Instant atOrBefore, int limit) {
        Objects.requireNonNull(atOrBefore, "atOrBefore must not be null");
        if (limit < 1) throw new IllegalArgumentException("limit must be >= 1");

        String sql = "SELECT message_id, consumer, topic, partition_key, received_at, processed_at, status, retry_count, last_failure, last_attempt, next_retry_time "
                + "FROM inbox WHERE status = ? AND next_retry_time IS NOT NULL AND next_retry_time <= ? ORDER BY next_retry_time ASC LIMIT ?";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, InboxStatus.FAILED.name());
                stmt.setTimestamp(2, Timestamp.from(atOrBefore));
                stmt.setInt(3, limit);
                try (ResultSet rs = stmt.executeQuery()) {
                    java.util.List<InboxRecord> list = new java.util.ArrayList<>();
                    while (rs.next()) {
                        java.util.UUID correlation = null;
                        java.util.UUID causation = null;
                        try { correlation = (java.util.UUID) rs.getObject("correlation_id"); } catch (Exception ignore) {}
                        try { causation = (java.util.UUID) rs.getObject("causation_id"); } catch (Exception ignore) {}
                        com.orchestrator.messaging.MessageHeaders headers = null;
                        if (correlation != null) headers = new com.orchestrator.messaging.MessageHeaders(correlation, causation);
                        list.add(new InboxRecord(
                            (UUID) rs.getObject("message_id"),
                            rs.getString("consumer"),
                            rs.getString("topic"),
                            rs.getString("partition_key"),
                            rs.getTimestamp("received_at").toInstant(),
                            rs.getTimestamp("processed_at") != null ? rs.getTimestamp("processed_at").toInstant() : null,
                            InboxStatus.valueOf(rs.getString("status")),
                            rs.getInt("retry_count"),
                            rs.getString("last_failure"),
                            rs.getTimestamp("last_attempt") != null ? rs.getTimestamp("last_attempt").toInstant() : null,
                            rs.getTimestamp("next_retry_time") != null ? rs.getTimestamp("next_retry_time").toInstant() : null,
                            rs.getBytes("payload"), headers));
                    }
                    return list;
                }
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to query due-for-retry inbox records", e);
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
