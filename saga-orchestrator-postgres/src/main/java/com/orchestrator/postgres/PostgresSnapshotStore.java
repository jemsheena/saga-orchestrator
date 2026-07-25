package com.orchestrator.postgres;

import com.orchestrator.core.snapshot.Snapshot;
import com.orchestrator.core.snapshot.SnapshotStore;

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
 * Simple Postgres-backed implementation of {@link SnapshotStore}.
 */
public final class PostgresSnapshotStore implements SnapshotStore {

    private final DataSource dataSource;

    public PostgresSnapshotStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void save(Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        String sql = "INSERT INTO snapshot_store (snapshot_id, aggregate_id, aggregate_type, aggregate_version, snapshot_version, payload, created_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (snapshot_id) DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, snapshot.snapshotId());
            ps.setString(2, snapshot.aggregateId());
            ps.setString(3, snapshot.aggregateType());
            ps.setLong(4, snapshot.aggregateVersion());
            ps.setInt(5, snapshot.snapshotVersion());
            ps.setBytes(6, snapshot.payload());
            ps.setTimestamp(7, Timestamp.from(snapshot.createdAt()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to save snapshot for " + snapshot.aggregateId(), e);
        }
    }

    @Override
    public Optional<Snapshot> loadLatest(String aggregateType, String aggregateId) {
        Objects.requireNonNull(aggregateType);
        Objects.requireNonNull(aggregateId);
        String sql = "SELECT snapshot_id, aggregate_version, snapshot_version, payload, created_at "
                + "FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY aggregate_version DESC LIMIT 1";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, aggregateType);
            ps.setString(2, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                Snapshot snapshot = new Snapshot(
                        (UUID) rs.getObject("snapshot_id"),
                        aggregateId,
                        aggregateType,
                        rs.getLong("aggregate_version"),
                        rs.getInt("snapshot_version"),
                        rs.getBytes("payload"),
                        rs.getTimestamp("created_at").toInstant());
                return Optional.of(snapshot);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to load latest snapshot for " + aggregateId, e);
        }
    }

    @Override
    public void delete(String aggregateType, String aggregateId) {
        Objects.requireNonNull(aggregateType);
        Objects.requireNonNull(aggregateId);
        String sql = "DELETE FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, aggregateType);
            ps.setString(2, aggregateId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to delete snapshots for " + aggregateId, e);
        }
    }

    @Override
    public int deleteOlderThan(String aggregateType, Instant olderThan) {
        Objects.requireNonNull(aggregateType);
        Objects.requireNonNull(olderThan);
        String sql = "DELETE FROM snapshot_store WHERE aggregate_type = ? AND created_at < ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, aggregateType);
            ps.setTimestamp(2, Timestamp.from(olderThan));
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to cleanup snapshots for type " + aggregateType, e);
        }
    }

    @Override
    public boolean exists(String aggregateType, String aggregateId) {
        Objects.requireNonNull(aggregateType);
        Objects.requireNonNull(aggregateId);
        String sql = "SELECT 1 FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ? LIMIT 1";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, aggregateType);
            ps.setString(2, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to check snapshot existence for " + aggregateId, e);
        }
    }

    @Override
    public int purgeExceptLatest(String aggregateType, String aggregateId, int keepLatest) {
        Objects.requireNonNull(aggregateType);
        Objects.requireNonNull(aggregateId);
        if (keepLatest < 0) throw new IllegalArgumentException("keepLatest must be >= 0");
        if (keepLatest == 0) {
            delete(aggregateType, aggregateId);
            return 0; // can't easily know count here without a separate query
        }
        String sql = "DELETE FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ? AND snapshot_id NOT IN (" +
                "SELECT snapshot_id FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY aggregate_version DESC LIMIT ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, aggregateType);
            ps.setString(2, aggregateId);
            ps.setString(3, aggregateType);
            ps.setString(4, aggregateId);
            ps.setInt(5, keepLatest);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to purge snapshots for " + aggregateId, e);
        }
    }
}
