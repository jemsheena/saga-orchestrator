package com.orchestrator.postgres;

import com.orchestrator.core.event.SagaDomainEvent;
import com.orchestrator.core.exception.ConcurrencyConflictException;
import com.orchestrator.core.repository.EventMetadata;
import com.orchestrator.core.repository.SagaEventStore;
import com.orchestrator.postgres.serialization.SagaEventSerializer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Plain-JDBC implementation of {@link SagaEventStore} against the schema in
 * {@code V1__event_store.sql}.
 *
 * <p><b>Milestone 2.5 change:</b> {@link #append} now obtains its connection
 * via {@link ManagedConnection#obtain}, which transparently joins an
 * already-active transaction bound by {@link JdbcTransactionRunner}
 * (typically one also encompassing a {@code PostgresSagaInstanceViewStore}
 * upsert, from {@code DefaultSagaInstanceRepository.save}) rather than
 * always opening and committing its own connection. When there is no bound
 * transaction (e.g. called standalone, or from an integration test), this
 * behaves exactly as it did in Milestone 2 — opens, manages, and closes its
 * own connection. See {@code ManagedConnection} javadoc for the full mechanism.
 */
public final class PostgresSagaEventStore implements SagaEventStore {

    private final DataSource dataSource;
    private final SagaEventSerializer serializer;

    public PostgresSagaEventStore(DataSource dataSource, SagaEventSerializer serializer) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
    }

    @Override
    public void append(UUID sagaId, long expectedVersion, List<SagaDomainEvent> newEvents, EventMetadata metadata) {
        Objects.requireNonNull(sagaId, "sagaId must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
        if (newEvents == null || newEvents.isEmpty()) {
            throw new IllegalArgumentException("newEvents must not be empty");
        }

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try {
                ensureStreamHeadExists(connection, sagaId);
                advanceStreamHeadOrThrow(connection, sagaId, expectedVersion, newEvents.size());
                insertEvents(connection, sagaId, expectedVersion, newEvents, metadata);
                managed.commitIfOwned();
            } catch (ConcurrencyConflictException e) {
                managed.rollbackIfOwned();
                throw e;
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to append events for saga " + sagaId, e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain/configure connection for saga " + sagaId, e);
        }
    }

    /** Unchanged from Milestone 2 — see original javadoc reasoning on the ON CONFLICT DO NOTHING race safety. */
    private void ensureStreamHeadExists(Connection connection, UUID sagaId) throws SQLException {
        String sql = "INSERT INTO saga_stream_head (saga_id, current_sequence_no) VALUES (?, 0) "
                + "ON CONFLICT (saga_id) DO NOTHING";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, sagaId);
            stmt.executeUpdate();
        }
    }

    private void advanceStreamHeadOrThrow(Connection connection, UUID sagaId, long expectedVersion, int batchSize)
            throws SQLException {
        String sql = "UPDATE saga_stream_head SET current_sequence_no = current_sequence_no + ? "
                + "WHERE saga_id = ? AND current_sequence_no = ?";
        int updatedRows;
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, batchSize);
            stmt.setObject(2, sagaId);
            stmt.setLong(3, expectedVersion);
            updatedRows = stmt.executeUpdate();
        }

        if (updatedRows == 0) {
            long actualVersion = currentVersion(connection, sagaId);
            throw new ConcurrencyConflictException(sagaId, expectedVersion, actualVersion);
        }
    }

    private long currentVersion(Connection connection, UUID sagaId) throws SQLException {
        String sql = "SELECT current_sequence_no FROM saga_stream_head WHERE saga_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, sagaId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("saga_stream_head row missing for " + sagaId
                            + " during conflict diagnosis — this should be unreachable.");
                }
                return rs.getLong(1);
            }
        }
    }

    private void insertEvents(Connection connection, UUID sagaId, long expectedVersion,
                               List<SagaDomainEvent> newEvents, EventMetadata metadata) throws SQLException {
        String sql = "INSERT INTO saga_event "
                + "(event_id, saga_id, sequence_no, event_type, event_schema_version, payload, "
                + " occurred_at, correlation_id, causation_id) "
                + "VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            long sequenceNo = expectedVersion + 1;
            for (SagaDomainEvent event : newEvents) {
                stmt.setObject(1, UUID.randomUUID());
                stmt.setObject(2, sagaId);
                stmt.setLong(3, sequenceNo);
                stmt.setString(4, serializer.eventType(event));
                stmt.setInt(5, serializer.schemaVersion(event));
                stmt.setObject(6, serializer.serialize(event), Types.OTHER);
                stmt.setTimestamp(7, Timestamp.from(event.occurredAt()));
                stmt.setObject(8, metadata.correlationId());

                // Milestone 2.5 fix (Important Finding #5): explicit setNull rather than
                // setObject(idx, null), matching the correct pattern already used in
                // PostgresSagaInstanceViewStore for duration_ms. Relying on setObject to
                // correctly infer SQL NULL from a null Object reference for a UUID column
                // is driver-behavior-dependent rather than guaranteed by the JDBC contract.
                if (metadata.causationId() != null) {
                    stmt.setObject(9, metadata.causationId());
                } else {
                    stmt.setNull(9, Types.OTHER);
                }

                stmt.addBatch();
                sequenceNo++;
            }
            stmt.executeBatch();
        }
    }

    @Override
    public List<SagaDomainEvent> loadEvents(UUID sagaId) {
        return loadEvents(sagaId, 0L);
    }

    @Override
    public List<SagaDomainEvent> loadEvents(UUID sagaId, long afterSequenceNo) {
        Objects.requireNonNull(sagaId, "sagaId must not be null");
        String sql = "SELECT event_type, event_schema_version, payload "
                + "FROM saga_event WHERE saga_id = ? AND sequence_no > ? ORDER BY sequence_no ASC";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, sagaId);
                stmt.setLong(2, afterSequenceNo);

                List<SagaDomainEvent> events = new ArrayList<>();
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String eventType = rs.getString("event_type");
                        int schemaVersion = rs.getInt("event_schema_version");
                        String payload = rs.getString("payload");
                        events.add(serializer.deserialize(eventType, schemaVersion, payload));
                    }
                }
                managed.commitIfOwned(); // read-only, but keeps an owned connection's transaction state clean
                return events;
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to load events for saga " + sagaId, e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to load events for saga " + sagaId, e);
        }
    }
}
