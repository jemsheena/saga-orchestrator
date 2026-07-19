package com.orchestrator.postgres;

import com.orchestrator.core.engine.SagaState;
import com.orchestrator.core.projection.SagaInstanceView;
import com.orchestrator.core.projection.SagaInstanceViewStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Plain-JDBC implementation of {@link SagaInstanceViewStore} against
 * {@code saga_instance_view}.
 *
 * <p><b>Milestone 2.5 change:</b> {@link #upsert} now obtains its connection
 * via {@link ManagedConnection#obtain} — this is the other half of the
 * Critical Finding #1 fix. When called (as it always is, in this codebase)
 * from within {@code SagaProjector.project} during
 * {@code DefaultSagaInstanceRepository.save}'s transaction, it joins the
 * SAME connection/transaction {@code PostgresSagaEventStore.append} is
 * using, rather than opening and auto-committing its own — this is what
 * makes the event append and the view update actually atomic.
 */
public final class PostgresSagaInstanceViewStore implements SagaInstanceViewStore {

    private final DataSource dataSource;

    public PostgresSagaInstanceViewStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void upsert(SagaInstanceView view) {
        Objects.requireNonNull(view, "view must not be null");
        String sql = "INSERT INTO saga_instance_view "
                + "(saga_id, saga_type, state, current_step_index, started_at, completed_at, duration_ms, last_error) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (saga_id) DO UPDATE SET "
                + "  state = EXCLUDED.state, "
                + "  current_step_index = EXCLUDED.current_step_index, "
                + "  completed_at = EXCLUDED.completed_at, "
                + "  duration_ms = EXCLUDED.duration_ms, "
                + "  last_error = EXCLUDED.last_error";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, view.sagaId());
                stmt.setString(2, view.sagaType());
                stmt.setString(3, view.state().name());
                stmt.setInt(4, view.currentStepIndex());
                stmt.setTimestamp(5, Timestamp.from(view.startedAt()));
                stmt.setTimestamp(6, view.completedAt() != null ? Timestamp.from(view.completedAt()) : null);
                if (view.durationMs() != null) {
                    stmt.setLong(7, view.durationMs());
                } else {
                    stmt.setNull(7, Types.BIGINT);
                }
                stmt.setString(8, view.lastError());
                stmt.executeUpdate();
                managed.commitIfOwned();
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to upsert saga_instance_view row for " + view.sagaId(), e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to upsert view for " + view.sagaId(), e);
        }
    }

    @Override
    public Optional<SagaInstanceView> findById(UUID sagaId) {
        Objects.requireNonNull(sagaId, "sagaId must not be null");
        String sql = "SELECT saga_type, state, current_step_index, started_at, completed_at, duration_ms, last_error "
                + "FROM saga_instance_view WHERE saga_id = ?";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, sagaId);
                try (ResultSet rs = stmt.executeQuery()) {
                    Optional<SagaInstanceView> result;
                    if (!rs.next()) {
                        result = Optional.empty();
                    } else {
                        Timestamp completedAtTs = rs.getTimestamp("completed_at");
                        long durationMs = rs.getLong("duration_ms");
                        boolean durationWasNull = rs.wasNull();

                        result = Optional.of(new SagaInstanceView(
                                sagaId,
                                rs.getString("saga_type"),
                                SagaState.valueOf(rs.getString("state")),
                                rs.getInt("current_step_index"),
                                rs.getTimestamp("started_at").toInstant(),
                                completedAtTs != null ? completedAtTs.toInstant() : null,
                                durationWasNull ? null : durationMs,
                                rs.getString("last_error")));
                    }
                    managed.commitIfOwned();
                    return result;
                }
            } catch (SQLException e) {
                managed.rollbackIfOwned();
                throw new PostgresAdapterException("Failed to load saga_instance_view row for " + sagaId, e);
            }
        } catch (SQLException e) {
            throw new PostgresAdapterException("Failed to obtain connection to load view for " + sagaId, e);
        }
    }
}
