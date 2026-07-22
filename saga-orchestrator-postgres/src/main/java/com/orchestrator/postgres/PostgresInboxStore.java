package com.orchestrator.postgres;

import com.orchestrator.messaging.inbox.InboxStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

/**
 * Plain-JDBC implementation of {@link InboxStore} against the {@code inbox}
 * table (schema in {@code V3__outbox_inbox.sql}).
 *
 * <p>{@link #recordIfNew}'s atomicity comes directly from
 * {@code INSERT ... ON CONFLICT (message_id) DO NOTHING} combined with
 * checking the JDBC update count: exactly one of two concurrent callers
 * inserting the same {@code messageId} will see {@code executeUpdate() == 1}
 * (genuinely new), and the other will see {@code 0} (already existed) —
 * enforced by Postgres's own primary-key uniqueness, not by any
 * check-then-act logic in this class.
 */
public final class PostgresInboxStore implements InboxStore {

    private final DataSource dataSource;

    public PostgresInboxStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public boolean recordIfNew(UUID messageId) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        String sql = "INSERT INTO inbox (message_id) VALUES (?) ON CONFLICT (message_id) DO NOTHING";

        try (ManagedConnection managed = ManagedConnection.obtain(dataSource)) {
            Connection connection = managed.connection();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setObject(1, messageId);
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
}
