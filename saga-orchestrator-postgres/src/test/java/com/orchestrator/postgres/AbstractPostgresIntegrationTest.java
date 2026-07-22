package com.orchestrator.postgres;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Base class for every {@code Postgres*} integration test in this module.
 * Spins up a real, ephemeral PostgreSQL container via Testcontainers and
 * applies both migration scripts before any test runs.
 *
 * <p><b>Requires a local Docker daemon to run. Not executed in the sandbox
 * this codebase was developed in</b> — that sandbox has neither Docker nor
 * Maven Central access (see this milestone's Step 4 write-up). This class
 * and everything extending it are real, complete, intended-to-run code —
 * run {@code ./gradlew :saga-orchestrator-postgres:test} locally with Docker
 * available to execute them.
 */
@Testcontainers
abstract class AbstractPostgresIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("saga_orchestrator_test")
            .withUsername("test")
            .withPassword("test");

    static DataSource dataSource;

    @BeforeAll
    static void setUpSchema() throws Exception {
        dataSource = new SimpleDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        try (Connection connection = dataSource.getConnection()) {
            runScript(connection, "/db/migration/V1__event_store.sql");
            runScript(connection, "/db/migration/V2__snapshots_and_view.sql");
            runScript(connection, "/db/migration/V3__outbox_inbox.sql");
            runScript(connection, "/db/migration/V4__timeout_fields.sql");
            runScript(connection, "/db/migration/V5__outbox_retry_columns.sql");
        }
    }

    private static void runScript(Connection connection, String classpathResource) throws IOException, java.sql.SQLException {
        String sql;
        try (InputStream in = AbstractPostgresIntegrationTest.class.getResourceAsStream(classpathResource)) {
            if (in == null) {
                throw new IllegalStateException("Migration script not found on classpath: " + classpathResource);
            }
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        // Remove SQL line comments ("-- ...") before splitting on ';' so that
        // semicolons inside comments don't split statements unexpectedly.
        String withoutLineComments = sql.lines()
                .filter(line -> !line.trim().startsWith("--"))
                .reduce(new StringBuilder(), (sb, l) -> sb.append(l).append('\n'), (a, b) -> a.append(b))
                .toString();

        try (Statement statement = connection.createStatement()) {
            for (String rawStatement : withoutLineComments.split(";")) {
                String trimmed = rawStatement.trim();
                if (!trimmed.isEmpty()) {
                    try {
                        statement.execute(trimmed);
                    } catch (java.sql.SQLException e) {
                        throw new java.sql.SQLException("Error executing SQL statement: " + System.lineSeparator() + trimmed, e);
                    }
                }
            }
        }
    }

    /** Clears all rows between tests so each test starts from a known-empty state. */
    void truncateAllTables() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE saga_event, saga_stream_head, saga_snapshot, saga_instance_view, outbox, inbox");
        }
    }
}
