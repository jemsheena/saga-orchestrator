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
        // Simple statement splitting on ';' - adequate for these two migration
        // files (no stored procedures/functions containing embedded semicolons).
        // Remove standalone comments first: otherwise the first CREATE TABLE
        // statement in each migration would be part of a chunk that begins with
        // "--" and would be skipped along with the comment.
        String executableSql = sql.lines()
                .filter(line -> !line.trim().startsWith("--"))
                .collect(java.util.stream.Collectors.joining("\n"));
        try (Statement statement = connection.createStatement()) {
            for (String rawStatement : executableSql.split(";")) {
                String trimmed = rawStatement.trim();
                if (!trimmed.isEmpty()) {
                    statement.execute(trimmed);
                }
            }
        }
    }

    /** Clears all rows between tests so each test starts from a known-empty state. */
    void truncateAllTables() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE saga_event, saga_stream_head, saga_snapshot, saga_instance_view");
        }
    }
}
