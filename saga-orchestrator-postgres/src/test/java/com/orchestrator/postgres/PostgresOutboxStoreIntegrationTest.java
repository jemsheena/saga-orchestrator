package com.orchestrator.postgres;

import com.orchestrator.messaging.outbox.OutboxRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link PostgresOutboxStore} against a real,
 * ephemeral PostgreSQL instance. See {@link AbstractPostgresIntegrationTest}
 * — requires local Docker, not executed in the sandbox this was developed in.
 */
class PostgresOutboxStoreIntegrationTest extends AbstractPostgresIntegrationTest {

    private PostgresOutboxStore store;

    @BeforeEach
    void setUp() throws Exception {
        truncateAllTables();
        store = new PostgresOutboxStore(dataSource);
    }

    @Test
    void appendThenClaimAndDispatch_dispatchesAndMarksRecord() {
        store.append(record("topic-a", "key-1"));

        AtomicInteger dispatchedCalls = new AtomicInteger();
        int dispatchedCount = store.claimAndDispatch(10, r -> dispatchedCalls.incrementAndGet());

        assertEquals(1, dispatchedCount);
        assertEquals(1, dispatchedCalls.get());

        // A second claim call must find nothing left - the record is durably marked dispatched.
        int secondClaim = store.claimAndDispatch(10, r -> dispatchedCalls.incrementAndGet());
        assertEquals(0, secondClaim);
    }

    @Test
    void claimAndDispatch_failingDispatcher_leavesRecordUndispatched_forRetry() {
        store.append(record("topic-a", "key-fails"));

        int firstAttempt = store.claimAndDispatch(10, r -> {
            throw new RuntimeException("simulated failure");
        });
        assertEquals(0, firstAttempt);

        // The failed record must still be claimable on a later poll.
        AtomicInteger successCalls = new AtomicInteger();
        int secondAttempt = store.claimAndDispatch(10, r -> successCalls.incrementAndGet());
        assertEquals(1, secondAttempt);
        assertEquals(1, successCalls.get());
    }

    @Test
    void claimAndDispatch_concurrentPollers_claimDisjointBatches_noDoubleDispatch() throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            store.append(record("topic-a", "key-" + i));
        }

        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        AtomicInteger totalDispatched = new AtomicInteger();
        List<UUID> dispatchedIds = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        for (int i = 0; i < 4; i++) {
            pool.execute(() -> {
                PostgresOutboxStore pollerStore = new PostgresOutboxStore(dataSource);
                int count = pollerStore.claimAndDispatch(5, r -> {
                    dispatchedIds.add(r.outboxId());
                    totalDispatched.incrementAndGet();
                });
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, java.util.concurrent.TimeUnit.SECONDS));

        // Every dispatched ID must be unique - SKIP LOCKED must never let two
        // pollers claim (and thus double-dispatch) the same row.
        assertEquals(dispatchedIds.size(), java.util.Set.copyOf(dispatchedIds).size());
    }

    @Test
    void claimAndDispatch_permanentlyFailsRecord_afterRetryLimit() {
        PostgresOutboxStore retryStore = new PostgresOutboxStore(dataSource, 2);
        retryStore.append(record("topic-a", "key-fails"));

        int firstAttempt = retryStore.claimAndDispatch(10, r -> {
            throw new RuntimeException("simulated failure 1");
        });
        assertEquals(0, firstAttempt);

        int secondAttempt = retryStore.claimAndDispatch(10, r -> {
            throw new RuntimeException("simulated failure 2");
        });
        assertEquals(0, secondAttempt);

        int thirdAttempt = retryStore.claimAndDispatch(10, r -> {
            throw new RuntimeException("simulated failure 3");
        });
        assertEquals(0, thirdAttempt, "Record should no longer be claimable after exceeding retry limit");

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT retry_count, failed_at IS NOT NULL FROM outbox WHERE message_key = 'key-fails'")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.getBoolean(2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static OutboxRecord record(String topic, String key) {
        return new OutboxRecord(UUID.randomUUID(), topic, key, "SagaCommand",
                new byte[]{1, 2, 3}, UUID.randomUUID(), null, Instant.now());
    }
}
