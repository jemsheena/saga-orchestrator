package com.orchestrator.postgres;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link PostgresInboxStore} against a real, ephemeral
 * PostgreSQL instance. Requires local Docker — not executed in this sandbox.
 */
class PostgresInboxStoreIntegrationTest extends AbstractPostgresIntegrationTest {

    private PostgresInboxStore store;

    @BeforeEach
    void setUp() throws Exception {
        truncateAllTables();
        store = new PostgresInboxStore(dataSource);
    }

    @Test
    void recordIfNew_firstCall_returnsTrue_secondCall_returnsFalse() {
        UUID messageId = UUID.randomUUID();
        assertTrue(store.recordIfNew(messageId));
        assertFalse(store.recordIfNew(messageId));
    }

    /**
     * The real proof, against real Postgres: many concurrent connections
     * racing to record the SAME message id must yield exactly one winner,
     * enforced by the database's own primary-key constraint - not by
     * anything this test or the application coordinates itself.
     */
    @Test
    void recordIfNew_manyConcurrentConnectionsRacingOnSameId_exactlyOneWins() throws InterruptedException {
        UUID messageId = UUID.randomUUID();
        int racerCount = 16;
        ExecutorService pool = Executors.newFixedThreadPool(racerCount);
        CountDownLatch startLine = new CountDownLatch(1);
        AtomicInteger winnerCount = new AtomicInteger();

        for (int i = 0; i < racerCount; i++) {
            pool.execute(() -> {
                try {
                    startLine.await();
                    PostgresInboxStore racerStore = new PostgresInboxStore(dataSource);
                    if (racerStore.recordIfNew(messageId)) {
                        winnerCount.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }
        startLine.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

        assertEquals(1, winnerCount.get());
    }
}
