package com.orchestrator.messaging.inbox;

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

class InboxStoreTest {

    @Test
    void recordIfNew_firstCall_returnsTrue() {
        InboxStore store = new InMemoryInboxStore();
        assertTrue(store.recordIfNew(UUID.randomUUID()));
    }

    @Test
    void recordIfNew_secondCallForSameId_returnsFalse() {
        InboxStore store = new InMemoryInboxStore();
        UUID messageId = UUID.randomUUID();

        assertTrue(store.recordIfNew(messageId));
        assertFalse(store.recordIfNew(messageId));
    }

    @Test
    void recordIfNew_differentIds_bothReturnTrue() {
        InboxStore store = new InMemoryInboxStore();
        assertTrue(store.recordIfNew(UUID.randomUUID()));
        assertTrue(store.recordIfNew(UUID.randomUUID()));
    }

    /**
     * The real proof this pattern exists for: many threads racing to record
     * the SAME message id concurrently (simulating a rebalance-driven
     * redelivery landing on multiple threads at once) - exactly one must see
     * {@code true} (first-and-only processor), every other must see
     * {@code false} (correctly recognized duplicate), with zero races.
     */
    @Test
    void recordIfNew_manyThreadsRacingOnSameId_exactlyOneWins() throws InterruptedException {
        InboxStore store = new InMemoryInboxStore();
        UUID messageId = UUID.randomUUID();
        int racerCount = 16;

        ExecutorService pool = Executors.newFixedThreadPool(racerCount);
        CountDownLatch startLine = new CountDownLatch(1);
        AtomicInteger winnerCount = new AtomicInteger(0);

        for (int i = 0; i < racerCount; i++) {
            pool.execute(() -> {
                try {
                    startLine.await();
                    if (store.recordIfNew(messageId)) {
                        winnerCount.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }
        startLine.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, winnerCount.get());
    }
}
