package com.orchestrator.messaging.inbox;

import java.util.UUID;

/**
 * Deduplication for at-least-once message delivery.
 *
 * <p><b>Why this is ONE atomic method, not "isDuplicate()" followed by a
 * separate "markProcessed()":</b> a check-then-act split has a real race —
 * two concurrent consumer threads (plausible during a rebalance, or across
 * two orchestrator pods if a message were ever misrouted to both) could both
 * check "not yet processed" before either one marks it, and both would then
 * proceed to process the same message. Making this one atomic operation
 * (an {@code INSERT ... ON CONFLICT DO NOTHING}, checking whether a row was
 * actually inserted — see {@code PostgresInboxStore}) eliminates that race
 * structurally, the same design principle already applied to
 * {@code SagaEventStore}'s conditional-UPDATE concurrency check in
 * Milestone 2 and {@code OutboxStore.claimAndDispatch} above: make the
 * correct behavior impossible to get wrong via caller discipline, not just
 * documented as a caller responsibility.
 */
public interface InboxStore {

    /**
     * @param messageId the incoming message's identity (a {@code SagaCommand}'s
     *                   {@code commandId}, or a {@code SagaReply}'s {@code eventId})
     * @return {@code true} if this is the FIRST time {@code messageId} has
     *         been seen (the caller should proceed with processing);
     *         {@code false} if it was already recorded (a duplicate — the
     *         caller should skip processing and, where applicable, safely
     *         re-send whatever reply/acknowledgment it would have sent the
     *         first time, since the original may never have been received)
     */
    boolean recordIfNew(UUID messageId);
}
