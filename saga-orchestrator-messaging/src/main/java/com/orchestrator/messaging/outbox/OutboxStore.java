package com.orchestrator.messaging.outbox;

/**
 * Durable, append-only staging for messages that must eventually be
 * published, plus atomic claiming for dispatch.
 *
 * <p><b>Design decision worth defending explicitly: why {@link #claimAndDispatch}
 * is one method taking a callback, not two methods
 * ("claimBatch" + "markDispatched"):</b> a naive two-method split has a real
 * race — claiming rows, then later marking them dispatched in a SEPARATE
 * transaction, means the claim's row lock is released before dispatch is
 * confirmed, so a second poller could re-claim and re-publish the same rows
 * concurrently. Keeping the claim, the dispatch callback, and the
 * dispatched-marking all inside ONE transaction (see
 * {@code PostgresOutboxStore}'s implementation) closes that race entirely —
 * this mirrors the exact reasoning behind {@code SagaEventStore}'s
 * conditional-UPDATE concurrency mechanism from Milestone 2: make the
 * correct behavior structural, not dependent on caller discipline across
 * multiple calls.
 *
 * <p><b>Per-record failure isolation:</b> if {@link OutboxDispatcher#dispatch}
 * throws for one claimed record, implementations must NOT abort the whole
 * batch — other records in the same batch that already dispatched
 * successfully must still be marked dispatched and the transaction must
 * still commit. The failed record simply remains undispatched, retried on
 * a future poll. A consequence worth stating plainly: if a record's publish
 * actually SUCCEEDED at the broker but the local "mark dispatched" step
 * then fails for an unrelated reason, that record WILL be republished on
 * the next poll — a duplicate at the transport level. This is intentional,
 * not a bug: Outbox's guarantee is at-least-once delivery, and the Inbox
 * pattern downstream (see {@code InboxStore}) is what absorbs exactly this
 * kind of duplicate.
 */
public interface OutboxStore {

    /** Durably records a message for later dispatch. Typically called in the same transaction as the business write that required it. */
    void append(OutboxRecord record);

    /**
     * Claims up to {@code limit} undispatched records, invokes
     * {@code dispatcher} for each, and marks each one dispatched if and
     * only if its dispatch call succeeded — all as one atomic unit of work.
     *
     * @return the number of records successfully dispatched in this call
     */
    int claimAndDispatch(int limit, OutboxDispatcher dispatcher);
}
