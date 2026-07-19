# Design Decisions

A record of the significant design decisions actually made in this codebase, why they were made, and what was rejected instead. Every claim below is traceable to a specific class, test, or code comment in this repository — this is not aspirational documentation.

## 1. Event Sourcing over CRUD persistence

**Decision:** `SagaInstance` never persists its current field values. Every state change is expressed as an immutable domain event (`SagaStarted`, `StepCompleted`, `SagaCompleted`, `StepFailed`, `SagaCompensationStarted`, `CompensationStepCompleted`, `SagaFailed`), and current state is always a derived projection of the full event history.

**Why:** A saga orchestrator's core value proposition is answering "what actually happened, and in what order, to this specific workflow instance" — for debugging, for audit, and because compensation logic depends on knowing exactly which steps already succeeded. A CRUD "current state" row answers "where is it now" but destroys the "how did it get here" information the moment a new value overwrites the old one. Event sourcing makes the history the source of truth instead of a side effect of logging.

**Rejected alternative:** Persisting a mutable `saga_instance` row with `current_state`, `current_step` columns, updated in place. Simpler to implement, but the audit trail becomes an afterthought (a separate `saga_event` log written "for logging purposes" is easy to let drift out of sync with the row that's actually authoritative). Event sourcing makes the log the only thing that can be authoritative, by construction.

## 2. Decide vs. Apply — two distinct methods, never one

**Decision:** `SagaInstance` splits business logic into two categories that never share a method:
- **Decide** (`completeCurrentStep`, `failCurrentStep`, `completeCompensationStep`): validates business rules against current state, and if legal, computes and records new events.
- **Apply** (`apply(SagaDomainEvent)`): takes an already-historical event as unquestionable ground truth and mutates state to match it — **no validation, no new event emission.**

**Why:** Conflating these is one of the most common event-sourcing mistakes. If replay re-ran business rules against history, today's rules might reject something that was legal when it actually happened (e.g. a validation rule added later). If replay re-emitted events, replaying a saga's history would duplicate its own event log. Keeping them as two distinct methods — with `reconstruct()` and `reconstructFromSnapshot()` driving `apply()` exclusively — makes this structurally impossible rather than a matter of discipline.

## 3. Definition by reference, not by object

**Decision:** `SagaInstance` stores only a `SagaDefinitionReference` (`sagaType` + `version`), never a live `SagaDefinition` object. Every business method requires the caller to resolve and pass in the actual `SagaDefinition`, and validates it against the pinned reference (`DefinitionMismatchException` if it doesn't match).

**Why:** Two concrete problems this solves:
1. **Rehydration has nothing to hand back.** What's on disk after replaying `saga_event` rows is a `sagaType` and `version` — never a serialized object graph. A reference-based design makes live execution and rehydration use the identical validation path.
2. **Version pinning becomes enforceable, not just a convention.** A saga that started against definition v1 must keep executing against v1, even if v2 is deployed mid-flight — otherwise step index 2 could mean something entirely different depending on which version's step list is consulted. This is validated at the exact call site that would otherwise cause a silent, hard-to-diagnose data corruption.

## 4. Aggregate version = event count, not a per-call counter

**Decision:** `SagaInstance.version()` is exactly the number of events applied to the instance — incremented once per event in `recordEvent()`/`apply()`, never once per business-method call.

**Why:** A single business-method call can legally emit more than one event (`failCurrentStep` on the first step emits both `StepFailed` and `SagaFailed`). An earlier per-call increment scheme under-counted relative to the real event history. Event-count-as-version is also what makes the `expectedVersion` arithmetic in `SagaEventStore.append(...)` correct and matches how EventStoreDB/Axon-style "expected version" checks work.

## 5. Optimistic concurrency, not pessimistic locking

**Decision:** `PostgresSagaEventStore.append(...)` takes an `expectedVersion` and enforces it via a conditional `UPDATE` on `saga_stream_head.current_sequence_no`, backed by a `UNIQUE (saga_id, sequence_no)` constraint on `saga_event` as a structural backstop.

**Why:** Two independent enforcement layers, deliberately redundant: the head-table check is the fast, explicit signal (`ConcurrencyConflictException` with clear expected-vs-actual values); the unique constraint is what makes a duplicate-position write structurally impossible even if the head-table logic ever had a bug. Verified under real concurrent load in `ConcurrencyConflictTest` (including a real multi-threaded race, not just a simulated one).

**Correct recovery, documented on the exception itself:** reload the aggregate fresh and retry the *original* business method — not a blind retry of the same append. Thanks to step-correlation validation, that retry very often legitimately throws `StepMismatchException` instead, correctly recognized as "someone else already handled this."

## 6. Synchronous CQRS projection (for now)

**Decision:** Event append and read-model (`saga_instance_view`) projection happen inside the **same database transaction**, in `DefaultSagaInstanceRepository.save()`.

**Why:** At this project's current scale, having the read model out of sync with the write side — even briefly — is a worse trade-off than the coupling this creates. `SagaProjector` itself is deliberately transaction-boundary-agnostic (it only needs an event and a place to write the resulting view), so migrating to asynchronous projection later (e.g. a Kafka consumer projecting minutes after the fact) is a change to *who calls this class and when*, never to the class itself.

**Correction applied during a Milestone 2 code review:** event-append and projection were originally two independently-committed operations — a real bug, since it contradicted this project's own documented "same-transaction" design. Milestone 2.5 wrapped both inside a single `TransactionRunner.runInTransaction(...)` call. This is exactly the kind of gap a code review is supposed to catch, and it's recorded here rather than quietly fixed and forgotten.

## 7. Snapshots are a disposable cache, never a second source of truth

**Decision:** `SagaSnapshotStore` persistence happens *outside* the event-append transaction, independently exception-guarded — a snapshot save failure is caught, logged, and swallowed; it must never fail (or roll back) the business operation that triggered it.

**Why:** The events a `save()` call just persisted are already durably committed by the time snapshotting runs. Nothing about a snapshot failure should be able to undo that. A snapshot with an incompatible `schemaVersion` is also handled by discarding it and falling back to full replay from event 0 — always correct, only ever slower.

**Correction applied during a Milestone 2 code review:** the original snapshot call site did not actually honor this "must never fail the operation" contract, despite both `SagaSnapshotStore` and `PostgresSagaSnapshotStore`'s own javadoc already promising it. Milestone 2.5 added the try/catch that makes the code match the contract it always claimed to have.

## 8. Sealed event hierarchy, exhaustive switch, no default branch reliance

**Decision:** `SagaDomainEvent` is a `sealed interface` with an explicit `permits` clause listing all seven event types. `SagaProjector.project(...)` and `SagaInstance.apply(...)` both switch over it exhaustively.

**Why:** An event-sourced system's entire value depends on its event vocabulary being fixed and completely known. An open/extensible event interface would let new event types silently fail to be handled by the projector or replay logic — a runtime bug discovered only when that event type actually occurs. A sealed interface turns "did I handle every event type" into a compile-time exhaustiveness check instead.

## 9. Domain-specific exceptions, not generic `IllegalStateException`

**Decision:** `ConcurrencyConflictException`, `DefinitionMismatchException`, `InvalidStateTransitionException`, and `StepMismatchException` are all dedicated types carrying structured context (the actual vs. expected values), not string messages wrapped in a generic exception.

**Why:** Calling code (a future REST controller or Kafka consumer) needs to distinguish "this is a legitimate concurrent-write conflict, retry the operation" from "this is a corrupted event stream, page someone" from "this is a duplicate/out-of-order message, safely ignore it." A generic exception forces fragile string-matching; a typed exception makes the failure mode part of the API contract.

## 10. Zero framework dependencies in the domain module

**Decision:** `saga-orchestrator-core` has no runtime dependency on Spring, JDBC, Jackson, or any other framework — only the JDK, plus JUnit for tests.

**Why:** Testing the domain model should mean testing *the domain model*, not incidentally testing a framework's behavior too. This also makes the domain layer trivially portable — a future REST layer, a Kafka consumer, or a completely different persistence technology all become new adapter modules, never a change to `core`.

## 11. Hand-written JSON serialization (`SimpleJson`) instead of Jackson

**Decision:** Event payloads are serialized to JSONB via a hand-written `SimpleJson`/`HandWrittenJsonEventSerializer`, not a general-purpose JSON library.

**Why:** Adding Jackson (or any JSON library) as a dependency was deliberately deferred until it was actually needed for something Jackson does well that a small, purpose-built serializer doesn't — for this project's actual event shapes (flat records with primitive and UUID/Instant fields), a minimal hand-written serializer is a smaller, more auditable surface area, and demonstrates the mechanics rather than delegating them. This is documented in-repo as a deliberate scope decision, not an oversight — the same "don't add infrastructure ahead of a measured need" discipline applied elsewhere in this project (see the Roadmap for where this calculus changes, e.g. Protobuf for the Kafka message layer).
