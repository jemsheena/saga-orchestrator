# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and version numbers correspond to this project's own milestone numbering rather than strict SemVer, since this is a portfolio project built milestone-by-milestone rather than a versioned library release train.

## [0.2.5] — Milestone 2.5: Code Review Fixes

### Fixed
- **Critical:** event append and CQRS read-model projection were being committed as two independent database operations, contradicting the project's own documented same-transaction design. `DefaultSagaInstanceRepository.save()` now wraps both in a single `TransactionRunner.runInTransaction(...)` call.
- **Critical:** a snapshot persistence failure was not actually isolated from the triggering business operation, despite `SagaSnapshotStore`'s javadoc already promising that isolation. Snapshot saves are now wrapped in a try/catch that reports (via `System.err`, pending real logging infrastructure) and swallows the failure without affecting already-committed events.

### Added
- `ManagedConnection` visibility widened to support a second real consumer within the postgres module.

## [0.2.0] — Milestone 2: Event Sourcing + CQRS over PostgreSQL

### Added
- `saga-orchestrator-postgres` module: PostgreSQL adapter implementing every persistence port defined by `saga-orchestrator-core`.
- `PostgresSagaEventStore`: append-only event log with optimistic concurrency control (conditional `UPDATE` on `saga_stream_head`, backed by a `UNIQUE (saga_id, sequence_no)` constraint as a structural backstop).
- `PostgresSagaSnapshotStore` and `PostgresSagaInstanceViewStore`.
- `SagaSnapshot` and snapshot-based fast rehydration (`SagaInstance.reconstructFromSnapshot`), with schema-version-aware invalidation.
- CQRS read model: `SagaInstanceView` / `SagaInstanceViewStore` / `SagaProjector`, projected synchronously alongside the event append.
- Hand-written JSON serialization (`SimpleJson`, `HandWrittenJsonEventSerializer`) for event payloads — no external JSON library dependency.
- Versioned SQL migrations (`V1__event_store.sql`, `V2__snapshots_and_view.sql`).
- Testcontainers-based integration test suite against a real, ephemeral PostgreSQL instance.
- Redefined `SagaInstance.version()` as event count (incremented once per event) rather than once per business-method call, correcting a Milestone 1 under-counting inconsistency.

## [0.1.5] — Milestone 1.5: Domain Events and Correlation

### Added
- Domain event vocabulary: `SagaStarted`, `StepCompleted`, `SagaCompleted`, `StepFailed`, `SagaCompensationStarted`, `CompensationStepCompleted`, `SagaFailed`, as a sealed `SagaDomainEvent` interface.
- `SagaInstance.pullDomainEvents()` — business methods now record events explicitly rather than leaving callers to infer state changes from field diffs.
- Definition-by-reference: `SagaInstance` stores a `SagaDefinitionReference` instead of a live `SagaDefinition`, with `DefinitionMismatchException` validation on every business method call.
- Step correlation validation: `completeCurrentStep`/`failCurrentStep` now require and validate the reporting step's name (`StepMismatchException` on mismatch).

## [0.1.0] — Milestone 1: Domain Model

### Added
- `SagaDefinition`, `SagaStep`, `SagaDefinitionReference` — immutable, versioned saga blueprints built via a validating `Builder`.
- `SagaInstance` — the aggregate root, with a validated finite state machine (`SagaState`) covering the full saga lifecycle: `STARTED -> STEP_COMPLETED* -> COMPLETED`, and the compensation path `STARTED/STEP_COMPLETED -> COMPENSATING* -> FAILED`.
- Zero runtime framework dependencies in the domain module, by design.
- Initial unit test suite covering the full state machine, including edge cases (first-step failure skipping compensation, single-step saga completing directly from `STARTED`).

---

## [Unreleased] — Milestone 3 (Architecture Approved, Not Yet Implemented)

Design work only — see [`docs/roadmap.md`](./docs/roadmap.md) for the full, reviewed architecture. Nothing in this section exists as code in this repository yet.

- Kafka-based command/reply messaging between the orchestrator and independent participant services
- Protobuf message contracts
- Outbox pattern (solving the dual-write problem now that Kafka is a second system alongside Postgres)
- Inbox pattern (idempotent handling of Kafka's at-least-once delivery), both participant-side and orchestrator-side
- REST API for starting/querying sagas
- DB-polling timeout sweeper (`SELECT ... FOR UPDATE SKIP LOCKED`)
- `participant-payment-service`, `participant-inventory-service`, `participant-shipping-service`
- `saga-dashboard-api`
- OpenTelemetry distributed tracing across service boundaries
