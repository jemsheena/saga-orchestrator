# Architecture

This document describes the architecture **as implemented today** (Milestones 1, 2, and 2.5). For the approved-but-not-yet-built Kafka/messaging architecture, see [`roadmap.md`](./roadmap.md) — nothing in that layer exists in this repository yet, and this document intentionally does not diagram it as current state.

## 1. Overall System Architecture

The system is a two-module Gradle build: a framework-free domain core, and a PostgreSQL adapter that implements the persistence ports the core defines.

```mermaid
flowchart TB
    subgraph App["Application code (tests today; a REST/Kafka layer later)"]
        Client["Caller: start a saga,\nreport step outcomes"]
    end

    subgraph Core["saga-orchestrator-core (framework-free)"]
        Repo["SagaInstanceRepository\n(DefaultSagaInstanceRepository)"]
        Instance["SagaInstance\n(aggregate root)"]
        Projector["SagaProjector"]
        Registry["SagaDefinitionRegistry"]
    end

    subgraph PG["saga-orchestrator-postgres (adapter)"]
        EventStoreImpl["PostgresSagaEventStore"]
        SnapshotImpl["PostgresSagaSnapshotStore"]
        ViewImpl["PostgresSagaInstanceViewStore"]
        TxRunner["JdbcTransactionRunner"]
    end

    subgraph DB["PostgreSQL"]
        EventTable[("saga_event\nsaga_stream_head")]
        SnapshotTable[("saga_snapshot")]
        ViewTable[("saga_instance_view")]
    end

    Client --> Repo
    Repo --> Instance
    Repo --> Projector
    Repo -. resolves versions via .-> Registry

    Repo -->|SagaEventStore port| EventStoreImpl
    Repo -->|SagaSnapshotStore port| SnapshotImpl
    Repo -->|SagaInstanceViewStore port| ViewImpl
    Repo -->|TransactionRunner port| TxRunner

    EventStoreImpl --> EventTable
    SnapshotImpl --> SnapshotTable
    ViewImpl --> ViewTable
    TxRunner -. wraps append + projection .-> EventTable
    TxRunner -. in one transaction .-> ViewTable
```

**Why this shape:** `saga-orchestrator-core` has zero runtime dependencies — no Spring, no JDBC, no Jackson. It defines ports (`SagaEventStore`, `SagaSnapshotStore`, `SagaInstanceViewStore`, `TransactionRunner`, `SagaDefinitionRegistry`) as plain interfaces; `saga-orchestrator-postgres` is the only module that knows PostgreSQL exists. This is a direct application of Clean Architecture / Hexagonal Architecture — the domain model is testable with pure in-memory fakes (see `saga-orchestrator-core`'s test sources), and swapping the persistence technology later would mean writing a new adapter module, not touching the domain at all.

## 2. Module Dependencies

```mermaid
flowchart LR
    postgres["saga-orchestrator-postgres"] --> core["saga-orchestrator-core"]
    postgres -.->|runtime only| jdbc["org.postgresql:postgresql"]

    style core fill:#2d5,stroke:#333,color:#000
    style postgres fill:#59d,stroke:#333,color:#000
```

`saga-orchestrator-core` depends on nothing but the JDK and JUnit (test-only). `saga-orchestrator-postgres` depends on `core` and the PostgreSQL JDBC driver. The dependency arrow only ever points one way — the core module has no idea the postgres module exists.

## 3. Saga Workflow (State Machine)

Every `SagaInstance` is a finite state machine. The legal-transition table lives directly on the `SagaState` enum (`legalNextStates()`), not in a separate switch statement, so the two can never drift apart.

```mermaid
stateDiagram-v2
    [*] --> STARTED
    STARTED --> STEP_COMPLETED: step succeeds\n(more steps remain)
    STARTED --> COMPLETED: step succeeds\n(single-step saga)
    STARTED --> COMPENSATING: unreachable in practice\u2014see note
    STARTED --> FAILED: first step fails\n(nothing to compensate)

    STEP_COMPLETED --> STEP_COMPLETED: next step succeeds
    STEP_COMPLETED --> COMPLETED: final step succeeds
    STEP_COMPLETED --> COMPENSATING: a later step fails

    COMPENSATING --> COMPENSATING: another step\nstill needs undoing
    COMPENSATING --> FAILED: last compensation finishes

    COMPLETED --> [*]
    FAILED --> [*]
```

Two edge cases are worth calling out explicitly (both are covered by dedicated tests in `SagaInstanceTest`):

- **First-step failure skips `COMPENSATING` entirely.** If step 0 fails, nothing has succeeded yet, so there is nothing to undo — the saga goes straight to `FAILED`.
- **A single-step saga can complete directly from `STARTED`.** `COMPLETED` is reachable from `STARTED` for the case where the one and only step succeeds.

`SagaState` tracks *coarse* status only. Fine-grained progress (which step index the saga is on) lives on `SagaInstance.currentStepIndex()` — the enum deliberately does not grow one value per step.

## 4. Event Sourcing Flow

`SagaInstance` never persists its current field values directly. Every mutation is expressed as a domain event first; state is a derived, replayable projection of that event log.

```mermaid
sequenceDiagram
    participant Caller
    participant Instance as SagaInstance
    participant Repo as DefaultSagaInstanceRepository
    participant Tx as TransactionRunner
    participant Store as SagaEventStore (Postgres)
    participant Proj as SagaProjector
    participant View as SagaInstanceViewStore (Postgres)
    participant Snap as SagaSnapshotStore (Postgres)

    Caller->>Instance: completeCurrentStep(definition, stepName)
    Instance->>Instance: validate definition + step name
    Instance->>Instance: record StepCompleted (+ SagaCompleted if last step)
    Instance->>Instance: version++ per event

    Caller->>Repo: save(instance, metadata)
    Repo->>Instance: pullDomainEvents()
    Instance-->>Repo: [StepCompleted, ...]

    Repo->>Tx: runInTransaction(...)
    activate Tx
    Tx->>Store: append(sagaId, expectedVersion, events, metadata)
    Store-->>Tx: OK or ConcurrencyConflictException
    Tx->>Proj: project(event, viewStore) for each event
    Proj->>View: upsert(SagaInstanceView)
    deactivate Tx
    Note over Tx: event append + view projection commit\nor roll back together \u2014 one atomic unit

    Repo->>Repo: crossed snapshot interval?
    opt snapshot threshold crossed
        Repo->>Snap: save(snapshot)
        Note over Snap: independently failure-isolated\u2014a snapshot\nfailure never invalidates already-committed events
    end
```

Rehydration is the mirror image: `SagaInstanceRepository.findById` looks for the latest compatible snapshot, loads only the events recorded after it, and calls `SagaInstance.reconstructFromSnapshot`; if no usable snapshot exists it falls back to `SagaInstance.reconstruct` over the full event history. Both paths funnel through the same `apply(event)` method used during live execution — there is no separate "replay" logic that could drift from the "decide" logic.

## 5. CQRS Flow

Write side and read side are deliberately separate stores, projected synchronously in the same transaction (see [`design-decisions.md`](./design-decisions.md) for why "synchronous" was the correct choice for this milestone).

```mermaid
flowchart LR
    subgraph Write["Write Side"]
        WCmd["Business method call\n(completeCurrentStep, failCurrentStep, ...)"]
        WEvents["Domain events"]
        WStore[("saga_event\n(append-only, source of truth)")]
    end

    subgraph Read["Read Side"]
        RProj["SagaProjector"]
        RView[("saga_instance_view\n(denormalized, query-optimized)")]
    end

    WCmd --> WEvents --> WStore
    WEvents --> RProj --> RView

    Query["\"All currently-FAILED sagas\"\ndashboard-style query"] --> RView
    Replay["Rehydrate a specific saga\nby full ID"] --> WStore
```

The read model exists to answer questions the write side cannot answer efficiently without replaying every saga's full history (e.g. "all sagas currently in a `FAILED` state"). The write side remains the only source of truth — the view can always be rebuilt from the event log if it is ever dropped or corrupted.

## 6. Optimistic Concurrency

```mermaid
sequenceDiagram
    participant W1 as Writer 1
    participant W2 as Writer 2
    participant Store as PostgresSagaEventStore

    W1->>Store: loadEvents(sagaId) -> version 4
    W2->>Store: loadEvents(sagaId) -> version 4
    W1->>Store: append(sagaId, expectedVersion=4, ...)
    Store-->>W1: OK, stream now at version 5
    W2->>Store: append(sagaId, expectedVersion=4, ...)
    Store-->>W2: ConcurrencyConflictException\n(actual version is 5, not 4)
    Note over W2: Correct recovery: reload the aggregate\nfresh and retry the ORIGINAL business method\u2014\nnot a blind retry of the same append.
```

Enforced two ways, deliberately redundant: a conditional `UPDATE` against `saga_stream_head.current_sequence_no` (the fast, explicit signal), and a `UNIQUE (saga_id, sequence_no)` constraint on `saga_event` (the structural backstop that makes a duplicate-position write impossible even if the head-table logic had a bug).

## Current Scope Boundary

Everything above exists and is covered by tests in this repository. There is **no Kafka, no REST API, no participant services, and no Outbox/Inbox pattern implemented yet** — those are an approved architecture for Milestone 3, not built code. See [`roadmap.md`](./roadmap.md).
