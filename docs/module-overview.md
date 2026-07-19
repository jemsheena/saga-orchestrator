# Module Overview

## `saga-orchestrator-core`

Framework-free domain model. Zero runtime dependencies beyond the JDK.

```
com.orchestrator.core
├── definition/          Immutable saga blueprints
│   ├── SagaDefinition           — versioned, ordered list of SagaStep, built via Builder
│   ├── SagaDefinitionReference  — (sagaType, version) identity pointer
│   └── SagaStep                 — one step's forward + compensation command names
│
├── engine/              The aggregate root and its supporting types
│   ├── SagaInstance             — the aggregate; decide/apply split, event-sourced
│   ├── SagaState                — the state machine enum (legal transitions live on the enum)
│   └── SagaSnapshot              — point-in-time capture for fast rehydration
│
├── event/               The closed, sealed event vocabulary
│   ├── SagaDomainEvent           — sealed interface, permits clause lists all 7 events
│   ├── SagaStarted, StepCompleted, SagaCompleted,
│   │   StepFailed, SagaCompensationStarted,
│   │   CompensationStepCompleted, SagaFailed
│
├── exception/           Domain-specific, structured exceptions
│   ├── ConcurrencyConflictException
│   ├── DefinitionMismatchException
│   ├── InvalidStateTransitionException
│   └── StepMismatchException
│
├── projection/           The read side (CQRS)
│   ├── SagaInstanceView          — denormalized, query-optimized row shape
│   ├── SagaInstanceViewStore     — port for the view store
│   └── SagaProjector             — event -> view mutation, transaction-boundary-agnostic
│
└── repository/           Ports the application layer depends on
    ├── EventMetadata              — correlation/causation tracing data
    ├── SagaDefinitionRegistry     — resolves references back to definitions
    ├── SagaEventStore             — append-only event log port
    ├── SagaInstanceRepository     — the single façade application code depends on
    ├── SagaSnapshotStore          — snapshot persistence port
    ├── TransactionRunner          — framework-agnostic transaction boundary
    └── support/
        ├── DefaultSagaInstanceRepository    — composes the ports above; save()/findById()
        └── InMemorySagaDefinitionRegistry   — ConcurrentHashMap-backed, legitimate prod default
```

**Test sources** (`src/test`) include full in-memory fakes for every port (`InMemorySagaEventStore`, `InMemorySagaSnapshotStore`, `InMemorySagaInstanceViewStore`, `ImmediateTransactionRunner`) — this is what lets `DefaultSagaInstanceRepository` and `SagaInstance` be unit tested with zero real infrastructure.

## `saga-orchestrator-postgres`

The only module that knows PostgreSQL exists. Depends on `saga-orchestrator-core` and the PostgreSQL JDBC driver (runtime only).

```
com.orchestrator.postgres
├── ManagedConnection              — connection-per-transaction wrapper
├── JdbcTransactionRunner           — TransactionRunner implementation
├── PostgresAdapterException        — wraps SQLException as an unchecked adapter exception
├── PostgresSagaEventStore          — SagaEventStore implementation (append-only log + optimistic concurrency)
├── PostgresSagaSnapshotStore       — SagaSnapshotStore implementation
├── PostgresSagaInstanceViewStore   — SagaInstanceViewStore implementation (the read model)
│
└── serialization/
    ├── SagaEventSerializer              — port: SagaDomainEvent <-> JSON payload
    ├── HandWrittenJsonEventSerializer    — concrete serializer for the 7 event types
    └── SimpleJson                        — minimal, purpose-built JSON writer/reader
```

```
src/main/resources/db/migration/
├── V1__event_store.sql          — saga_event, saga_stream_head
└── V2__snapshots_and_view.sql   — saga_snapshot, saga_instance_view
```

**Test sources** include Testcontainers-backed integration tests (`AbstractPostgresIntegrationTest` and its subclasses) that spin up a real, ephemeral PostgreSQL instance — these require a local Docker daemon to run.

## Root

```
saga-orchestrator/
├── settings.gradle.kts     — multi-module wiring; commented placeholders for
│                             Milestone 3+ modules (saga-orchestrator-api,
│                             participant-*-service, saga-dashboard-api) not yet built
├── docs/                   — this documentation
└── .github/                — CI workflow, issue/PR templates
```

There is intentionally no root `build.gradle.kts` applying shared configuration — each module's `build.gradle.kts` is self-contained and explicit about its own dependencies, which keeps the "core has zero framework dependencies" guarantee visible at a glance rather than inherited from a shared block.
