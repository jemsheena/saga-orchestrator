# Contributing

Thanks for considering contributing to this project. It started as a structured, milestone-by-milestone learning exercise in building an event-sourced saga orchestrator correctly, and documenting *why* each decision was made — that spirit is worth preserving in any contribution.

## Before you start

- Read [`docs/architecture.md`](./docs/architecture.md) and [`docs/design-decisions.md`](./docs/design-decisions.md) first. A lot of "why isn't this simpler" questions are already answered there, including the alternatives that were considered and rejected.
- Check [`docs/roadmap.md`](./docs/roadmap.md) to see what's already planned vs. what's genuinely open.
- For anything beyond a small fix, please open an issue first to discuss the approach before submitting a PR — this project has a strong existing set of design constraints (framework-free domain core, event sourcing, single-writer-per-aggregate concurrency model) and it's much easier to align on those before code is written than after.

## Development setup

- JDK 21
- Docker, if you want to run the PostgreSQL integration test suite (Testcontainers-based)
- Gradle 8.x (a wrapper is not yet checked into the repo — see the README's "Manual Steps" note)

```bash
gradle :saga-orchestrator-core:test        # domain model — no Docker required
gradle :saga-orchestrator-postgres:test    # adapter tests — Docker required
```

## Code style and conventions

- `saga-orchestrator-core` must remain free of framework dependencies (no Spring, no JDBC, no Jackson). If your change needs one of those, it almost certainly belongs in an adapter module instead.
- Keep the **decide vs. apply** split intact in `SagaInstance` — business methods validate and record events; `apply()` only ever mutates state from an already-historical event, with no validation and no new event emission. See [`docs/design-decisions.md`](./docs/design-decisions.md#2-decide-vs-apply--two-distinct-methods-never-one).
- New domain events must be added to `SagaDomainEvent`'s `permits` clause and handled in both `SagaInstance.apply(...)` and `SagaProjector.project(...)` — the compiler's exhaustive-switch checking is there specifically to catch a missed case.
- Favor a dedicated, structured exception type over a generic `RuntimeException`/`IllegalStateException` for any new domain-level failure mode.
- Match the existing javadoc style: explain *why*, including what was considered and rejected, not just *what* the code does.

## Tests

- New behavior in `saga-orchestrator-core` should be covered by an in-memory unit test (see the existing `InMemory*` fakes in `src/test`) before it needs a real database to verify.
- Changes to the PostgreSQL adapters should include or update a Testcontainers integration test.
- Please don't submit a PR with reduced test coverage on a path you touched.

## Submitting a change

1. Fork the repository and create a branch from `main`.
2. Make your change, with tests.
3. Run the full test suite locally.
4. Open a pull request using the provided template, describing what changed and why.

## Reporting bugs / proposing features

Please use the issue templates in `.github/ISSUE_TEMPLATE/`. For anything touching the approved-but-unimplemented Milestone 3 design, please reference the relevant section of `docs/roadmap.md` in your issue.
