# fom examples

Runnable, self-contained examples for [fom](../README.md). Each is a single
class with a `main()`, wired to its own Gradle task, so you can run any of them
directly:

```bash
./gradlew :examples:quickstart
```

Everything except the Postgres example runs with **no external services** (they
use the in-memory or local-file log backend). The `examples` module is part of
the build — `./gradlew build` compiles it — but it is **not** published to Maven
Central.

## The examples

| Task | Class | What it shows |
|---|---|---|
| `:examples:quickstart` | `QuickstartExample` | The smallest useful program: one process, `init` + `load`, a single typed query. |
| `:examples:warmRestart` | `WarmRestartExample` | **Idempotent restart** — two engine runs over the same file; `init` executes once, the second start recovers via `load` only. |
| `:examples:multiProcess` | `MultiProcessExample` | A dependency edge: `Products` queries `Inventory` during its own `init` (topological spawn order). |
| `:examples:triggersWatchers` | `TriggersAndWatchersExample` | Forcing re-init with `trigger`, a `ScheduledWatcher` poll, and the reactive cascade to a dependent. |
| `:examples:snapshots` | `SnapshotExample` | Compacting the log with `snapshot()` — log shrinks, the old contents are archived, warm restart still works. |
| `:examples:multiTenant` | `MultiTenantExample` | One process per tenant + `TenantAwareEngine` authorization (allow own tenant, deny others). |
| `:examples:observability` | `ObservabilityExample` | `MicrometerEngineObserver` metrics (push) and `introspect()` (pull). |
| `:examples:furySerde` | `FurySerdeExample` | Swapping in `FurySerDe` — the production-recommended serializer. |
| `:examples:kotlinDsl` | `KotlinDslExample.kt` | The Kotlin `graph { }` DSL, a `SuspendingProcess`, and `queryAs<T>`. |
| `:examples:multiNodePostgres` | `MultiNodePostgresExample` | Leader election across instances via the Postgres advisory-lock backend. **Needs a database** (see below). |
| `:examples:dbWatcher` | `DbWatcherExample` | A `ScheduledWatcher` polls a **database** row and re-initialises a process when the value changes. **Needs a database** (see below). |

## Running the Postgres examples

`multiNodePostgres` and `dbWatcher` need a running Postgres. Spin up a
throwaway one with Docker:

```bash
docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=test \
  --name fom-pg postgres:16-alpine

# leader election across instances:
./gradlew :examples:multiNodePostgres \
  -PexampleArgs="jdbc:postgresql://localhost:5432/postgres postgres test"

# re-init a process when a DB value changes:
./gradlew :examples:dbWatcher \
  -PexampleArgs="jdbc:postgresql://localhost:5432/postgres postgres test"

docker rm -f fom-pg
```

You can also point them at any database via the `FOM_PG_URL` / `FOM_PG_USER` /
`FOM_PG_PASSWORD` environment variables. With no URL they just print these
instructions and exit.

`dbWatcher` creates (and drops) a tiny `catalog_version` table; the watcher's
`check` runs on the engine scheduler thread, so keep such polls to a quick
indexed read (the example does a single `SELECT`).

## Notes

- Console logging is set to `warn` (see `src/main/resources/simplelogger.properties`).
  Raise it to `info` to watch the engine narrate cold/warm starts, the reactive
  cascade, snapshot rotation, and so on.
- These examples use `JavaSerializableSerDe` for brevity. For anything real,
  prefer `FurySerDe` (see `furySerde`) and read the
  [security guide](../docs/security.md) before exposing the log to
  untrusted input.

## Where to go next

- [Getting started](../docs/getting-started/quickstart.md)
- [Concepts](../docs/concepts/index.md) — the mental model behind these examples
- [Guides](../docs/guides/configuration.md) — task-oriented how-tos
