# FOM

A JVM library for scheduling long-lived processes organised as a directed
acyclic graph of dependencies. Each process initialises its state once
(possibly heavy) and persists it to an append-only log so that a JVM restart
recovers without recomputing.

Idiomatic core: Java 21 + Project Loom + zero non-JDK runtime dependencies
(except `slf4j-api`). Kotlin DSL and other integrations ship as separate
modules.

## Modules

| Module | Purpose |
|---|---|
| `fom-core` | Java 21 runtime: FSM, GraphMachine, log SPI (InMemory + LocalFile), JavaSerializableSerDe, snapshot rotation, triggers, watchers, reactive cascade, dedup window, in-place graph swap, hot-reload config, `EngineObserver` SPI |
| `fom-fury` | Apache Fury SerDe — recommended for prod |
| `fom-config-hocon` | HOCON parser for `EngineConfig` + Quartz cron for `SnapshotPolicy` |
| `fom-guice` | Guice DI adapter — `SerializableSupplier` resolves bindings |
| `fom-spring` | Spring DI adapter — `SerializableSupplier` resolves beans |
| `fom-kotlin` | Kotlin DSL `graph { … }` + `SuspendingProcess` + suspend extensions |
| `fom-micrometer` | Micrometer counters/timers via `EngineObserver` |
| `fom-otel` | OpenTelemetry spans for `init`, `load`, `query` |
| `fom-test` | `InterruptContractTest` + reusable contract bases |
| `fom-tenant` | Tenant-aware engine wrapper (per-tenant authz + lifecycle) |
| `fom-log` | Standalone CLI: `inspect`, `diagnose`, `migrate` |
| `fom-log-maintenance` | `SizeBasedSnapshotPolicy` + `CompositeSnapshotPolicy` |
| `fom-jdbc` | Postgres LogBackend for multi-node (advisory-lock leadership, Testcontainers-tested) |

## Quickstart

```java
var graph = new GraphBuilder()
    .add("Inventory", InventoryInit::new, InventoryInit::new)
        .handles(GetInventoryModel.class)
    .add("Products", ProductsInit::new, ProductsInit::new, "Inventory")
        .handles(GetProductModel.class)
    .build();

try (var backend = new LocalFileLogBackend(Path.of("/var/lib/fom/log.bin"));
     var engine = new Engine(EngineConfig.defaults(),
                             backend,
                             new FurySerDe(),
                             true)) {
    engine.newGraph(graph);

    var pm = (ProductModel) engine
        .query(new GetProductModel("PUB1"))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
}
```

On a subsequent JVM start against the same log file, the engine skips
`init()` for every process whose state was persisted and goes straight to
`load()` — that's the "idempotent restart" goal.

### Kotlin DSL

```kotlin
val graph = graph {
    process("Inventory", ::InventoryInit, ::InventoryInit)
        .handles<GetInventoryModel>()

    process("Products", ::ProductsInit, ::ProductsInit, dependsOn = listOf("Inventory"))
        .handles<GetProductModel>()
}

Engine(EngineConfig.defaults(), backend, FurySerDe(), true).use { engine ->
    engine.newGraph(graph)
    val pm: ProductModel = engine.queryAs(GetProductModel("PUB1"))
}
```

### Triggers and watchers

```java
// Force a process to re-initialise on demand:
engine.trigger("Inventory", new RefreshSignal("ad-hoc"));

// Poll an external source every minute; on change, re-init Inventory:
engine.watch(new ScheduledWatcher<>(
        "Inventory",
        Long.class,
        /* initial value */ 0L,
        /* initial delay */ Duration.ZERO,
        /* interval */     Duration.ofMinutes(1),
        prevVersion -> fetchLatestVersion().filter(v -> v > prevVersion),
        null));
```

Reactive consumers cascade automatically: when `Inventory` re-initialises,
every process declared with `Dependency.reactive("Inventory")` is re-run too.
Use `Dependency.stable("Inventory")` (via `.addDeps(...)`) to opt out.

### In-place graph swap

```java
// Replace a node definition (e.g. new param) without bouncing the JVM.
// Unchanged nodes keep their Sid; changed/added cold-init; removed shut down.
// Reactive consumers of changed nodes cascade automatically.
var nextGraph = new GraphBuilder()
    .addWithParam("Inventory", InventoryInit::new, InventoryInit::new,
                  new InventoryParams("v2"))   // ← param changed
        .handles(GetInventoryModel.class)
    .add("Products", ProductsInit::new, ProductsInit::new, "Inventory")
        .handles(GetProductModel.class)
    .build();
engine.newGraph(nextGraph);   // returns true → diff applied
```

### Observability

```java
var registry = new SimpleMeterRegistry();
var engine = new Engine(cfg, backend, serDe, true, new MicrometerEngineObserver(registry));
// Counters / timers exposed: engine_process_init_duration_seconds{name},
// engine_query_duration_seconds{name}, engine_query_failures_total{name,reason},
// engine_dedup_collapsed_total{name}, etc.

// Snapshot live state for Prometheus or a debug endpoint:
EngineReport report = engine.introspect().toCompletableFuture().get();
```

### Multi-tenant

```java
var aware = TenantAwareEngine.builder(engine)
    .tenantResolver(TenantResolver.suffixAfter("_"))            // "Inventory_PUB1" → tenant "PUB1"
    .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant))
    .build();

aware.query(TenantCaller.of("alice", TenantId.of("PUB1")),
            new GetInventoryModel("PUB1"));
```

### Multi-node (Postgres)

```java
var backend = new PostgresLogBackend(dataSource, "fom_main");   // pg_advisory_lock = leader
// Second instance opening the same logId → IllegalStateException("advisory lock").
```

## Build

```
./gradlew build
```

Requires JDK 21+ as toolchain. The Gradle wrapper itself runs on the JDK that
launches it (currently exercised on JDK 25; toolchain auto-downloads JDK 21
via foojay-resolver). The `fom-jdbc` module uses Testcontainers and requires
a working Docker daemon for its integration tests.

## Documentation

Full documentation lives in [`docs/`](docs/index.md) (and builds into a site
with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) via
`mkdocs.yml`):

- **Getting started** — [installation](docs/getting-started/installation.md),
  [quickstart](docs/getting-started/quickstart.md),
  [your first graph](docs/getting-started/first-graph.md).
- **Concepts** — [the mental model](docs/concepts/index.md): lifecycle, the log,
  Sids, routing, reactive cascade, snapshots, graph swap, idempotent restart.
- **Guides** — [configuration](docs/guides/configuration.md),
  [serialization](docs/guides/serialization.md),
  [persistence backends](docs/guides/persistence-backends.md),
  [observability](docs/guides/observability.md),
  [multi-node](docs/guides/multi-node.md),
  [multi-tenancy](docs/guides/multi-tenancy.md),
  [Kotlin DSL](docs/guides/kotlin-dsl.md),
  [DI](docs/guides/dependency-injection.md), [CLI](docs/guides/cli.md),
  [testing](docs/guides/testing.md).
- **[Security](docs/security.md)** — read before any production / multi-tenant
  deployment.

To preview the site locally: `pip install mkdocs-material && mkdocs serve`.

Документация также доступна на русском: [`docs/ru/`](docs/ru/index.md).

## Roadmap status

| Stage | Scope | Status |
|---|---|---|
| 0 | Skeleton, log SPI, types, JavaSerializableSerDe | ✅ |
| 1 | `ProcessFSM` lifecycle on virtual threads, backoff, timeouts | ✅ |
| 2 | `GraphMachine`, type routing, cross-process queries | ✅ |
| 3 | `LocalFileLogBackend` (CRC + FileLock), `fom-fury`, snapshot rotation | ✅ |
| 4 | Triggers, watchers, reactive cascade, dedup, `fom-guice`/`fom-spring`/`fom-kotlin`/`fom-config-hocon` | ✅ |
| 5 | `EngineObserver`, `introspect()`, `fom-micrometer`, `fom-otel`, `fom-test` | ✅ |
| 6 | `fom-tenant`, `fom-log` CLI, `fom-log-maintenance`, hot-reload, `fom-jdbc` (Postgres) | ✅ |
| 7 | In-place graph swap via `Engine.newGraph` diff | ✅ |
| — | mkdocs site, Maven Central publishing, GitHub Actions, JMH benchmarks, native-image smoke, examples/ projects | Not yet |

## License

[Apache License 2.0](LICENSE).
