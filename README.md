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
| `fom-core` | Java 21 runtime: FSM, GraphMachine, log SPI (`InMemoryLogBackend`, `FileLogBackend`), `JavaSerializableSerDe`, snapshot policies, triggers, watchers, reactive cascade, dedup window, re-init that keeps the old version serving, in-place graph swap, hot-reload config, `EngineObserver` SPI |
| `fom-fury` | Apache Fury SerDe — recommended for prod |
| `fom-config-hocon` | HOCON parser for `EngineConfig` + Quartz cron for `SnapshotPolicy` |
| `fom-kotlin` | Kotlin DSL `graph { … }` + `SuspendingProcess` + suspend extensions |
| `fom-micrometer` | Micrometer counters/timers via `EngineObserver` |
| `fom-otel` | OpenTelemetry spans for `init`, `load`, `query` and `fom.reinit` |
| `fom-test` | Contract tests for your own implementations: `LogBackendContractTest`, `SerDeContractTest`, `InterruptContractTest` |
| `fom-tenant` | Tenant-aware engine wrapper (per-tenant authz + lifecycle) |
| `fom-log` | Standalone CLI for log files: `inspect`, `diagnose`, `events`, `compact` |
| `fom-jdbc` | Postgres LogBackend for multi-node (advisory-lock leadership, Testcontainers-tested) |

## Installation

Not on Maven Central yet. Build and install the snapshot locally, then depend on it:

```
./gradlew publishToMavenLocal
```

```kotlin
repositories { mavenLocal(); mavenCentral() }
dependencies {
    implementation("io.github.altspacetg:fom-core:0.1.0-SNAPSHOT")
    implementation("io.github.altspacetg:fom-fury:0.1.0-SNAPSHOT")      // optional: faster SerDe
    implementation("io.github.altspacetg:fom-kotlin:0.1.0-SNAPSHOT")    // optional: Kotlin DSL
}
```

Other modules follow the same pattern; see [installation](docs/getting-started/installation.md).

## Quickstart

```java
var graph = new GraphBuilder()
    .add("Stations", StationsInit::new, StationsInit::new)
        .handles(GetStationsModel.class)
    .add("Forecasts", ForecastsInit::new, ForecastsInit::new, "Stations")
        .handles(GetForecastModel.class)
    .build();

try (var backend = new FileLogBackend(Path.of("/var/lib/fom/log.bin"));
     var engine = new Engine(EngineConfig.defaults(),
                             backend,
                             new FurySerDe())) {
    engine.newGraph(graph);

    var pm = (ForecastModel) engine
        .query(new GetForecastModel("ST-1"))
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
    process("Stations", ::StationsInit, ::StationsInit)
        .handles<GetStationsModel>()

    process("Forecasts", ::ForecastsInit, ::ForecastsInit, dependsOn = listOf("Stations"))
        .handles<GetForecastModel>()
}

Engine(EngineConfig.defaults(), backend, FurySerDe()).use { engine ->
    engine.newGraph(graph)
    val pm: ForecastModel = engine.queryAs(GetForecastModel("ST-1"))
}
```

### Triggers and watchers

```java
// Force a process to re-initialise on demand:
engine.trigger("Stations", new RefreshSignal("ad-hoc"));

// Poll an external source every minute; on change, re-init Stations:
engine.watch(new ScheduledWatcher<>(
        "Stations",
        Long.class,
        /* initial value */ 0L,
        /* initial delay */ Duration.ZERO,
        /* interval */     Duration.ofMinutes(1),
        prevVersion -> fetchLatestVersion().filter(v -> v > prevVersion),
        null));
```

Reactive consumers cascade automatically: when `Stations` re-initialises,
every process declared with `Dependency.reactive("Stations")` is re-run too.
Use `Dependency.stable("Stations")` (via `.addDeps(...)`) to opt out.

### Re-init keeps the old version serving

While a process re-initialises, its old version keeps answering queries; the new
version inits and loads beside it and takes over in one step. If the new version
fails (any error, including `OutOfMemoryError`), the old one keeps serving, the
node is reported `stale`, and the re-init is retried with backoff — a new trigger
retries at once. After a restart the old state is served immediately and the
re-init finishes in the background.

```java
// Defaults: keep the old version, retry failed re-inits with backoff.
var cfg = EngineConfig.defaults()
    .withReinitRetryBackoff(Duration.ofMinutes(1), Duration.ofMinutes(10));

// A node too big to hold twice: retire first, then init (the old behaviour).
new GraphBuilder()
    .add("HugeIndex", IndexInit::new, IndexInit::new)
        .reinitStrategy(ReinitStrategy.RELEASE_FIRST);
```

See [re-initialisation](docs/concepts/process-lifecycle.md#re-initialisation).

### Snapshots

The log is append-only; a snapshot compacts it to the live state and archives the
old file. Archives are kept unless a policy is told how many to keep:

```java
var cfg = EngineConfig.defaults()
    .withSnapshotPolicy(new SnapshotPolicy.FixedInterval(Duration.ofHours(1), 7)); // keep 7 archives
engine.snapshot();         // or on demand
engine.purgeArchives(3);   // or prune by hand
```

### In-place graph swap

```java
// Replace a node definition (e.g. new param) without bouncing the JVM.
// Unchanged nodes keep their Sid; changed/added cold-init; removed shut down.
// (A changed node is replaced cold: it does not keep serving while its new definition starts.)
// Reactive consumers of changed nodes cascade automatically.
var nextGraph = new GraphBuilder()
    .addWithParam("Stations", StationsInit::new, StationsInit::new,
                  new StationsParams("v2"))   // ← param changed
        .handles(GetStationsModel.class)
    .add("Forecasts", ForecastsInit::new, ForecastsInit::new, "Stations")
        .handles(GetForecastModel.class)
    .build();
engine.newGraph(nextGraph);   // returns true → diff applied
```

### Observability

```java
var registry = new SimpleMeterRegistry();
var engine = new Engine(cfg, backend, serDe, new MicrometerEngineObserver(registry));
// Among the meters: engine_process_init_duration_seconds{name},
// engine_query_duration_seconds{name}, engine_query_failures_total{name,reason},
// engine_process_init_failures_total{name}, engine_process_dead{name},
// engine_process_stale{name}, engine_process_reinit_failures_total{name},
// engine_process_reinit_duration_seconds{name}, engine_watcher_stops_total{reason}.

// Snapshot live state for Prometheus or a debug endpoint:
EngineReport report = engine.introspect().toCompletableFuture().get();
```

### Multi-tenant

```java
var aware = TenantAwareEngine.builder(engine)
    .tenantResolver(TenantResolver.suffixAfter("_"))            // "Stations_PUB1" → tenant "PUB1"
    .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant))
    .build();

aware.query(TenantCaller.of("alice", TenantId.of("PUB1")),
            new GetStationsModel("PUB1"));
```

### Multi-node (Postgres)

```java
// The constructors declare `throws SQLException`.
var backend = new PostgresLogBackend(dataSource, "fom_main");   // pg_advisory_lock = leader
// A second instance opening the same logId fails fast:
// IllegalStateException("Could not acquire advisory lock for … — another instance holds it").
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
| 3 | `FileLogBackend` (CRC + FileLock), `fom-fury`, snapshot rotation | ✅ |
| 4 | Triggers, watchers, reactive cascade, dedup, `fom-kotlin`/`fom-config-hocon` (the Guice/Spring adapters were later dropped: factories are plain `Supplier`s) | ✅ |
| 5 | `EngineObserver`, `introspect()`, `fom-micrometer`, `fom-otel`, `fom-test` | ✅ |
| 6 | `fom-tenant`, `fom-log` CLI, size-based/composite snapshot policies (now in `fom-core`), hot-reload, `fom-jdbc` (Postgres) | ✅ |
| 7 | In-place graph swap via `Engine.newGraph` diff | ✅ |
| — | Re-init keeps the old version serving until the new one serves, and on failure (`ReinitStrategy.KEEP_OLD`, default; `RELEASE_FIRST` per engine or per node); automatic retry with backoff; a restart finishes a persisted replacement in the background; Micrometer/OTel re-init metrics | ✅ |
| — | Graph swap that keeps a *changed* node serving until its new definition is ready (today it is replaced cold) | ❌ not yet |
| — | Runnable [`examples/`](examples/README.md) (11 programs, run end-to-end) | ✅ |
| — | Performance budget test (1000-node start < 5 s, query p99 < 5 ms) — measured on `InMemoryLogBackend`; the file backend pays one fsync per log append, so wide graphs start/stop in seconds there | ✅ in-memory only |
| — | Bilingual docs content (EN+RU) under [`docs/`](docs/index.md), links validated | ✅ |
| — | `mkdocs build` of the docs site | ⏳ content ready; build not yet run |
| — | Maven Central publishing wired (`./gradlew centralBundle`, signing, POMs) | ✅ wiring only |
| — | Released to Maven Central | ❌ needs namespace verification + a release version |
| — | GitHub Actions CI | ❌ not yet |
| — | Native-image smoke test | ❌ not yet |

## License

[Apache License 2.0](LICENSE).
