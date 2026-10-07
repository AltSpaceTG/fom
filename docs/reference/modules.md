# Modules

FOM is a multi-module build. `fom-core` is the only required dependency;
everything else is optional and additive.

| Module | JPMS name | Depends on | Purpose |
|---|---|---|---|
| **fom-core** | `io.fom.core` | slf4j only | Runtime: `Engine`, `ProcessFSM`, `GraphMachine`, graph/builder, log SPI + `InMemory`/`LocalFile` backends, `JavaSerializableSerDe`, snapshots, triggers/watchers, reactive cascade, in-place graph swap, hot-reload, `EngineObserver` SPI |
| **fom-fury** | `io.fom.fury` | fom-core, Apache Fury | `FurySerDe` — compact, fast, schema-evolution-friendly binary serializer (**recommended for prod**) |
| **fom-config-hocon** | `io.fom.config.hocon` | fom-core, Typesafe Config, cron-utils | Parse `EngineConfig` from HOCON; Quartz cron → `CronSnapshotPolicy` |
| **fom-kotlin** | _(automatic module)_ | fom-core, kotlinx-coroutines | `graph { }` DSL, `SuspendingProcess`, suspend extensions |
| **fom-micrometer** | `io.fom.micrometer` | fom-core, Micrometer | `MicrometerEngineObserver` — counters/timers |
| **fom-otel** | `io.fom.otel` | fom-core, OpenTelemetry | `OtelEngineObserver` — spans for init/load/query |
| **fom-tenant** | `io.fom.tenant` | fom-core | `TenantAwareEngine` — per-tenant authz + lifecycle |
| **fom-log** | _(automatic module)_ | fom-core, picocli | Standalone CLI: `inspect`, `diagnose`, `compact` |
| **fom-jdbc** | `io.fom.jdbc` | fom-core, postgresql | `PostgresLogBackend` — multi-node leader coordination (advisory lock) |
| **fom-test** | `io.fom.test` | fom-core, JUnit 5, AssertJ | `LogBackendContractTest`, `SerDeContractTest`, `InterruptContractTest` — contract tests for your own implementations |

## Dependency direction

```mermaid
graph TD
  core["fom-core"]
  fury["fom-fury"] --> core
  hocon["fom-config-hocon"] --> core
  kotlin["fom-kotlin"] --> core
  mm["fom-micrometer"] --> core
  otel["fom-otel"] --> core
  tenant["fom-tenant"] --> core
  cli["fom-log"] --> core
  jdbc["fom-jdbc"] --> core
  test["fom-test"] --> core
```

Everything points at `fom-core` and nothing else points across — you can adopt
any subset without pulling in serializers, DI, or databases you don't use.

## Key types by package (fom-core)

| Package | Highlights |
|---|---|
| `io.fom` | `Engine`, `EngineConfig`, `Graph`, `GraphBuilder`, `ProcessNode`, `ProcessRef`, `Dependency`, `QueryRoute`, `Sid`, `ScheduledWatcher`, `SnapshotPolicy`, `LogCompaction`, `EngineReport`, `Properties`/`TypedKey`/`Codec`/`Codecs` |
| `io.fom.api` | `Process`, `ProcessInitializer`/`ProcessLoader` (+ `Param…` variants), `QueryableContext`, `ProcessContext`, `Routable`, `EngineObserver`, exceptions |
| `io.fom.log` | `LogBackend`, the `Log*` event records, `InMemoryLogBackend`, `FileLogBackend`, `LogBackendReport` |
| `io.fom.serde` | `SerDe`, `JavaSerializableSerDe`, `ObjectInputFilters` |

In **Java**, `io.fom.api.Process` shares its simple name with `java.lang.Process`, and an
on-demand import never beats the implicit `java.lang` one. `import
io.fom.api.*;` on its own is legal — the ambiguity is only reported where the
simple name `Process` is actually **used**, and then the compile fails with
"reference to Process is ambiguous". Import it explicitly — `import
io.fom.api.Process;` — just as `io.fom.Properties` needs an explicit import next
to `java.util.*`. This is Java-only: in Kotlin, `import io.fom.api.*` compiles
and `Process` resolves to `io.fom.api.Process`, so no explicit import is needed.
