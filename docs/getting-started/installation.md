# Installation

FOM is a multi-module library. Always depend on **`fom-core`**; add the optional
modules you need.

!!! warning "Not yet on Maven Central"
    Coordinates below use the `io.fom` group id and the current development
    version `0.1.0-SNAPSHOT`. Until artifacts are published you consume the
    modules from a local build (`./gradlew publishToMavenLocal`) or via a
    composite/included build.

## Requirements

- **JDK 21+** at runtime (the core uses virtual threads and pattern matching).
- The Gradle build targets a **JDK 21 toolchain** and is exercised on newer JDKs
  via the foojay toolchain resolver.
- `fom-jdbc` integration tests require a working **Docker** daemon (Testcontainers).

## Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("io.fom:fom-core:0.1.0-SNAPSHOT")

    // Recommended production serializer:
    implementation("io.fom:fom-fury:0.1.0-SNAPSHOT")

    // Pick any of the optional modules:
    implementation("io.fom:fom-kotlin:0.1.0-SNAPSHOT")        // Kotlin DSL + coroutines
    implementation("io.fom:fom-config-hocon:0.1.0-SNAPSHOT")  // HOCON config parsing
    implementation("io.fom:fom-micrometer:0.1.0-SNAPSHOT")    // metrics
    implementation("io.fom:fom-otel:0.1.0-SNAPSHOT")          // tracing
    implementation("io.fom:fom-jdbc:0.1.0-SNAPSHOT")          // Postgres backend
}
```

## Maven

```xml
<dependency>
  <groupId>io.fom</groupId>
  <artifactId>fom-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.fom</groupId>
  <artifactId>fom-fury</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## The module map

| Module | Add it when you need… |
|---|---|
| `fom-core` | always — the runtime, FSM, graph, log SPI, in-memory + local-file backends, Java SerDe |
| `fom-fury` | a compact, fast, schema-evolution-friendly binary serializer (recommended for prod) |
| `fom-config-hocon` | to build `EngineConfig` from a HOCON file |
| `fom-kotlin` | the `graph { … }` Kotlin DSL and `suspend` bridges |
| `fom-guice` / `fom-spring` | to resolve process factories from a DI container |
| `fom-micrometer` / `fom-otel` | metrics / distributed tracing |
| `fom-jdbc` | a Postgres-backed log for multi-node leader coordination |
| `fom-tenant` | a per-tenant authorization wrapper over the engine |
| `fom-log` | a standalone CLI to inspect/diagnose log files |
| `fom-log-maintenance` | size-based and composite snapshot policies |
| `fom-test` | reusable test contracts and a process test kit |

See [Modules reference](../reference/modules.md) for the full catalogue.

## Java Platform Module System

Every module ships a `module-info.java`. `fom-core` is `module io.fom.core` and
exports `io.fom`, `io.fom.api`, `io.fom.log`, and `io.fom.serde`. If your
application is modular, `requires io.fom.core;` (and the integration modules you
use).
