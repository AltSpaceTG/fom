# FOM

A JVM library for scheduling **long-lived processes** organised as a directed
acyclic graph of dependencies. Each process initialises its state once
(possibly an expensive computation) and persists it to an append-only log, so
that a JVM restart recovers the state **without recomputing it**.

```java
var graph = new GraphBuilder()
    .add("Inventory", InventoryInit::new, InventoryInit::new)
        .handles(GetInventoryModel.class)
    .add("Products", ProductsInit::new, ProductsInit::new, "Inventory")
        .handles(GetProductModel.class)
    .build();

try (var backend = new LocalFileLogBackend(Path.of("/var/lib/fom/log.bin"));
     var engine = new Engine(EngineConfig.defaults(), backend, new FurySerDe(), true)) {
    engine.newGraph(graph);

    var pm = (ProductModel) engine.query(new GetProductModel("PUB1"))
        .toCompletableFuture().get(5, TimeUnit.SECONDS);
}
```

## What problem does it solve?

Some application state is **expensive to build but cheap to keep**: a search
index, a denormalised read model, a cache warmed from many sources. Rebuilding
it on every deploy or restart is wasteful — sometimes prohibitively so.

FOM models each such piece of state as a **process** with two phases:

- **`init`** — compute the state once and write the result to a log as a set of
  byte "property cells".
- **`load`** — reconstruct a live, queryable object from those persisted cells.

On a cold start a process runs `init` then `load`. On a **warm restart** against
the same log, the engine sees the state was already initialised and runs only
`load` — that is the *idempotent restart* goal. See
[Idempotent restart](concepts/idempotent-restart.md).

Processes form a **graph**: a consumer may depend on producers and query them
while serving. When a producer's state changes, *reactive* consumers
automatically re-initialise in dependency order. See
[Graph & routing](concepts/graph-and-routing.md) and
[Reactive cascade](concepts/reactive-cascade.md).

## Design principles

- **The log is the source of truth.** All durable state changes are appended to
  an [append-only log](concepts/the-log.md); the in-memory engine is a
  projection that can always be rebuilt by replaying it.
- **One writer at a time.** A single *leader* may append; leadership is enforced
  by the backend (a file lock, or a Postgres advisory lock). See
  [Multi-node](guides/multi-node.md).
- **Idiomatic, dependency-light core.** `fom-core` is pure **Java 21 + Project
  Loom** with **zero non-JDK runtime dependencies** except `slf4j-api`. Every
  integration (Kotlin DSL, Fury, Spring, Micrometer, …) is a separate, optional
  module.
- **Actor-like isolation.** Each process is driven by a single virtual-thread
  dispatcher reading a mailbox; user `compute` runs on its own virtual thread,
  so the dispatcher is never blocked. See
  [Process lifecycle](concepts/process-lifecycle.md).

## Where to go next

<div class="grid cards" markdown>

- :material-rocket-launch: **[Getting started](getting-started/installation.md)**
  — add the dependency and run your first graph.
- :material-lightbulb-on: **[Concepts](concepts/index.md)**
  — the mental model: lifecycle, the log, Sids, routing, cascade, snapshots.
- :material-book-open-variant: **[Guides](guides/configuration.md)**
  — task-oriented how-tos for configuration, serialization, observability, multi-node, and more.
- :material-shield-lock: **[Security](security.md)**
  — deserialization hardening, tenant isolation, and a production deployment checklist.

</div>

!!! note "Status"
    FOM is pre-1.0. The on-disk log format and the SPIs are stable within a
    major version; public APIs may still evolve. Read the
    [Security](security.md) page before any multi-tenant or production
    deployment.

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
