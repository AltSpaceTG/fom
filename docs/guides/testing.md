# Testing

FOM is built test-first; the same harnesses it uses internally are available to
you. There are three things you'll typically test: your processes, your custom
backends, and your overall graph behaviour.

## Testing with the in-memory backend

The fastest way to exercise a graph end-to-end is `InMemoryLogBackend` with a
"fast" config (tiny timeouts, snapshots disabled):

```java
EngineConfig fast = new EngineConfig(
    Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5),
    Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5),
    Duration.ofMillis(100), Duration.ofMillis(100),
    Duration.ofMillis(10), Duration.ofMillis(100), 1,
    SnapshotPolicy.Disabled.INSTANCE);

try (var backend = new InMemoryLogBackend();
     var engine  = new Engine(fast, backend, new JavaSerializableSerDe(), true)) {
    engine.newGraph(graph);
    Object r = engine.query(new GetGreeting("world")).toCompletableFuture().get();
    assertThat(r).isEqualTo(new Greeting("Hello, world!"));
}
```

To test a **warm restart**, use `LocalFileLogBackend` against a temp path, close
the first engine, then open a second against the same file and assert `init`
didn't run (e.g. via an `EngineObserver` counter, or a side effect counter in
your `init`).

## `fom-test`

The `fom-test` module provides **`InterruptContractTest`** — a reusable abstract
base that asserts a `Process` implementation is interrupt-safe: it drives the
process with a deliberately-expired `Deadline` and checks it honours the budget,
doesn't hang a thread, and releases resources on failure.

```kotlin
dependencies {
    testImplementation("io.fom:fom-test:0.1.0-SNAPSHOT")
}
```

```java
class MyProcessInterruptTest extends InterruptContractTest {
    @Override protected Process newProcess()       { return new MyProcess(...); }
    @Override protected Object  cancellableQuery()  { return new LongQuery(); }
}
```

## Reusing the `LogBackend` contract

If you implement your own [backend](persistence-backends.md), extend the shared
`LogBackendContractTest` — it's published from `fom-core`'s **test fixtures** —
to inherit all the SPI invariants (clock-0, atomic append, single leader,
out-of-range handling, persistence round-trip):

```kotlin
dependencies {
    testImplementation(testFixtures("io.fom:fom-core:0.1.0-SNAPSHOT"))
}
```

```java
class MyBackendTest extends LogBackendContractTest {
    @Override protected LogBackend create() { return new MyBackend(...); }
    @Override protected LogBackend reopen(LogBackend original) { return new MyBackend(...); }
    // add your backend-specific tests alongside the inherited ones
}
```

`PostgresLogBackendTest` does exactly this against a Testcontainers Postgres.

## Tips

- Prefer `InMemoryLogBackend` for unit/behaviour tests — it's deterministic and
  needs no I/O.
- Give tests a fast `EngineConfig` so backoff/timeout paths don't dominate
  wall-clock.
- Use an `EngineObserver` to assert *which* lifecycle events fired (e.g. "load
  ran, init didn't" for warm-restart tests).
- Reserve `fom-jdbc`/Testcontainers tests for backend-specific behaviour; they
  need Docker.
