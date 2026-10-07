# Testing

FOM is built test-first; the same harnesses it uses internally are available to
you. There are three things you'll typically test: your processes, your custom
backends, and your overall graph behaviour.

## Testing with the in-memory backend

The fastest way to exercise a graph end-to-end is `InMemoryLogBackend` with a
"fast" config (tiny timeouts, snapshots disabled):

```java
EngineConfig fast = new EngineConfig(
    Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(100),
    Duration.ofMillis(10), Duration.ofMillis(100), 1,
    SnapshotPolicy.Disabled.INSTANCE);

try (var backend = new InMemoryLogBackend();
     var engine  = new Engine(fast, backend, new JavaSerializableSerDe())) {
    engine.newGraph(graph);
    Object r = engine.query(new GetGreeting("world")).toCompletableFuture().get();
    assertThat(r).isEqualTo(new Greeting("Hello, world!"));
}
```

To test a **warm restart**, use `FileLogBackend` against a temp path, close
the first engine, then open a second against the same file and assert `init`
didn't run (e.g. via an `EngineObserver` counter, or a side effect counter in
your `init`).

## `fom-test` — contract tests

The `fom-test` module (package `io.fom.test`) publishes the contract tests fom
runs against its own implementations, so yours can run the same ones. Each is an
abstract JUnit 5 class: extend it, supply the implementation, and the inherited
tests run as part of your suite.

```kotlin
dependencies {
    testImplementation("io.github.altspacetg:fom-test:0.1.0-SNAPSHOT")
    // fom-test brings the JUnit API; the engine and launcher are yours to pick:
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test { useJUnitPlatform() }
```

| Contract | You implement | It checks |
|---|---|---|
| `LogBackendContractTest` | a [`LogBackend`](persistence-backends.md) | clocks start at 0 and increase by one per append, leader rules and takeover, range handling, closed-backend behaviour, persistence round-trip; an interrupted caller doesn't break the log; compaction keeps the supplied clocks, the `LogSnapshot` marker and the leader, appends continue after the highest clock — also past `Integer.MAX_VALUE` and after a reopen (clocks are `long`; a backend that narrows them to `int` fails) — and it refuses clocks that don't strictly increase or a plan not led by the leader's `LogLeader` (leaving the log intact); `purgeArchives` never touches the live log and rejects a negative `keepHistory` |
| `SerDeContractTest` | a [`SerDe`](serialization.md) | params round-trip to an `equals` value — records, nested records and JDK types, [overridable](#a-custom-serde). Params only: a `SerDe` never sees trigger values or process properties, so neither is checked |
| `InterruptContractTest` | a `Process` | one test, `honours_expired_deadline`: `compute(ctx, cancellableQuery())` with a context whose `currentQueryDeadline()` has already expired must finish — normally, exceptionally or cancelled — within `graceMillis()` + 500 ms (`graceMillis()` defaults to 200 and can be overridden). The context has no dependencies (`ctx.query` fails). Thread leaks and resource cleanup are not checked |

### A custom backend

```java
import io.fom.test.LogBackendContractTest;

class MyBackendTest extends LogBackendContractTest {
    @Override protected LogBackend create() { return new MyBackend(...); }
    @Override protected LogBackend reopen(LogBackend original) { return new MyBackend(...); }
    // add your backend-specific tests alongside the inherited ones
}
```

`reopen` opens the same storage again for the persistence round-trip; return
`null` for a backend that doesn't persist. `InMemoryLogBackendTest`,
`FileLogBackendTest` and `PostgresLogBackendTest` do exactly this.

Two requirements that catch new backends out:

- **A closed backend throws `IllegalStateException`.**
  `close_then_operations_throw` closes the backend and expects the next
  `append` to fail with `IllegalStateException` — not an `IOException`
  wrapped in something else, and not a silent no-op. Check a `closed` flag at
  the top of every operation.
- **`forEachBetween` is the scan path.** The engine reads whole logs through
  `LogBackend.forEachBetween` (startup, snapshots, `fom-log compact`). The
  inherited default reads `getBetween` batches of 1,000 events, so a backend
  works without it; override it when 1,000 decoded records at once is too much
  heap for your records (the file backend decodes one at a time, Postgres caps
  each window at 8 MiB of payload). See
  [the SPI contract](../concepts/the-log.md#the-logbackend-spi).

### A custom SerDe

```java
import io.fom.test.SerDeContractTest;

class MySerDeTest extends SerDeContractTest {
    @Override protected SerDe createSerDe() { return new MySerDe(); }
}
```

The default sample params are `fom-test`'s own records
(`SerDeContractTest.ContractParam`, `NestedParam`) plus `Integer`, `Long`,
`String` and `ArrayList`. A reflective `SerDe` (Java serialization, Fury) takes
them as is. A schema- or registration-based one (Protobuf, Avro, a class
registry) should not have to carry adapters for `io.fom.test` types: override
`sampleParams()` — and `jdkParams()` if it only accepts its own types — with
params from your model:

```java
class MyProtoSerDeTest extends SerDeContractTest {
    @Override protected SerDe createSerDe() { return new MyProtoSerDe(); }
    @Override protected List<Serializable> sampleParams() {
        return List.of(new SubscriptionParams("EU", 3), new TenantParams("t1", Map.of("k", "v")));
    }
    @Override protected List<Serializable> jdkParams() { return List.of(); }
}
```

### An interrupt-safe process

```java
import io.fom.test.InterruptContractTest;

class MyProcessInterruptTest extends InterruptContractTest {
    @Override protected Process newProcess()       { return new MyProcess(...); }
    @Override protected Object  cancellableQuery()  { return new LongQuery(); }
}
```

## Tips

- Prefer `InMemoryLogBackend` for unit/behaviour tests — it's deterministic and
  needs no I/O. Appends are amortised O(1) and a range read copies only the
  requested range (earlier versions copied the whole log on each), so long
  or wide test runs stay fast.
- Give tests a fast `EngineConfig` so backoff/timeout paths don't dominate
  wall-clock.
- Use an `EngineObserver` to assert *which* lifecycle events fired (e.g. "load
  ran, init didn't" for warm-restart tests).
- Reserve `fom-jdbc`/Testcontainers tests for backend-specific behaviour; they
  need Docker.
