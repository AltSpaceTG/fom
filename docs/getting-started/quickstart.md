# Quickstart

This walks through the smallest useful program: one process, one query, one
warm restart.

## 1. Define a process

A process is two interfaces — an **initializer** and a **loader**. They are
often the same class. The initializer computes the persisted byte cells; the
loader builds a live object that answers queries.

```java
import io.fom.api.*;
import io.fom.api.Process; // not java.lang.Process
import java.util.*;
import java.util.concurrent.*;

// A query message and its result.
record GetGreeting(String who) implements java.io.Serializable {}
record Greeting(String text) implements java.io.Serializable {}

final class GreeterInit implements ProcessInitializer, ProcessLoader {

    // init: runs once on cold start; returns the property cells to persist.
    @Override
    public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
        // Pretend this is expensive — it only runs once, then it's in the log.
        byte[] prefix = "Hello".getBytes();
        return CompletableFuture.completedFuture(Map.of("prefix", prefix));
    }

    // load: runs on every start; rebuilds the live Process from the cells.
    @Override
    public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
        String prefix = new String(props.get("prefix"));
        Process live = (c, query) -> {
            var q = (GetGreeting) query;
            return CompletableFuture.completedFuture(new Greeting(prefix + ", " + q.who() + "!"));
        };
        return CompletableFuture.completedFuture(live);
    }
}
```

`io.fom.api.Process` shares its simple name with `java.lang.Process`. Both
`java.lang` and `io.fom.api.*` are on-demand imports, so a bare `Process` is
ambiguous and javac rejects it ("reference to Process is ambiguous"); the
explicit single-type `import io.fom.api.Process;` above wins over both, so it is
required, not cosmetic.

!!! tip "Property cells, typed"
    Hand-rolling `byte[]` is fine for one field, but the
    [`Properties`](../guides/configuration.md) helper with `TypedKey`/`Codec`
    gives you type-safe cells: `Properties.empty().put(KEY, value).asRaw()` in `init`,
    `Properties.of(props).get(KEY)` in `load`, with codecs such as
    `Codecs.stringCodec()` and `Codecs.longCodec()`. Import it explicitly —
    `import io.fom.Properties;` — or the `java.util.*` above makes the simple
    name ambiguous.

    `Properties` is immutable: every `put`/`putRaw` copies the whole map and
    returns a new instance, so chaining `put` for thousands of cells is
    quadratic. For many cells use the builder, which is linear —
    `Properties.builder()` from scratch, or `toBuilder()` to extend existing
    cells:

    ```java
    Properties.Builder b = Properties.builder();      // or Properties.of(props).toBuilder()
    for (var e : readings.entrySet()) b.put(new TypedKey<>(e.getKey(), Codecs.longCodec()), e.getValue());
    Map<String, byte[]> cells = b.build().asRaw();    // the builder is spent after build()
    ```

## 2. Build a graph

```java
import io.fom.*;

Graph graph = new GraphBuilder()
    .add("Greeter", GreeterInit::new, GreeterInit::new)
        .handles(GetGreeting.class)   // route GetGreeting to "Greeter"
    .build();
```

`GraphBuilder.add(name, initFactory, loadFactory, deps...)` registers a node.
The factories are `Supplier`s (here method references) that build a
fresh initializer/loader instance. `.handles(GetGreeting.class)` attaches a
static type route to the most-recently-added node, so `engine.query(...)` knows
where to send a `GetGreeting`.

## 3. Run the engine

```java
import io.fom.log.*;
import io.fom.serde.*;
import java.util.concurrent.TimeUnit;

try (var backend = new InMemoryLogBackend();
     var engine  = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {

    engine.newGraph(graph);     // spawns the FSM, runs init+load, blocks until Serving

    Greeting g = (Greeting) engine.query(new GetGreeting("world"))
        .toCompletableFuture().get(5, TimeUnit.SECONDS);

    System.out.println(g.text());   // Hello, world!
}
```

The three `Engine` constructor arguments are: the
[configuration](../guides/configuration.md), the
[log backend](../guides/persistence-backends.md), and the
[serializer](../guides/serialization.md). An optional fourth one is an
[observer](../guides/observability.md). The engine claims leadership of the log
when `newGraph` installs the first graph.

## 4. Warm restart

Swap the in-memory backend for a file backend and run the program twice:

```java
import java.nio.file.Path;

// FileLogBackend(Path) is the one constructor in the API that throws a
// checked exception, so the enclosing method must declare it.
public static void main(String[] args) throws Exception {
    try (var backend = new FileLogBackend(Path.of("/tmp/greeter.bin"));
         var engine  = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {
        engine.newGraph(graph);
        // ... query ...
    }
}
```

For real workloads use `FurySerDe` (`import io.fom.fury.FurySerDe;`) from the
`io.github.altspacetg:fom-fury:0.1.0-SNAPSHOT` module instead — see
[Serialization](../guides/serialization.md).

- **First run:** the log is empty → `Greeter` runs `init` (writes
  `LogInitialized`) then `load`.
- **Second run:** the engine finds the persisted `LogInitialized` in the file
  and runs **only `load`** — `init` is skipped. That is idempotent restart;
  see [Idempotent restart](../concepts/idempotent-restart.md).

## Where to go next

- Add a dependency between two processes →
  [Your first graph](first-graph.md).
- Understand what just happened →
  [Process lifecycle](../concepts/process-lifecycle.md).
- Make state changes propagate →
  [Reactive cascade](../concepts/reactive-cascade.md).
