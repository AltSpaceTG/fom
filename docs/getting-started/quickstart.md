# Quickstart

This walks through the smallest useful program: one process, one query, one
warm restart.

## 1. Define a process

A process is two interfaces — an **initializer** and a **loader**. They are
often the same class. The initializer computes the persisted byte cells; the
loader builds a live object that answers queries.

```java
import io.fom.api.*;
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

!!! tip "Property cells, typed"
    Hand-rolling `byte[]` is fine for one field, but the
    [`Properties`](../guides/configuration.md) helper with `TypedKey`/`Codec`
    gives you type-safe cells (`Codecs.STRING`, `Codecs.LONG`, `Codecs.BYTES`).

## 2. Build a graph

```java
import io.fom.*;

Graph graph = new GraphBuilder()
    .add("Greeter", GreeterInit::new, GreeterInit::new)
        .handles(GetGreeting.class)   // route GetGreeting to "Greeter"
    .build();
```

`GraphBuilder.add(name, initFactory, loadFactory, deps...)` registers a node.
The factories are `SerializableSupplier`s (here method references) that build a
fresh initializer/loader instance. `.handles(GetGreeting.class)` attaches a
static type route to the most-recently-added node, so `engine.query(...)` knows
where to send a `GetGreeting`.

## 3. Run the engine

```java
import io.fom.log.*;
import io.fom.serde.*;
import java.util.concurrent.TimeUnit;

try (var backend = new InMemoryLogBackend();
     var engine  = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true)) {

    engine.newGraph(graph);     // spawns the FSM, runs init+load, blocks until Serving

    Greeting g = (Greeting) engine.query(new GetGreeting("world"))
        .toCompletableFuture().get(5, TimeUnit.SECONDS);

    System.out.println(g.text());   // Hello, world!
}
```

The four `Engine` constructor arguments are: the
[configuration](../guides/configuration.md), the
[log backend](../guides/persistence-backends.md), the
[serializer](../guides/serialization.md), and `leaderAtStart` (whether this
instance should claim leadership immediately — `true` for single-node).

## 4. Warm restart

Swap the in-memory backend for a file backend and run the program twice:

```java
try (var backend = new LocalFileLogBackend(Path.of("/tmp/greeter.bin"));
     var engine  = new Engine(EngineConfig.defaults(), backend, new FurySerDe(), true)) {
    engine.newGraph(graph);
    // ... query ...
}
```

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
