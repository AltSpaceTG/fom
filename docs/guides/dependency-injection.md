# Dependency injection

Process factories are `SerializableSupplier`s — they're serialized into the
graph in the log, so they can't capture live objects like a Spring bean or a
Guice-bound service directly. The `fom-guice` and `fom-spring` modules solve
this: each gives you a **serializable** supplier that closes over only the
target `Class` and resolves the instance from a **statically-registered
container** at call time.

## Guice

```java
import io.fom.guice.GuiceFactories;

// 1. Register your injector once at startup, before installing the graph:
GuiceFactories.setInjector(injector);

// 2. Use serializable suppliers that resolve from it:
new GraphBuilder()
    .add("Orders",
         GuiceFactories.bound(OrdersInit.class),   // init factory
         GuiceFactories.bound(OrdersInit.class))   // load factory
    .build();
```

| Method | Purpose |
|---|---|
| `setInjector(Injector)` | register the injector (call once at startup) |
| `requireInjector()` | the registered injector, or `IllegalStateException` if unset |
| `bound(Class<T>)` | a `SerializableSupplier<T>` resolving `T` from the injector at `get()` time |

`bound(...)` calls `injector.getInstance(type)` on every `get()`, so
`@Singleton` bindings yield the same instance and unscoped bindings yield fresh
ones. If a supplier runs before `setInjector(...)`, it throws a descriptive
`IllegalStateException`.

## Spring

Symmetric API in `fom-spring`:

```java
import io.fom.spring.SpringFactories;

SpringFactories.setContext(applicationContext);

new GraphBuilder()
    .add("Orders",
         SpringFactories.bean(OrdersInit.class),
         SpringFactories.bean(OrdersInit.class))
    .build();
```

| Method | Purpose |
|---|---|
| `setContext(ApplicationContext)` | register the context (call once at startup) |
| `requireContext()` | the registered context, or `IllegalStateException` if unset |
| `bean(Class<T>)` | a `SerializableSupplier<T>` resolving a bean of type `T` via `getBean(type)` |

## Lifecycle ordering

Because the supplier resolves lazily, the container **must be registered before
the supplier runs** — i.e. before `engine.newGraph(graph)` on first start *and*
before recovery on every JVM restart. Do the `setInjector(...)` /
`setContext(...)` call as part of your bootstrap, ahead of constructing and
starting the engine.

!!! note "One container per JVM"
    Both registries hold a single container in a static reference. If you need
    several, resolve through one root container (a parent context / a single
    injector with child modules) rather than juggling multiple registrations.
