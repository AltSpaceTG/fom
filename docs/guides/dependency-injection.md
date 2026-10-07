# Dependency injection

Process factories are plain `java.util.function.Supplier`s. The engine only calls
them in the JVM that installed the graph and never writes them to the log — the
log records just each node's name, dependencies and `param` — so a factory may
capture anything: a Spring `ApplicationContext`, a Guice `Injector`, a
`DataSource`. No adapter module is needed.

## Guice

```java
Injector injector = Guice.createInjector(new AppModule());

Graph graph = new GraphBuilder()
    .add("Subscriptions",
         () -> injector.getInstance(SubscriptionsInit.class),   // init factory
         () -> injector.getInstance(SubscriptionsInit.class))   // load factory
    .build();

engine.newGraph(graph);
```

## Spring

```java
@Bean
Graph processGraph(ApplicationContext context) {
    return new GraphBuilder()
        .add("Subscriptions",
             () -> context.getBean(SubscriptionsInit.class),
             () -> context.getBean(SubscriptionsInit.class))
        .build();
}
```

## Things to keep in mind

- **When the factory runs.** Factories are called lazily — on every `init` and
  `load`, including re-initialisations. `@Singleton` / singleton-scoped beans
  yield the same instance each time; prototype scopes yield fresh ones. Pick
  the scope that matches whether your initializer/loader keeps state.
- **Restarts.** After a JVM restart the application builds the graph from code
  and calls `newGraph` again, so the container just has to exist before that
  call — there is no static registration step.
- **`param` is different.** A node's `param` *is* recorded (through the
  `SerDe`) to detect definition changes across restarts, so it must stay
  `Serializable`, immutable and implement `equals`. Keep live objects in the
  factory, not in the `param`.
