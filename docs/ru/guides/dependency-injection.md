# Внедрение зависимостей

Фабрики процессов — это обычные `java.util.function.Supplier`. Движок вызывает их
только в той JVM, которая установила граф, и никогда не пишет их в лог — в логе
хранится лишь имя, зависимости и `param` каждого узла, — поэтому фабрика может
захватывать что угодно: Spring `ApplicationContext`, Guice `Injector`,
`DataSource`. Отдельный модуль-адаптер не нужен.

## Guice

```java
Injector injector = Guice.createInjector(new AppModule());

Graph graph = new GraphBuilder()
    .add("Subscriptions",
         () -> injector.getInstance(SubscriptionsInit.class),   // фабрика init
         () -> injector.getInstance(SubscriptionsInit.class))   // фабрика load
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

## Что стоит учитывать

- **Когда вызывается фабрика.** Фабрики вызываются лениво — на каждый `init` и
  `load`, включая переинициализации. Бины `@Singleton` / singleton-scope дают
  один и тот же экземпляр, prototype — новые. Выбирайте scope в зависимости от
  того, хранит ли ваш инициализатор/загрузчик состояние.
- **Рестарты.** После рестарта JVM приложение строит граф из кода и снова вызывает
  `newGraph`, так что контейнер просто должен существовать к этому вызову —
  никакой статической регистрации нет.
- **`param` — другое дело.** `param` узла *записывается* в лог (через `SerDe`),
  чтобы замечать изменения определения между рестартами, поэтому он должен
  оставаться `Serializable`, неизменяемым и реализовывать `equals`. Живые объекты
  держите в фабрике, а не в `param`.

> [English version](../../guides/dependency-injection.md)
