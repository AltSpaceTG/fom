# Тестирование

FOM построен test-first; те же харнессы, что он использует внутри, доступны вам.
Обычно тестируют три вещи: ваши процессы, ваши кастомные бэкенды и поведение графа
в целом.

## Тестирование с in-memory бэкендом

Самый быстрый способ прогнать граф end-to-end — `InMemoryLogBackend` с «быстрым»
конфигом (крошечные таймауты, снапшоты выключены):

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

Чтобы протестировать **тёплый рестарт**, используйте `LocalFileLogBackend` против
временного пути, закройте первый движок, откройте второй против того же файла и
проверьте, что `init` не выполнялся (например, счётчиком в `EngineObserver` или
счётчиком побочного эффекта в вашем `init`).

## `fom-test`

Модуль `fom-test` предоставляет **`InterruptContractTest`** — переиспользуемый
абстрактный базовый класс, проверяющий, что реализация `Process` устойчива к
прерыванию: он прогоняет процесс с заведомо истёкшим `Deadline` и проверяет, что
тот соблюдает бюджет, не подвешивает поток и освобождает ресурсы при сбое.

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

## Переиспользование контракта `LogBackend`

Если вы реализуете свой [бэкенд](persistence-backends.md), унаследуйте общий
`LogBackendContractTest` — он публикуется из **test-fixtures** `fom-core` — чтобы
получить все инварианты SPI (clock-0, атомарный append, один лидер, обработка
выхода за диапазон, round-trip персистентности):

```kotlin
dependencies {
    testImplementation(testFixtures("io.fom:fom-core:0.1.0-SNAPSHOT"))
}
```

```java
class MyBackendTest extends LogBackendContractTest {
    @Override protected LogBackend create() { return new MyBackend(...); }
    @Override protected LogBackend reopen(LogBackend original) { return new MyBackend(...); }
    // добавьте тесты, специфичные для вашего бэкенда, рядом с унаследованными
}
```

`PostgresLogBackendTest` делает именно это против Postgres в Testcontainers.

## Советы

- Предпочитайте `InMemoryLogBackend` для unit/поведенческих тестов — он
  детерминирован и не требует I/O.
- Давайте тестам быстрый `EngineConfig`, чтобы пути backoff/таймаутов не
  доминировали по времени.
- Используйте `EngineObserver`, чтобы утверждать, *какие* события жизненного цикла
  сработали (например, «load выполнился, init — нет» для тестов тёплого рестарта).
- Оставьте тесты `fom-jdbc`/Testcontainers для поведения, специфичного для
  бэкенда; им нужен Docker.

> [English version](../../guides/testing.md)
