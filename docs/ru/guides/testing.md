# Тестирование

FOM построен test-first; те же харнессы, что он использует внутри, доступны вам.
Обычно тестируют три вещи: ваши процессы, ваши кастомные бэкенды и поведение графа
в целом.

## Тестирование с in-memory бэкендом

Самый быстрый способ прогнать граф end-to-end — `InMemoryLogBackend` с «быстрым»
конфигом (крошечные таймауты, снапшоты выключены):

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

Чтобы протестировать **тёплый рестарт**, используйте `FileLogBackend` против
временного пути, закройте первый движок, откройте второй против того же файла и
проверьте, что `init` не выполнялся (например, счётчиком в `EngineObserver` или
счётчиком побочного эффекта в вашем `init`).

## `fom-test` — контрактные тесты

Модуль `fom-test` (пакет `io.fom.test`) публикует контрактные тесты, которые fom
прогоняет против своих реализаций, — чтобы ваши реализации проходили те же самые.
Каждый — абстрактный класс JUnit 5: унаследуйте его, передайте реализацию, и
унаследованные тесты запустятся в вашем наборе.

```kotlin
dependencies {
    testImplementation("io.github.altspacetg:fom-test:0.1.0-SNAPSHOT")
    // fom-test приносит JUnit API; движок и launcher подключаете вы:
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test { useJUnitPlatform() }
```

| Контракт | Вы реализуете | Что проверяет |
|---|---|---|
| `LogBackendContractTest` | [`LogBackend`](persistence-backends.md) | clock'и начинаются с 0 и растут на единицу с каждым добавлением, правила лидерства и захват, обработку диапазонов, поведение закрытого бэкенда, round-trip персистентности; прерванный вызывающий поток не ломает лог; компакция сохраняет переданные clock'и, маркер `LogSnapshot` и лидера, добавления продолжают отсчёт после наибольшего clock'а — в том числе за пределами `Integer.MAX_VALUE` и после переоткрытия (clock'и имеют тип `long`; бэкенд, сужающий их до `int`, тест не пройдёт), а clock'и, которые не строго возрастают, или план, который не начинается с `LogLeader` текущего лидера, отклоняются (лог при этом не меняется); `purgeArchives` никогда не трогает живой лог и отклоняет отрицательный `keepHistory` |
| `SerDeContractTest` | [`SerDe`](serialization.md) | round-trip param в значение, равное по `equals`, — записи, вложенные записи и типы JDK, [переопределяемые](#свой-serde). Только param: значения триггеров и свойства процесса через `SerDe` не проходят и не проверяются |
| `InterruptContractTest` | `Process` | один тест, `honours_expired_deadline`: `compute(ctx, cancellableQuery())` с контекстом, у которого `currentQueryDeadline()` уже истёк, должен завершиться — нормально, с ошибкой или отменой — за `graceMillis()` + 500 мс (`graceMillis()` по умолчанию 200, его можно переопределить). У контекста нет зависимостей (`ctx.query` падает). Утечки потоков и освобождение ресурсов не проверяются |

### Свой бэкенд

```java
import io.fom.test.LogBackendContractTest;

class MyBackendTest extends LogBackendContractTest {
    @Override protected LogBackend create() { return new MyBackend(...); }
    @Override protected LogBackend reopen(LogBackend original) { return new MyBackend(...); }
    // добавьте тесты, специфичные для вашего бэкенда, рядом с унаследованными
}
```

`reopen` снова открывает то же хранилище для round-trip персистентности; для
бэкенда без персистентности верните `null`. Так устроены `InMemoryLogBackendTest`,
`FileLogBackendTest` и `PostgresLogBackendTest`.

Два требования, на которых спотыкаются новые бэкенды:

- **Закрытый бэкенд бросает `IllegalStateException`.**
  `close_then_operations_throw` закрывает бэкенд и ждёт, что следующий
  `append` упадёт с `IllegalStateException`, — не с `IOException`, обёрнутым во
  что-то ещё, и не молча ничего не сделает. Проверяйте флаг `closed` в начале
  каждой операции.
- **`forEachBetween` — путь сканирования.** Движок читает лог целиком через
  `LogBackend.forEachBetween` (старт, снапшоты, `fom-log compact`). Унаследованный
  метод по умолчанию читает пачки `getBetween` по 1 000 событий, так что бэкенд
  работает и без него; переопределите его, если 1 000 декодированных записей
  разом — слишком много кучи для ваших записей (файловый бэкенд декодирует по
  одной, Postgres ограничивает каждое окно 8 МиБ payload). См.
  [контракт SPI](../concepts/the-log.md#spi-logbackend).

### Свой SerDe

```java
import io.fom.test.SerDeContractTest;

class MySerDeTest extends SerDeContractTest {
    @Override protected SerDe createSerDe() { return new MySerDe(); }
}
```

По умолчанию образцы param — собственные записи `fom-test`
(`SerDeContractTest.ContractParam`, `NestedParam`) плюс `Integer`, `Long`,
`String` и `ArrayList`. Рефлексивный `SerDe` (Java-сериализация, Fury) принимает
их как есть. Схемному или основанному на регистрации (Protobuf, Avro, реестр
классов) не нужно держать адаптеры для типов `io.fom.test`: переопределите
`sampleParams()` — и `jdkParams()`, если он принимает только свои типы, — своими
param из вашей модели:

```java
class MyProtoSerDeTest extends SerDeContractTest {
    @Override protected SerDe createSerDe() { return new MyProtoSerDe(); }
    @Override protected List<Serializable> sampleParams() {
        return List.of(new SubscriptionParams("EU", 3), new TenantParams("t1", Map.of("k", "v")));
    }
    @Override protected List<Serializable> jdkParams() { return List.of(); }
}
```

### Процесс, устойчивый к прерыванию

```java
import io.fom.test.InterruptContractTest;

class MyProcessInterruptTest extends InterruptContractTest {
    @Override protected Process newProcess()       { return new MyProcess(...); }
    @Override protected Object  cancellableQuery()  { return new LongQuery(); }
}
```

## Советы

- Предпочитайте `InMemoryLogBackend` для unit/поведенческих тестов — он
  детерминирован и не требует I/O. Добавление — амортизированно O(1), а чтение
  диапазона копирует только запрошенный диапазон (прежние версии копировали на
  каждом весь лог), так что длинные или широкие тестовые прогоны остаются
  быстрыми.
- Давайте тестам быстрый `EngineConfig`, чтобы пути backoff/таймаутов не
  доминировали по времени.
- Используйте `EngineObserver`, чтобы утверждать, *какие* события жизненного цикла
  сработали (например, «load выполнился, init — нет» для тестов тёплого рестарта).
- Оставьте тесты `fom-jdbc`/Testcontainers для поведения, специфичного для
  бэкенда; им нужен Docker.

> [English version](../../guides/testing.md)
