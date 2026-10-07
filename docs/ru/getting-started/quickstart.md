# Быстрый старт

Минимальная полезная программа: один процесс, один запрос, один тёплый рестарт.

## 1. Опишите процесс

Процесс — это два интерфейса: **инициализатор** и **загрузчик**. Часто это один и
тот же класс. Инициализатор вычисляет сохраняемые байтовые ячейки; загрузчик
строит живой объект, отвечающий на запросы.

```java
import io.fom.api.*;
import io.fom.api.Process; // не java.lang.Process
import java.util.*;
import java.util.concurrent.*;

// Сообщение-запрос и его результат.
record GetGreeting(String who) implements java.io.Serializable {}
record Greeting(String text) implements java.io.Serializable {}

final class GreeterInit implements ProcessInitializer, ProcessLoader {

    // init: выполняется один раз при холодном старте; возвращает ячейки для сохранения.
    @Override
    public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
        // Представим, что это дорого — выполнится один раз, потом лежит в логе.
        byte[] prefix = "Hello".getBytes();
        return CompletableFuture.completedFuture(Map.of("prefix", prefix));
    }

    // load: выполняется при каждом старте; пересобирает живой Process из ячеек.
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

`io.fom.api.Process` совпадает по простому имени с `java.lang.Process`. И
`java.lang`, и `io.fom.api.*` — импорты по требованию, поэтому голое `Process`
неоднозначно, и javac его отвергает («reference to Process is ambiguous»);
явный одиночный `import io.fom.api.Process;` выше приоритетнее обоих, поэтому
он обязателен, а не украшение.

!!! tip "Типизированные ячейки свойств"
    Ручной `byte[]` подходит для одного поля, но помощник
    [`Properties`](../guides/configuration.md) с `TypedKey`/`Codec` даёт
    типобезопасные ячейки: `Properties.empty().put(KEY, value).asRaw()` в `init`,
    `Properties.of(props).get(KEY)` в `load`, с кодеками вроде
    `Codecs.stringCodec()` и `Codecs.longCodec()`. Импортируйте его явно —
    `import io.fom.Properties;` — иначе из-за `java.util.*` выше простое имя
    станет неоднозначным.

    `Properties` неизменяем: каждый `put`/`putRaw` копирует всю карту и
    возвращает новый экземпляр, поэтому цепочка `put` на тысячи ячеек
    квадратична. Для множества ячеек используйте билдер — он линеен:
    `Properties.builder()` с нуля или `toBuilder()`, чтобы дополнить
    существующие ячейки:

    ```java
    Properties.Builder b = Properties.builder();      // или Properties.of(props).toBuilder()
    for (var e : readings.entrySet()) b.put(new TypedKey<>(e.getKey(), Codecs.longCodec()), e.getValue());
    Map<String, byte[]> cells = b.build().asRaw();    // после build() билдер использовать нельзя
    ```

## 2. Постройте граф

```java
import io.fom.*;

Graph graph = new GraphBuilder()
    .add("Greeter", GreeterInit::new, GreeterInit::new)
        .handles(GetGreeting.class)   // маршрутизировать GetGreeting в "Greeter"
    .build();
```

`GraphBuilder.add(name, initFactory, loadFactory, deps...)` регистрирует узел.
Фабрики — это `Supplier`'ы (здесь — ссылки на методы), создающие
свежий экземпляр инициализатора/загрузчика. `.handles(GetGreeting.class)`
прикрепляет статический маршрут по типу к последнему добавленному узлу.

## 3. Запустите движок

```java
import io.fom.log.*;
import io.fom.serde.*;
import java.util.concurrent.TimeUnit;

try (var backend = new InMemoryLogBackend();
     var engine  = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {

    engine.newGraph(graph);     // создаёт FSM, выполняет init+load, ждёт Serving

    Greeting g = (Greeting) engine.query(new GetGreeting("world"))
        .toCompletableFuture().get(5, TimeUnit.SECONDS);

    System.out.println(g.text());   // Hello, world!
}
```

Три аргумента конструктора `Engine`: [конфигурация](../guides/configuration.md),
[бэкенд лога](../guides/persistence-backends.md) и
[сериализатор](../guides/serialization.md). Необязательный четвёртый —
[наблюдатель](../guides/observability.md). Движок заявляет лидерство в логе, когда
`newGraph` устанавливает первый граф.

## 4. Тёплый рестарт

Замените in-memory бэкенд на файловый и запустите программу дважды:

```java
import java.nio.file.Path;

// FileLogBackend(Path) — единственный конструктор в API с проверяемым
// исключением, поэтому объемлющий метод обязан его объявить.
public static void main(String[] args) throws Exception {
    try (var backend = new FileLogBackend(Path.of("/tmp/greeter.bin"));
         var engine  = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {
        engine.newGraph(graph);
        // ... запрос ...
    }
}
```

Для реальных нагрузок используйте вместо него `FurySerDe`
(`import io.fom.fury.FurySerDe;`) из модуля
`io.github.altspacetg:fom-fury:0.1.0-SNAPSHOT` — см.
[Сериализацию](../guides/serialization.md).

- **Первый запуск:** лог пуст → `Greeter` выполняет `init` (пишет
  `LogInitialized`), затем `load`.
- **Второй запуск:** движок находит сохранённый `LogInitialized` и выполняет
  **только `load`** — `init` пропускается. Это и есть идемпотентный рестарт; см.
  [Идемпотентный рестарт](../concepts/idempotent-restart.md).

## Куда дальше

- Добавить зависимость между двумя процессами →
  [Ваш первый граф](first-graph.md).
- Понять, что произошло → [Жизненный цикл процесса](../concepts/process-lifecycle.md).
- Сделать так, чтобы изменения распространялись →
  [Реактивный каскад](../concepts/reactive-cascade.md).

> [English version](../../getting-started/quickstart.md)
