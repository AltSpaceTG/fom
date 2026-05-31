# Триггеры и watcher'ы

И то и другое — способы заставить процесс **переинициализироваться**, чтобы учесть
изменение во внешнем мире. Триггер — это одноразовый push; watcher — повторяющийся
опрос, который делает push, увидев изменение.

## Триггеры

`engine.trigger(processName, value)` записывает `LogTrigger` и планирует reinit
названного процесса (с учётом [окна дедупа](reactive-cascade.md#окно-дедупликации)).
`value` сериализуется [SerDe](../guides/serialization.md) движка и доступен
процессу во время reinit.

```java
engine.trigger("Inventory", new RefreshSignal("ad-hoc"));
```

Триггер нескольких процессов атомарно одной записью лога:

```java
engine.trigger(Map.of(
    "Inventory", new RefreshSignal("nightly"),
    "Pricing",   new RefreshSignal("nightly")));
```

Переинициализация `Inventory` каскадирует на его реактивных потребителей — см.
[Реактивный каскад](reactive-cascade.md).

!!! note "Значение должно быть `Serializable`"
    `trigger` принимает `Serializable`-значение, чтобы записать его в лог. С
    `JavaSerializableSerDe` оно должно реализовывать `java.io.Serializable`; с
    `FurySerDe` подойдёт любой зарегистрированный/регистрируемый класс.

## Watcher'ы

`ScheduledWatcher<V>` опрашивает внешний источник по фиксированному расписанию.
Когда его функция `check` возвращает значение, движок запускает триггер для
наблюдаемого процесса с этим значением.

```java
import io.fom.ScheduledWatcher;
import java.time.Duration;

AutoCloseable handle = engine.watch(new ScheduledWatcher<>(
    "Inventory",            // процесс для триггера
    Long.class,             // тип наблюдаемого значения
    0L,                     // начальное значение
    Duration.ZERO,          // начальная задержка
    Duration.ofMinutes(1),  // интервал опроса
    prevVersion -> fetchLatestVersion().filter(v -> v > prevVersion),  // check
    null));                 // sentinel (опционально)

// позже, чтобы остановить опрос:
handle.close();
```

`check` получает **предыдущее** значение и возвращает `Optional<V>`:

- **present** → значение изменилось; движок обновляет хранимое значение и
  запускает триггер с ним.
- **empty** → изменения нет; на этом тике ничего не происходит.

`engine.watch(...)` возвращает `AutoCloseable`, отменяющий расписание. Watcher,
чей `check` бросает исключение, логируется и пропускается — один плохой тик не
убивает watcher.

### Поля watcher'а

| Поле | Смысл |
|---|---|
| `processName` | процесс для триггера при изменении |
| `valueType` | `Class<V>` наблюдаемого значения |
| `initialValue` | значение, которое первый `check` видит как «предыдущее» |
| `initialDelay` | задержка перед первым опросом |
| `interval` | время между опросами (> 0) |
| `check` | `Function<V, Optional<V>>` — новое значение для триггера или empty |
| `sentinel` | опциональное маркерное значение |

## Когда что

- **Триггер** — вы уже знаете, что что-то изменилось (webhook, действие
  администратора, событие другой системы).
- **Watcher** — нужно *обнаруживать* изменения опросом (счётчик версии, mtime
  файла, число строк).

> [English version](../../concepts/triggers-and-watchers.md)
