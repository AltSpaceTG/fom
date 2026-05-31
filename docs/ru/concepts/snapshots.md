# Снапшоты

Лог растёт с каждым изменением. **Снапшот** компактизирует его: пишет свежий,
минимальный лог, фиксирующий только *текущее* живое состояние, и архивирует
старый. Без снапшотов время воспроизведения и размер на диске/в таблице растут
без ограничений.

## Что содержит снапшот

Снапшот заменяет весь лог на:

1. свежий `LogLeader` (текущий лидер перезаявляется),
2. маркер `LogSnapshot` с `checkpointClock`,
3. текущий `LogChangeGraph` (сериализованный граф) и
4. по одному `LogInitialized` на **живой** (не-отозванный) Sid.

Всё остальное — старые лидеры, перекрытые инициализации, мёртвые Sid'ы, триггеры,
записи смены зависимостей — отбрасывается. Результат воспроизводится в точно то же
состояние движка, но гораздо короче.

## Запуск снапшота

### Вручную

```java
SnapshotResult result = engine.snapshot().toCompletableFuture().get();
// result.logId(), result.archivedLogId(), result.checkpointClock(), result.eventsWritten()
```

`engine.snapshot()` выполняется на common pool (не на планировщике движка),
поэтому никогда не тормозит таймеры дедупа/повторов/watcher'ов.

### Автоматически — `SnapshotPolicy`

Движок читает `EngineConfig.snapshotPolicy()`:

- **`SnapshotPolicy.Disabled.INSTANCE`** (по умолчанию) — без авторотации.
- **`SnapshotPolicy.FixedInterval(interval, keepHistory)`** — снапшот каждые
  `interval`, храня новейшие `keepHistory` архивов (старые удаляются).

См. [Конфигурацию](../guides/configuration.md) для полного конструктора и
[`fom-config-hocon`](../guides/configuration.md#hocon) для вывода `FixedInterval`
из cron-строки Quartz.

## Кастомные политики

`SnapshotPolicy` — открытый интерфейс. Кастомная политика реализует
`activate(SnapshotContext)`, который движок вызывает один раз при установке графа;
политика ведёт собственное расписание и возвращает `AutoCloseable` для остановки.
`SnapshotContext` даёт всё необходимое:

```java
public interface SnapshotContext {
    CompletionStage<SnapshotResult> snapshot();   // снапшот сейчас
    ScheduledExecutorService scheduler();          // планировщик движка
    LogBackend logBackend();                        // активный бэкенд
    void purgeArchives(int keepHistory);
}
```

Модуль `fom-log-maintenance` (пакет `io.fom.maintenance`) поставляет две:

```java
import io.fom.maintenance.*;

// снапшот, когда лог превысит maxEvents; опрос длины каждые pollInterval; хранить N архивов
var bySize = new SizeBasedSnapshotPolicy(/* maxEvents */ 10_000,
                                         /* pollInterval */ Duration.ofMinutes(1),
                                         /* keepHistory */ 7);

// срабатывает, когда срабатывает ЛЮБАЯ из составляющих политик
var both = new CompositeSnapshotPolicy(
    bySize,
    new SnapshotPolicy.FixedInterval(Duration.ofHours(24), 7));
```

- **`SizeBasedSnapshotPolicy(int maxEvents, Duration pollInterval, int keepHistory)`**
  — снапшот, когда лог вырастает за `maxEvents`.
- **`CompositeSnapshotPolicy(SnapshotPolicy... policies)`** — активирует
  несколько; снапшот срабатывает, когда срабатывает любая.

## Архивы

Компактизация архивирует предыдущий лог, а не удаляет:

- **файловый бэкенд** переносит его в соседний файл `…​.archived.<timestamp>`;
- **бэкенд Postgres** переименовывает таблицу в `…_archived_<timestamp>`.

`FixedInterval.keepHistory` (и `SnapshotContext.purgeArchives`) ограничивают число
хранимых архивов.

## Связь с лидерством

Снапшот — это `compact`, то есть запись только для лидера. Фолловер не может делать
снапшот. См. [Лог](the-log.md) и [Несколько узлов](../guides/multi-node.md).

> [English version](../../concepts/snapshots.md)
