# Лог

Append-only лог — это **источник истины**. Состояние движка в памяти — проекция
лога; воспроизведение лога восстанавливает движок. Всё долговечное — лидерство,
граф, инициализация и отзыв каждого процесса — это событие в логе.

## Типы событий

Все события реализуют sealed-интерфейс `LogEvent` и несут `clock`, `timestamp`
(epoch ms) и `formatVersion`.

| Событие | Когда пишется | Примечания |
|---|---|---|
| `LogLeader` | экземпляр JVM претендует на лидерство | последнее называет текущего лидера |
| `LogChangeGraph` | граф установлен или заменён | содержит байты сериализованного `Graph` |
| `LogInitialized` | процесс завершил `init` | имя процесса + ячейки свойств; определяет [Sid](sid-and-clock.md) |
| `LogLoaded` | процесс завершил `load` и в Serving | ссылается на Sid |
| `LogTrigger` | `engine.trigger(...)` или watcher | содержит сериализованные значения триггера |
| `LogDependencyChanged` | Sid реактивной зависимости изменился | трассировка + дедуп; *расширенное* |
| `LogDead` | Sid отозван (reinit/replace) | потребители должны переключиться на новый Sid |
| `LogCleanedUp` | `cleanUp` завершился (или упал по таймауту) | флаг `ok`; *расширенное* |
| `LogSnapshot` | начинается [снапшот](snapshots.md) | граница ротации; *расширенное* |

**Базовые** события (`LogLeader`, `LogChangeGraph`, `LogInitialized`,
`LogLoaded`, `LogTrigger`, `LogDead`) стабильны навсегда в пределах мажорной
версии. **Расширенные** (`LogDependencyChanged`, `LogCleanedUp`, `LogSnapshot`)
старые читатели могут пропускать с предупреждением.

Точные формы записей — в [справочнике событий лога](../reference/log-events.md).

## Лидерство: один писатель

Добавлять может только **лидер**. Правило обеспечивается `append(event,
leaderInstanceId)`:

- В пустом логе первое добавление должно само быть `LogLeader` — это и есть
  заявка на лидерство.
- Далее добавление успешно, только если последний `LogLeader` в логе имеет
  `instanceId == leaderInstanceId`. Устаревший лидер получает `Optional.empty()`.
- Запись нового `LogLeader` — это **захват**: новый экземпляр становится лидером,
  старый больше не может добавлять.

Вдобавок *бэкенд* защищает от того, чтобы два процесса считали себя лидером:
файловый бэкенд берёт эксклюзивную ОС-блокировку файла; бэкенд Postgres берёт
`pg_advisory_lock`. См. [Несколько узлов](../guides/multi-node.md).

## SPI `LogBackend`

Бэкенд — это любая реализация `io.fom.log.LogBackend`:

```java
public interface LogBackend extends Closeable {
    String logId();
    int length();                                  // clock'и [0, length())
    LogEvent get(int clock);                        // IndexOutOfBoundsException вне диапазона
    LogEvent[] getBetween(int fromClock, int toClock);
    Optional<LogEvent> append(LogEvent event, String leaderInstanceId);
    LogBackendReport introspect();
    SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId);
    void close();
}
```

Гарантии контракта, которые должна соблюдать реализация:

- **Атомарные добавления.** Частичное добавление никогда не видно для
  `get`/`getBetween`.
- **Один лидер.** Как описано выше.
- **Параллельные чтения.** `get`/`getBetween` безопасны, пока `append` выполняется
  в другом потоке.
- **Плотные clock'и с нуля.** Первое событие — clock 0.

Общий `LogBackendContractTest` (в test-fixtures `fom-core`) кодирует эти
инварианты; каждый бэкенд прогоняется против него. См.
[Бэкенды хранения](../guides/persistence-backends.md) и
[Тестирование](../guides/testing.md).

## Компактизация

`compact(snapshotEvents, leader)` атомарно заменяет весь лог новым, более коротким
набором событий (свежий `LogLeader`, маркер `LogSnapshot`, текущий
`LogChangeGraph` и по одному `LogInitialized` на живой Sid), архивируя старый лог.
Так [снапшоты](snapshots.md) держат лог ограниченным.

> [English version](../../concepts/the-log.md)
