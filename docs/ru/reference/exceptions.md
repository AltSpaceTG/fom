# Исключения

Все исключения FOM — unchecked (наследники `RuntimeException`). Они делятся на три
группы: те, что **бросаете вы** из кода процесса, сигнализируя о сбое фазы; те, что
**движок бросает обратно** вызывающему; и те, что из помощников
сериализации / типизированных ячеек.

## Бросаются вашим кодом процесса

| Исключение | Пакет | Откуда бросать | Эффект |
|---|---|---|---|
| `InitializationException` | `io.fom.api` | `ProcessInitializer.init` | движок делает backoff и [повторяет](../concepts/process-lifecycle.md) до `defaultInitTimeout` |
| `LoadException` | `io.fom.api` | `ProcessLoader.load` | повторы до `maxLoadRetries`, затем откат к `init` |

Бросание этих (или любого исключения) из `init`/`load` запускает механику
повторов/отката. `compute`, упавший с ошибкой, завершает stage запроса
исключительно вашим исключением.

## Бросаются обратно вызывающему

| Исключение | Пакет | Когда |
|---|---|---|
| `InitializationTimeoutException` | `io.fom.api` | исчерпан общий бюджет `defaultInitTimeout` по всем повторам `init`; процесс теперь `Dead` |
| `InitInProgressException` | `io.fom.api` | запрос пришёл, пока цель ещё в `NotPresent` (init ещё не дал обслуживаемое состояние) |
| `QueryException` | `io.fom.api` | сбой маршрутизации — нет маршрута для типа, резолвер вернул неизвестный/пустой процесс, или запрос к необъявленной зависимости |
| `QueryRejectedException` | `io.fom.api` | цель в `CleaningUp`/`Dead`, или её мейлбокс отклонил сообщение |
| `TenantAccessDeniedException` | `io.fom.tenant` | [обёртка тенантов](../guides/multi-tenancy.md) отказала (не авторизован, или не-`Routable` `query`) |

Они проявляются либо синхронно (например, `QueryException` от плохого маршрута),
либо как сбой возвращённого `CompletionStage`. При ожидании stage через `.get()`
они приходят обёрнутыми в `ExecutionException` — разворачивайте через
`getCause()`:

```java
try {
    engine.query(msg).toCompletableFuture().get();
} catch (ExecutionException e) {
    if (e.getCause() instanceof QueryException qe) { /* плохой маршрут */ }
}
```

## Сериализация и типизированные ячейки

| Исключение | Пакет | Когда |
|---|---|---|
| `SerDeException` | `io.fom.serde` | [`SerDe`](../guides/serialization.md) не смог (де)сериализовать — плохие байты, отклонение фильтром, неожиданный тип |
| `CodecException` | `io.fom` | [`Codec`](../guides/configuration.md#типизированные-ячейки-свойств) не смог закодировать/декодировать значение |
| `NoSuchPropertyException` | `io.fom` | `Properties.get(key)` для отсутствующего в ячейках ключа |

## Прочие стандартные исключения

- **`IllegalArgumentException`** — невалидный граф (цикл, отсутствующая
  зависимость, маршрут на неизвестный процесс), невалидный `EngineConfig`
  (неположительная длительность, `maxLoadRetries < 1`), или плохой SQL-идентификатор
  в `fom-jdbc`.
- **`IllegalStateException`** — использование закрытого `Engine`/бэкенда, второй
  процесс пытается взять блокировку лидерства бэкенда, или DI-суплаер разрешает до
  регистрации контейнера.
- **`IndexOutOfBoundsException`** — `LogBackend.get`/`getBetween` вне
  `[0, length())`.

> [English version](../../reference/exceptions.md)
