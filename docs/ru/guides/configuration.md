# Конфигурация

Движок конфигурируется неизменяемым record'ом `EngineConfig`. Постройте его через
`EngineConfig.defaults()` или канонический конструктор, распарсите из
[HOCON](#hocon) и [горячо перезагрузите](#hot-reload) во время выполнения.

## Поля

| Поле | По умолчанию | Смысл |
|---|---|---|
| `defaultInitTimeout` | 30 с | общий бюджет `init` узла на все повторы; также ограничивает каждую попытку |
| `defaultLoadTimeout` | 30 с | бюджет на одну попытку `load` |
| `defaultCleanupTimeout` | 30 с | бюджет на `cleanUp` (и слив воркеров при остановке) |
| `defaultComputeTimeout` | 60 мин | бюджет на `compute` запроса по умолчанию |
| `defaultCancelInitTimeout` | 60 с | бюджет на административный `cancelInit` |
| `queryTimeout` | 10 с | дедлайн `engine.query(...)` по умолчанию |
| `logLookupInterval` | 100 мс | интервал опроса лога (учёт фолловера) |
| `dedupWindow` | 100 мс | [окно дебаунса reinit](../concepts/reactive-cascade.md#окно-дедупликации) |
| `backoffMin` | 50 мс | минимальный backoff повторов init/load |
| `backoffMax` | 5 мин | максимальный backoff повторов init/load |
| `maxLoadRetries` | 1 | попыток `load` до отката к `init` |
| `snapshotPolicy` | `Disabled` | автоматическая [ротация лога](../concepts/snapshots.md) |

Порядок полей выше — это в точности порядок канонического конструктора record'а.
Все длительности должны быть **строго положительными** (включая `dedupWindow`),
`backoffMax >= backoffMin`, `maxLoadRetries >= 1`. Конструктор это валидирует и
бросает `IllegalArgumentException`.

## Программно

```java
import io.fom.*;
import java.time.Duration;

// Самый простой старт:
EngineConfig defaults = EngineConfig.defaults();

// …или явно все поля (порядок канонического конструктора):
EngineConfig cfg = new EngineConfig(
    Duration.ofSeconds(30),   // defaultInitTimeout
    Duration.ofSeconds(30),   // defaultLoadTimeout
    Duration.ofSeconds(30),   // defaultCleanupTimeout
    Duration.ofMinutes(60),   // defaultComputeTimeout
    Duration.ofSeconds(60),   // defaultCancelInitTimeout
    Duration.ofSeconds(10),   // queryTimeout
    Duration.ofMillis(100),   // logLookupInterval
    Duration.ofMillis(100),   // dedupWindow
    Duration.ofMillis(50),    // backoffMin
    Duration.ofMinutes(5),    // backoffMax
    1,                        // maxLoadRetries
    SnapshotPolicy.Disabled.INSTANCE);
```

Поскольку это record, выведите вариант через канонические аксессоры:

```java
var withRotation = new EngineConfig(
    cfg.defaultInitTimeout(), cfg.defaultLoadTimeout(), cfg.defaultCleanupTimeout(),
    cfg.defaultComputeTimeout(), cfg.defaultCancelInitTimeout(), cfg.queryTimeout(),
    cfg.logLookupInterval(), cfg.dedupWindow(), cfg.backoffMin(), cfg.backoffMax(),
    cfg.maxLoadRetries(),
    new SnapshotPolicy.FixedInterval(Duration.ofHours(6), 7));
```

## HOCON { #hocon }

Модуль `fom-config-hocon` парсит `EngineConfig` из Typesafe `Config`:

```java
import io.fom.config.EngineConfigHocon;

EngineConfig cfg = EngineConfigHocon.parse();              // ConfigFactory.load()
EngineConfig cfg2 = EngineConfigHocon.parse(myConfig);     // из собственного Config
```

Он читает `engine.graph.default.*` и `engine.system.*`. Базовый набор (любой путь
можно переопределить):

```hocon
engine {
  graph.default {
    init    { timeout = 30s, min-backoff = 50ms, max-backoff = 5m }
    load    { timeout = 30s, max-retries = 1 }
    cleanup.timeout = 30s
    compute.timeout = 60m
    cancel-init.timeout = 60s
  }
  system {
    query-timeout = 10s
    log.look-up-interval = 100ms
    dedup-window = 100ms
    # опциональная ротация:
    log.rotate { cron = "0 0 */6 * * ?", keep-history = 7 }   # cron Quartz из 6 полей
  }
}
```

`log.rotate.cron` — выражение **Quartz из 6 полей**
(`sec min hour day-of-month month day-of-week`). Оно превращается в
`SnapshotPolicy.FixedInterval` вычислением следующего срабатывания от «сейчас».
Значение `never` (или пусто) оставляет ротацию `Disabled`.

!!! warning "cron из недоверенного ввода"
    Cron-строка парсится `cron-utils`. Не подавайте cron из недоверенных
    источников без валидации. См. [Безопасность](../security.md).

## Горячая перезагрузка { #hot-reload }

Сменить живой конфиг во время выполнения:

```java
engine.updateConfig(newConfig);
```

Операции «в полёте» сохраняют захваченные значения; последующие читают новый
конфиг. Если изменился `snapshotPolicy`, запланированная ротация отменяется и
переустанавливается под новую политику. `updateConfig` сериализуется относительно
`newGraph` control-lock'ом движка.

## Типизированные ячейки свойств

Не связано с `EngineConfig`, но полезно: внутри `init`/`load` можно типобезопасно
читать и писать сохраняемые ячейки через `Properties`, `TypedKey` и `Codec`:

```java
import io.fom.*;

static final TypedKey<String> NAME = new TypedKey<>("name", Codecs.STRING);
static final TypedKey<Long>   VER  = new TypedKey<>("ver",  Codecs.LONG);

// в init:
Map<String, byte[]> cells = new Properties()
    .put(NAME, "catalogue")
    .put(VER, 42L)
    .asMap();

// в load:
var props = new Properties(persisted);
String name = props.get(NAME);
long ver    = props.get(VER);    // бросает NoSuchPropertyException, если отсутствует
```

`Codecs` предоставляет `STRING`, `LONG`, `BYTES`; для своих типов реализуйте
`Codec<T>`.

> [English version](../../guides/configuration.md)
