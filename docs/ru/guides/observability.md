# Наблюдаемость

Два дополняющих механизма: push-based SPI **`EngineObserver`** для событий по мере
их возникновения и pull-based **`introspect()`** для снимка на момент времени.

## Встроенное логирование жизненного цикла

Из коробки — без всякого наблюдателя — движок логирует жизненный цикл каждого
процесса на slf4j-логгере **`io.fom.fsm.ProcessFSM`** на уровне `INFO`:

```
[INFO] Process 'Inventory' init started
[INFO] Process 'Inventory' init completed in 3 ms (sid=2)
[INFO] Process 'Inventory' load started
[INFO] Process 'Inventory' load completed in 2 ms — now Serving (sid=2)
```

Повторы и сбои тоже логируются (повторы init на `INFO`, сбои на `WARN`).
Подключите любой slf4j-биндинг (Logback, slf4j-simple, …), чтобы это увидеть;
уровень логгера `io.fom.fsm.ProcessFSM` поднимайте/опускайте по вкусу.
[`EngineObserver`](#engineobserver) нужен, когда те же события нужны как
структурированные данные (метрики, спаны), а не строки лога.

## EngineObserver

Передайте наблюдателя пятым аргументом конструктора `Engine`. Движок вызывает его
на каждом событии жизненного цикла; все методы имеют пустые реализации по
умолчанию, переопределяйте только нужные.

```java
var engine = new Engine(cfg, backend, serDe, true, new EngineObserver() {
    @Override public void onInitCompleted(String name, Sid sid, Duration d) {
        System.out.printf("%s init за %s%n", name, d);
    }
    @Override public void onQueryFailed(String name, UUID id, String reason, Throwable cause) {
        log.warn("запрос {} на {} упал ({})", id, name, reason, cause);
    }
});
```

Колбэки: `onStateTransition`, `onInitStarted/Completed/Failed`,
`onLoadStarted/Completed/Failed`, `onQuerySent/Completed/Failed`,
`onComputeDuration`, `onDedupCollapsed`, `onSidPromotion`, `onCleanupCompleted`,
`onOrphanedInit`.

!!! warning "Наблюдатели должны быть быстрыми и неблокирующими"
    Колбэки выполняются на потоках движка. Движок оборачивает каждый вызов, чтобы
    исключение не сломало FSM, но *медленный* наблюдатель всё равно замедляет
    движок. Тяжёлую работу выносите.

## Метрики — `fom-micrometer`

`MicrometerEngineObserver` адаптирует SPI наблюдателя к Micrometer
`MeterRegistry`:

```java
import io.fom.micrometer.MicrometerEngineObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

var registry = new SimpleMeterRegistry();
var engine = new Engine(cfg, backend, serDe, true,
    new MicrometerEngineObserver(registry));
```

Регистрируемые метрики, все с тегом `name=<процесс>` (сбои также с тегом
`reason`):

| Метрика | Тип |
|---|---|
| `engine_process_init_duration_seconds` | timer |
| `engine_process_load_duration_seconds` | timer |
| `engine_query_duration_seconds` | timer |
| `engine_process_compute_duration_seconds` | timer |
| `engine_process_cleanup_duration_seconds` | timer |
| `engine_query_failures_total` `{reason}` | counter |
| `engine_query_cancellations_total` | counter |
| `engine_dedup_collapsed_total` | counter |

`reason` у `engine_query_failures_total` — одно из `init-failed`, `load-failed`,
`cleanup-failed`, `cancelled`, `timeout`, `rejected`, `exception`. Подключайте
реестр к Prometheus/OTLP/и т. д. как обычно для Micrometer.

## Трассировка — `fom-otel`

`OtelEngineObserver` эмитит спаны OpenTelemetry для `init`, `load` и `query`:

```java
import io.fom.otel.OtelEngineObserver;

var engine = new Engine(cfg, backend, serDe, true,
    new OtelEngineObserver(openTelemetry));            // или new OtelEngineObserver(tracer)
```

## Комбинирование наблюдателей

`EngineObserver` — обычный интерфейс — чтобы использовать метрики *и* трассировку
*и* своё логирование, напишите небольшой fan-out наблюдатель, делегирующий
нескольким. (Движок принимает ровно одного наблюдателя.)

## Снимок на момент времени — `introspect()`

```java
EngineReport report = engine.introspect().toCompletableFuture().get();

report.instanceId();          // id экземпляра движка
report.leaderConfigured();    // был ли leaderAtStart=true?
report.isLeader();            // является ли экземпляр текущим лидером лога?
for (var node : report.graph().nodes()) {       // List<NodeReport>
    System.out.printf("%s: state=%s sid=%s%n",
        node.name(), node.state(), node.currentSid());
}
report.graph().mailboxSizes();   // Map<String,Integer> глубина мейлбокса по процессам
report.log();                    // LogBackendReport: length, currentLeader, counts, last ts
```

`EngineReport` — неизменяемый record, идеален для `/debug`-эндпоинта или
health-check. Каждый `NodeReport` несёт `name`, `currentSid`, `state`,
`initRetries`, `loadRetries`, `lastError`. `LogBackendReport` даёт `length`,
`currentLeader`, `eventCountsByType`, `lastTimestampMillis`.

> [English version](../../guides/observability.md)
