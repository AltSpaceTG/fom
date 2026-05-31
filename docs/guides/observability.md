# Observability

Two complementary mechanisms: a push-based **`EngineObserver`** SPI for events
as they happen, and a pull-based **`introspect()`** for a point-in-time snapshot.

## EngineObserver

Pass an observer as the fifth `Engine` constructor argument. The engine calls
it on every lifecycle event; all methods have no-op defaults, so override only
what you need.

```java
var engine = new Engine(cfg, backend, serDe, true, new EngineObserver() {
    @Override public void onInitCompleted(String name, Sid sid, Duration d) {
        System.out.printf("%s init in %s%n", name, d);
    }
    @Override public void onQueryFailed(String name, UUID id, String reason, Throwable cause) {
        log.warn("query {} on {} failed ({})", id, name, reason, cause);
    }
});
```

Callbacks include: `onStateTransition`, `onInitStarted/Completed/Failed`,
`onLoadStarted/Completed/Failed`, `onQuerySent/Completed/Failed`,
`onComputeDuration`, `onDedupCollapsed`, `onSidPromotion`,
`onCleanupCompleted`, `onOrphanedInit`.

!!! warning "Keep observers fast and non-blocking"
    Callbacks run on engine threads. The engine wraps each call so an exception
    can't break the FSM, but a *slow* observer still slows the engine. Offload
    heavy work.

## Metrics — `fom-micrometer`

`MicrometerEngineObserver` adapts the observer SPI to a Micrometer
`MeterRegistry`:

```java
import io.fom.micrometer.MicrometerEngineObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

var registry = new SimpleMeterRegistry();
var engine = new Engine(cfg, backend, serDe, true,
    new MicrometerEngineObserver(registry));
```

Meters registered, all tagged `name=<process>` (failures also tagged
`reason`):

| Meter | Type |
|---|---|
| `engine_process_init_duration_seconds` | timer |
| `engine_process_load_duration_seconds` | timer |
| `engine_query_duration_seconds` | timer |
| `engine_process_compute_duration_seconds` | timer |
| `engine_process_cleanup_duration_seconds` | timer |
| `engine_query_failures_total` `{reason}` | counter |
| `engine_query_cancellations_total` | counter |
| `engine_dedup_collapsed_total` | counter |

`reason` on `engine_query_failures_total` is one of `init-failed`,
`load-failed`, `cleanup-failed`, `cancelled`, `timeout`, `rejected`,
`exception`. Wire the registry to Prometheus/OTLP/etc. as usual for Micrometer.

## Tracing — `fom-otel`

`OtelEngineObserver` emits OpenTelemetry spans for `init`, `load`, and `query`:

```java
import io.fom.otel.OtelEngineObserver;

var engine = new Engine(cfg, backend, serDe, true,
    new OtelEngineObserver(openTelemetry.getTracer("io.fom")));
```

## Combining observers

`EngineObserver` is a plain interface — to use metrics *and* tracing *and* your
own logging, write a small fan-out observer that delegates to several, or layer
them in your own composite. (The engine takes exactly one observer.)

## Point-in-time — `introspect()`

```java
EngineReport report = engine.introspect().toCompletableFuture().get();

report.instanceId();          // this engine's instance id
report.leaderConfigured();    // was leaderAtStart=true?
report.isLeader();            // is this instance currently the log leader?
for (var node : report.graph().nodes()) {       // List<NodeReport>
    System.out.printf("%s: state=%s sid=%s%n",
        node.name(), node.state(), node.currentSid());
}
report.graph().mailboxSizes();   // Map<String,Integer> per-process mailbox depth
report.log();                    // LogBackendReport: length, currentLeader, counts, last ts
```

`EngineReport` is an immutable record — ideal for a `/debug` endpoint or a
health check. Each `NodeReport` carries `name`, `currentSid`, `state`,
`initRetries`, `loadRetries`, and `lastError`. `LogBackendReport` gives
`length`, `currentLeader`, `eventCountsByType`, and `lastTimestampMillis`.
