# Configuration

The engine is configured with an immutable `EngineConfig` record. Build one with
`EngineConfig.defaults()` or the canonical constructor, parse one from
[HOCON](#hocon), and [hot-reload](#hot-reload) it at runtime.

## Fields

| Field | Default | Meaning |
|---|---|---|
| `defaultInitTimeout` | 30 s | total budget for a node's `init` across all retries; also bounds each attempt |
| `defaultLoadTimeout` | 30 s | per-attempt budget for `load` |
| `defaultCleanupTimeout` | 30 s | budget for `cleanUp` (and worker drain on shutdown) |
| `defaultComputeTimeout` | 60 m | default budget for a query's `compute` |
| `defaultCancelInitTimeout` | 60 s | budget for an admin `cancelInit` |
| `queryTimeout` | 10 s | default deadline for `engine.query(...)` |
| `logLookupInterval` | 100 ms | log polling interval (follower bookkeeping) |
| `dedupWindow` | 100 ms | [re-init debounce window](../concepts/reactive-cascade.md#the-dedup-window) |
| `backoffMin` | 50 ms | minimum init/load retry backoff |
| `backoffMax` | 5 m | maximum init/load retry backoff |
| `maxLoadRetries` | 1 | `load` attempts before falling back to `init` |
| `snapshotPolicy` | `Disabled` | automatic [log rotation](../concepts/snapshots.md) |

The field order above is exactly the canonical record constructor order. All
durations must be **strictly positive** (including `dedupWindow`),
`backoffMax >= backoffMin`, and `maxLoadRetries >= 1`. The constructor validates
these and throws `IllegalArgumentException` otherwise.

## Programmatic

```java
import io.fom.*;
import java.time.Duration;

// EngineConfig.defaults() is the simplest starting point:
EngineConfig defaults = EngineConfig.defaults();

// …or spell out every field (canonical constructor order):
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

Because it's a record, derive a variant with the canonical accessors:

```java
var withRotation = new EngineConfig(
    cfg.defaultInitTimeout(), cfg.defaultLoadTimeout(), cfg.defaultCleanupTimeout(),
    cfg.defaultComputeTimeout(), cfg.defaultCancelInitTimeout(), cfg.queryTimeout(),
    cfg.logLookupInterval(), cfg.dedupWindow(), cfg.backoffMin(), cfg.backoffMax(),
    cfg.maxLoadRetries(),
    new SnapshotPolicy.FixedInterval(Duration.ofHours(6), 7));
```

## HOCON { #hocon }

The `fom-config-hocon` module parses an `EngineConfig` from a Typesafe
`Config`:

```java
import io.fom.config.EngineConfigHocon;

EngineConfig cfg = EngineConfigHocon.parse();              // ConfigFactory.load()
EngineConfig cfg2 = EngineConfigHocon.parse(myConfig);     // from your own Config
```

It reads `engine.graph.default.*` and `engine.system.*`. The baseline (any path
may be overridden):

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
    # optional rotation:
    log.rotate { cron = "0 0 */6 * * ?", keep-history = 7 }   # Quartz 6-field cron
  }
}
```

`log.rotate.cron` is a **Quartz 6-field** expression
(`sec min hour day-of-month month day-of-week`). It is translated into a
`SnapshotPolicy.FixedInterval` by computing the next fire time from "now". A
value of `never` (or blank) leaves rotation `Disabled`.

!!! warning "cron from untrusted input"
    The cron string is parsed by `cron-utils`. Don't feed cron expressions from
    untrusted sources without validation. See [Security](../security.md).

## Hot-reload { #hot-reload }

Swap the live config at runtime:

```java
engine.updateConfig(newConfig);
```

In-flight operations keep the values they captured; subsequent operations read
the new config. If the `snapshotPolicy` changed, the scheduled rotation is
cancelled and re-armed against the new policy. `updateConfig` is serialized
against `newGraph` by the engine's control lock.

## Typed property cells

Unrelated to `EngineConfig` but worth knowing: inside `init`/`load` you can read
and write the persisted property cells type-safely with `Properties`,
`TypedKey`, and `Codec`:

```java
import io.fom.*;

static final TypedKey<String> NAME = new TypedKey<>("name", Codecs.STRING);
static final TypedKey<Long>   VER  = new TypedKey<>("ver",  Codecs.LONG);

// in init:
Map<String, byte[]> cells = new Properties()
    .put(NAME, "catalogue")
    .put(VER, 42L)
    .asMap();

// in load:
var props = new Properties(persisted);
String name = props.get(NAME);
long ver    = props.get(VER);    // throws NoSuchPropertyException if absent
```

`Codecs` provides `STRING`, `LONG`, and `BYTES`; implement `Codec<T>` for your
own types.
