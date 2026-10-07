# Configuration

The engine is configured with an immutable `EngineConfig` record. Start from
`EngineConfig.defaults()` and [derive](#programmatic) the fields you care about
(or use a constructor), parse one from [HOCON](#hocon), and
[hot-reload](#hot-reload) it at runtime.

## Fields

| Field | Default | Meaning |
|---|---|---|
| `initTimeout` | 30 s | total budget for a node's `init` across all retries; also bounds each attempt |
| `loadTimeout` | 30 s | per-attempt budget for `load` |
| `cleanupTimeout` | 30 s | per-process budget for draining in-flight queries plus `cleanUp`; `close()` stops processes of the same dependency depth in parallel, so it takes at most about (dependency levels) × this |
| `queryTimeout` | 10 s | default deadline for `engine.query(...)` / `queryProcess(...)` |
| `dedupWindow` | 100 ms | [re-init debounce window](../concepts/reactive-cascade.md#the-dedup-window) |
| `backoffMin` | 50 ms | minimum init/load retry backoff |
| `backoffMax` | 5 m | cap on the base init/load retry backoff; jitter ×[0.5, 1.5) is applied after the cap, so a delay can reach ~1.5 × `backoffMax` ([backoff](../concepts/process-lifecycle.md#backoff)) |
| `maxLoadRetries` | 1 | `load` attempts before falling back to `init` |
| `snapshotPolicy` | `Disabled` | automatic [log rotation](../concepts/snapshots.md) |
| `reinitRetryBackoffMin` | max(`initTimeout`, 30 s), at most an explicit `reinitRetryBackoffMax` | first delay before a failed re-init is tried again (the old version keeps serving meanwhile); doubles with jitter ×[0.5, 1.5) up to the max; `Duration.ZERO` turns automatic retries off; `null` = the default |
| `reinitRetryBackoffMax` | max(10 min, `reinitRetryBackoffMin`) | cap on that delay; `null` = the default |
| `reinitStrategy` | `KEEP_OLD` | `KEEP_OLD`: the old version serves until the new one is loaded and keeps serving if it fails; `RELEASE_FIRST`: the old version is retired first (queries wait, a failed re-init leaves the node `Dead`, never two versions in memory). A node can override it with `GraphBuilder.reinitStrategy(...)`; `null` = the default |

The field order above is exactly the order of the canonical record
constructor (all twelve components); a secondary nine-argument constructor
(without the last three) is kept and uses their defaults. The constructor
validates and throws `IllegalArgumentException` otherwise:

- the first seven durations must be **strictly positive** (including
  `dedupWindow`), `backoffMax >= backoffMin`, and `maxLoadRetries >= 1`;
- the re-init retry bounds may each be `null` (= derived, see below);
- `reinitRetryBackoffMin` may be `Duration.ZERO` (automatic re-init retries
  off), but not negative;
- an explicit `reinitRetryBackoffMax` must be **strictly positive** and
  `>= reinitRetryBackoffMin`.

Derive the last three fields with `withReinitRetryBackoff(min, max)` and
`withReinitStrategy(strategy)`.

**Derived re-init retry bounds follow the init budget.** A retry bound you did
not give (`null`) is not fixed when the config is built: it is worked out on
every read from the current `initTimeout`, so `withInitTimeout(...)` moves it.
A bound you gave stays as given — even if it equals what would have been
derived:

```java
var d = EngineConfig.defaults();
d.withInitTimeout(Duration.ofMinutes(20)).reinitRetryBackoffMin();    // 20 min (derived)
d.withInitTimeout(Duration.ofMinutes(20)).reinitRetryBackoffMax();    // 20 min (derived)
d.withReinitRetryBackoff(Duration.ofSeconds(5), Duration.ofSeconds(50))
 .withInitTimeout(Duration.ofMinutes(2)).reinitRetryBackoffMin();     // 5 s (explicit)
```

A derived min never exceeds an explicit max: with only the max given (say
1 min, via `withReinitRetryBackoff(null, Duration.ofMinutes(1))`) and
`initTimeout` of 5 min, the min reads 1 min. Since the record keeps `null` for a
derived bound, a config with a derived bound is **not** `equals` to one that
gives the same value explicitly, and `toString()` shows `null` for it
(`reinitRetryBackoffMin=null`); read the accessors for the effective values.

When a query's reply fails — its `queryTimeout` (or per-call timeout) passes, it
is cancelled, or a dependency query hits the deadline it inherited — the engine
cancels the compute's `CompletionStage` and frees its in-flight slot at once —
even when the compute cannot be stopped (it is still blocked inside `compute()`,
or its stage ignores `cancel()`, like a `minimalCompletionStage()`). Such a
compute is abandoned: it keeps running in the background, but it does not hold
a re-init, pause or close for the `cleanupTimeout`, is not reported as
a failed cleanup and reports no `onComputeDuration` even if it completes later — so `cleanUp` may run while it is still running. See
[Process lifecycle](../concepts/process-lifecycle.md#retries-backoff-timeouts).

Likewise, when an `init` or `load` attempt exceeds `initTimeout` /
`loadTimeout` the engine cancels the `CompletionStage` that `init` /
`load` returned (a Kotlin coroutine is cancelled; a plain `CompletableFuture`'s
work is not interrupted). A load that stops being wanted for any *other* reason
(a cancelled start, a pause, a removal, a swap, `close()`) is cancelled the same
way; see
[the load cancellation contract](../concepts/process-lifecycle.md#load-cancellation).

!!! note "The three budgets are measured and reported, not enforced"
    `initTimeout`, `loadTimeout` and `cleanupTimeout` are
    measured over the **whole attempt**, not just the wait on the
    `CompletionStage` it returns: work an implementation does before handing that
    stage back — a synchronous `init` that returns `completedFuture(...)` — is
    measured too, and an attempt that has spent its budget is reported as a
    timeout even when its stage is already complete. An init or load that
    blocks **before returning its stage** is ended at its budget as well (a
    watchdog fails the attempt with "… had not even returned its stage"), so a
    synchronous `init` that spins for 4 s under a 1 s budget fails after about
    1 s and the node retries or goes `Dead` as for any timeout. The attempt
    ends, but the work is not **stopped**: a spent budget cancels the stage (a
    no-op when that stage is already complete) and never interrupts the thread
    running your code, so that `init` keeps computing for the full 4 s and its
    result is discarded. Only an
    explicit stop — `cancelInit`, a pause, a removal, a replacing graph change,
    `close()` — interrupts that thread; see
    [Process lifecycle](../concepts/process-lifecycle.md#cancelling-an-init).
    `cleanupTimeout` is one budget *shared* between draining in-flight
    queries and `cleanUp`, in that order; at its end the node moves on, but
    nothing ever interrupts a `cleanUp` body, which may keep running in the
    background.

## Programmatic

```java
import io.fom.*;
import java.time.Duration;

// EngineConfig.defaults() is the starting point; change only what you need:
EngineConfig cfg = EngineConfig.defaults()
    .withInitTimeout(Duration.ofMinutes(2))
    .withQueryTimeout(Duration.ofSeconds(2))
    .withBackoff(Duration.ofMillis(100), Duration.ofSeconds(30))
    .withSnapshotPolicy(new SnapshotPolicy.FixedInterval(Duration.ofHours(6), 7));
    // FixedInterval(Duration.ofHours(6)) alone would keep every archive
```

Every derivation returns a **new** `EngineConfig` (the record is immutable) and
runs the same validation as the constructor, so an invalid value is rejected
right there:

| Method | Field it replaces |
|---|---|
| `withInitTimeout(Duration)` | `initTimeout` (re-init retry bounds not given explicitly follow it) |
| `withLoadTimeout(Duration)` | `loadTimeout` |
| `withCleanupTimeout(Duration)` | `cleanupTimeout` |
| `withQueryTimeout(Duration)` | `queryTimeout` |
| `withDedupWindow(Duration)` | `dedupWindow` |
| `withBackoff(Duration min, Duration max)` | `backoffMin` **and** `backoffMax` (rejected unless `max >= min`) |
| `withMaxLoadRetries(int)` | `maxLoadRetries` (must be `>= 1`) |
| `withSnapshotPolicy(SnapshotPolicy)` | `snapshotPolicy` |
| `withReinitRetryBackoff(Duration min, Duration max)` | `reinitRetryBackoffMin` **and** `reinitRetryBackoffMax`; either may be `null` (= derived); `min` may be `ZERO` (retries off); an explicit `max` must be `> 0` and `>= min` |
| `withReinitStrategy(ReinitStrategy)` | `reinitStrategy` (non-null) |

If you would rather spell out the components at once, the secondary
nine-argument constructor takes the first nine, in the order of the table under
[Fields](#fields), and leaves the re-init fields at their defaults (the
canonical constructor takes all twelve; pass `null` for a re-init field to keep
its default):

```java
EngineConfig explicit = new EngineConfig(
    Duration.ofSeconds(30),   // initTimeout
    Duration.ofSeconds(30),   // loadTimeout
    Duration.ofSeconds(30),   // cleanupTimeout
    Duration.ofSeconds(10),   // queryTimeout
    Duration.ofMillis(100),   // dedupWindow
    Duration.ofMillis(50),    // backoffMin
    Duration.ofMinutes(5),    // backoffMax
    1,                        // maxLoadRetries
    SnapshotPolicy.Disabled.INSTANCE);
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
    reinit {
      strategy = keep-old          # or release-first
      # optional; absent = max(init.timeout, 30s) and max(10m, min-backoff); min-backoff = 0 disables retries
      retry { min-backoff = 30s, max-backoff = 10m }
    }
  }
  system {
    query-timeout = 10s
    dedup-window = 100ms
    # optional rotation:
    log.rotate { cron = "0 0 */6 * * ?", keep-history = 7 }   # Quartz 6-field cron
                                                               # keep-history optional: absent = keep all
  }
}
```

`log.rotate.cron` is a **Quartz 6-field** expression
(`sec min hour day-of-month month day-of-week`). It becomes an
`io.fom.config.CronSnapshotPolicy`: each snapshot is scheduled for the cron's
next execution time (JVM default time zone), and the following one is
recomputed after every fire. The engine's scheduler thread is never blocked;
a slot that comes up while the previous snapshot is still running is skipped.
A value of `never` (or blank) leaves rotation `Disabled`.
`log.rotate` must be an object with a `cron` key — only an explicit
`cron = never` (or blank) turns rotation off, and leaving `log.rotate` out
entirely keeps it `Disabled`. A scalar (`log.rotate = "0 0 3 * * ?"`), an object
without `cron`, or an unknown key such as a misspelled `corn` fails with a
`ConfigException` naming the full key (`engine.system.log.rotate: must be an object ...`,
`engine.system.log.rotate.cron: missing ...`,
`engine.system.log.rotate.corn: unknown key 'corn' ...`) instead of silently
disabling rotation.

**Archive retention.** `log.rotate.keep-history` is optional. Left out (or set to
`all`), every archive a snapshot leaves behind is **kept** — the policy gets
`keepHistory = SnapshotPolicy.KEEP_ALL` and runs no purge at all. A number `>= 1`
deletes all but that many newest archives after each snapshot. Kept archives
grow the disk without bound (each is about the size of the log it replaced), so
either set `keep-history` or delete archives yourself with
`engine.purgeArchives(n)`; see
[Snapshots — Archive retention](../concepts/snapshots.md#archive-retention).

**Time zone and DST.** The policy built from HOCON uses the 2-argument
constructor `CronSnapshotPolicy(String expression, int keepHistory)` (with
`KEEP_ALL` when `keep-history` is absent), which
evaluates the cron in `ZoneId.systemDefault()`. HOCON has no key for the zone —
`log.rotate` accepts only `cron` and `keep-history`, so a `zone = UTC` key fails
as an unknown key. To pin a zone, either start the JVM with
`-Duser.timezone=UTC`, or replace the parsed policy with the 3-argument
constructor:

```java
EngineConfig cfg = EngineConfigHocon.parse()
        .withSnapshotPolicy(new CronSnapshotPolicy("0 0 3 * * ?", 7, ZoneOffset.UTC));
// or keep every archive: new CronSnapshotPolicy("0 0 3 * * ?", ZoneOffset.UTC)
```

In a zone with daylight-saving time the next slot comes from `cron-utils`'
`nextExecution`: a slot that falls in the spring-forward gap (e.g. `02:30` on
the night clocks jump from 02:00 to 03:00) is **skipped** for that day, and a
fixed-time slot in the repeated fall-back hour fires **once** (an hourly cron,
however, fires in both copies of the repeated hour). We recommend running the
cron in **UTC**, which has neither effect.

Both namespaces are **strict**: every key under `engine.graph.default` and
`engine.system` must be one of those listed above. A misspelling anywhere in them —
a parent one level up (`engine.system.logs.rotate { ... }`) or a leaf
(`engine.system.query-timout = 1s`) — fails with a `ConfigException` naming the
full unknown key and the allowed ones, e.g.
`engine.system.query-timout: unknown key 'query-timout' in engine.system (allowed: dedup-window, log, query-timeout)`,
instead of being ignored while the default quietly applies. A scalar where one
of those objects (or one on the way to it) belongs is reported against that key
itself: `engine.system = "fast"` fails with
`engine.system: must be an object, was string "fast" (...)`. Keys outside those two
objects — your application's own settings, even other `engine.*` keys — are not
checked.

An expression that does not parse fails fast with an `IllegalArgumentException`
naming the offending string instead of `cron-utils`' own "Failed to parse cron
expression": `Invalid Quartz cron expression '0 */6 * * *' — this looks like a
5-field Unix cron; Quartz starts with seconds, so prepend a seconds field and use
'?' for one day field (e.g. '0 0 */6 * * ?')`. The suggested
form is offered only if it really parses: Quartz rejects `*` in both day fields at
once, so a plain seconds prefix (`'0 0 */6 * * *'`) would not; when no simple fix
parses, the hint just says to use `?` in either day-of-month or day-of-week.
A **numeric** day-of-week gets no concrete suggestion either: Unix counts from
Sunday = 0 (Monday = 1), Quartz from Sunday = 1 (Monday = 2), so `'30 2 * * 1'`
copied across would fire on Sunday — the hint gives the general advice instead
and notes that names such as `MON-FRI` mean the same in both.
Any other field count is reported the same way, naming the six
Quartz fields and how many were given. `keep-history` must be `>= 1` or `all`
(or be left out — the same as `all`).

Every error from `EngineConfigHocon.parse` starts with the **full HOCON key** it is
about, with the original exception as its cause — e.g.
`engine.graph.default.init.min-backoff: must be > 0, was PT0S`,
`engine.graph.default.init.max-backoff: must be >= engine.graph.default.init.min-backoff (PT10S), was PT1S`,
`engine.system.log.rotate.cron: Invalid Quartz cron expression ...`. A malformed or
mistyped value (`init.timeout = soon`) is a `com.typesafe.config.ConfigException`,
a well-formed but invalid one an `IllegalArgumentException`.

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
import io.fom.Properties; // not java.util.Properties
import java.util.*;

static final TypedKey<String> NAME = new TypedKey<>("name", Codecs.stringCodec());
static final TypedKey<Long>   VER  = new TypedKey<>("ver",  Codecs.longCodec());

// in init (Properties is immutable: each put returns a new instance):
Map<String, byte[]> cells = Properties.empty()
    .put(NAME, "stations")
    .put(VER, 42L)
    .asRaw();

// in load (properties = the Map<String, byte[]> handed to load):
var props = Properties.of(properties);
String name = props.get(NAME);
long ver    = props.get(VER);    // throws NoSuchPropertyException if absent
Optional<Long> maybe = props.find(VER);
```

`Codecs` provides `stringCodec()`, `longCodec()`, `intCodec()` and `uri()`;
implement `Codec<T>` for your own types. Raw cells stay reachable via
`putRaw`/`getRaw`.
