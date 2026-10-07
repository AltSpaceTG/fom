# Triggers & watchers

Both are ways to make a process **re-initialise** — to pick up a change in the
outside world. A trigger is a one-shot push; a watcher is a recurring poll that
pushes when it sees a change.

## Triggers

`engine.trigger(processName, value)` records a `LogTrigger` and schedules a
re-init of the named process (subject to the [dedup window](reactive-cascade.md#the-dedup-window)).
The `value` only tags the request (it shows up in the engine's log lines); it is
not written to the log and not passed to the process.

```java
engine.trigger("Stations", new RefreshSignal("ad-hoc"));
```

Trigger several processes atomically with one log record:

```java
engine.trigger(Map.of(
    "Stations", new RefreshSignal("nightly"),
    "Alerts",   new RefreshSignal("nightly")));
```

Re-initialising `Stations` cascades to its reactive consumers — see
[Reactive cascade](reactive-cascade.md). While a triggered re-init runs, the
old version keeps answering queries at once; the new one takes over in a single
step once it has initialised and loaded, and the old one finishes the computes
it started (within `cleanupTimeout`) before its `cleanUp`
([Process lifecycle](process-lifecycle.md#re-initialisation)). Triggers
therefore need no retry loop around `QueryRejectedException`. A trigger that
arrives while a re-init is already running is not lost: it runs one more
re-init right after that one — at once if that one is cancelled with
`cancelInit`.

**A failed re-init keeps the old version.** If the new version cannot be built
(init or load fails for good, its budget runs out, even an
`OutOfMemoryError`), the old version keeps serving, the node is reported
stale, the engine logs `WARN` "FSM[&lt;name&gt;] re-init gave up; keeps serving
&lt;sid&gt;: …" and calls `EngineObserver.onReinitFailed`. The re-init is then
retried on its own, with backoff from `EngineConfig.reinitRetryBackoffMin` up
to `reinitRetryBackoffMax` (`Duration.ZERO` turns that off; see
[Configuration](../guides/configuration.md)). A new trigger does not wait for
that backoff: it starts a re-init at once and replaces the scheduled retry.
Permanent failures are not retried: lost leadership, an append the log
refuses, an undeclared dependency, or an operator's `cancelInit`. A
`cancelInit(name)` while a retry is only scheduled cancels that retry too: the
node keeps serving, stays stale and waits for the next trigger or start.

With `ReinitStrategy.RELEASE_FIRST` (engine-wide or per node, for nodes too big
to hold twice) a trigger works the old way: the old version is retired first,
queries that arrive meanwhile are stashed and answered from the new state
([Process lifecycle](process-lifecycle.md#queries-while-not-yet-serving)), a
compute already running is drained within `cleanupTimeout` (one that outlives
it fails with `QueryRejectedException`: "… is shutting down: the query outlived
the cleanup timeout"), and a re-init that fails leaves the node `Dead`.

A trigger for a `Dead` process (for example one whose init timed out or was
cancelled) **restarts** it immediately, warm-loading its persisted state where
possible, one restart at a time per process; a query sent right after waits for
it rather than being rejected — see
[Process lifecycle](process-lifecycle.md#restarting-a-dead-process). Its
`LogTrigger` is recorded first, exactly like any other trigger: the request is
durable (a restart that the JVM interrupts is replayed on the next start) and it
checks leadership, so on a log another instance owns it throws
`LeadershipLostException` and restarts nothing.

A trigger for a node that is being started right now — at startup, or while a
graph swap replaces it — is not dropped. Under `KEEP_OLD` the node warm-loads
its persisted state, serves it, and re-initialises in the background; under
`RELEASE_FIRST` it cold-inits instead. A node that a graph swap *changed*
cold-inits either way.

!!! warning "The log record comes first"
    `trigger()` appends its `LogTrigger` before it applies anything, so an
    `append` that fails makes `trigger()` throw synchronously and unretried:
    nothing is recorded, no re-init starts, and queries keep being served from
    the current in-memory state. If the append is *refused* because another
    instance owns the log, `trigger()` throws `LeadershipLostException` (an
    `IllegalStateException`: "Engine &lt;id&gt; is no longer the leader of
    &lt;logId&gt;; the trigger for [...] was not recorded") instead of reporting
    success, and a watcher whose trigger hits it stops itself with one `WARN` —
    nothing it triggers could be recorded any more, whether the watched process
    is `Serving` or `Dead`. `trigger(Map)` records **one** `LogTrigger` covering
    every requested name, `Dead` ones included, before it re-initialises or
    restarts anything: a refused append leaves them all untouched, and the
    failure message names every requested process. See
    [Exceptions](../reference/exceptions.md#leadership).

!!! note "Leadership lost after the record was accepted"
    If another instance takes the log over *between* an accepted `trigger` and
    the re-init it scheduled, the re-init is **dropped**: the engine logs an
    `ERROR` and the process keeps serving the state it has, marked stale, and
    the re-init is not retried. The loss is reported once per serving Sid;
    later triggers for that process are dropped silently
    ([Process lifecycle](process-lifecycle.md#re-initialisation),
    [Multi-node](../guides/multi-node.md)).

!!! note "A requested re-init survives a restart"
    The `LogTrigger` is written *before* the re-init runs. If the JVM stops while
    the request is still waiting in the dedup window, the next start sees a
    trigger newer than the process's persisted state and replays it through the
    ordinary trigger path once the process has warm-loaded: the re-init fires
    one full `dedupWindow` later (invisible at the 100 ms default, noticeable
    with a long window). The same holds for a reactive dependency change that
    had not cascaded yet, and it survives compaction.

    The same record covers a trigger that races `close()`: once its
    `LogTrigger` is recorded, `trigger()` returns normally even if the engine
    closed before the re-init could be scheduled, and the next start applies
    it. A trigger that finds the engine already closed throws instead.

!!! note "A pending trigger is dropped with its process"
    A trigger still waiting in the dedup window when its process is removed
    (`remove`, a graph swap without it) is dropped together with the
    process: adding the name back cold-inits it once, with no extra re-init
    from the old trigger. That covers triggers already pending when the removal
    begins (and, in a removal of several nodes, one accepted by a node still
    waiting for its turn to stop). A **new** `engine.trigger(name, …)` sent
    once the removal has started stopping the node is not dropped silently: the
    removal installs the new graph first, so the call throws
    `IllegalArgumentException` ("Unknown process: '&lt;name&gt;'"), as for any
    name not in the graph. The same goes for a graph swap that **changes**
    the node: its pending trigger is dropped, since the replacement cold-inits
    anyway.

### Your own store and the engine: the dual write

The engine does not see your data — only the `trigger()` calls you make. The
`value` you pass is neither persisted nor delivered to the process (see above):
on re-init, `init` must read the current state from the source itself. So when
your application first writes to its own store and then calls
`engine.trigger()`, the two writes are not atomic. If the JVM crashes after
your write but before `trigger()` returns, nothing is recorded in the engine's
log, and the next start **warm-loads the old state** as if nothing had
happened.

Close that gap in one of two ways:

- **Reconcile on startup.** Keep a version (a counter, an `updated_at`, a
  change-log offset) in your store and have `init` record which version it
  built from. After `newGraph(...)` returns, compare the process's version with
  the source's and call `trigger()` if it is behind.
- **Watch the source version.** A [watcher](#watchers) polling that version
  re-initialises the process whenever it moves, crash or no crash — at the
  cost of up to one `interval` of staleness.

!!! warning "Each trigger is a durable append"
    Every `trigger()` call appends one `LogTrigger` — on the file backend, an
    `fsync` — **before** the dedup window collapses it with others. Dedup
    saves re-inits, not log writes: a burst of a thousand triggers for one
    process re-initialises it once but still appends a thousand records. The
    file backend tops out around **~1.5k triggers/s** (bounded by your disk's
    `fsync` latency; Postgres is slower, see
    [Persistence backends](../guides/persistence-backends.md)). Don't call
    `trigger()` once per incoming event: batch several processes into one
    record with `trigger(Map)`, and coalesce or rate-limit producers (one
    trigger per process per window).

## Watchers

A `ScheduledWatcher<V>` polls an external source on a fixed schedule. When its
`check` function returns a value, the engine fires a trigger for the watched
process with that value.

```java
import io.fom.ScheduledWatcher;
import java.time.Duration;

AutoCloseable handle = engine.watch(new ScheduledWatcher<>(
    "Stations",            // process to trigger
    Long.class,             // stateClass: watched value type
    0L,                     // initial value
    Duration.ZERO,          // initial delay
    Duration.ofMinutes(1),  // poll interval
    prevVersion -> fetchLatestVersion().filter(v -> v > prevVersion),  // check
    null));                 // executor for check (null = a virtual thread of the engine)

// later, to stop polling:
handle.close();
```

The `check` receives the **previous** value and returns an `Optional<V>`:

- **present** → the value changed; the engine fires a trigger with it and then
  updates the held value.
- **empty** → no change; nothing happens this tick.

`engine.watch(...)` returns an `AutoCloseable` that cancels the schedule. A
watcher whose `check` throws is logged and skipped — one bad tick doesn't kill
the watcher. The held value advances **only after the trigger is recorded**. If
the trigger cannot be appended (a log outage, say, but not a lost leadership, which stops the watcher as
described below), the held value stays as it was,
so the next tick sees the same change again and triggers it again. Nothing is
lost. Both kinds of failure log the same
`WARN`: "watcher &lt;name&gt; tick failed (check or trigger; retried on the next
tick): …". A value returned by a check that was still running when the engine
closed is dropped quietly.

A recorded trigger is not a successful re-init. If the re-init it starts fails,
the held value has already advanced, so the watcher does not fire again for the
same change; the old version keeps serving and the engine retries the re-init
on its own ([see above](#triggers)). With automatic retries turned off
(`reinitRetryBackoffMin = Duration.ZERO`), only the next change the watcher sees
— or a manual `trigger` — tries again.

Besides `close()` on that handle and `Engine.close()`, the engine stops a watcher
**by itself** in three cases. Each logs a single `WARN` and schedules no further
ticks:

| Why it stops | The `WARN` | Reported `reason` |
|---|---|---|
| its process is no longer in the graph (`remove`, a graph swap without it) — noticed on the next tick even if `check` never returns a value, and also when the process is removed between the check and the trigger. Presence is checked at each tick, so a removal followed by adding the name back **within one poll interval** goes unnoticed: the watcher keeps running against the new process | "watcher for '&lt;name&gt;' stopped: the process is no longer in the graph" | `PROCESS_REMOVED` |
| its trigger was refused because another instance owns the log (`LeadershipLostException`), whether the watched process is `Serving` or `Dead` — leadership is never regained, so retrying tick after tick is pointless | "watcher for '&lt;name&gt;' stopped: &lt;the exception's message&gt;" | `LEADERSHIP_LOST` |
| its own `executor` is an `ExecutorService` that `isShutdown()` and therefore rejects the check — nothing will ever run that check again | "watcher for '&lt;name&gt;' stopped: its executor is shut down" | `EXECUTOR_SHUT_DOWN` |

A rejection from an executor that is merely **saturated** is not permanent, so it
does not stop the watcher: it is logged on every tick ("watcher &lt;name&gt;
could not dispatch its check: …") and polling continues.

All three self-stops are also **reported**, not only logged: the engine calls
[`EngineObserver.onWatcherStopped(processName, reason)`](../guides/observability.md#engineobserver)
with an `io.fom.api.WatcherStopReason` — the enum constant in the last column
above, one of exactly three: `PROCESS_REMOVED`, `EXECUTOR_SHUT_DOWN`,
`LEADERSHIP_LOST`. It is a closed set on purpose, so it is safe as a metric tag;
the detail (the process, the exception) stays in the `WARN` beside it. A
dashboard can therefore alert on "the thing that was supposed to trigger
re-inits is no longer running". `fom-micrometer` counts it as
`engine_watcher_stops_total{reason=…}` — with **no** `name` tag, deliberately:
tagging it with the process would re-create the per-process meters that
`onProcessRemoved` deletes. A watcher **you** stop — `close()`
on the handle `watch` returned, or `Engine.close()` — is not reported: it is not
the engine giving up.

### Watcher fields

| Field | Meaning |
|---|---|
| `processName` | process to trigger on a change |
| `stateClass` | `Class<V>` of the watched value |
| `initialValue` | the value the first `check` sees as "previous" |
| `initialDelay` | delay before the first poll |
| `interval` | time between polls (must be > 0; a sub-millisecond interval is treated as 1 ms) |
| `check` | `Function<V, Optional<V>>` — returns a new value to trigger with, or empty |
| `executor` | optional `Executor` that runs `check`; `null` runs it on a virtual thread of the engine. Either way `check` never runs on the engine's scheduler, so it may block; a tick is skipped while the previous check is still running (an executor must run each task it accepts or throw from `execute`). Closing the handle returned by `watch` stops further checks, but one already running may still trigger; `Engine.close()` interrupts and waits for checks on the engine's threads, while checks on your own executor are yours to stop — though shutting that `ExecutorService` down stops the watcher instead of failing every tick (see above) |

## When to use which

- **Trigger** — you already know something changed (a webhook, an admin action,
  another system's event).
- **Watcher** — you need to *discover* changes by polling (a version counter, a
  file's mtime, a row count).
