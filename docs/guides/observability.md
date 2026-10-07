# Observability

Two complementary mechanisms: a push-based **`EngineObserver`** SPI for events
as they happen, and a pull-based **`introspect()`** for a point-in-time snapshot.

## Built-in lifecycle logging

Out of the box — no observer required — the engine logs each process's lifecycle
on the **`io.fom.fsm.ProcessFSM`** slf4j logger at `INFO`:

```
[INFO] Process 'Stations' init started
[INFO] Process 'Stations' init completed in 3 ms (sid=2)
[INFO] Process 'Stations' load started
[INFO] Process 'Stations' load completed in 2 ms — now Serving (sid=2)
```

Retries and failures are logged too (init retries at `INFO`, failures at `WARN`).
Provide any slf4j binding (Logback, slf4j-simple, …) to see it; raise or lower
the `io.fom.fsm.ProcessFSM` logger to taste. Use an [`EngineObserver`](#engineobserver)
when you need the same events as structured data (metrics, spans) rather than log
lines.

Budget for about **six `INFO` lines per (re)init of each process**. A re-init
(a trigger, a reactive cascade) logs, with the default
[`KEEP_OLD`](../concepts/process-lifecycle.md#re-initialisation) strategy:

```
[INFO] FSM[Stations] re-init started; Sid[processName=Stations, clock=2] keeps serving (cause=…)
[INFO] Process 'Stations' init started
[INFO] Process 'Stations' init completed in 3 ms (sid=3)
[INFO] Process 'Stations' load started
[INFO] Process 'Stations' load completed in 2 ms (sid=3)
[INFO] Process 'Stations' replaced Sid[processName=Stations, clock=2] with Sid[processName=Stations, clock=3]
```

The replacement's load line has no "— now Serving": the new version is loaded
but does not answer yet. The "replaced … with …" line marks the switch — the
old version answers queries until that line. A `KEEP_OLD` re-init that gives
up leaves it serving and logs just this one `WARN` — no `ERROR` "FSM[X] giving
up: …" line, which is kept for a node that ends `Dead` — followed by the
automatic retry
(`EngineConfig.reinitRetryBackoffMin`/`Max`; none when retries are off or the
cause is permanent):

```
[WARN] FSM[Stations] re-init gave up; keeps serving Sid[processName=Stations, clock=2]: io.fom.api.InitializationTimeoutException: …
[INFO] FSM[Stations] retrying the re-init in 31250 ms
[INFO] FSM[Stations] retrying the re-init that failed
```

See [When a re-init fails](../concepts/process-lifecycle.md#when-a-re-init-fails).
Under `RELEASE_FIRST` the first line is `FSM[Stations] beginning reinit cycle
(cause=…)` and there is no "replaced" line.

A start logs the four init/load lines without the re-init ones, plus one line on the
`io.fom.fsm.GraphMachine` logger ("warm start for X from clock N" / "cold start
for X"). A warm start logs only the load pair. Retries add a line per attempt.
With a high trigger rate or many processes, this adds up quickly. In that case,
set `io.fom.fsm.ProcessFSM` to `WARN` (failures stay visible), or filter those
messages in your logging backend, and take the lifecycle from an
`EngineObserver` or [metrics](#metrics--fom-micrometer) instead.

The start line ("warm start for X from clock N" / "cold start for X") runs on a
`fom-start-<N>` thread, one per node being started by `newGraph` (a resume
logs it on the caller's thread), and carries the same
`fom.engine` / `fom.process` [MDC keys](../concepts/process-lifecycle.md#observability)
as the dispatcher's lines.

Control-plane calls log one `INFO` line each on the `io.fom.Engine` logger, on
the calling thread (no MDC), naming the processes in sorted order:

```
[INFO] Engine[fom-…] pausing [Stations_PUB1, Subscriptions_PUB1]
[INFO] Engine[fom-…] resuming [Stations_PUB1, Subscriptions_PUB1]
[INFO] Engine[fom-…] removing [Stations_PUB1, Subscriptions_PUB1]
[INFO] Engine[fom-…] closing
```

`pausing` lists only the processes that were not paused already, and is
skipped when none are new. `removing` is logged only once the removal is
accepted: a `remove` that is refused (a remaining process depends on a removed
one, or nothing would be left) logs nothing. A resume that a concurrent
operation cancels logs `resuming [..] was cancelled by another operation: …`.
A successful snapshot is **not** logged at `INFO` — only failures (`WARN`), and
for scheduled snapshots one "scheduled snapshot succeeded after N failure(s)"
line once they recover; read `SnapshotResult` or the log's `LogSnapshot` events
when you need a record of each compaction.

## EngineObserver

Pass an observer as the fourth `Engine` constructor argument. The engine calls
it on every lifecycle event; all methods have no-op defaults, so override only
what you need.

```java
var engine = new Engine(cfg, backend, serDe, new EngineObserver() {
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
`onCleanupCompleted`, `onProcessRemoved`, `onWatcherStopped`,
`onReinitStarted`, `onReinitFailed`.

**Re-init callbacks.** With `KEEP_OLD` (the default) a re-init does not change
the process state, so it fires **no** `onStateTransition`. A successful one
reports, in order:

```
onReinitStarted(name, servingSid)        // the old version keeps answering
onInitStarted → onInitCompleted(newSid)
onLoadStarted(newSid) → onLoadCompleted(newSid)
onSidPromotion(name, servingSid, newSid) // the switch
onCleanupCompleted(name, servingSid, …)  // the old version, once its computes drained
```

`onReinitFailed(name, keptSid, cause)` fires instead of the promotion when the
re-init gives up: `keptSid` goes on serving, the node is reported `stale`, and
the re-init is retried after `reinitRetryBackoffMin` unless `cause` is permanent
(lost leadership, a refused append, an undeclared dependency, `cancelInit`; see
[Exceptions](../reference/exceptions.md#re-init-failures)). The failed attempts
themselves still come through `onInitFailed` / `onLoadFailed`. A restart that
finishes a re-init from a persisted candidate fires `onReinitStarted` and then
only the load callbacks. Under `RELEASE_FIRST` neither callback fires: the
re-init goes through `Serving → CleaningUp → Initializing → Loading → Serving`
with the usual transitions.

`onSidPromotion(processName, previousSid, newSid)` fires once per new Sid.
Resuming a paused process that warm-loads the same state does not fire it
again. Restarting from `Dead` or replacing a node reports `previousSid` = the
Sid the process had before; `previousSid` is `null` only for the first Sid of a
process in this engine (a cold start, a warm load after a JVM restart, or the
first Sid after the process was removed and added back).

`onInitFailed` for a cancelled init (`cancelInit`, pause, removal, graph swap,
`close()`) is delivered once, with an `io.fom.api.AttemptCancelledException`
(a `CancellationException` subclass), before the process leaves `Initializing`.
Only the engine throws that type: a plain `CancellationException` from your own
`init` or `load` code (a timeout inside it, say) is an ordinary failed attempt —
retried, and it can end in `Dead`.

Every `onInitStarted` and every `onLoadStarted` gets **exactly one** terminal
callback (`onInitCompleted`/`onInitFailed`, `onLoadCompleted`/`onLoadFailed`),
also when a stop races the attempt's completion — e.g. a pause that lands just
as the attempt finishes, whose result is then dropped: it is reported as
`onInitFailed`/`onLoadFailed` with an `AttemptCancelledException`. An observer that
pairs starts with ends (a tracer building spans) never leaks an open start.
(The unpaired case goes the other way round: a dropped re-init's
`onInitFailed` has no `onInitStarted` — see [below](#tracing--fom-otel).)

A **dropped re-init** is reported the same way: `onInitFailed` with attempt `1`,
even though the node never leaves `Serving`; it keeps serving the state it
already has, which is now known to be stale. That is a `RELEASE_FIRST` re-init
whose `LogDead` the log cannot store. Under `KEEP_OLD` a re-init that loses
leadership is reported once per serving Sid — `onInitFailed`/`onLoadFailed` for
its attempt and `onReinitFailed` with the `LeadershipLostException` — and later
requests for the node are then dropped silently, with no further callback; the
node stays stale. The exception you receive says which of the three permanent
answers the backend gave:

- the append was **refused** (another instance took the log over between the
  trigger and the debounced re-init) — the engine's own `LeadershipLostException`
  ("cannot re-initialise `<name>`: no longer the leader of the log");
- the backend **threw** `LeadershipLostException` (a fenced Postgres) — the
  backend's own exception and message;
- the backend threw `IllegalArgumentException` (an event it can never store) —
  that `IllegalArgumentException`.

The same exception becomes `NodeReport.lastException`. It is delivered **once per
stuck state**: the engine remembers the Sid it gave up on, so repeated triggers
against that same state drop silently without reporting again, and only a re-init
that really happens clears the memory. So treat one callback as "this node is
serving stale state until further notice", not as a per-trigger counter. See
[Re-initialisation](../concepts/process-lifecycle.md#re-initialisation).

`onCleanupCompleted(processName, sid, ok, duration)` reports the **measured**
cleanup duration (earlier versions always passed zero), so
`engine_process_cleanup_duration_seconds` records real values. `duration`
measures the **`cleanUp` stage only**: draining in-flight queries happens first,
inside the same `cleanupTimeout`, and is not included — so an over-budget
cleanup can report a very small `duration` together with `ok=false`. `ok` is
`false` when `cleanUp` threw or outlived `cleanupTimeout` — and a
`cleanUp` that outlives that budget has its stage cancelled and is logged naming
the process and the budget ("`<name>` cleanUp did not finish within its share of
`PT…`"; the budget also covers draining in-flight queries). `ok=false` means
"over budget", not "stopped".

**An over-budget cleanup reports at the budget — also an inline one.** The
engine calls `process.cleanUp(ctx)` on a thread of its own, and the budget bounds
that call as well as the stage it returns. So a `cleanUp` that blocks *before*
returning its stage no longer holds the node up: at the budget the node moves on
— a `RELEASE_FIRST` re-init proceeds, queries stashed for the node are served
by the new generation, `close()` returns (a `KEEP_OLD` re-init never waits for
the old version's cleanup: the new one already serves) — and `onCleanupCompleted(ok=false)` fires then, at
the budget, before `close()` returns, with a `duration` of about the budget's
remaining share. Nothing interrupts the blocking body itself, though: it runs to
its end in the background, after this callback fired and after `close()`
returned, so the reported `duration` is not how long it really took. Make yours
return its stage early and react to cancellation
([Process lifecycle](../concepts/process-lifecycle.md#retries-backoff-timeouts)).

`onProcessRemoved(String processName)` fires when a process leaves the graph for
good (`remove`, or a graph swap without it) — release any per-process
state you keep there.

`onWatcherStopped(String processName, WatcherStopReason reason)` fires when a
[`ScheduledWatcher` stops itself](../concepts/triggers-and-watchers.md#watchers)
and will not tick again. `reason` is an `io.fom.api.WatcherStopReason`, an enum
with exactly three constants:

| Constant | Why the watcher gave up |
|---|---|
| `PROCESS_REMOVED` | its process is no longer in the graph; re-adding the process does not revive the watcher |
| `EXECUTOR_SHUT_DOWN` | the `Executor` the caller gave it was shut down, so its check can never run again |
| `LEADERSHIP_LOST` | its trigger could not be recorded: this instance no longer leads the log |

The set is closed on purpose, so the reason is safe to use as a metric tag or in
a `switch`; the detail (which process, which exception) is in the `WARN` the
engine writes beside it. A watcher the
caller closes by hand (`close()` on the `watch` handle, or `Engine.close()`) is
**not** reported: alert on this callback and you alert only on the engine giving
up on a poller you still expect to run.

`onQuerySent` is called on the thread that submits the query, and every sent
query then gets exactly one `onQueryCompleted` or `onQueryFailed` when its reply
settles. `onQueryFailed`'s `reason` is `timeout` (the caller's deadline passed —
a query sent from inside `compute`, via `ctx.query` or the outer `Engine`,
inherits the deadline of the query being computed), `rejected`
(a query the engine had accepted is then refused — the process reached `Dead`,
is shutting down, or was paused or removed while the query waited for it, or the
engine is closing), `init-in-progress`, `cancelled` or
`exception` (`compute` threw); `cause` carries the detail. The reason is
**classified by the exception type** the reply failed with, not by who raised
it: a `TimeoutException` gives `timeout`, a `QueryRejectedException`
`rejected`, an `InitInProgressException` `init-in-progress`, a
`CancellationException` `cancelled`, and anything else `exception`. So a
`compute` that fails with **its own** `TimeoutException` (a timed-out HTTP
client call, say) is reported as `timeout` — the same as a missed query
deadline — and one that throws a `CancellationException` as `cancelled`. Wrap
such failures in your own exception type if you need to tell them apart. Queries that are
refused **before** they reach a process fire no hooks at all: one to a process
that is already unknown or paused, and one to a **closed engine** (`query` /
`queryProcess` short-circuit with `IllegalStateException: Engine <id> is closed`
before `GraphMachine` could fire `onQuerySent`). None of them ever appears under
`rejected`, or in any metric derived from these callbacks (see
[the caveat](#metrics--fom-micrometer) below).

!!! note "Query accounting is eventually consistent"
    `onQueryCompleted` / `onQueryFailed` are registered as dependents of the very
    future the caller holds, so they can still be pending when the caller's
    `get()` has already returned. An invariant like `sent == completed + failed`
    therefore holds *eventually*, not the instant a query returns — in a test,
    await it (Awaitility) rather than asserting it straight after the query.
    Both run on an engine-owned virtual thread, never on the caller's thread, so
    a slow observer cannot stretch a caller's timed `get()`.

Its signature is `onQuerySent(String processName, UUID queryId, Class<?>
messageType, UUID parentQueryId)`. A dependency query runs on a compute worker,
not on the thread that sent the outer query, so `parentQueryId` names the query
whose `compute` sent it — via `ctx.query`, or via `Engine.query`/`queryProcess`
called while that `compute` runs — use it to nest the two. It is `null` for
`Engine.query`/`queryProcess` called from outside a compute and for queries
sent from `init`/`load`. A child whose parent query fails is cancelled
(`reason` `cancelled`).

### Stopping a process from a callback { #stopping-from-a-callback }

Don't call a control-plane method that **stops a process** —
`pause`, `remove`, `TenantAwareEngine.pauseTenant` /
`removeTenant`, a `newGraph` / `updateGraph` that drops or redefines it, or
`close()` — synchronously from a callback about **that same process**. Such a
callback may run on the process's own dispatcher, and the stop is a message to
that dispatcher: the call would wait for the very thread it is blocking.

The engine detects this and refuses at once: when the calling thread is the
dispatcher of a process the call would stop, the call throws
`IllegalStateException` before taking the control lock and before changing
anything — e.g. "pause('X') called on that process's own dispatcher
(from an EngineObserver callback about it); call it from another thread", or "a graph change that removes
or redefines 'X' was called on that process's own dispatcher …" for
`newGraph`/`updateGraph` (checked against the installed graph, and again under
the lock against the graph the change is applied to). A pause, removal or graph
change that only touches **other** processes is still allowed from the callback,
as long as no other control-plane call is in progress (see below).
`close()` on a dispatcher throws the same way ("close() called on the dispatcher
of 'X' …") — except while the engine is **already closing**, when it just
returns, since there is nothing left for it to do. (Before this check such a
call stalled for the whole `cleanupTimeout` while holding the engine's
control lock.)

A second rule covers **any** control-plane call — `newGraph`, `updateGraph`,
`updateConfig`, `remove`, `pause`, `resume`, `resumeUnblocked` — made from a
callback running on a process dispatcher while **another** control-plane call
holds the engine's control lock: for example, the first `newGraph`, which holds
it while it waits for every node to reach `Serving`. That call may be waiting
for this very dispatcher, so the engine does not block: the call throws
`IllegalStateException` at once — "pause(...) called from an EngineObserver
callback while another control-plane call is in progress; call it from another
thread" (with the name of the call). Earlier versions blocked here instead, so
the dispatcher stalled for the whole start budget and the first `newGraph`
failed. When no other control-plane call is in progress, calls that only touch
**other** processes still work from a callback. On a thread that is not a
process dispatcher, control-plane calls wait for the lock as usual.

In both cases the exception propagates out of your callback, where the engine
logs and swallows it like any callback failure — so the call silently does not
happen. Hand control-plane calls off to another thread instead
(`Thread.startVirtualThread(...)` or `Thread.ofVirtual().start(...)`):

```java
@Override public void onInitFailed(String name, int attempt, Throwable cause) {
    if (attempt >= 3) {
        Thread.ofVirtual().start(() -> engine.pause(Set.of(name)));
    }
}
```

### Queries that wait for a node { #queries-that-wait-for-a-node }

A query to a graph node that has **no running FSM** right now — one still
starting (`Starting` in `introspect()`), or a node between the old and the new
FSM of a [graph swap](../concepts/graph-swap.md) — is reported too, exactly
once: `onQuerySent` when the query starts waiting, then `onQueryCompleted` once
the node serves it, or `onQueryFailed` with `reason` `timeout` (the deadline
passed before the node started) or `rejected` (the node was removed or paused,
or the engine closed). The FSM that eventually serves it takes it over with the
same `queryId`, so it is never counted twice.

Mind the timing: the span and the latency timer start when the **wait** starts,
not when the process receives the query, so such a query measures queueing plus
compute. In Micrometer and OpenTelemetry it therefore shows the node's startup
time as query latency.

!!! warning "Keep observers fast and non-blocking"
    Callbacks run on engine threads — except `onQuerySent`, which runs on the
    thread that submits the query (the caller's own thread for
    `Engine.query`). The engine wraps each call so an exception
    can't break the FSM — an exception from `onSidPromotion` or
    `onDedupCollapsed` doesn't stop the cascade or drop a re-init either — but a
    *slow* observer still slows the engine. Offload heavy work. A callback that
    keeps throwing is logged at `WARN` only on its 1st, 2nd, 4th, 8th… failure,
    at `DEBUG` otherwise. The counter is per callback, kept per process for the
    lifecycle and query callbacks (`onInit*`, `onLoad*`, `onQuery*`, …) and
    engine-wide for the graph callbacks (`onSidPromotion`, `onDedupCollapsed`,
    `onProcessRemoved`). A [composite](#combining-observers) counts its own
    delegates' failures instead, per callback for the whole engine.

## Metrics — `fom-micrometer`

`MicrometerEngineObserver` adapts the observer SPI to a Micrometer
`MeterRegistry`:

```java
import io.fom.micrometer.MicrometerEngineObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

var registry = new SimpleMeterRegistry();
var engine = new Engine(cfg, backend, serDe,
    new MicrometerEngineObserver(registry));
```

Meters registered, all tagged `name=<process>` except
`engine_watcher_stops_total` (query failures also tagged `reason`):

| Meter | Type |
|---|---|
| `engine_process_init_duration_seconds` | timer |
| `engine_process_load_duration_seconds` | timer |
| `engine_query_duration_seconds` | timer |
| `engine_process_compute_duration_seconds` | timer |
| `engine_process_cleanup_duration_seconds` | timer |
| `engine_process_reinit_duration_seconds` | timer (`onReinitStarted` → the promotion) |
| `engine_query_failures_total` `{reason}` | counter |
| `engine_query_cancellations_total` | counter |
| `engine_process_init_failures_total` | counter (excludes cancellations and leadership loss) |
| `engine_process_load_failures_total` | counter (excludes cancellations and leadership loss) |
| `engine_process_leadership_lost_total` | counter (init/load attempts refused because this node lost leadership) |
| `engine_process_init_cancellations_total` | counter |
| `engine_process_load_cancellations_total` | counter |
| `engine_process_cleanup_failures_total` | counter |
| `engine_process_dead` | gauge (`1` while Dead after a failure, else `0`; `0` once paused) |
| `engine_process_reinit_failures_total` | counter (re-inits that gave up, permanent causes included) |
| `engine_process_stale` | gauge (`1` from a failed re-init until the next Sid promotion, else `0`) |
| `engine_dedup_collapsed_total` | counter (increments by the **number of collapsed triggers**) |
| `engine_watcher_stops_total` `{reason}` (no `name` tag) | counter |

The three re-init meters follow the `KEEP_OLD` callbacks. `engine_process_reinit_failures_total`
goes up by one per `onReinitFailed`; `engine_process_stale` reads `1` from then
until the next `onSidPromotion` — the node answers from a version it failed to
replace (folded, it reads `1` if any of the folded names is stale); and
`engine_process_reinit_duration_seconds` records the time from `onReinitStarted`
to the promotion. A re-init that fails, or that a pause, removal or `close()`
cuts short, records no time. A `RELEASE_FIRST` re-init fires neither callback,
so it moves none of the three.

`engine_query_failures_total` counts only failed queries, tagged with the
engine's `reason` (`timeout`, `rejected`, `init-in-progress`, `cancelled`,
`exception`). `engine_watcher_stops_total` counts self-stopped watchers
(`onWatcherStopped`) tagged **only** with `reason` — `process_removed`,
`executor_shut_down` or `leadership_lost`, the lower-cased
[`WatcherStopReason`](#engineobserver) — and deliberately *not* with `name`: the
commonest reason is that the process was removed, so a `name` tag would re-create
the very per-process meters `onProcessRemoved` drops, and process names can be
tenant-shaped and unbounded. The process is named in the engine's `WARN` beside
the count. `engine_query_cancellations_total` counts query
failures whose reason is `cancelled` or whose cause is a
`CancellationException`.
`engine_query_duration_seconds` runs from send to reply; the compute part alone
is `engine_process_compute_duration_seconds`.
`engine_dedup_collapsed_total` counts **collapsed triggers, not collapse
events**: `onDedupCollapsed(name, n)` increments it by `n`, so six triggers that
land inside one dedup window add `6.0` — one re-init that absorbed six requests,
not six collapses. Divide by the re-init count if you want an average fan-in.

`engine_process_compute_duration_seconds` — and the `onComputeDuration` callback
behind it — times the `CompletionStage` your `compute` returned: from the call
until that stage settles. The engine **cancels** that stage when the caller's
deadline passes, and a cancelled `CompletableFuture` settles at once, so a
compute that outran the query timeout is recorded once with the time to
*cancellation*, not with the time its body actually ran. The histogram is
therefore truncated at the query timeout: it shows how long computes took **up
to** the deadline, and under-reports every one that ran past it. A body that
ignores cancellation goes on working afterwards, and that extra time appears in
no metric at all — to see the real cost of slow computes, instrument the
`compute` body itself, and read
`engine_query_failures_total{reason="timeout"}` for how often the deadline
truncated a measurement. The engine calls `onComputeDuration` only **after** it
has completed the query's reply, and on an engine virtual thread — not on the
thread that completed the compute's stage — so a slow observer never delays the
answer (or turns a compute that answered in time into a timeout) or other
computes. A compute that could not be stopped when its query failed (still
blocked inside `compute()`, or a stage that ignores `cancel()`) is abandoned and
not reported to `onComputeDuration` at all, even if it completes later.

!!! warning "`rejected` does not count the commonest rejection"
    A query aimed at a **paused or unknown** process — or any query on a
    **closed engine** — is refused synchronously (in the graph, or by `Engine`
    itself before the graph), before any process FSM is involved, so no
    `onQuerySent` / `onQueryFailed` fires and `engine_query_failures_total{reason="rejected"}` is
    never touched — even though every caller gets a `QueryRejectedException`.
    Measured: 100 queries to a paused node → 100 `QueryRejectedException` for the
    callers, observer counts unchanged (`sent 1→1, failed 0→0`), counter `0`. Do
    not build a "callers are failing" alert on this counter alone: pair it with
    `introspect()` (a `state=Paused` node, or a name missing from the report) or
    with your own call-site error rate. What `rejected` *does* count are
    rejections of queries the engine had already accepted: a query parked for a
    node that is then paused or removed, one to a process that has since reached
    `Dead` or is shutting down, and one refused because the engine itself is
    closing.

Init, load and cleanup failures have their own counters and never show up as
query failures.

**Cancellations are not failures.** When the engine itself cuts an init or load
attempt short — the process is paused, removed, replaced by a graph swap, the
engine is closed, or `cancelInit` is called — it reports the attempt through
`onInitFailed` / `onLoadFailed` with an `AttemptCancelledException` (so a tracer
can close the attempt's span). The observer counts those in
`engine_process_init_cancellations_total` / `engine_process_load_cancellations_total`
and **not** in the `*_failures_total` counters, so an "init failures" alert does
not fire on a pause or a shutdown. A plain `CancellationException` thrown by your
own `init`/`load` is a **failure**: it is counted in `*_failures_total`, and a
process that keeps failing that way ends up with `engine_process_dead` = `1`.
(This differs from `engine_query_cancellations_total`, which is a subset of
`engine_query_failures_total`.)

**Leadership loss is not a process failure either.** An init or load attempt
that failed because this engine is no longer the leader of its log — a
`LeadershipLostException` anywhere in the cause chain: a fenced or deposed node,
including the one `onInitFailed` of a dropped re-init — is counted in
`engine_process_leadership_lost_total{name}` and **not** in
`engine_process_init_failures_total` / `engine_process_load_failures_total`, so
an "init failures" alert does not fire on every failover. It is a node-level
event, not a broken `init`: page on it (or on `isLeader`, below) separately.

**`engine_process_dead`** is `1` while the process is `Dead` because it failed —
its init ran out of its budget, its load gave up, it lost leadership, it asked
for an undeclared dependency — and `0` otherwise. A process that reaches `Dead`
because it was *stopped* (pause, removal, swap, close, `cancelInit`) reads `0`.
The gauge is registered when the process is first seen, goes back to `0` when
the process is **paused** (also a process that was `Dead` after a failure —
`introspect()` then says `Paused`) and when the name starts again (resume,
re-add, a respawning `newGraph`), and is dropped with the rest of the process's
meters on removal. `Engine.close()` does not reset it: after a close the gauge
keeps the last value it had, so a process that was `Dead` when the engine closed
still reads `1` in a registry that outlives the engine — until a **new engine**,
with a new observer on the same registry and the same common tags, reports that
process again: from then on the gauge follows the new engine (its value lives in
a holder shared per registry, not in the observer that happened to register the
gauge first, which Micrometer would otherwise keep it bound to; the new observer
finds the existing gauge instead of registering it again, so Micrometer logs no
"Gauge has been already registered" warning). The failure counters
stop moving once a process is `Dead` — nothing retries it — so a rate-based
alert on them goes quiet exactly when the process is down for good; alert on
the gauge instead. `Engine.introspect()` has the full per-process state if you
need more than this one flag; the observer only sees events, so it has no
mailbox-size or general state gauge.

**The lifecycle counters start at `0`.** `engine_process_init_failures_total`,
`engine_process_load_failures_total`, both `*_cancellations_total`,
`engine_process_leadership_lost_total`,
`engine_process_cleanup_failures_total`, `engine_process_reinit_failures_total`,
`engine_dedup_collapsed_total` and the `engine_process_dead` and
`engine_process_stale` gauges are registered as soon as the process is first seen
(its first state transition or attempt start), before anything is counted. That
matters for `increase()`: Prometheus needs a sample *before* the increment, and
a counter whose first scraped value is already `1` shows no increase at all — a
one-off failure would never fire the recipe below. `engine_watcher_stops_total`
is not per process, so it is primed once instead: one series per `reason`
(`process_removed`, `executor_shut_down`, `leadership_lost`, with the observer's
common tags) is registered at `0` when the observer is constructed, before
it is attached to an engine.

Alert recipe (PromQL):

```promql
# A process is down and will not come back on its own
max by (name) (engine_process_dead) == 1

# Init/load keeps failing (retries in progress) — cancellations are already excluded
sum by (name) (increase(engine_process_init_failures_total[10m])) > 0
sum by (name) (increase(engine_process_load_failures_total[10m])) > 0

# A node answers from a version it failed to replace (the re-init is retried or waits for a trigger)
max by (name) (engine_process_stale) == 1

# cleanUp threw or timed out (resources may have leaked)
sum by (name) (increase(engine_process_cleanup_failures_total[1h])) > 0

# This node lost leadership of the log (failover, fencing) — a node alert, not a process one
sum by (instance) (increase(engine_process_leadership_lost_total[10m])) > 0

# A watcher stopped for a reason other than its process being removed (fires on the first one)
sum by (instance, reason) (increase(engine_watcher_stops_total{reason!="process_removed"}[10m])) > 0
```

!!! warning "HA: a stepped-down node's `engine_process_dead` keeps its value"
    After a failover the old leader's engine is fenced or closed, and — as
    described above — its `engine_process_dead` series keep the **last value
    they had** (a process that died of the lost leadership reads `1`; one that
    was serving reads `0` and never updates again). Summed or `max`-ed across
    the cluster, these stale series look like a live process being down, or
    hide one that is. Scope the alerts: either alert per `instance` (the
    scrape target, or an `engine`/node common tag on the observer) and only on
    the node that leads — export `engine.introspect().isLeader()` as your own
    gauge — `introspect()` returns a `CompletionStage<EngineReport>`, and it
    throws once the engine is closed, so unwrap it and guard the closed case:

    ```java
    Gauge.builder("engine_is_leader", engine, e -> {
        try {
            return e.introspect().toCompletableFuture().join().isLeader() ? 1 : 0;
        } catch (RuntimeException closedOrFailed) {   // a closed engine throws IllegalStateException
            return Double.NaN;                          // or 0, if "closed" should read "not leader"
        }
    }).register(registry);
    ```

    and join on it: `max by (name) (engine_process_dead * on (instance) group_left engine_is_leader) == 1`
    — or drop the old node's series (remove the process, or restart it with a
    fresh registry) once it has stepped down. Page on `engine_is_leader`
    itself for "no node leads".

    Logs have the same scoping problem: the dispatcher's lines carry the
    `fom.engine` / `fom.process` [MDC keys](../concepts/process-lifecycle.md#observability),
    but those are set on the process dispatcher thread (and on the "warm/cold
    start for X" line) only. **Engine-level**
    `WARN`/`ERROR` lines — logged by `io.fom.Engine` or `io.fom.fsm.GraphMachine`
    on the calling or scheduler thread (a graph swap or control-plane write
    refused after leadership was lost, for example) — are outside the dispatcher
    MDC and carry no `fom.engine`. Tell nodes apart for those by host/instance
    (your log shipper's labels) and logger name, not by `fom.engine`.

!!! warning "The query meters are still registered lazily"
    `engine_query_failures_total{reason}`, `engine_query_cancellations_total` and
    all the timers are registered on their **first** recording (a `reason` series
    only exists once a query failed for that reason), so their first scraped
    value already includes that event and `increase()` / `rate()` over them miss
    the first occurrence of each series. For a "queries failing" alert use a
    ratio or a sustained rate rather than "any increase", or compare against
    absence: `engine_query_failures_total > 0 unless engine_query_failures_total offset 10m`
    fires for the ten minutes after a new series appears — the first failure
    of that `reason`, which `increase()` cannot see.

!!! note "No leadership gauge, no metric for a stalled log write"
    An instance that lost leadership keeps serving the state it has: a refused
    re-init reports one `onInitFailed` (counted once in
    `engine_process_leadership_lost_total`), then nothing — there is no
    `isLeader` meter. Under `RELEASE_FIRST`, a re-init whose `LogDead` append
    keeps failing retries in silence apart from its `ERROR` lines (under
    `KEEP_OLD` a failing append is a failed init attempt and ends in
    `onReinitFailed`). To page on them, poll
    `engine.introspect()` and export `isLeader` and each node's
    `lastException` as your own gauges, or alert on the `ERROR` log lines.

When a process leaves the graph for good (`onProcessRemoved`),
every meter *this observer* registered for that name is removed from the
registry, so dynamic process names don't pile up meters (meters registered by
anything else — another engine's observer included — are left alone). Late callbacks for a removed
process (queries still settling) don't re-create them; if a process with that
name is added back, recording resumes when it starts. A meter that several
process names share after a `MeterFilter` folded them (see
[cardinality](#cardinality-and-folding-process-names)) is removed only with the
last of those names. To tell a late callback
from a live one the observer remembers removed names, but only for a bounded
while: a name is forgotten 15 minutes after its removal, and at most the 10,000
most recent removals are kept (the oldest go first), so tenant-shaped names that
are removed and never added back cost bounded memory. Late callbacks trail a
removal by at most the query/cleanup timeouts — the engine reports the removal
only after the node was shut down and its parked queries failed — so they fall
well inside that window. Wire the registry to
Prometheus/OTLP/etc. as usual for Micrometer.

### Percentiles and histograms { #histograms }

The observer registers its timers **without** percentiles or a histogram, so
out of the box they export only `_count`, `_sum` and `_max` — enough for a
rate and a mean, not for a p99. Turn histograms on with a `MeterFilter` on the
registry, configured **before** the engine starts (filters apply when a meter
is registered):

```java
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

registry.config().meterFilter(new MeterFilter() {
    @Override
    public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
        if (!id.getName().startsWith("engine_")) return config;
        return DistributionStatisticConfig.builder()
                .percentilesHistogram(true)
                .build()
                .merge(config);
    }
});
```

Prometheus then gets `engine_query_duration_seconds_bucket{le=…}` series, and
a p99 panel per process is

```promql
histogram_quantile(0.99,
  sum by (le, name) (rate(engine_query_duration_seconds_bucket[5m])))
```

Buckets multiply the series count — by several dozen per timer per process
name — so see [Cardinality](#cardinality-and-folding-process-names), and narrow
the filter to the timers you chart (e.g. only `engine_query_duration_seconds`)
on a large graph.

### Several engines, one registry

Give **each engine an observer instance of its own**. The instance remembers
which process names were removed and ignores their late callbacks, so one
instance shared by two engines would silence engine B's `Subscriptions` once engine A
removed a process called `Subscriptions`.

When those engines record into one `MeterRegistry`, also give each observer a
distinguishing tag. The `(registry, Iterable<Tag>)` constructor adds its common
tags to every meter the observer registers:

```java
import io.micrometer.core.instrument.Tags;

var stations = new Engine(cfg, stationsLog, serDe, new MicrometerEngineObserver(registry, Tags.of("engine", "stations")));
var alerts   = new Engine(cfg, alertsLog,   serDe, new MicrometerEngineObserver(registry, Tags.of("engine", "alerts")));
```

Without such a tag both engines' meters for a process of the same name are one
and the same meter (their counts merge), and removing that process in one engine
would drop the meter the other one still records into. The keys `name` and
`reason` are the observer's own and are rejected as common tags. Two **live**
engines with identical common tags on one registry are not supported: besides
the merged counts, `engine_process_dead` follows whichever observer saw the
process last. Closing an engine and building a new one with the same tags is
fine — see `engine_process_dead` above.

### Cardinality and folding process names { #cardinality-and-folding-process-names }

Every process name is a `name` tag value, so it costs a set of meters of its own:
**10** as soon as it is first seen (eight lifecycle counters and the dead and
stale gauges), **16** once each of the six timers has recorded, and up to **22** with
`engine_query_cancellations_total` and `engine_query_failures_total` for each of
its five reasons — and each timer is itself several series in Prometheus
(`_count`, `_sum`, `_max`, plus buckets if you enable histograms). With
tenant-shaped names (`t1/subscriptions`, `t2/subscriptions`, …) that multiplies by the number
of tenants.

To keep that bounded, fold the names with a `MeterFilter` on the registry,
configured **before** the engine starts (Micrometer applies filters at
registration):

```java
import io.micrometer.core.instrument.config.MeterFilter;

// t1/subscriptions, t2/subscriptions, ... all record into name="any/subscriptions"
registry.config().meterFilter(
        MeterFilter.replaceTagValues("name", n -> n.replaceFirst("^[^/]+/", "any/")));
```

The observer is safe with such a filter: it counts, per registered (post-filter)
meter, how many of its process names map to it, and `onProcessRemoved` removes a
meter only when no remaining process uses it — removing tenant `t1` leaves
`any/subscriptions` in place for `t2`. The folded `engine_process_dead` reads `1` while
**any** of the folded processes is dead after a failure; it is registered once,
for the first name, and every further name that the registry's filters map to
it joins it rather than registering it again (so Micrometer logs no "Gauge has
been already registered" warning). Counts and timings
merge across the folded processes, which is the point; keep per-name detail in
logs or `introspect()`.

!!! note "No exemplars on fom's timers"
    The timers are recorded off the caller's thread — query timings on an engine
    virtual thread named `fom-observer`, init/load/cleanup timings on the
    process's own threads — so no tracing context (an OpenTelemetry or
    Micrometer Tracing span) is current when they are recorded. Registries that
    attach exemplars (Prometheus with a `SpanContext`) therefore attach **none**
    to fom's histograms. To go from a slow query to its trace, use the
    `fom.query` spans of [`fom-otel`](#tracing--fom-otel) and their durations.

## Tracing — `fom-otel`

`OtelEngineObserver` emits OpenTelemetry spans for `init`, `load`, `query`, and
`KEEP_OLD` re-inits.
A `fom.query` span is a child of the caller's current span (it is started on the
submitting thread), and it ends when the reply settles — including a caller-side
timeout — so no span stays open. A dependency query sent from `compute` via
`ctx.query` gets a `fom.query` span that is a child of the span of the query it
serves (matched by `parentQueryId`), so the whole fan-out stays in the caller's
trace instead of starting orphan root spans. That holds even when the dependency
query is sent **after** the query it serves has settled — from an async
continuation of `compute` — as long as it is sent within 5 minutes: the observer
remembers the span context of ended query spans for that long (at most the
10,000 most recent), so the late child still nests under its parent. A child
sent later than that starts a new trace. The OTel context is **not**
propagated into `compute`, though: inside `compute`, `Span.current()` is not the
`fom.query` span, so a span you create there yourself starts a new trace
instead of nesting under the query. When a process leaves the graph
(`onProcessRemoved`) the observer forgets any init/load start it was still
holding for it, so removing processes mid-attempt leaves nothing behind; no span
is emitted for that — the engine reports an attempt cut short as `onInitFailed`
/ `onLoadFailed`, which already produces the `ERROR` span:

```java
import io.fom.otel.OtelEngineObserver;

var engine = new Engine(cfg, backend, serDe,
    new OtelEngineObserver(openTelemetry.getTracer("io.fom")));
```

| Span | Emitted | Attributes |
|---|---|---|
| `fom.init` | once per completed init (status unset) and once per failed attempt (`ERROR`, with the exception recorded) | `process.name`; `sid.clock` (completed); `init.attempt` (failed) |
| `fom.load` | likewise for `load` | `process.name`; `sid.clock` (completed, and on a failure when the Sid is known); `load.attempt` (failed) |
| `fom.reinit` | once per `KEEP_OLD` re-init, from `onReinitStarted` to the promotion (status unset) or to `onReinitFailed` (`ERROR`, with the exception recorded); none when a pause, removal or `close()` cuts it short | `process.name`, `sid.serving.clock`; `sid.new.clock` (completed) |
| `fom.query` | one per query, from submission until the reply settles; `ERROR` with the failure reason as the status description | `process.name`, `query.id` (UUID string), `query.type` (message class name) |

All spans are `SpanKind.INTERNAL`. Init, load and re-init spans are built after
the fact from the terminal callback (start timestamp from `onInitStarted` /
`onLoadStarted` / `onReinitStarted`), so no span is open while an attempt runs.

!!! note "A dropped re-init shows up as a near-zero-duration ERROR `fom.init` span"
    A **dropped re-init** (`RELEASE_FIRST`) reports `onInitFailed` with no matching
    `onInitStarted` ([above](#engineobserver)), so the observer has no start
    timestamp to give the span. It handles the missing start without throwing,
    but the span it emits then starts at the failure: an operator sees a
    `fom.init` span with status `ERROR`, `init.attempt = 1`, a
    near-zero duration and either a `LeadershipLostException` (the engine's "no
    longer the leader of the log", or a fenced backend's own message) or an
    `IllegalArgumentException` from a backend that can never store the
    `LogDead` (`RELEASE_FIRST`) — for a process that never began an init. You get one such span per
    stuck state, not one per trigger. Read it as "the
    re-init was dropped and the node kept serving stale state", **not** as "an
    init ran and failed": the process stays `Serving` and goes on answering
    queries from the state it already had. It is a signal to route traffic away
    from this node (its `isLeader` is `false`), not to look for a slow or broken
    `init`.

## Combining observers

The engine takes exactly one observer, so use the built-in composite to run
metrics *and* tracing *and* your own logging side by side:

```java
import io.fom.api.EngineObserver;

var engine = new Engine(cfg, backend, serDe,
    EngineObserver.composite(
        new MicrometerEngineObserver(registry),
        new OtelEngineObserver(openTelemetry.getTracer("io.fom")),
        myAuditObserver));
```

Every callback is forwarded to each delegate in order. A delegate that throws
never stops the ones after it — the failure is logged (WARN on the 1st, 2nd,
4th… failure of a callback, DEBUG in between), as the engine guards a single
observer. The composite does that counting itself, with one counter per callback
for the whole engine — not per process, as the engine's own guard counts the
lifecycle and query callbacks. `composite()` with no observers returns `EngineObserver.NOOP`,
and with exactly one returns that observer unchanged; a `Collection` overload
exists too.

Prefer it over a hand-written fan-out: the composite overrides every method of
the SPI, so it keeps forwarding everything when the SPI grows, while a
hand-written one silently no-ops the callbacks it forgot.

## Point-in-time — `introspect()`

```java
EngineReport report = engine.introspect().toCompletableFuture().get();

report.instanceId();          // this engine's instance id
report.isLeader();            // is this instance currently the log leader?
for (var node : report.graph().nodes()) {       // List<NodeReport>
    System.out.printf("%s: state=%s sid=%s retries=%d/%d last=%s%n",
        node.name(), node.state(), node.sid(),
        node.initRetries(), node.loadRetries(), node.lastException());
}
report.graph().mailboxSizes();   // Map<String,Integer> per-process mailbox depth
report.log();                    // LogBackendReport: length, currentLeader, counts, last ts
```

`EngineReport` is an immutable record — ideal for a `/debug` endpoint or a
health check. Each `NodeReport` carries:

- `name`, `sid` (`null` until the process has state; during a `KEEP_OLD` re-init
  it is the version still answering, during a `RELEASE_FIRST` one `null` until
  the new Sid is promoted), `state` (FSM state name, `Serving` throughout a
  `KEEP_OLD` re-init; `Paused`; or `Starting` for a node still waiting to start — e.g. for its
  dependencies, or behind one that failed; this also covers nodes a failed or
  interrupted start left unstarted, which wait for a retried `newGraph` — a
  trigger does not start them). Every graph node is listed exactly
  once, also while its FSM is being replaced. A `Paused` node reports the Sid
  of the live state it keeps (what a resume warm-loads). A node paused mid
  `KEEP_OLD` re-init reports the version that was serving: resume warm-loads
  it and finishes the re-init (it loads a candidate that was already written,
  without a second init — unless the node runs under `RELEASE_FIRST` by then:
  the candidate is dropped and the node cold-inits). A node paused **mid `RELEASE_FIRST` re-init** reports
  `null`: its old state is already retired (`LogDead`) and a resume
  re-initialises it — the same a restart would show. One paused while that
  re-init's new state was already loading, or just as it completed, reports
  that new Sid;
- `replacement` — `"Initializing"` or `"Loading"` while a `KEEP_OLD` re-init
  makes the new version beside the serving one, otherwise `null`;
- `stale` — the serving version was asked to be replaced and is not yet: a
  re-init is in flight, queued (including one waiting in the dedup window or
  replayed at start), or failed (and waiting for its automatic retry, or for
  the next trigger or restart when the cause was permanent). A node paused
  while its already-written new version was loading is **not** `stale`:
  resume loads that version without an `init`;
- `initRetries` / `loadRetries` — failed `init` / `load` attempts in the
  process's latest (re)initialisation;
- `lastException` — the **last** init/load failure this node ever hit, as
  `"fully.qualified.Class: message"`, or `null` if it never hit one. It is
  **not cleared when the node recovers**: a fresh start resets the retry counters
  and deliberately keeps this text, so read it as "the last thing that went
  wrong here", never as "the current problem". That holds as long as the same
  process instance recovers — a retried `init`/`load`, or a re-init of a node
  that kept serving. It does **not** hold across a replacement of the node's
  FSM: `lastException` lives on the FSM instance, so a `Dead` node that is
  **restarted by a trigger**, or respawned by a `newGraph` retry (or replaced by
  a graph swap), comes back on a *fresh* FSM and reports `lastException=null`
  again. So a `null` here means "nothing has failed on the FSM serving this node
  now", not "this node has never failed" — the log keeps the history. `initRetries` / `loadRetries`
  back at `0` while `state` is `Serving` is *consistent with* the node having
  recovered, but it does **not** prove it: a re-init stalled by a transient
  write outage (below) has the same signature — `state=Serving`, retries at `0`
  — precisely because no init attempt ever started. What tells the two apart is
  `lastException` (does it name a log-write failure, and is the `sid` still the
  old one?) together with the engine's `ERROR` lines for the retry loop. Measured: a node whose first `init` attempt threw and whose second
  succeeded reports `state=Serving, initRetries=1,
  lastException="java.lang.IllegalStateException: feed unavailable (attempt
  1)"`; after a later, fully clean trigger-driven re-init it reports
  `initRetries=0` with the **same** `lastException`. After `cancelInit` it reads
  `"io.fom.api.InitInProgressException: Init of process '<name>' was
  cancelled"`. A node that died because another instance took the log over while
  its init or load result was being appended reads
  `"io.fom.api.LeadershipLostException: Lost leadership for <name> during init"`
  (or `load`) — **if** the backend refused the append; a backend that reports the
  takeover by throwing (a fenced `PostgresLogBackend`) puts its own message there
  instead ("… no longer holds the advisory lock for &lt;table&gt; …"), so match
  the exception type rather than one exact string — see
  [Exceptions](../reference/exceptions.md#leadership). A node whose `state` is
  still `Serving` can carry
  `"io.fom.api.LeadershipLostException: cannot re-initialise <name>: no longer
  the leader of the log"`: a re-init the log refused, so the node keeps serving
  state that is known to be stale. Under `KEEP_OLD` the re-init that first
  found the log taken over leaves `"…LeadershipLostException: Lost leadership
  for <name> during init"` (or `load`) on a node that is still `Serving`, with
  `stale=true`.

Under `RELEASE_FIRST`, a re-init stalled by a **transient** log-write outage
looks almost the same but is a different case. While the backend keeps failing
the `LogDead` append with,
say, an `UncheckedIOException`, the engine retries with backoff (attempts 1, 2,
4, 8, … logged at `ERROR`, the rest at `DEBUG`) and the node keeps serving its
old state. `introspect()` then shows `state=Serving`, the **old** `sid`,
`initRetries=0` — no init attempt has started — and `lastException` naming the
write failure. Read that combination as "this node is serving state it was asked
to replace". Unlike the permanent refusal above, this one fires no
`onInitFailed` (the retry loop is still running), so the log and
`lastException` are the only places it shows. Under `KEEP_OLD` nothing is
written before the new init, so a failing append of its `LogInitialized` is an
ordinary failed init attempt: `initRetries` counts it, and when the budget runs
out the node reports `stale=true` and `onReinitFailed` fires.

!!! note "No time-in-state: a stuck init and a slow one look the same"
    `NodeReport` has no "in this state since" field — only `state`, the retry
    counters and `lastException`. From `introspect()` alone you cannot tell an
    `init` that hangs from one that is just slow. Track it yourself from
    `EngineObserver.onStateTransition` (or `onInitStarted`; a `KEEP_OLD`
    re-init fires no transition, so use `onReinitStarted`) if you need to
    alert on it. A hang does not last forever, though: an init or load that
    blocks **before returning its stage** — a synchronous call without a
    timeout — is ended at its budget by a scheduler-side watchdog
    ("… had not even returned its stage") and then retries or goes `Dead`
    like any other timeout (see
    [Process lifecycle](../concepts/process-lifecycle.md#retries-backoff-timeouts)).

`mailboxSizes()` counts the envelopes waiting in each process's **mailbox**
only. Queries a node **stashed** while it initialises, loads or re-initialises
under `RELEASE_FIRST` (a `KEEP_OLD` re-init stashes nothing)
(see [Queries that wait for a node](#queries-that-wait-for-a-node)) have already
left the mailbox and are not counted, so a node with hundreds of queries
waiting for its init can report `0`; `Paused` and `Starting` nodes always
report `0`. To see that backlog, count it from the observer (`onQuerySent`
minus `onQueryCompleted` / `onQueryFailed` per process).

`LogBackendReport` gives `length`, `currentLeader`, `eventCounts`, and
`maxTimestampMillis`.
