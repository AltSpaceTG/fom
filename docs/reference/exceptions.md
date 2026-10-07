# Exceptions

All FOM exceptions are unchecked (`RuntimeException` subclasses). They fall into
three groups: ones **you throw** from your process code to signal a phase
failure, ones **the engine throws back** to a caller, and ones from the
serialization / typed-cell helpers.

## Thrown by your process code

| Exception | Package | Throw it from | Effect |
|---|---|---|---|
| `InitializationException` | `io.fom.api` | `ProcessInitializer.init` | the engine backs off and [retries](../concepts/process-lifecycle.md) until `initTimeout` |
| `LoadException` | `io.fom.api` | `ProcessLoader.load` | attempted up to `maxLoadRetries` times in total (default `1`: no retry), then falls back to `init` |

Throwing these (or any exception) from `init`/`load` drives the retry/fallback
machinery. A `compute` that fails completes the query's stage exceptionally with
your exception.

### One init or load failure that is never retried { #undeclared-dependency-in-init }

`ctx.query(name, …)` for a name the node does not declare as a dependency throws
`UndeclaredDependencyException`. When that happens during `init` **or `load`**,
the engine recognises it as a **programming error** rather than a transient
fault: no amount of backing off will make the graph grow the dependency. So the
attempt is not retried — no init retry, no load retry, and no fallback from a
failing `load` to a fresh `init` — and the process goes `Dead` immediately (in a
`KEEP_OLD` re-init only the re-init gives up, and the old version keeps
serving; see [below](#re-init-failures)). The exception names both the requested name and the dependencies the node *did*
declare.

It is recognised even when the node's own code **wraps and rethrows** it (the
engine walks the cause chain). What each party sees then differs:

| Who | Receives |
|---|---|
| `newGraph` (and anything else awaiting the node's start) | the bare `UndeclaredDependencyException` |
| `onInitFailed` / `onLoadFailed`, `NodeReport.lastException` | what the attempt actually threw — **your wrapper** when you wrapped it |
| a query that was waiting for the node | `QueryRejectedException` ("&lt;name&gt; is Dead") with the `UndeclaredDependencyException` as its cause |

The verdict belongs to the node that made the typo, not to its consumers. The
exception records **who asked**: `requester()` names the process whose own
`ctx.query` named the undeclared dependency (constructor
`UndeclaredDependencyException(String requester, String message)`), and the
engine treats it as permanent **only for that process**. Another process's typo
can reach a consumer too — inside the `QueryRejectedException` it gets when that
dependency dies, or simply as the answer of the dependency's `compute`, which
asked for an undeclared name while serving the query, with no rejection around
it. Either way that is **not** permanent for the consumer: its `init`/`load`
fails and is retried like after any other failed dependency query, and it
recovers once the dependency is fixed (within its budgets).

Before, the same mistake was retried until `initTimeout` ran out and
surfaced as an `InitializationTimeoutException` — a slow failure whose message
pointed at the budget instead of at the typo.

## Thrown back to a caller

| Exception | Package | When |
|---|---|---|
| `InitializationTimeoutException` | `io.fom.api` | the total `initTimeout` budget elapsed across all `init` retries, or `load` kept failing on freshly initialised state within that budget; the process is now `Dead` (a
`KEEP_OLD` re-init that runs out of its budget fails no caller: the old version
keeps serving, see [Re-init failures](#re-init-failures)). An `init` or `load` that asked for an undeclared dependency is **not** reported this way — it fails at once with `UndeclaredDependencyException` [instead](#undeclared-dependency-in-init) |
| `InitInProgressException` | `io.fom.api` | the target's init/load was cancelled while a query waited for it — message "Init of process '&lt;name&gt;' was cancelled" (a query to a process that is still starting waits instead). The waiting **query** does not get it directly: it fails with `QueryRejectedException` carrying it as its cause — "&lt;name&gt; is Dead" after a user's [`cancelInit`](../concepts/process-lifecycle.md#cancelling-an-init), "&lt;name&gt; is shutting down" when a pause or removal cancelled the start. It is thrown directly by a `newGraph` blocked on a node's first init, and by a `resume`/`resumeTenant` blocked on a node it is resuming, when an accepted `pause`/`remove` (or `pauseTenant`/`removeTenant`) of that node [cancels the start](../concepts/graph-swap.md#concurrency) |
| `QueryException` | `io.fom.api` | routing failure — no route for the message type ("No route for type &lt;class&gt;", which is also what a static `.handles` route gives once its node was removed with it), or a `Routable` message / a dynamic `.route(...)` resolver that returned a blank name or one not in the graph |
| `UndeclaredDependencyException` | `io.fom.api` | a subclass of `QueryException`: `ctx.query(name, …)` for a name the calling process did **not** declare as a dependency — "No such dependency: &lt;name&gt;; declared: [&lt;names&gt;]". The check is against the *declared* dependencies, so a name that is not in the graph at all gives the same message. Thrown synchronously out of `ctx.query`; in `init` or `load` — even wrapped by your code — it also [fails the process at once](#undeclared-dependency-in-init), but only for the process that asked (`requester()`); one that reaches a consumer from a dependency — inside its `QueryRejectedException` or as its `compute`'s answer — is retried like any dependency failure |
| `QueryRejectedException` | `io.fom.api` | `queryProcess(name)` / `queryProcess(ProcessRef)` for a name not in the graph ("Unknown process: '&lt;name&gt;'"); the target is paused, `Dead`, or really stopping ("&lt;name&gt; is shutting down" — a pause, a removal or `close()`, **not** a re-init — also for a query waiting on a start that a pause or removal cancelled, with the `InitInProgressException` as cause); its mailbox refused the message; the node was removed or paused, or the engine closed, while the query waited for it; or the query was still computing when the cleanup budget ran out ("&lt;name&gt; is shutting down: the query outlived the cleanup timeout" — this one happens on **any** cleanup, a triggered re-init and a graph swap that replaces the node included, not only on pause/removal/`close()`) |
| `TenantAccessDeniedException` | `io.fom.tenant` | the [tenant wrapper](../guides/multi-tenancy.md) denied the call (unauthorized, or a non-`Routable` `query`) |
| `TimeoutException` | `java.util.concurrent` | the query reached its deadline (`queryTimeout` or the per-call timeout) — message "Query to '&lt;name&gt;' did not complete within &lt;timeout&gt;", or "&lt;name&gt; did not start before the query deadline" for a query still waiting for a node to start; a nested `ctx.query(dep, …)` that runs out the deadline it inherited from its calling query reports "Query to '&lt;dep&gt;' did not complete within the deadline inherited from its calling query" — whichever deadline fires first wins |
| `IllegalStateException` | `java.lang` | `engine.query`/`queryProcess` on a closed engine, also through `TenantAwareEngine` — "Engine &lt;id&gt; is closed"; also a `remove` / `removeTenant` / node-dropping `newGraph` that applied the graph change but could not retire a removed process's state because the **log write failed** — "… could not retire the state of [...]: the log write failed", with that write's exception as the cause. A *refused* write gives the `LeadershipLostException` subclass instead: the distinction (make the log writable again vs. replace the engine — the removal itself can never be retried) is [below](#leadership-half-applied) |

`QueryRejectedException` and `InitInProgressException` can be **transient**: a
paused or `Dead` process rejects queries until it resumes or is restarted (a
query that raced a pause or removal gets "&lt;name&gt; is shutting down", not "is
Dead"). Retry those with a short backoff. "Unknown process" for a name that really is not in
the graph is permanent. Several cases that used to reject now **wait** instead,
so they need no retry: a query racing a re-init is answered by the old version at
once (`KEEP_OLD`) or stashed and answered from the new state (`RELEASE_FIRST`), a query to a node being replaced by a graph swap is handed to the new
FSM, a query to a node that has not started yet waits, and so does one sent right
after [`trigger`](../concepts/process-lifecycle.md#restarting-a-dead-process)
restarted a `Dead` process.

For `engine.query`/`queryProcess` these surface as the failure of the returned
`CompletionStage` — including `QueryException` from a bad route and the
closed-engine `IllegalStateException`; only a `null` argument throws
synchronously. When awaiting a stage with
`.get()`, they arrive wrapped in `ExecutionException` — unwrap with
`getCause()`:

```java
try {
    engine.query(msg).toCompletableFuture().get();
} catch (ExecutionException e) {
    if (e.getCause() instanceof QueryException qe) { /* bad route */ }
}
```

## Reported to observers only { #observer-only }

| Exception | Package | When |
|---|---|---|
| `AttemptCancelledException` | `io.fom.api` | a subclass of `java.util.concurrent.CancellationException` passed to `onInitFailed` / `onLoadFailed` when the **engine itself** cut an init or load attempt short — `cancelInit`, a pause, a removal, a graph swap or `close()` ("&lt;name&gt; init attempt N cancelled" / "… load attempt N cancelled"). Use it to tell an operator's stop from a real failure. A plain `CancellationException` thrown by *your* `init`/`load` is **not** this: it is an ordinary failed attempt, retried and counted against the budget. See [Cancelling an init](../concepts/process-lifecycle.md#cancelling-an-init) |

## Re-init failures { #re-init-failures }

With `KEEP_OLD` (the default) a re-init throws nothing at any caller: queries
keep being answered by the old version. When the re-init gives up,
`EngineObserver.onReinitFailed(name, keptSid, cause)` receives the cause, the
`WARN` "re-init gave up; keeps serving …" names it, `NodeReport.lastException`
holds it and `NodeReport.stale` is `true`.

| `cause` | When | Retried by itself |
|---|---|---|
| `InitializationTimeoutException` | the new version's `init` kept failing — any exception or error, `OutOfMemoryError` included — until `initTimeout` ran out, or its `load` kept failing on freshly initialised state within that budget | yes, after `reinitRetryBackoffMin`, doubling up to `reinitRetryBackoffMax` (`Duration.ZERO` turns this off) |
| `UndeclaredDependencyException` | the new version's `init` or `load` asked for an undeclared dependency | no |
| `LeadershipLostException` | the log refused the new version's `LogInitialized` or `LogLoaded`, or the backend threw this | no; reported once per serving Sid, later re-init requests are dropped silently and the node stays stale (see [below](#leadership)) |
| `IllegalArgumentException` | the backend refuses that event outright (over its payload limits, say) | no |
| `InitInProgressException` | [`cancelInit`](../concepts/process-lifecycle.md#cancelling-an-init) cancelled the re-init | no; a request queued behind it starts at once. `cancelInit(name)` also cancels a retry that is only scheduled — the node stays stale |

A permanent failure waits for the next trigger, reactive change or restart; a
trigger also starts a new re-init at once while an automatic retry is still
pending. With `RELEASE_FIRST` the old version is retired first, so a failed
re-init ends `Dead` like a failed first start, and the exceptions above reach
queries waiting for it through `QueryRejectedException`. See
[When a re-init fails](../concepts/process-lifecycle.md#when-a-re-init-fails).

## Leadership — `LeadershipLostException` { #leadership }

"I am no longer the leader of my log" is **one** exception type everywhere:
`io.fom.api.LeadershipLostException`, a subclass of `IllegalStateException`. It
is always thrown synchronously, and the refused write itself never landed — so
catch it once and handle every case the same way:

| Thrown by | Message |
|---|---|
| `trigger(name, value)` / `trigger(Map)` whose `LogTrigger` append is refused | "Engine &lt;id&gt; is no longer the leader of &lt;logId&gt;; the trigger for [...] was not recorded" |
| `pause` / `remove` / `newGraph` — also what `pauseTenant` / `removeTenant` surface | "Engine[&lt;id&gt;] lost leadership while persisting &lt;event&gt;" / "… while persisting graph" / "GraphMachine[&lt;id&gt;] lost leadership while persisting &lt;event&gt;" |
| `resume` / `resumeTenant` whose `LogResumed` append fails — the node is stopped again and stays paused (see below) | when the backend **refused** the append: "GraphMachine[&lt;id&gt;] lost leadership while persisting LogResumed for &lt;name&gt;; it stays paused" / "…; already resumed: [...]"; when it **threw** (a fenced Postgres leader), the backend's own exception is rethrown unchanged — "PostgresLogBackend &lt;logId&gt; no longer holds the advisory lock …", with no node name and no "already resumed" list |
| `purgeArchives(keepHistory)` on a deposed leader — the archives belong to whoever leads now | "Engine &lt;id&gt; is not the leader of &lt;logId&gt; (&lt;other&gt; is); no archive was removed" |
| `remove` / `removeTenant` (or a `newGraph` that drops nodes) that swapped the graph but could not retire the removed processes' state **because the `LogDead` was refused, or the backend threw `LeadershipLostException`** (any other reason gives an `IllegalStateException` instead) — see [below](#leadership-half-applied) | "GraphMachine[&lt;id&gt;] applied the graph change but could not retire the state of [...]: no longer the leader of the log" — with the backend's own exception attached as the **cause** when there was one |
| `pause` / `pauseTenant` interrupted part-way — see [below](#leadership-half-applied) | "… lost leadership while persisting LogPaused; nothing was paused" / "…; already paused: [...]" |
| any write on a [fenced](../guides/multi-node.md#failover) Postgres leader | "… no longer holds the advisory lock … leader session was lost; another instance may own the log" |
| `LogBackend.compact` — so `engine.snapshot()` — when the log's latest `LogLeader` names another instance | "Cannot compact &lt;logId&gt;: instance &lt;id&gt; is no longer the leader (&lt;other&gt; is); nothing was written" |
| a process's own `init`/`load` result being appended | "Lost leadership for &lt;name&gt; during init" / "… during load" — but only when the backend **refused** the append; a backend that reports the takeover by *throwing* (the fenced Postgres case above) surfaces its own message instead |

Leadership is never regained by retrying: close the engine and open a new one to
compete for it. Reads keep working from the state already in memory
(`engine.query` still answers), but no durable change can be recorded again. A
[watcher](../concepts/triggers-and-watchers.md#watchers) whose trigger hits it
stops itself with one `WARN`, `Dead` watched process included.

Where the failed process ends up depends on the path:

- a node whose **own** cold/warm start could not append its result goes `Dead`,
  and `NodeReport.lastException` names it instead of being `null`. Two texts are
  possible, so match on the **type**, not on one string: the engine synthesises
  `"io.fom.api.LeadershipLostException: Lost leadership for <name> during init"`
  (or `load`) when the backend *refused* the append, while a backend that reports
  the takeover by *throwing* passes its own message through — e.g.
  `"io.fom.api.LeadershipLostException: PostgresLogBackend <logId> no longer
  holds the advisory lock for <table> — leader session was lost; another instance
  may own the log"`;
- a node being **resumed** does **not** die. `resume` / `resumeTenant`
  throws a `LeadershipLostException` in one of two shapes, so match on the
  **type**, not on one string. When the backend *refused* the append, the
  engine's own message names the node and, when earlier nodes of the same call
  did resume, those too ("…; already resumed: [...]"). When the backend reports
  the takeover by *throwing* — a [fenced](../guides/multi-node.md#failover)
  `PostgresLogBackend` does this rather than refusing — that exception is
  rethrown as is: `"PostgresLogBackend <logId> no longer holds the advisory lock
  for <table> — leader session was lost; another instance may own the log"`,
  naming neither the node nor what already resumed (`engine.introspect()` shows which nodes are still `Paused`).
  Either way the node that failed is stopped again and stays `Paused` with its last Sid
  and `lastException == null`, still rejecting queries with
  `QueryRejectedException` ("Process '&lt;name&gt;' is paused"). Retry the
  resume: by this engine if appends start working again, or by whoever leads
  next.

    The `LogResumed` itself never landed, but the node's state in the log is
    only *usually* untouched. A resume that **cold-initialised** the node runs
    its `init` before it writes `LogResumed`, so a fresh
    `LogInitialized`/`LogLoaded` for a **new Sid** is already in the log when
    the refusal comes, and this instance can no longer write and so cannot
    retire it; `NodeReport.sid` for the paused node still reports the **old**
    Sid. Nothing serves that orphaned state — the node is paused and a retried
    resume cold-initialises it again — but do not read the log as if the resume
    had left no trace;

    Any append that **throws** instead of being refused is handled the same
    way — the node is stopped again and stays paused — and the backend's own
    exception reaches the caller: a `LeadershipLostException` for a fenced
    Postgres leader, or whatever the backend throws for an I/O or SQL error;
- leadership lost *after* a trigger was accepted throws nothing at all: the
  process keeps serving the state it has. Under `KEEP_OLD` the re-init gives up
  (`onReinitFailed` with the `LeadershipLostException`, not retried, reported
  once per serving Sid) and later requests are dropped silently; under `RELEASE_FIRST` the
  [re-init is dropped](../concepts/process-lifecycle.md#re-initialisation) with
  an `ERROR`.

### Two operations that report what they half-applied { #leadership-half-applied }

Both of these apply per process, so the exception says how far it got instead of
pretending nothing happened:

- **`remove` / `removeTenant`** swaps the graph first and retires the
  removed processes' state afterwards. If the graph change went in but the state
  could not be retired, the call throws and **names the processes whose state it
  could not retire**. The graph change stands — those processes are gone from
  this engine — but their state is still live in the log, so whoever leads next
  will warm-load it if the name comes back.

    *Which* exception you get says what to do about it — the two are not
    interchangeable:

    | What went wrong | Exception & message | Remedy |
    |---|---|---|
    | the `LogDead` append was **refused**, or the backend reported the takeover by **throwing** `LeadershipLostException` | `LeadershipLostException`: "GraphMachine[&lt;id&gt;] applied the graph change but could not retire the state of [...]: no longer the leader of the log", with the backend's exception as the cause when there was one | leadership is never regained: close this engine and open a new one |
    | the append **failed for another reason** — an I/O error, a connection blip — which may well be transient | `IllegalStateException`: "GraphMachine[&lt;id&gt;] applied the graph change but could not retire the state of [...]: the log write failed", with that exception as the cause | make the log writable again; the state left behind is dealt with below |
    | the FSM **never got as far as recording the retire**: the shutdown wait ran out before it reached its `LogDead`, so whether the state was retired is simply unknown | reported the same way as a failed write, with a cause reading "the shutdown of &lt;name&gt; did not finish within &lt;budget&gt;, and it had not recorded the state as retired" | treat the state as **not** retired; same as above |

    A shutdown that merely **overran its cleanup budget** is *not* reported here
    at all: the `LogDead` is appended *before* `cleanUp` starts, so a slow
    `cleanUp` never puts the state at risk. Only a shutdown that ran out before
    the FSM recorded the retire is reported — the message says exactly that.

    With several removed processes failing at once, a lost leadership wins: one
    process whose append was refused makes the whole call a
    `LeadershipLostException`, since no further write can succeed anyway.

    **Do not retry the removal** — it cannot undo what was half-applied. The
    graph change already applied, so the name is no longer in the current graph,
    and the two entry points then behave differently:

    - `Engine.remove(names)` takes the names you pass, so a retry with
      the same set is refused with `IllegalArgumentException: Unknown process:
      '&lt;name&gt;'` before it could retire anything (removal is
      [not idempotent](#other-standard-exceptions-you-may-see)).
    - `TenantAwareEngine.removeTenant(caller, tenant)` re-derives the tenant's
      process names from the **current** graph every time it is called, and that
      graph no longer contains them — so a retry finds nothing to remove, does
      nothing and simply returns an **empty set**. It does *not* retire the state
      the failed call left behind, so a quiet empty result is not a repaired
      removal.

    What you can actually do:

    - **Accept the graph change.** The process is gone from this engine's graph;
      queries and triggers for the name now fail as for an unknown process.
    - **Fix the cause.** For a lost leadership, nothing on this engine can write
      again: close it and open a new one. For a failed write, make the log
      writable again (disk space, the connection, the file's directory).
    - **Then decide about the state left in the log.** It stays there. If the
      name comes back in a later graph, that is exactly what you want: whoever
      leads the log next warm-loads it. If it will not come back, drop it with a
      **compaction** — a compaction keeps only the state of processes in the
      *latest* graph, so it is what removes the orphan: `Engine.snapshot()` on
      the engine that leads the log once writes work again, or offline with
      `LogCompaction.compact(backend)` / [`fom-log compact`](../guides/cli.md)
      while no engine is running. See [Snapshots](../concepts/snapshots.md).
- **`pause` / `pauseTenant`** writes one `LogPaused` per process. A
  refusal part-way through throws and **names what it had already paused**
  ("already paused: [...]", or "nothing was paused"); this engine can neither
  finish the pause nor undo it.

## Serialization & typed cells

| Exception | Package | When |
|---|---|---|
| `SerDeException` | `io.fom.serde` | a [`SerDe`](../guides/serialization.md) failed to (de)serialize — bad bytes, a filter rejection, an unexpected type |
| `CodecException` | `io.fom` | a [`Codec`](../guides/configuration.md#typed-property-cells) failed to encode/decode a typed value |
| `NoSuchPropertyException` | `io.fom` | `Properties.get(key)` for a key that isn't present in the cells |

## Other standard exceptions you may see

- **`IllegalArgumentException`** — invalid graph (cycle, missing dependency,
  route to unknown process), a `remove`/`pause`/
  `resume` naming a process that is not in the current graph — an
  already removed one included, so removal is **not idempotent** ("Unknown
  process: '&lt;name&gt;'"), the same message thrown **synchronously** by
  `trigger(name, value)`, `trigger(Map)` and `watch(watcher)` for a name that is
  not in the graph (`trigger(Map)` checks every name before it writes anything,
  so nothing is recorded and nothing is applied), a `remove`/`Graph.without` that would take
  every process out ("Removing [...] would leave an empty graph" — close the
  engine instead), invalid `EngineConfig` (non-positive duration,
  `maxLoadRetries < 1`), or a bad SQL identifier in `fom-jdbc` (including a
  reserved SQL keyword as a table name).
- **`IllegalStateException`** — using a closed `Engine`/backend (a query
  gets it as a failed stage, see above), a second process trying to take a
  backend's leadership lock, `pause` / `remove` (and so
  `pauseTenant` / `removeTenant`) naming a process, a `newGraph` / `updateGraph`
  that removes or redefines it, or `close()`, called **on that process's own
  dispatcher** — i.e. synchronously from an `EngineObserver` callback about it
  ("… called on that process's own dispatcher (from an EngineObserver callback
  about it) …"; thrown at once, nothing changed; `close()` there just returns if
  the engine is already closing — see
  [Stopping a process from a callback](../guides/observability.md#stopping-from-a-callback)), `Engine.close()` while a `newGraph` (the first install or a
  later swap) is still starting ("Engine closed while 'X' was starting"), a DI supplier
  resolving before its container was registered, or a graph change that
  [could not retire a removed process's state](#leadership-half-applied) because
  the log write failed or the process's shutdown did not finish in time. Losing
  leadership is the `LeadershipLostException` subclass [above](#leadership).
- **a plain `java.lang.RuntimeException`** — a node that did not reach `Serving`
  within `initTimeout + loadTimeout` fails the call that started it
  (`newGraph`, a graph swap, `resume`): "Node '&lt;name&gt;' did not
  reach Serving within PT1M (state=&lt;State&gt;)", plus "; last failure: …" when
  the node had already failed an attempt. That is the *startup's* budget, not
  the node's, and it does not stop the node:
  `introspect()` right afterwards can still show it `Loading` with
  `lastException == null`. See
  [the startup budget](../concepts/process-lifecycle.md#startup-budget).
- **`IndexOutOfBoundsException`** — `LogBackend.get`/`getBetween` outside
  `[0, length())`.
- **`io.fom.log.LogCorruptedException`** — a checked `IOException` from the
  `FileLogBackend` constructor: the file has damage a crash cannot produce
  (a bad frame followed by more data, an oversized frame, or an intact frame that
  cannot be decoded). The file is left untouched; `offset()`, `readableEvents()`
  and `path()` say where. See
  [Persistence backends](../guides/persistence-backends.md#filelogbackend)
  for recovery.
- **`IllegalArgumentException`** from `new PostgresLogBackend(ds, logId)` — the
  `logId` contains characters other than ASCII letters, digits and `_`.

!!! warning "`trigger()` appends before it applies"
    `engine.trigger(...)` records its `LogTrigger` **first**, so whatever the
    backend's `append` throws (an I/O or SQL failure, a lost leadership) comes
    out of `trigger()` synchronously and is **not** retried: nothing is logged,
    the re-init never starts, and queries keep being served from the current
    in-memory state. Retry the `trigger` yourself once the backend is healthy.
    This holds for a `Dead` process too — its `LogTrigger` is recorded before
    the restart, so a refused append restarts nothing — and `trigger(Map)`
    records **one** record covering every requested name, `Dead` ones included,
    before it applies anything.
