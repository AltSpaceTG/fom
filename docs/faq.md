# FAQ

#### How is this different from a cache or a job scheduler?

A cache stores values you can recompute on demand; FOM stores *processes* whose
state is expensive to build and is recovered by `load` (cheap) rather than `init`
(expensive) after a restart. A job scheduler runs tasks on a clock; FOM keeps
long-lived, queryable state and re-initialises it *reactively* when its
dependencies change. The closest mental model is "a small graph of durable,
self-recovering read models".

#### Does my state have to fit in memory?

The *live* object a `load` produces lives in memory and answers queries; the
*persisted* form is the `byte[]` property cells in the log. If your state is
huge, persist a compact descriptor and have `load` build a structure that
streams or pages from elsewhere — keep `load` fast and defer heavy work to the
first `compute`.

#### What happens if `init` keeps failing?

It retries with exponential backoff until the total `initTimeout` budget
is spent, then the process goes to `Dead` with
`InitializationTimeoutException`. The exception: if the log backend refuses
to store the init result (e.g. over the log payload limits), the process goes
`Dead` at once without retrying. A `load` that keeps failing falls back to a
fresh `init`; if it fails again on that fresh state, further fallbacks back off
and share the same budget before the process goes `Dead`. A `trigger` restarts a
`Dead` process. See
[Process lifecycle](concepts/process-lifecycle.md).

A failing **re-init** costs less: by default (`ReinitStrategy.KEEP_OLD`) the
old version keeps answering queries while the new one initialises, and keeps
serving if it fails. The node is then reported `stale`, the observer gets
`onReinitFailed`, and the re-init is retried by itself after
`reinitRetryBackoffMin` (unless the cause is permanent). To stop a re-init
that keeps failing, call `engine.cancelInit(name)`: it cancels the running
attempt or the scheduled retry, the node keeps serving, stays `stale` and is
not retried automatically — the next `trigger` or start re-initialises it (a
request already queued behind the cancelled attempt starts at once). Under `RELEASE_FIRST`
the old state is retired *before* the new `init` runs, so a failed re-init ends
`Dead` with no state — see
[When a re-init fails](concepts/process-lifecycle.md#when-a-re-init-fails).

#### My `init` calls a rate-limited API (HTTP 429, `Retry-After`) — how do I back off?

The engine's retry [backoff](concepts/process-lifecycle.md#backoff) is
**engine-wide** (`EngineConfig.backoffMin` / `backoffMax`, the same for every
node) and knows nothing about your upstream: it **ignores `Retry-After`**, so the
next attempt may come before the upstream allows it (and burn another attempt
on a 429) or long after. Handle the rate limit inside `init`:

- **honour `Retry-After` in `init`** — sleep and call again, but only while the
  wait fits in a cap you keep well below `initTimeout`. The budget covers
  *all* attempts and their backoffs, and `init` cannot ask how much of it is
  left, so track your own deadline;
- otherwise **fail fast** — throw, and let the engine's backoff retry within the
  budget.

Never wait past the init budget: once it is spent the attempt is over (a late
result is dropped) and the node goes `Dead` with `InitializationTimeoutException`.

```java
static final Duration MAX_RATE_LIMIT_WAIT = Duration.ofSeconds(10);  // well inside initTimeout

@Override
public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
    long giveUpAt = System.nanoTime() + MAX_RATE_LIMIT_WAIT.toNanos();
    while (true) {
        HttpResponse<byte[]> rsp = fetch();                       // your HTTP call
        if (rsp.statusCode() != 429) {
            return CompletableFuture.completedFuture(Map.of("data", rsp.body()));
        }
        Duration wait = retryAfter(rsp).orElse(Duration.ofSeconds(1)); // parse the header yourself
        if (System.nanoTime() + wait.toNanos() > giveUpAt) {
            // Does not fit: fail this attempt; the engine's backoff retries within the budget.
            throw new InitializationException("rate limited, Retry-After " + wait);
        }
        try {
            Thread.sleep(wait);                                    // init runs on its own virtual thread
        } catch (InterruptedException e) {                         // a stop or cancelInit
            Thread.currentThread().interrupt();
            throw new InitializationException("interrupted while rate limited", e);
        }
    }
}
```

#### Can I run more than one instance?

Yes, with `PostgresLogBackend`: one instance becomes leader (advisory lock); the
others fail fast on construction and can stand by. See
[Multi-node](guides/multi-node.md). The file and in-memory backends are
single-process.

#### Can two `Engine`s share one `LogBackend` instance?

No — use **one `Engine` per backend instance**. Nothing stops you from passing
the same backend object to a second `Engine`, but its first `newGraph` claims
leadership of the log (it appends its own `LogLeader`). From then on the first
engine's appends are refused — its triggers, re-inits and graph changes fail
with `LeadershipLostException` — while its processes **keep serving the state
they hold in memory**, which no longer follows the log. Nothing fails on the
query path, so the first engine silently serves stale state;
`introspect().isLeader()` is the only place that shows it. To run a second
engine, give it its own log (file, table) — or, for a standby, see
[Multi-node](guides/multi-node.md).

#### How do I run it under a service manager (systemd, Kubernetes)?

fom registers **no JVM shutdown hook** — it is a library, and the process
lifecycle belongs to you. Register one that closes the engine and then the log
backend (the engine never closes the backend you gave it):

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    engine.close();     // stops processes, consumers first
    backend.close();    // releases the file lock / DB connection
}));
```

If `close()` is called twice concurrently — a shutdown hook and a
try-with-resources, say — the second call **waits** until the first has
finished stopping the processes before it returns. So closing the backend right
after your `close()` returns is safe whichever call returns first. Only a
`close()` made on the very thread that is running the first `close()` returns
at once instead of deadlocking, and so does one made on a process's
dispatcher (below). Any other thread waits — including an `EngineObserver`
callback that is not running on a dispatcher. Callbacks run on **engine threads**, and which one
depends on the callback and the moment: a process's dispatcher, its
init/load worker or budget watchdog (`onInitFailed`, for one, is called from
all three), or the engine's observer threads — never assume which. A `close()`
in a callback that happens to run on a process's dispatcher throws
`IllegalStateException` at once ("close() called on the dispatcher of 'X' (from
an EngineObserver callback about it); call it from another thread") instead of
blocking that dispatcher — unless the
engine is already closing, in which case it just returns. Use
`Thread.startVirtualThread(engine::close)` if a callback really must trigger the
shutdown.

"Safe" has one limit: a hung backend. `close()` waits for each process's queued
log writes at most `cleanupTimeout`; past that it logs `WARN` "FSM[X] log writes
still pending after …; not waiting for them any longer" and returns. That stuck
append may still land after `close()` has returned (or fail against the closed
backend). If it lands it is a valid record, and the next start handles it like
any other — see
[Process lifecycle](concepts/process-lifecycle.md#retries-backoff-timeouts).

Don't call `close()` from an `EngineObserver` callback, a `cleanUp` or a
watcher check. Some callbacks run on the thread of a `newGraph`,
`updateGraph` or `remove` in progress (`onProcessRemoved`, for
one): a `close()` there shuts the engine down under that call, which then
fails with "Engine closed while 'X' was starting" although its caller never
closed anything. Signal your own shutdown code instead.

The same rule applies to any control-plane call that **stops a process**
(`pause`, `remove`, `TenantAwareEngine.pauseTenant` /
`removeTenant`, a `newGraph` / `updateGraph` that removes or redefines it) made
synchronously from an observer callback about **that same process**: the
callback may be running on its dispatcher, and the stop would wait for that
very dispatcher. The engine refuses such a call up front with
`IllegalStateException` ("… called on that process's own dispatcher …"),
before taking the control lock and without changing anything.

Calls that only touch other processes are allowed — unless **another**
control-plane call holds the control lock at that moment (the first `newGraph`
does, while it waits for every node to reach `Serving`). Then any control-plane
call from a callback on a process dispatcher — `newGraph`, `updateGraph`,
`updateConfig`, `remove`, `pause`, `resume`, `resumeUnblocked` — throws
`IllegalStateException` at once ("…(...) called from an EngineObserver callback
while another control-plane call is in progress; call it from another thread")
instead of blocking; earlier versions stalled the dispatcher for the whole start
budget and the first `newGraph` failed. The engine logs and swallows the
exception like any callback failure, so the call silently does not happen.
Hand control-plane calls off to another thread, e.g.
`Thread.startVirtualThread(() -> engine.pause(Set.of(name)))`. See
[Stopping a process from a callback](guides/observability.md#stopping-from-a-callback).

`close()` stops the processes of one dependency depth together, each with
`cleanupTimeout`, so it can take up to **(dependency levels) ×
`cleanupTimeout`**. Give the service manager's stop timeout (systemd
`TimeoutStopSec`, Kubernetes `terminationGracePeriodSeconds`) at least that.
If the JVM is killed anyway (`SIGKILL`) in the middle of `close()`, nothing is
lost: a graceful stop writes no `LogDead`, so the next start warm-loads every
process as usual, and triggers that were recorded but not yet applied are
replayed. See [Idempotent restart](concepts/idempotent-restart.md).

#### How do I know the graph is settled before I shut down?

`introspect()` does not show everything that is still to come. A re-init asked
for by a `trigger`, or a [reactive cascade](concepts/reactive-cascade.md) step,
first waits out the [dedup window](concepts/reactive-cascade.md#the-dedup-window)
— and while it waits nothing in the report shows it: every node reads
`Serving`, mailboxes are empty. A graph that looks settled can therefore still
have re-inits queued. They are not lost — the `LogTrigger` /
`LogDependencyChanged` behind each one is written to the log first, and
`close()` drops only the scheduled timers — but they are applied at the **next
start**, not before this one ends.

If you want them applied now (a test, a deploy that expects fresh state on the
way out), quiesce before `close()`: stop sending triggers, wait **longer than
`dedupWindow`** after the last one, and then wait for the re-inits that fired to
finish — no node reports a `replacement` or `stale` and each re-inited one has
a new Sid (`introspect()`, or `onSidPromotion`). A re-init that failed leaves
`stale=true` until its retry succeeds. A cascade goes one dependency level per step, and each step
waits its own window plus the re-init, so for a chain repeat until no Sid
changes for longer than `dedupWindow`.

#### How much heap does it need, and can I limit concurrent inits?

Size the heap for the **property payloads** (the `byte[]` cells `init`
returns), not just the live objects — and for **several copies** of them: the
engine and the log backend copy a payload on its way into the log and again on
its way back. Measured on `FileLogBackend` with a 50 MB payload spread
over 8 cells:

- a **cold start** peaked at roughly **6.5–7 × the payload**. While one node's
  `init` result is recorded, the heap holds at the same time: the map `init`
  returned; the engine's `LogInitialized` record (its constructor deep-copies
  every cell); the clock-stamped copy the backend actually appends (another deep
  copy); the serialized record (a `ByteArrayOutputStream` that grows by
  doubling, then its `toByteArray()` copy); for a record over 1 MB, a
  **read-back check** that deserializes it again to prove it can be read after a
  restart (and decoding a `LogInitialized` deep-copies its cells once more); and
  the frame buffer (length, CRC, payload) that is written to the file.
  Independent nodes init concurrently, so these peaks add up across the nodes
  initialising at that moment;
- a **warm start** needed **more than 3 × the payload**. Opening a
  `FileLogBackend` reads and decodes **every** record once to verify the
  file (CRC, clocks, decodability); the engine's startup scan then reads the log
  again and keeps the latest `LogInitialized` of every process it warm-loads,
  which is what `load` is handed. Every decode
  allocates the raw frame, the decoded arrays and the deep copy the record's
  constructor makes — plus whatever your `load` builds from the cells.

Superseded records cost time, not heap: both passes decode **one record at a
time** and drop it before the next, and the startup scan keeps only the latest
`LogInitialized` per process name. So a warm start, an `engine.snapshot()` and
an offline [`fom-log compact`](guides/cli.md) need heap for the **live state**
(a state retired by a `LogDead` is dropped as soon as the scan reaches it) and the copies of the **largest
single record** being decoded — not for the whole uncompacted log. On
`PostgresLogBackend` the scan reads windows of at most 1,000 events and 8 MiB of
payload (one larger row alone), so add up to that much decoded data. A log much
larger than the heap still opens; it just takes longer than a
[snapshotted](concepts/snapshots.md) one.

These are measurements of one setup, not guarantees. To keep the peaks low,
**split big state into several cells** rather than one huge `byte[]` — each
copy is then a set of smaller arrays, which the collector places far more
easily than one giant contiguous one — and across **several nodes** where the
domain allows, so each record (and each node's set of copies) stays smaller and
no record nears the log payload limit. Then **measure** your own cold and warm start (e.g.
`-Xlog:gc` or a heap profiler) before fixing `-Xmx`.

**A re-init holds two versions.** With the default `KEEP_OLD` strategy the old
version keeps serving while the new one is built, so the peak during a re-init
is the old live state **plus** the new one **plus** the init's working set (and
the record copies above), until the old version's computes drain and its
`cleanUp` runs. A trigger that arrives meanwhile can start the next re-init
while the old version is still draining, so briefly there can be three
versions ([details](concepts/process-lifecycle.md#re-initialisation)). For a node too big to hold twice, choose `RELEASE_FIRST` —
engine-wide with `EngineConfig.withReinitStrategy(...)` or for that node only
with `GraphBuilder.reinitStrategy(ReinitStrategy.RELEASE_FIRST)` right after
its `add(...)` (Kotlin:
`process(..., reinitStrategy = ...)`). It retires the old version first, at the
price of queries waiting during the re-init and a `Dead` node if it fails. The
per-node setting is not part of the node's definition: changing it restarts
nothing.

All of these copies are **heap**. Direct (native) memory is not proportional
to record size: `FileLogBackend` reads and writes a frame in bounded
256 KiB slices, so the JDK's per-thread temporary direct buffers stay small
however large a record is, and the default `-XX:MaxDirectMemorySize` is enough.

An `OutOfMemoryError` thrown inside `init` (or while the engine records its
result) is treated as a **failed attempt** and retried with backoff within the
init budget — it does not crash the engine, but a retry that needs the same
memory fails the same way until the node goes `Dead`. During a `KEEP_OLD`
re-init the same budget runs out without killing anything: the old version
keeps serving, and the re-init is retried after `reinitRetryBackoffMin` — an
OOM counts as transient, so it succeeds once memory is back.

There is **no setting** that bounds how many independent nodes init at the
same time: every node whose dependencies are ready starts at once. If your inits
are memory- or connection-hungry, bound them yourself with a semaphore shared
by your initializers:

```java
static final Semaphore HEAVY_INITS = new Semaphore(4);   // at most 4 at a time

@Override
public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
    try {
        HEAVY_INITS.acquire();
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InitializationException("interrupted waiting for an init slot", e);
    }
    try {
        return CompletableFuture.completedFuture(Map.of("model", buildModel()));
    } finally {
        HEAVY_INITS.release();
    }
}
```

`init` runs on its own virtual thread, so blocking there is fine — but the time
spent waiting for a permit counts against `initTimeout`: give the budget
room for the queue.

#### Can I hand over to a new instance on the same host (blue-green)?

Not with both running on one file log. `FileLogBackend` takes an OS file
lock in its constructor; while the old instance holds it, the new instance's
`new FileLogBackend(path)` fails **at once** with
`IllegalStateException` ("Cannot acquire leader lock on … — another process
holds it"). There is no wait-for-lock option. Stop the old instance (its
`engine.close()` and `backend.close()`, or process exit, release the lock),
then start the new one — if they overlap, retry the constructor:

```java
FileLogBackend backend = null;
long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
while (backend == null) {
    try {
        backend = new FileLogBackend(path);
    } catch (IllegalStateException lockHeld) {   // the old instance still holds the log
        if (System.nanoTime() > deadline) throw lockHeld;
        Thread.sleep(500);
    }
}
```

For a real overlap (a standby taking over), use `PostgresLogBackend` — see
[Multi-node](guides/multi-node.md).

#### I changed `EngineConfig` — what happens to the old runs?

`EngineConfig` is **never persisted**: the log records graphs, states and
triggers, not timeouts, backoff or dedup settings. Whatever config the engine is
constructed with applies to everything it does from then on — including
warm-loads after a restart, and recorded triggers replayed at startup. A
trigger recorded under a 30 s init budget and replayed after you lowered it to
5 s runs under 5 s.

#### Can factories capture objects (a DI container, a data source)?

Yes. Factories are plain `Supplier`s: they are only called in the JVM that
installed the graph and are never written to the log — `LogChangeGraph` records
only each node's name, dependencies and `param`. After a restart your
application builds and installs the graph from code again. See
[Dependency injection](guides/dependency-injection.md).

#### Which serializer should I use?

`FurySerDe` (from `fom-fury`) for anything real — compact, fast,
schema-evolution friendly. `JavaSerializableSerDe` is fine for tests and the
quickstart. For untrusted log storage, harden the serializer — see
[Security](security.md).

#### Will the log grow forever?

Not if you enable [snapshots](concepts/snapshots.md): a snapshot compacts the
log to only the current live state and archives the rest. Use
`SnapshotPolicy.FixedInterval`, `SizeBasedSnapshotPolicy`, `CompositeSnapshotPolicy`,
or a custom policy. The **archives** do grow, though: by default no policy deletes
them. Give the policy a `keepHistory` (HOCON: `log.rotate.keep-history`) or call
`engine.purgeArchives(n)` yourself — see
[Archive retention](concepts/snapshots.md#archive-retention).

#### How do I change the graph without downtime?

Call `engine.newGraph(newGraph)` again — the engine diffs it against the running
graph and applies only the difference. To change the installed graph (add or
drop a node), use `engine.updateGraph(current -> …)`, which does the
read-modify-write atomically. See
[In-place graph swap](concepts/graph-swap.md#read-modify-write-updategraph).

#### How do I verify a warm restart actually skipped `init`?

Attach an [`EngineObserver`](guides/observability.md) and assert you see
`onLoad*` but not `onInitStarted` for each node — see
[Idempotent restart](concepts/idempotent-restart.md).

#### Is `fom-core` really dependency-free?

At runtime it depends only on `slf4j-api`. Everything else — serializers, DI,
metrics, tracing, Postgres, the Kotlin DSL — is a separate optional module.

#### What JDK do I need?

JDK 21+ at runtime (virtual threads, pattern matching). The build targets a
JDK 21 toolchain.

#### Does fom run under a `SecurityManager`?

No. fom runs `init`, `load`, `compute`, watcher checks and its dispatchers on
virtual threads, and on JDK 21 a virtual thread has **no permissions** when a
`SecurityManager` is installed, so any permission check along those paths fails.
The `SecurityManager` is deprecated for removal and cannot be enabled at all
from JDK 24. Isolate untrusted code with processes or containers instead.
