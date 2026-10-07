# Kotlin DSL & coroutines

`fom-kotlin` adds an idiomatic `graph { … }` builder, a `suspend`-based process
base class, and suspend extensions over the engine's `CompletionStage` APIs.

## The `graph { }` DSL { #the-graph-dsl }

```kotlin
import io.fom.kotlin.graph

val g = graph {
    process("Stations", ::StationsInit, ::StationsInit)
        .handles<GetStations>()

    // a dependency is a name, a ProcessRef, or a ready-made Dependency — mixed freely:
    process("Forecasts", ::ForecastsInit, ::ForecastsInit, dependsOn = listOf(STATIONS))   // STATIONS: ProcessRef.of("Stations")
        .handles<GetForecastModel>()

    // parameterised node:
    processWithParam("Tenant_X", ::TenantInit, ::TenantInit, param = TenantParam("X"))

    // dynamic route (resolver picks the node per query):
    route<GetTenantReport> { q -> "Tenant_${q.tenantId}" }   // "Tenant_X" for tenantId = "X"
}
```

- `process(name, ::Init, ::Load, dependsOn = …, stableDependsOn = …)` adds a
  node; `dependsOn` names become reactive dependencies (a new version
  re-initialises this node), `stableDependsOn` names become
  [stable](../concepts/reactive-cascade.md#reactive-vs-stable) ones (only
  queried). Both are also available on `processWithParam`.
- An optional last parameter `reinitStrategy: ReinitStrategy? = null` on
  `process(...)` / `processWithParam(...)` (name and `ProcessRef` forms)
  overrides [`EngineConfig.reinitStrategy`](configuration.md) for that node;
  `null` keeps the engine's. It is not part of the node's definition, so
  changing only the strategy restarts nothing on a graph swap:

    ```kotlin
    process("Forecasts", ::ForecastsInit, ::ForecastsInit,
            dependsOn = listOf("Stations"),
            reinitStrategy = ReinitStrategy.RELEASE_FIRST)   // too big to hold two versions
    ```
- A dependency may be given as a `String` name, as a typed `ProcessRef`, or as a
  ready-made `Dependency` — and the kinds mix inside one list, whichever way the
  node itself was named. So a `String`-named node can depend on refs, and a
  ref-named node on names:

    ```kotlin
    process("Caller", ::CallerInit, ::CallerInit,
            dependsOn = listOf(STATIONS, "Alerts"),
            stableDependsOn = listOf(ProcessRef.of("Audit")))
    ```

    A `Dependency` element keeps its own kind, so `Dependency.stable(ref)` stays
    stable even when listed under `dependsOn`. Anything that is not a `String`,
    a `ProcessRef` or a `Dependency` throws `IllegalArgumentException`.
- `processWithParam(...)` adds a parameterised node.
- `.handles<Q>()` attaches a static route for query type `Q` to that node — and
  it binds to the **right node by name**, so reordering or adding later nodes
  won't misroute it.
- `route<Q> { … }` registers a dynamic route. A query type can have only one
  route — either `.handles<Q>()` or `route<Q> { … }`; the second registration
  throws `IllegalArgumentException` right there ("Query type … already routed
  to …"), not at `build()`.
- For a query type that implements `Routable`, `.handles<Q>()` and
  `route<Q> { … }` are **accepted but never used**: `engine.query(msg)` asks
  `msg.targetProcess()` first, so `Routable` always wins and the type route is
  dead configuration. Route such a type one way only — through `Routable`.

Factories (`::StationsInit` or any lambda) and route resolvers are ordinary
Kotlin functions. They are never written to the log, so they may capture
anything — a DI container, a client, configuration.

### Params need value equality { #param-equality }

A `param` **is** written to the log, and on every restart the engine compares
the recorded copy with the one in your graph to decide between a warm load and
a cold init. That comparison is `equals`, so make params a **`data class`** —
or a **`data object`** for a parameter without fields. A plain `object` (or a
plain `class`) compares by identity: the decoded copy is a different instance,
so it never equals yours, the engine logs `WARN` "… does not override
equals(), so it never equals its persisted copy and the process cold-inits on
every restart …", and the process does exactly that.

```kotlin
data class TenantParam(val tenant: String) : java.io.Serializable   // ✓ equal by value
data object DefaultRegion : java.io.Serializable                    // ✓ equal by type
object Broken : java.io.Serializable                                // ✗ cold-inits every restart
```

See [Upgrades](../concepts/idempotent-restart.md#upgrades) for what changing a
param class does to that comparison.

## Suspending processes

Extend `SuspendingProcess` to write `compute`/`cleanUp` as `suspend` functions;
the framework bridges to/from `CompletionStage`.

```kotlin
import io.fom.kotlin.SuspendingProcess
import io.fom.kotlin.queryAwait          // ctx.queryAwait is a top-level extension
import io.fom.api.QueryableContext

class AlertsProcess : SuspendingProcess() {
    override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? {
        val readings = ctx.queryAwait("Stations", GetReadings((query as GetAlert).stationId))
        return alertFor(readings)
    }
}
```

`SuspendingProcess` groups its bridged coroutines into *generations*: one
`CoroutineScope` over a single `SupervisorJob` per loaded version (keyed by
`ctx.sid()`), so it does **not** leak a
coroutine job per `compute`/`cleanUp` call. Computes run on `Dispatchers.Default`
unless you pass another context to the constructor — pass `Dispatchers.IO` for
blocking work (JDBC, file I/O):

```kotlin
class JdbcProcess(private val ds: DataSource) : SuspendingProcess(Dispatchers.IO) { … }
```

The context may also carry a `Job` of yours — passing `appScope.coroutineContext`
is supported:

```kotlin
class AlertsProcess(appScope: CoroutineScope) : SuspendingProcess(appScope.coroutineContext) { … }
```

fom never adopts that `Job` as its own. Every generation gets a `SupervisorJob`
of its own (the dispatcher and other elements of your context are kept), linked
to your `Job` for **cancellation only**:

- **Cancelling your `Job` stops fom's coroutines.** Once it has been cancelled
  (or has failed), every generation built on it is cancelled: in-flight
  `computeAsync` and `cleanUpAsync` coroutines are cancelled, and any later
  compute on that instance fails with `CancellationException` — the node stays
  broken until it is loaded with a live context. A cleanup that then fails
  *only* with that cancellation is reported as successful
  (`onCleanupCompleted(ok=true)`, no WARN): your app already cancelled what
  there was to clean up. A `cleanUpAsync` that throws anything else is still
  reported as failed.
- **fom never cancels your `Job`.** Retiring a generation on re-init or
  `engine.close()`, a cleanup that runs over its budget, or a `computeAsync`
  that throws cancel only fom's own `Job`s; your scope and its other work carry
  on, and a failing compute does not affect the next compute.
- **fom's coroutines are not structured children of your `Job`.** A generation
  lives until fom retires it, so being a child would keep your scope from
  completing for as long as the process serves (a `runBlocking { }` or a
  graceful shutdown that joins its children would hang on the engine). The flip
  side: fom's coroutines are cancelled once your `Job` *has completed* its
  cancellation, and `job.join()` does not wait for them — `engine.close()` does.

When a query's reply fails — its timeout, a cancellation, or a dependency
query's inherited deadline — the engine cancels the compute, which cancels the
`computeAsync` coroutine.

`cleanUp` runs `cleanUpAsync` and then cancels **only the generation being
retired**, so abandoned computes never outlive the process. That is what makes it
safe for `load()` to return the *same* `SuspendingProcess` instance again after a
re-init (a singleton service, say): during a `KEEP_OLD` re-init the new version
serves before the old one is cleaned up, and the old generation's cleanup
cancels only computes of the old Sid, never the new version's.

`cleanUp` waits for `cleanUpAsync`, but **not** for the retired generation's
abandoned computes to finish: they are cancelled, and the stage completes right
away. A compute stuck in blocking, non-cancellable code (`Thread.sleep` or a
blocking JDBC call on `Dispatchers.IO`) after its query timed out therefore
does not hold a pause, a re-init or `engine.close()` for the cleanup budget; it
finishes in the background, and its result is discarded.

Generations are bookkept **per owning process** — keyed by `ctx.sid()` *and*
the identity of `ctx.executor()`, which the engine creates once per process
state machine — so one instance may also be shared by *several nodes at
once* (a service held by a DI container and handed back by two loaders), even
by nodes of the same name in *two engines* in one JVM. Re-initialising one of
those nodes, or closing one of those engines, retires only that owner's
generation; a `computeAsync` still in flight elsewhere is not cancelled. (If you
call `compute`/`cleanUp` by hand with test-double contexts, return the same
`executor()` instance and the same `sid()` from both for the same version.)

What sharing does *not* change: `cleanUpAsync` still runs once per retired
generation, i.e. once per node per re-init. A shared instance must therefore not
release shared state there — closing a pooled `DataSource` in the `cleanUpAsync`
of one node would break the other node still serving out of the same instance.

A `cleanUpAsync` that outlives `cleanupTimeout` is **cancelled**: the
engine cancels the stage `cleanUp` returned, `SuspendingProcess` forwards that to
the cleanup coroutine and to the whole retiring generation, so its `finally`
blocks run.

That bounds a body that is *cancellable* at its suspension points — one sitting
at such a point when the budget runs out stops there, and nothing of it survives
`engine.close()`. It does **not** bound every body. A body that blocks before it
ever suspends (`Thread.sleep`, a blocking JDBC call) is not bounded:
`SuspendingProcess.cleanUp` starts the coroutine `UNDISPATCHED` on purpose, so
the body is entered — and its `finally` blocks armed — before any cancellation
can arrive, and a body that never reaches a suspension point runs to completion
in the background. The *node* is still bounded: the engine calls `cleanUp` on a
thread of its own, so at the cleanup budget the node moves on, `close()` returns
and `onCleanupCompleted(ok=false)` fires, while that body keeps running. Neither is a body that suspends
inside `withContext(NonCancellable)`: its suspension points ignore the
cancellation by design, so `close()` returns at the end of the cleanup budget
(with `onCleanupCompleted(ok=false)`) while that cleanup keeps running. Write `cleanUpAsync` so it is cancellation-safe *and* actually suspends
(release in `finally`, prefer cancellable suspending calls over blocking ones,
use `withContext(NonCancellable)` only for a short, bounded flush).

## Suspending init and load

`init` and `load` have suspend bridges too. Implement `SuspendingInitializer`
(`suspend fun initAsync(ctx)`) and `SuspendingLoader`
(`suspend fun loadAsync(ctx, properties)`) — one class can implement both and be
passed as `::MyProcess, ::MyProcess`. For parameterised nodes use
`SuspendingParamInitializer<P>` / `SuspendingParamLoader<P>`, whose functions also
take the `param`.

```kotlin
import io.fom.kotlin.*
import io.fom.api.Process
import io.fom.api.QueryableContext
import kotlinx.coroutines.Dispatchers
import kotlin.coroutines.CoroutineContext

class Stations : SuspendingInitializer, SuspendingLoader {
    override val initContext: CoroutineContext get() = Dispatchers.IO   // init blocks on JDBC

    override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
        val version = ctx.queryAwait(OBSERVATIONS, GetVersion) as String      // OBSERVATIONS: ProcessRef
        return mapOf("rows" to fetchRows(version))
    }

    override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process =
        StationsProcess(properties.getValue("rows"))
}

val g = graph {
    process(OBSERVATIONS, ::Observations, ::Observations)   // the dependency must be a node too, or build() throws
    process(STATIONS, ::Stations, ::Stations, dependsOn = listOf(OBSERVATIONS))
}
```

Each call runs in its own coroutine on `initContext` / `loadContext`
(`Dispatchers.Default` unless overridden). When `init` runs out of its
`initTimeout` budget, or the process is cancelled or dies, the engine
cancels the init stage, which cancels the `initAsync` coroutine. A `load` stage
is cancelled the same way — both when it is no longer wanted and when it merely
outruns `loadTimeout` — so `loadAsync`'s coroutine is cancelled too; see
[the load cancellation contract](../concepts/process-lifecycle.md#load-cancellation).

`initContext` / `loadContext` may carry a `Job` on the same terms as the
`SuspendingProcess` context above: each call runs under a `SupervisorJob` of
its own that your `Job`'s cancellation cancels, and a failing `initAsync` or
`loadAsync` never cancels your `Job` — so one transient failure does not doom
every retry. But a `Job` that is **already cancelled** makes every init attempt
fail at once with `CancellationException`, and the node keeps retrying — with
its ordinary backoff, never faster than the [floor](../concepts/process-lifecycle.md#backoff)
of half `backoffMin` — until the init budget runs out and it goes `Dead`.

!!! warning "Don't block the dispatcher your `initContext` runs on"
    `engine.newGraph(...)` is a **blocking** call: it waits for the nodes to
    reach `Serving`. Called on a single-threaded dispatcher — `runBlocking`'s
    event loop, say — while `initContext` is that loop's `coroutineContext`,
    the `initAsync` coroutine is dispatched to the very thread `newGraph` is
    blocking, so it never runs: the call hangs until the init budget runs out
    and then fails. Call `newGraph` off the loop
    (`withContext(Dispatchers.IO) { engine.newGraph(g) }`), or keep
    `initContext` on `Dispatchers.Default` / `Dispatchers.IO`.

## Suspend extensions

```kotlin
import io.fom.kotlin.*
import java.time.Duration

// every call below is a suspend call, i.e. inside a `suspend fun` or a coroutine
val r: Any?  = engine.queryAwait(GetStations("PUB1"))
val typed: ForecastModel = engine.queryAs(GetForecastModel("ST-1"))   // reified cast
val byName: Any? = engine.queryProcessAwait("Forecasts", msg)
val byRef: Any?  = engine.queryProcessAwait(FORECASTS, msg)                      // FORECASTS: ProcessRef
val bounded: Any? = engine.queryProcessAwait(FORECASTS, msg, Duration.ofSeconds(2))  // also (name, q, timeout)

// inside a process:
val dep: Any? = ctx.queryAwait("Stations", GetReadings("ST-1"))
val depByRef: Any? = ctx.queryAwait(STATIONS, GetReadings("ST-1"))   // STATIONS: ProcessRef
```

`queryAwait`/`queryProcessAwait` return `Any?` because a process is free to
complete a query with `null`. `ctx.queryAwait` has no timeout overload: a
dependency query inherits the caller's deadline. `queryAs<R>` does a reified cast to `R` for
convenience.

**From inside `computeAsync`, query dependencies with `ctx.queryAwait`, not
through the outer `Engine`.** A query sent through `ctx` is tied to the query
being computed: it inherits its deadline and is cancelled once nobody waits for
that query any more. The engine can only tie an *outer* `engine.query…` call to
the issuing query while it runs on the thread that called `compute`, and
`SuspendingProcess` always dispatches `computeAsync` to its dispatcher — so an
`engine.queryAwait(...)` made there is a top-level query of its own: it gets its
own `queryTimeout` instead of inheriting the issuing query's deadline, and
`onQuerySent` shows no parent id. (The only exception is
`Dispatchers.Unconfined`, and only before the body's first suspension.) It is
**not** left running once the issuing query fails, though: when that query's
reply fails (timeout, cancellation) the engine cancels its compute, which
cancels the `computeAsync` coroutine — and cancelling a coroutine that awaits
`queryAwait` cancels the engine query (see the next paragraph). What you lose
is the shared deadline, and the parent link in observers.

**Cancelling the coroutine that awaits `queryAwait` / `queryProcessAwait`
cancels the engine query** (the underlying future is cancelled), and with it
the target's compute — a `computeAsync` there is cancelled. Observers see the
query fail with `reason = "cancelled"`. So `withTimeout { engine.queryAwait(q) }`
or cancelling the calling scope does not leave work running in the engine.

!!! warning "`runTest` and `withTimeout` around engine calls"
    Under `kotlinx-coroutines-test`'s `runTest`, `withTimeout` runs on
    **virtual time**: while the test coroutine waits for the engine — whose
    work happens on real threads — virtual time jumps ahead and the timeout
    fires at once. Wrap engine calls in `withContext(Dispatchers.Default) { … }`
    (real time) inside `runTest`, or leave out `withTimeout` and rely on the
    engine's own `queryTimeout` / per-call timeout.

## Changing the running graph

`Engine.newGraph` and `Engine.updateGraph` **block** the calling thread: an
install or swap returns only once every added or changed node has started
(reached `Serving`, or the call throws). From a coroutine, use the suspend
wrappers, which run that blocking call on `Dispatchers.IO`:

```kotlin
import io.fom.kotlin.*

engine.newGraphAwait(graph { process("Stations", ::StationsInit, ::StationsInit) })

// read-modify-write under the engine's control lock — no removal can slip in between
val changed: Boolean = engine.updateGraphAwait { current ->
    current.extend {
        process("Reports", ::ReportsInit, ::ReportsInit, dependsOn = listOf("Stations"))
            .handles<GetReport>()
    }
}

// the same, shorter
engine.extendGraphAwait {
    process("Audit", ::AuditInit, ::AuditInit, stableDependsOn = listOf(STATIONS))
}
```

- `updateGraphAwait(change)` is [`Engine.updateGraph`](../concepts/graph-swap.md):
  `change` gets the graph installed now and returns the graph to install, with no
  other control-plane call (`newGraph`, `remove`, `pause`, …) in
  between. `change` runs under the engine's control lock: keep it quick. It
  returns what `newGraph` returns — `false` if the new graph changes nothing.
- `Graph.extend { … }` returns the graph plus the nodes and routes declared with
  the usual [`graph { }` DSL](#the-graph-dsl). New nodes may depend on existing
  ones (names, `ProcessRef`s or `Dependency`s, reactive or stable); existing nodes
  and routes are kept as they are. A name already in the graph, a query type that
  is already routed, or a missing dependency throws `IllegalArgumentException`.
  Use it inside `updateGraphAwait` rather than on a graph read earlier with
  `currentGraph()` — installing a stale copy puts back a process removed in the
  meantime.
- `extendGraphAwait { … }` is `updateGraphAwait { it.extend { … } }`.
- **Cancelling the coroutine does not abort the install or swap.** Exactly:
    - a coroutine already cancelled when it calls a `…GraphAwait` function does
      not start the install;
    - once started, the blocking engine call runs to its end on its
      `Dispatchers.IO` thread (nodes keep starting) and the coroutine resumes
      only then;
    - a coroutine cancelled by that time **always** resumes with a
      `CancellationException` — whether the install succeeded or failed. A
      failure (`InitializationTimeoutException`, "Engine closed while …") is
      attached to it as a suppressed exception, so a cancelled `launch` ends as
      *cancelled*, never as failed: it does not reach a `CoroutineExceptionHandler`
      or fail its parent;
    - the outcome of the install is visible through the engine:
      `engine.currentGraph()` and `engine.introspect()` (node states).

    Bound a slow start with the engine's start budget, not with `withTimeout`.

!!! danger "Never call the control plane from the `change` function"
    `change` runs on the engine call's `Dispatchers.IO` thread while the engine
    holds its control lock. What happens depends on the thread the call is made
    on:

    - **On that same thread** — a direct `newGraph`, `updateGraph`,
      `remove`, `pause`, `resume` or `updateConfig`,
      also from a plain `runBlocking { … }`, which stays on the calling thread —
      the call is **refused** with `IllegalStateException` ("… called from
      inside an updateGraph change function; …"). It propagates out of
      `updateGraphAwait` unless `change` catches it, and nothing is installed.
    - **On another thread that `change` waits for** — `newGraphAwait` /
      `updateGraphAwait` (they hop to `Dispatchers.IO`),
      `withContext(Dispatchers.IO) { … }`, another dispatcher or executor — the
      call is not refused: it waits for the lock `change`'s thread holds while
      `change` waits for it. The lock is not reentrant across threads, so the
      engine **deadlocks for good**, `close()` included.

    Don't call `close()` from `change` either. Compute what you need *before*
    calling `updateGraphAwait`, and do follow-up control-plane calls *after* it
    returns.

    Related: an `initAsync` that calls the control plane while an install or
    swap is running does not deadlock, but it stalls — the install holds the
    lock while it waits for that very init — until the init budget runs out and
    the node fails with `InitializationTimeoutException`. The stalled call is
    **not** abandoned with the init: once the install releases the lock, it
    still runs — a late side effect (a graph change, a pause, …) of an init that
    has already timed out. Don't call the control plane from `initAsync`.

## Timeouts inside `computeAsync`

`withTimeout` throws `TimeoutCancellationException`, which **is a
`CancellationException`**. Thrown out of `computeAsync`, it fails the query with
that exception, so:

- a caller's `engine.queryAwait(...)` rethrows a `CancellationException`. Inside
  `launch { }` that ends the coroutine *silently* — as cancelled, not failed —
  so no exception handler sees it and the code after the call just never runs;
- observers count it as `reason = "cancelled"` (in `fom-micrometer`:
  `engine_query_cancellations_total`), not as a failure of your process.

Keep a compute's own timeout from masquerading as a cancellation: use
`withTimeoutOrNull` and return a fallback, or catch the timeout and rethrow it as
an ordinary exception.

```kotlin
override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? =
    withTimeoutOrNull(500) { fetchForecast(query) } ?: Forecast.Unavailable

// or, to fail the query as an error:
override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? =
    try {
        withTimeout(500) { fetchForecast(query) }
    } catch (e: TimeoutCancellationException) {
        throw ForecastTimeoutException("forecast service timed out", e)   // not a CancellationException
    }
```

A *query* deadline (`queryProcessAwait(..., timeout)`, `defaultQueryTimeout`) is
different: the engine fails the reply with a timeout and cancels the compute —
that is not affected by the above.

## Coroutine context elements (MDC, `ThreadLocal`)

The engine is a Java boundary: a query travels through a mailbox and a
`CompletionStage`, so **the caller's coroutine context does not cross it**. An
MDC or `ThreadLocal` element installed around `engine.queryAwait(...)` (with
`MDCContext()` from `kotlinx-coroutines-slf4j` or `threadLocal.asContextElement(v)`)
is *not* visible inside `computeAsync`.

What *is* visible is the context fom runs your code in:

- elements in the `SuspendingProcess(context)` constructor argument are seen by
  every `computeAsync` and `cleanUpAsync` of that instance;
- elements in `initContext` / `loadContext` are seen by `initAsync` / `loadAsync`.

```kotlin
val service = ThreadLocal<String?>()

class Alerts : SuspendingProcess(Dispatchers.Default + service.asContextElement("alerts")) {
    override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? {
        check(service.get() == "alerts")            // visible here
        …
    }
}
```

That context is fixed when the instance is built, so it suits static values
(service name, tenant of a per-tenant instance). For per-query values — a
request or trace id — pass them in the query message and install them inside the
compute: `withContext(MDCContext(mapOf("requestId" to q.requestId))) { … }`.

fom gives the coroutines of a `SuspendingProcess` **no `CoroutineName`**, so in
a coroutine dump (`DebugProbes`, the IDE's coroutine view) the computes of
different nodes look alike. If you need to tell them apart, add one to the
constructor's context yourself — it is kept like any other element:

```kotlin
class Alerts(node: String) : SuspendingProcess(Dispatchers.Default + CoroutineName("fom-$node"))
```

Since the context is fixed per instance, an instance shared by several nodes
carries one name for all of them.

Where the name shows up: `DebugProbes.dumpCoroutinesInfo()` returns it in each
`CoroutineInfo.context`, and the IDE's coroutine view shows it. The **text**
dump `DebugProbes.dumpCoroutines()` prints a coroutine's name only when the JVM
runs with `-Dkotlinx.coroutines.debug` (or `-ea`, which turns the debug mode on);
without it the dump shows the coroutines unnamed, as if you had set no name.

## Sid changes as a `Flow` { #sid-changes-as-a-flow }

`fom-kotlin` has **no `Flow` API**. To observe new versions of processes as a
flow, bridge `EngineObserver.onSidPromotion` into a `MutableSharedFlow` or
`MutableStateFlow` yourself. The observer is the fourth `Engine` constructor
argument, so create the flow first and wire it in when you build the engine.
Callbacks run on engine threads: emit with `tryEmit` / `update`, never suspend
or block in them.

```kotlin
val promotions = MutableSharedFlow<Pair<String, Sid>>(
    extraBufferCapacity = 1024, onBufferOverflow = BufferOverflow.DROP_OLDEST)
val currentSids = MutableStateFlow<Map<String, Sid>>(emptyMap())

val engine = Engine(config, backend, serDe, object : EngineObserver {
    override fun onSidPromotion(processName: String, previousSid: Sid?, newSid: Sid) {
        promotions.tryEmit(processName to newSid)
        currentSids.update { it + (processName to newSid) }
    }
})

// elsewhere:
promotions.filter { it.first == "Stations" }.collect { (_, sid) -> refreshCache(sid) }
```

A `SharedFlow` without replay misses promotions that happen before a collector
subscribes (the first Sids at startup, typically); the `StateFlow` of current
Sids does not. To combine this with another observer (metrics, tracing), see
[Combining observers](observability.md#combining-observers).

## Build setup

```kotlin
dependencies {
    implementation("io.github.altspacetg:fom-kotlin:0.1.0-SNAPSHOT")  // brings fom-core + coroutines-core
}
```

`fom-kotlin` declares both `kotlinx-coroutines-core` and `kotlinx-coroutines-jdk8`
as `api` dependencies, so both land on your compile classpath transitively.
