# Multi-tenancy

A common pattern is one process **per tenant** — e.g. `Stations_PUB1`,
`Stations_PUB2` — routed by a [`Routable`](../concepts/graph-and-routing.md)
message or a dynamic route. The `fom-tenant` module adds a **defence-in-depth
authorization wrapper** so a caller can only touch tenants it is allowed to.

!!! info "It's a wrapper, not a replacement"
    `TenantAwareEngine` delegates to a normal `Engine`. The engine stays the
    source of truth; the wrapper adds an authz check on top of `query` /
    `queryProcess` / `trigger` and the tenant lifecycle calls. The wrapped
    engine is deliberately **not** exposed, so code that only holds the wrapper
    cannot bypass the checks.

!!! tip "Not using tenants?"
    Then don't use the wrapper. A plain `Engine` has no authorization layer:
    every process is reachable by whoever holds the engine. `TenantAwareEngine`
    is an opt-in for code paths that serve tenant callers.

## Building the wrapper

```java
import io.fom.tenant.*;

var aware = TenantAwareEngine.builder(engine)
    .tenantResolver(TenantResolver.suffixAfter("_"))                  // "Stations_PUB1" → "PUB1"
    .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant))
    .globalProcesses("Units", "Calendar")                             // open to every caller
    .build();
```

## How access is decided

Every access to a process goes through the same three steps — the wrapper is
**fail-closed** at each of them:

1. **Declared global** (`globalProcesses`) → `query`/`queryProcess` allowed for
   every caller; `trigger` only for callers accepted by `globalTriggerPolicy`
   (nobody by default).
2. **Resolves to a tenant** → allowed only if `authzPolicy` accepts the caller
   for that tenant. The default policy denies everyone.
3. **Anything else** — the name belongs to no tenant, or is ambiguous →
   **denied**.

### Resolvers never guess

A `TenantResolver` maps a process name to a `TenantId`, or to nothing:

- **`TenantResolver.suffixAfter("_")`** (the default) is strict: the name must
  be exactly `<base>_<tenant>` with one separator and both parts non-empty.
  `Stations_PUB1` → `PUB1`, but `Stations_ACME_EU`, `Units`, `_PUB1` resolve
  to nothing and are therefore denied (unless global). It never picks "the part
  after the last `_`", which could hand one tenant's process to another.
- **`TenantResolver.registry(Map.of("Stations_ACME_EU", TenantId.of("ACME_EU")))`**
  — an explicit process → tenant map, for names that contain the separator.
  It **copies** the map when it is built, so a tenant (or process) you add at
  runtime does not resolve through it: build a new resolver — and a new
  `TenantAwareEngine` over the same `Engine`, since the wrapper's resolver is
  fixed — or use `TenantResolver.of(...)` backed by a live lookup of your own
  (a concurrent map, your tenant table).
- **`TenantResolver.regex(Pattern.compile("Stations@(.+)"))`** — the whole
  name must match; the first capture group is the tenant.
- **`TenantResolver.of(name -> …)`** — any custom rule. Return empty when unsure.

A resolver must return `Optional.empty()`, never `null`: `null` fails with
`IllegalStateException` naming the process (for `query` / `queryProcess` it
fails the returned stage).

### Global processes

Shared processes that belong to no tenant (reference data, calendars…) must be
listed with `globalProcesses(...)`. A process that is neither global nor a
tenant's is **not** reachable through the wrapper — there is no implicit
"everything without a tenant is public" rule. A name listed as global is global
even if the resolver would map it to a tenant.

Global processes are **query-only** for ordinary callers. Re-initialising
shared data cascades into every tenant, so `aware.trigger(caller, "Units", …)`
throws `TenantAccessDeniedException` unless the caller is accepted by
`globalTriggerPolicy`:

```java
var aware = TenantAwareEngine.builder(engine)
    .globalProcesses("Units", "Calendar")
    .globalTriggerPolicy(caller -> caller.identity().equals("ops"))   // default: nobody
    .build();
```

## Callers and tenants

```java
TenantId pub1 = TenantId.of("PUB1");
TenantCaller alice = TenantCaller.of("alice", pub1);     // may act on PUB1
TenantCaller anon  = TenantCaller.anonymous();           // no tenants
```

Repeated tenant ids passed to `TenantCaller.of` (e.g. from token claims) are
collapsed.

## Authorized operations

`ProcessRef` constants are **fields**, not statements — declare them on a class,
the way [Graph & routing](../concepts/graph-and-routing.md#typed-process-references--processref)
does, and pass them where a name is expected:

```java
final class Refs {
    static final ProcessRef STATIONS_PUB1 = ProcessRef.of("Stations_PUB1");
}
```

```java
// query: only Routable messages are accepted (target known up front)
aware.query(alice, new GetStations("PUB1"));            // ok if alice has PUB1

// explicit addressing
aware.queryProcess(alice, "Stations_PUB1", msg);

// trigger
aware.trigger(alice, "Stations_PUB1", new RefreshSignal("x"));

// per-query timeout — as Engine.query(msg, timeout) / queryProcess(name, msg, timeout)
aware.query(alice, new GetStations("PUB1"), Duration.ofMillis(200));
aware.queryProcess(alice, "Stations_PUB1", msg, Duration.ofMillis(200));

// ProcessRef instead of a bare name
aware.queryProcess(alice, Refs.STATIONS_PUB1, msg);
aware.queryProcess(alice, Refs.STATIONS_PUB1, msg, Duration.ofMillis(200));
aware.trigger(alice, Refs.STATIONS_PUB1, new RefreshSignal("x"));

// batch trigger — one durable record for several processes, all of them alice's own
// tenant (a foreign name anywhere in the map denies the whole batch, see below)
aware.trigger(alice, Map.of(
    "Stations_PUB1", new RefreshSignal("x"),
    "Alerts_PUB1",    new RefreshSignal("y")));
```

Every overload runs the same authorization as its plain counterpart: the
`ProcessRef` is resolved to its `name()` and checked before anything is
dispatched, and a denial fails the returned stage (`query`/`queryProcess`) or
throws (`trigger`).

### Batch trigger { #batch-trigger }

`trigger(TenantCaller, Map<String, Serializable>)` is the tenant-aware form of
[`Engine.trigger(Map)`](../concepts/triggers-and-watchers.md#triggers), and it
adds two guarantees a loop over the single-name form cannot give you:

- **Authorization is all-or-nothing.** Every name in the map is checked first —
  tenant ownership, and the builder's `globalTriggerPolicy` for a global process.
  One foreign name, or one global name the `globalTriggerPolicy` does not accept
  this caller for, throws `TenantAccessDeniedException` and **nothing** is
  triggered; there is no partially applied batch.
- **The batch is one durable record.** A loop of single-name triggers writes one
  `LogTrigger` per process, so a log lost midway can leave the batch half
  applied. This records all of the names or none of them.

The map is **copied** (order preserved) before it is authorized, and the copy is
what gets triggered, so a caller cannot change the map — or make it answer
differently — between the check and the write. A `null` key or a `null` value is
rejected with `NullPointerException`, whose message names the offending entry
("trigger value for '&lt;name&gt;'"). An empty map triggers nothing and returns
`false` — but on a closed engine it throws `IllegalStateException` like every
other call, instead of quietly returning `false`.

### Why `query` rejects non-`Routable` messages

`aware.query(caller, msg)` must know the target tenant *before* dispatch to
authorize it. A `Routable` message carries its `targetProcess()`, so the tenant
is known. A non-`Routable` message would be type-routed *inside* the engine to a
process the wrapper never checked — a fail-open hole. So `query` rejects
non-`Routable` messages with `TenantAccessDeniedException`; use
`queryProcess(caller, name, msg)` for explicit addressing.

The wrapper reads `targetProcess()` **once**, authorizes that name and dispatches
to exactly that process, so a message cannot name one process for the check and
another for the dispatch.

## Tenant lifecycle

`processesOf(caller, tenant)` answers the same question without changing
anything: it returns the tenant's process names in the current graph (globals
excluded), and throws `TenantAccessDeniedException` for a caller that may not
manage that tenant — so it cannot be used to probe another tenant's processes.
On a closed engine it throws `IllegalStateException` ("Engine &lt;id&gt; is closed"), like
every other call on the wrapper.

The caller must be authorized for the tenant (`authzPolicy`). Each call acts on
the tenant's processes in the current graph (global processes are never
touched) and returns their names. **Check the returned set**: an empty set means
the resolver maps no running process to that tenant — e.g. a tenant id that
contains the separator under the default strict `suffixAfter("_")` resolver —
so nothing was paused, resumed or removed.

```java
// Read-only: which processes the lifecycle calls below would act on.
Set<String> names = aware.processesOf(alice, pub1);   // TenantAccessDeniedException for bob

// Pause: stop the tenant's processes but keep their state.
aware.pauseTenant(alice, pub1);    // queries to them → QueryRejectedException ("paused";
                                   //   "<name> is shutting down" while the pause drains)
aware.resumeTenant(alice, pub1);   // warm-loads the kept state, no init
                                   // (+ global consumers paused only because of them,
                                   //    if alice passes globalTriggerPolicy)

// Remove for good: an in-place graph swap without the tenant's processes.
aware.removeTenant(alice, pub1);   // state retired; adding the tenant back cold-inits

// Offboarding several tenants: one graph swap for all of them.
aware.removeTenants(admin, List.of(pub1, pub2, pub3));
```

| | `pauseTenant` / `resumeTenant` | `removeTenant` |
|---|---|---|
| Processes | stopped, still in the graph | removed from the graph |
| Persisted state | kept — resume warm-loads | retired (`LogDead`) — re-adding cold-inits |
| Queries meanwhile | `QueryRejectedException` ("paused"; a query racing the pause may instead get "&lt;name&gt; is shutting down" while the process drains) | unknown process |
| Triggers / dependency changes meanwhile | remembered: the process re-inits on resume | — |
| JVM restart | pause is persisted: processes stay paused until resumed | stays removed if you install the graph without them |

!!! warning "Offboarding: leaving a tenant out of the graph retires nothing"
    Installing the graph at a restart **without** a tenant's processes does not
    retire their state: no `LogDead` is written for a node that is simply absent.
    Re-create the same tenant id later and its processes **warm-load the old
    tenant's state** (as long as no snapshot has dropped it meanwhile — a
    snapshot keeps only the state of the current graph's processes). To retire a
    tenant for good, call `removeTenant` (or `Engine.remove`), which
    writes a `LogDead` for each of its processes, so re-adding it cold-inits.

    Retiring is not deleting: the retired state stays in the live log until the
    next [snapshot](../concepts/snapshots.md) and in its archives until
    [`purgeArchives`](../concepts/snapshots.md) removes them. If the tenant's
    data must actually be deleted, `removeTenant`, then `engine.snapshot()`,
    then `engine.purgeArchives(0)` (or a `keepHistory` your retention policy
    allows). That removes the tenant's process definitions (their persisted
    `param`s) too; with `FileLogBackend` also delete any
    `<log>.truncated.<millis>` files, which `purgeArchives` leaves alone — see
    [Security](../security.md#data-retention). Params are persisted in plaintext,
    so never put a tenant's secrets in a `param`.

!!! tip "Offboard tenants in batches: `removeTenants`"
    Every removal is a graph swap, and every graph swap persists the **whole
    remaining graph** (a `LogChangeGraph`) to the log. Removing `N` tenants one
    `removeTenant` at a time therefore writes `N` ever-so-slightly smaller copies
    of the graph — log growth quadratic in `N`, which a large offboarding (say
    thousands of tenants from a graph of tens of thousands of processes) turns
    into gigabytes. `removeTenants(caller, tenants)` removes the processes of all
    the given tenants in one `Engine.remove`: one `LogChangeGraph`, one
    `LogDead` per process. Authorisation is all or nothing — if the caller may not
    manage one of the tenants, nothing is removed. Batch an offboarding job into
    chunks (hundreds of tenants per call) rather than looping over `removeTenant`.

!!! note "A remembered trigger is applied asynchronously"
    A trigger (or reactive dependency change) that arrived while the tenant was
    paused is replayed on resume through the normal trigger path, i.e. after the
    [`dedupWindow`](../concepts/reactive-cascade.md#the-dedup-window). So a query
    issued right after `resumeTenant` returns can still be answered from the
    pre-trigger state for about one `dedupWindow`, until the re-init produces a
    new Sid. Wait for the new Sid (`introspect()`/`onSidPromotion`) if you need
    to read the post-trigger state.

Both are built on engine calls you can use directly without tenants:
`Engine.pause`, `Engine.resume`, `Engine.remove`.

### Adding a tenant at runtime { #adding-a-tenant-at-runtime }

The wrapper has no "add tenant" call: onboarding a tenant is a graph swap that
adds its processes. Do it with
[`Engine.updateGraph`](../concepts/graph-swap.md#read-modify-write-updategraph),
which reads the installed graph and installs your modified copy under the
engine's control lock:

```java
engine.updateGraph(current -> {
    var nodes = new LinkedHashMap<>(current.nodes());
    // ProcessNode(name, dependencies, param, initFactory, loadFactory)
    nodes.put("Stations_PUB3", new ProcessNode("Stations_PUB3",
            List.of(Dependency.reactive("Units")),   // a global that is already installed
            "PUB3",                                  // param: Serializable, immutable, with equals (or null)
            () -> new StationsInit("PUB3"),          // your ProcessInitializer
            () -> new StationsLoad("PUB3")));        // your ProcessLoader
    return new Graph(current.top(), nodes, current.typeRouting());
});
```

Build the tenant's nodes as `ProcessNode`s, not with `GraphBuilder`:
`GraphBuilder.build()` returns a `Graph`, whose constructor validates a
**complete** graph, so a fragment whose nodes depend on globals such as `Units`
fails with "node 'Stations_PUB3' depends on missing 'Units'". The `Graph`
constructor above checks the merged graph instead (missing dependencies,
cycles, static routes). To route a new query type, copy
`current.typeRouting()` into a map and put a `QueryRoute.Static` /
`QueryRoute.Dynamic` for it. In Kotlin, `fom-kotlin`'s `Graph.extend { … }` /
`engine.extendGraphAwait { … }` do this merge for you with the DSL — new nodes
may depend on existing ones by name (see [Kotlin DSL](kotlin-dsl.md)).

Do **not** write it as `engine.newGraph(withTenant(engine.currentGraph()))`.
That read-modify-write is not atomic: a `removeTenant` (for this or another
tenant) that runs between the `currentGraph()` read and the `newGraph` is
undone by the stale copy — the removed tenant's processes are put back into the
graph (cold-initialised, since their state was retired), and its queries start
succeeding again. `updateGraph` lets no other control-plane call — `newGraph`,
`remove`/`removeTenant`, `pause`/`pauseTenant`, … — run
between the read and the install.

!!! warning "A `Dead` node of another tenant makes onboarding block and throw"
    A graph swap also restarts every unchanged node that is `Dead` (that is how
    retrying the same graph recovers it). So if **another** tenant has a `Dead`
    node — its init exhausted its budget, say — `updateGraph` that only adds a
    healthy new tenant starts that `Dead` node again, waits for it for up to
    its whole start budget (`initTimeout` + `loadTimeout`), and
    then throws **its** failure (e.g. its `InitializationTimeoutException`).
    The new tenant was installed nonetheless: its nodes start concurrently with
    the `Dead` one and are `Serving`. Don't trust the exception alone — check
    `aware.processesOf(caller, newTenant)` and `engine.introspect()` to see what
    actually serves. To avoid the wait, `removeTenant` (or `pauseTenant`) the
    tenants with `Dead` nodes before onboarding new ones.

After a restart the engine starts a global process that depends on a paused
tenant process paused too — a pause
[for a dependency](../concepts/graph-swap.md#removing-pausing-and-resuming-processes),
listed by `Engine.pausedByDependency()`. `resumeTenant` resumes such a global
process as well, once none of its dependencies is paused any more, and so on
down a chain of such globals — but only for a caller accepted by
`globalTriggerPolicy`, since a global process is shared by every tenant. For
anyone else it stays paused until an operator resumes it. Globals are resumed
through the atomic
[`Engine.resumeUnblocked`](../concepts/graph-swap.md#removing-pausing-and-resuming-processes)
— only those still paused for a dependency and not blocked — so a global an
operator paused on purpose, even concurrently, is never resumed by
`resumeTenant`. If another operation (e.g. an operator's `pause`)
cancels a global's start, `resumeTenant` does not throw: that global stays
paused. If a global fails to start for any other reason, `resumeTenant` throws a
`RuntimeException` after the tenant's own processes are already resumed; that
global stays paused, and calling `resumeTenant` again retries it.

The three calls retry a refusal caused by a concurrent graph change: if the
engine refuses with `IllegalArgumentException` because another call changed the
graph meanwhile (e.g. two concurrent `removeTenant`), the call retries on the
processes the tenant has now. A concurrent duplicate removal returns the names
it removed — possibly none — instead of failing. Nothing else is retried.
They follow the engine's graph-swap concurrency rule: an authorized
`pauseTenant`/`removeTenant` that cancels a node's start makes the call blocked
on that start fail with `InitInProgressException` ("Init of process
'&lt;name&gt;' was cancelled") — a concurrent `newGraph` blocked on the node's
**first** init, and likewise a `resumeTenant` of the same tenant that is still
starting it (see [Graph swap](../concepts/graph-swap.md#concurrency)). Retry the
`newGraph`; a `resumeTenant` that lost to a `removeTenant` has nothing left to
resume.

!!! warning "Lifecycle calls of all tenants run one at a time"
    `pauseTenant`, `resumeTenant` and `removeTenant` go through
    `Engine.pause`/`resume`/`remove`, which serialise
    on the engine's control lock (with `newGraph` and `updateConfig`). A
    `resumeTenant` — or any call that starts nodes — waits for each node it
    starts for up to `initTimeout` + `loadTimeout`, so **one
    tenant's slow or hanging resume holds every other tenant's
    `pauseTenant`/`resumeTenant`/`removeTenant`** for that long.
    The same goes for stopping: `pauseTenant`/`removeTenant` of a tenant whose
    computes hang waits for them to drain — up to the query's own deadline
    (`queryTimeout`) or the `cleanupTimeout` budget, whichever ends
    first — and holds every other tenant's lifecycle call meanwhile.
    Don't call `pauseTenant`/`removeTenant` synchronously from an
    `EngineObserver` callback about one of that tenant's processes: if the
    callback runs on that process's dispatcher, the call throws
    `IllegalStateException` ("pause('X') called on that process's own
    dispatcher …") at once and pauses nothing — hand it to another thread
    (see [Stopping a process from a callback](observability.md#stopping-from-a-callback)).
    `query`, `queryProcess`, `trigger` and `processesOf` do not take that lock,
    and queries are not held by it. A `trigger` can still wait, though: it
    appends a `LogTrigger`, and while a resume scans the log (which keeps
    snapshots out) a snapshot that is queued for the log gate makes new appends
    queue **behind** it — the gate is a non-fair read/write lock — so that
    `trigger` returns only after the resume's scan and then the snapshot.

!!! warning "`resumeTenant` reads the whole log"
    To find the state to warm-load, `resumeTenant` (like
    `Engine.resume`) scans and deserialises the **entire** log — not
    just the tenant's events (only the recorded params of the nodes being
    resumed are decoded, though) — while holding the control lock (and with
    snapshots held off). Its cost therefore grows with the **total** log length,
    whatever the tenant's size, and it holds every other tenant's lifecycle call
    for that long. Keep the log bounded with a
    [`SnapshotPolicy`](../concepts/snapshots.md#automatically--snapshotpolicy)
    (e.g. `SizeBasedSnapshotPolicy`).

!!! warning "Wide graphs on the file backend"
    On `FileLogBackend` every durable change is one synced append (an
    `fsync`), and appends go one at a time. Each node a tenant call starts,
    cascades or stops writes at least one record (a warm start a `LogLoaded`, a
    cold start `LogInitialized` + `LogLoaded`, a stop at `close()` a
    `LogCleanedUp`), so an operation over N nodes costs roughly N fsyncs or
    more. With thousands of nodes that is **seconds per operation** on the file
    backend. `close()` stops the nodes of one dependency depth together under a
    single `cleanupTimeout` deadline, so a very wide level can run past it
    and log `… did not finish stopping within its cleanup budget …` WARNs. The
    README's performance budget (1000-node start < 5 s) was measured on
    `InMemoryLogBackend`, which does no I/O.

!!! warning "The resolver is consulted for every process, on every call"
    `processesOf`, `pauseTenant`, `resumeTenant` and `removeTenant` find the
    tenant's processes by calling the `TenantResolver` for **every non-global
    node of the current graph** (after one `authzPolicy` check for the tenant).
    A resolver that returns `null` (`IllegalStateException`) or throws for
    **any** node therefore makes these calls fail for **every** tenant — fail
    closed — and a slow resolver slows them all. After the tenant's processes
    are resumed, `resumeTenant` evaluates `globalTriggerPolicy` whenever the
    graph has global processes, even if none of them depends on this tenant: a
    policy that throws makes `resumeTenant` throw although the tenant's
    processes already serve.

!!! note "A global that stays paused is invisible through the wrapper"
    After `resumeTenant` by a caller **without** `globalTriggerPolicy`, a global
    paused for a dependency is left paused, and the wrapper's tenant view does
    not list it (it lists tenant processes only). `Engine.pausedByDependency()`
    is the **only** way to see them, and `TenantAwareEngine` deliberately hides
    the engine it wraps — so keep your own reference to the `Engine` you passed
    to `TenantAwareEngine` and call `pausedByDependency()` on it to see which
    globals are still waiting for an operator.

`removeTenant` inherits `Engine.remove`' refusal to empty the graph: if
the tenant owns *every* process in the current graph, the call throws
`IllegalArgumentException` ("Removing [...] would leave an empty graph") and
nothing is removed — close the engine instead.

!!! warning "Dependencies across the tenant boundary"
    Pausing or removing fails with `IllegalArgumentException` if a process that
    stays running depends on one of the tenant's processes. Keep global
    processes independent of tenant processes. A dependency from one tenant's
    process on **another tenant's** process is not refused when the graph is
    built either, but then `pauseTenant`/`removeTenant` of the producer's tenant
    fail with the engine's message, which names the other tenant's dependent
    ("Cannot pause '&lt;producer&gt;': '&lt;dependent&gt;' depends on it and
    would keep running", "Cannot remove '&lt;producer&gt;': '&lt;dependent&gt;'
    depends on it"). Likewise `resumeTenant` of the dependent's tenant, while
    the producer's tenant is still paused, fails with a message that names the
    other tenant's producer ("Cannot resume '&lt;dependent&gt;': its dependency
    '&lt;producer&gt;' is still paused"). Don't build edges across tenants.

!!! warning "Leadership"
    The lifecycle calls persist their change, so
    `pauseTenant`/`resumeTenant`/`removeTenant` — like `aware.trigger` — throw
    [`LeadershipLostException`](../reference/exceptions.md#leadership) ("lost
    leadership while persisting …") when another instance owns the log. The
    refused write never landed, and the tenant's processes keep serving from the
    state in memory. Two of them can be interrupted part-way and then say so: a
    `removeTenant` that swapped the graph but could not retire the removed
    processes' state **names those processes** (the graph change stands; their
    state is still in the log and whoever leads next picks it up), and a
    `pauseTenant` interrupted between two processes **names what it had already
    paused**. See
    [half-applied operations](../reference/exceptions.md#leadership-half-applied).

## Operating many tenants

### Onboarding cost: every graph change logs the whole graph

Each `newGraph`/`updateGraph` that changes the graph appends one
`LogChangeGraph` holding **every** node of the new graph — name, dependencies
and serialized `param` — not just the added ones. Onboarding tenants one at a
time with `updateGraph` therefore writes 1 + 2 + … + N node definitions:
**O(N²) log bytes** for N tenants. `SizeBasedSnapshotPolicy` does not save you
here: it counts *events*, not bytes, so a few hundred such records can be
gigabytes before it reacts. Instead:

- onboard in **batches** — one `updateGraph` adding many tenants' nodes;
- keep `param`s **small** (an id to look the configuration up by, not the
  configuration itself);
- call `engine.snapshot()` after a bulk onboarding, which keeps only the
  current graph.

### Keep your own tenant list

The engine cannot rebuild a graph from its log: `LogChangeGraph` records the
structure and params but not the factories or the type routes, which live in
your code. After a restart **your application** installs the graph again, so it
must persist which tenants exist (in its own database or configuration) and
build every tenant's nodes from that list before calling `newGraph`. A tenant
left out is not retired — see
[Offboarding](#tenant-lifecycle) above — but it is not running either.

### Provisioning bypasses the wrapper's authorization

Onboarding goes through the raw `Engine` (`updateGraph`), which knows nothing
about callers or `authzPolicy`. If tenants can trigger their own provisioning,
check that the caller owns the tenant id — and that the id does not collide
with an existing tenant or a global process — **yourself**, before calling
`updateGraph`.

### Tenant-initiated triggers

Every `aware.trigger(...)` is a durable log append (an `fsync` on the file
backend) made **before** the dedup window collapses repeats, so a tenant that
triggers in a tight loop costs every tenant log throughput, even though its
process re-initialises only once per window. Rate-limit tenant-facing triggers
per tenant (and prefer the [batch trigger](#batch-trigger)); see
[Triggers](../concepts/triggers-and-watchers.md#triggers).

### Changing the authorization policy at runtime

There is no API to swap `authzPolicy` (or `globalTriggerPolicy`) on a built
wrapper. You can build a new `TenantAwareEngine` over the same `Engine`, or
pass a policy that reads a mutable reference of yours. With the latter, keep in
mind that the wrapper evaluates the policy **per name**: a
[batch trigger](#batch-trigger) calls it once for every name in the batch, so a
policy swapped while the batch is being checked can be seen in two versions —
some names authorized by the old one, the rest by the new. If that matters:

- inside the predicate, read the reference **once** and decide on that
  snapshot, so one evaluation never mixes two versions;
- for a batch that must be decided under one version, check it yourself first
  against a single snapshot, then call `aware.trigger(caller, batch)`.

```java
AtomicReference<Policy> current = new AtomicReference<>(Policy.initial());

var aware = TenantAwareEngine.builder(engine)
    .authzPolicy((caller, tenant) -> {
        Policy p = current.get();              // one snapshot per evaluation
        return p.allows(caller, tenant) && !p.suspended(tenant);
    })
    .build();

current.set(reloadedPolicy);                   // takes effect from the next evaluation
```

### Shared globals: one re-init for every tenant

A global process is shared by all tenants, and so is its re-initialisation. A
trigger (or a dependency change) on a global runs `init` again, once, for
every tenant. What tenants see meanwhile depends on the
[re-init strategy](../concepts/process-lifecycle.md#re-initialisation):

- **`KEEP_OLD` (the default).** The global's old version keeps answering every
  query that reaches it — directly, or through a tenant process's `compute`
  calling `ctx.query` on it — for every tenant, until the new version is
  loaded; then all tenants switch to it together, and the processes that depend
  on it re-initialise after it. If the re-init fails, the old version keeps
  serving **all tenants**: the global is reported `stale`, and the re-init is
  [retried automatically](../concepts/process-lifecycle.md#when-a-re-init-fails).
  The price is memory: the old and the new version are held at once while the
  new one is built.
- **`RELEASE_FIRST`.** The global's state is retired first; for the whole
  re-init every query that reaches it **waits**, for every tenant at once. If
  that re-init fails for good, the global goes `Dead` **for all tenants**.

So:

- give globals **stable** dependencies, and trigger them rarely — not from
  tenant-facing paths (`globalTriggerPolicy` denies everyone by default);
- keep globals on `KEEP_OLD` unless one is too large to hold twice; if you
  switch a global to `RELEASE_FIRST`, make its `init` **resilient**: retry its
  source within the budget, and fall back to the previous value rather than
  failing.

### Budgets are engine-wide

`initTimeout`, `loadTimeout`, `cleanupTimeout` and the
backoff bounds live in `EngineConfig` and apply to **every** node: there is no per-node or per-tenant budget. A tenant whose data
needs a 10-minute init forces that budget on all of them (which also delays how
soon a hung init of any tenant is given up). The workaround is **one engine per
class of tenant** (e.g. small / large), each with its own `EngineConfig` and its
own log. Its cost: globals are per engine, so every engine carries — inits,
stores and serves — its **own copy** of each global the tenants need, and
re-initialises it on its own.

### Computes are not isolated between tenants

All tenants' `compute`, `init` and `load` calls run on virtual threads that
share the JVM's one carrier pool, sized to the number of CPU cores. One
tenant's compute that blocks while **pinned** (inside `synchronized`, on JDK 21)
or that runs a **CPU-bound** loop without parking holds a carrier; a few of them
can stall every tenant — their queries time out, their stops wait out the
cleanup budget. There is no per-tenant thread budget; see
[Threads your code runs on](../concepts/process-lifecycle.md#virtual-threads)
for how to keep computes from pinning and where to run CPU-heavy work.

### Moving a tenant to another engine

There is no export/import API, and a paused process cannot be queried
(`QueryRejectedException`), so a running engine will not hand you a tenant's
state. What you can do:

1. Add the tenant's nodes to the **target** engine (`updateGraph`) — they
   cold-init there from the tenant's source data. `updateGraph` blocks until
   they have settled; check with `introspect()` that they are `Serving`
   (not `Dead`) before going on.
2. Switch your routing so the tenant's callers go to the target engine.
3. `removeTenant` on the **source** engine to retire its state there, and drop
   the tenant from the source's tenant list.

If your `init` cannot rebuild the state from an external source, copy the
persisted cells out of the source's log yourself. The log is read with the
public `io.fom.log` API, so this needs no engine:

1. **Stop the source engine** (`close()`), so the state you copy is final —
   before `removeTenant`, which retires that state.
   Copy its log file and work on the copy — opening a `FileLogBackend`
   takes the file's lock and cuts off a torn last frame.
2. Scan the copy and keep, per tenant process, the **latest `LogInitialized`
   not followed by a `LogDead` for it** — the state a warm start would load:

    ```java
    Map<String, LogInitialized> live = new HashMap<>();
    try (var log = new FileLogBackend(copyOfSourceLog)) {
        log.forEachBetween(0, log.length(), event -> {
            switch (event) {
                case LogInitialized init when init.processName().endsWith("_" + tenant) ->
                        live.put(init.processName(), init);
                case LogDead dead -> live.computeIfPresent(dead.sid().processName(),
                        (name, init) -> dead.sid().clock() >= init.clock() ? null : init);
                default -> { }
            }
        });
    }
    // live.get(name).properties() is the Map<String, byte[]> that process's init returned
    ```

   On `PostgresLogBackend`, query the table instead and decode the payloads as
   described under [CDC](persistence-backends.md#consuming-the-log-with-cdc).
3. Store those cells where the target can read them, and let the target
   node's `init` return them (`CompletableFuture.completedFuture(cells)`)
   instead of computing — then add the nodes to the target (`updateGraph`) and
   switch routing as in the first list above. Its `load` builds the process from the same bytes.

Caveats: the cells are bytes your **process code** wrote, so the target must run
a version of that code (and of any classes it serializes into them) that reads
them; the engine's `SerDe` is not involved in cells, but must still decode the
target's params. The target records the cells as a fresh `init` (a new Sid) —
nothing of the source's history, pauses or pending triggers carries over.

### Quotas and per-tenant state size

There is no per-process state size in `introspect()` or in any `EngineObserver`
callback, so a quota on stored state has to be measured by you. Wrap the
initializer: the cells it returns are exactly what the log stores.

```java
static ProcessInitializer measured(String name, ProcessInitializer delegate) {
    return ctx -> delegate.init(ctx).thenApply(cells -> {
        long bytes = cells.values().stream().mapToLong(c -> c.length).sum();
        stateBytes.put(name, bytes);          // your gauge / quota check
        if (bytes > LIMIT) throw new IllegalStateException(name + " state is " + bytes + " bytes");
        return cells;
    });
}
```

A failing `init` is retried and can end in `Dead` (see
[process lifecycle](../concepts/process-lifecycle.md#retries-backoff-timeouts)),
so throw only for a hard limit; otherwise record and alert.

## Denied access

A denied call fails the returned stage with `TenantAccessDeniedException` (for
`query`/`queryProcess`) or throws it directly (for `trigger` and the lifecycle
calls). If `authzPolicy` or the resolver itself throws during `query` /
`queryProcess`, that exception fails the returned stage too (still fail-closed)
rather than being thrown synchronously. Authorization happens **before** the
engine is touched at all, so a closed engine does not change the verdict: an
unauthorised caller still gets `TenantAccessDeniedException` — `anon` below does,
closed engine or not — and only an *authorised* caller gets the engine's
`IllegalStateException` ("Engine &lt;id&gt; is closed"):

```java
aware.query(anon, new GetStations("PUB1"))
    .whenComplete((r, err) -> {
        if (err instanceof TenantAccessDeniedException) { /* 403 */ }
    });
```

A [batch trigger](#batch-trigger) is denied as a whole, and nothing is
triggered — not even the caller's own names:

```java
// alice may act on PUB1 only: Stations_PUB2 makes the whole batch throw
// TenantAccessDeniedException, and Stations_PUB1 is NOT triggered either.
aware.trigger(alice, Map.of(
    "Stations_PUB1", new RefreshSignal("x"),
    "Stations_PUB2", new RefreshSignal("y")));
```

## Recap of the defaults

| Setting | Default | Implication |
|---|---|---|
| `tenantResolver` | strict `suffixAfter("_")` | only `<base>_<tenant>` resolves; ambiguous names are denied |
| `authzPolicy` | `(c, t) -> false` | **deny-all** until configured |
| `globalProcesses` | none | a process without a tenant is denied |
| `globalTriggerPolicy` | `c -> false` | global processes are query-only |
| non-`Routable` `query` | rejected | use `queryProcess` |

See [Security](../security.md) for the wider threat model.
