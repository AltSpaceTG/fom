# Log events reference

Every durable change is a `LogEvent` (sealed interface, `extends Serializable`).
All carry `long clock()`, `long timestamp()` (epoch millis), and
`short formatVersion()`. See [The log](../concepts/the-log.md) for how they fit
together.

## Common shape

```java
public sealed interface LogEvent extends Serializable
        permits LogLeader, LogChangeGraph, LogInitialized, LogLoaded,
                LogTrigger, LogDependencyChanged, LogDead, LogCleanedUp, LogSnapshot,
                LogPaused, LogResumed {
    long clock();
    long timestamp();
    short formatVersion();
}
```

!!! warning "Order by `clock`, not by `timestamp`"
    `clock` is assigned by the backend at append time and strictly increases.
    `timestamp` is wall-clock context only: it is taken with
    `System.currentTimeMillis()` where the event is *created*, before it queues
    for the append. Under concurrent appends the timestamp column can therefore
    go backwards while `clock` keeps increasing (measured on Postgres: 490
    inversions in 6472 events, the worst 154 ms). Never sort, diff or dedup log
    events by `timestamp`.

## Core events

Stable forever within a major version.

### LogLeader
`(clock, timestamp, formatVersion, String instanceId)` — a JVM instance claimed
leadership. The latest one names the current leader and gates all appends.

### LogChangeGraph
`(clock, timestamp, formatVersion, List<Node> nodes)` — a graph was installed or
swapped. Each `Node(String name, List<String> reactiveDependencies,
List<String> stableDependencies, byte[] param)` records one node's definition;
`param` is produced by `SerDe.serializeParam` (or `null`). This is what a
restart compares against to decide whether persisted state still matches the
node. Factories and routes are **not** logged — they live in code. A
`newGraph` that changes neither nodes nor routes writes none. The serialized
`param` is stored as is, readable by anyone who can read the log — keep secrets
out of params ([Security](../security.md#data-retention)).

### LogInitialized
`(clock, timestamp, formatVersion, String processName, Map<String, byte[]> properties, Sid replaces)`
— a process finished `init`. Defines a [Sid](../concepts/sid-and-clock.md):
`sid() == new Sid(processName, clock)`. `properties` are the persisted cells
(deep-copied on construction).

`replaces` is the version still serving that this init is meant to replace (a
[`KEEP_OLD` re-init](../concepts/process-lifecycle.md#re-initialisation)), or
`null` for a cold init and for every `RELEASE_FIRST` re-init. Such an init is a
**candidate**: the replaced Sid stays the live one until a `LogLoaded` of the
candidate promotes it. A restart that finds a candidate without its `LogLoaded`
serves the replaced version and loads the candidate in the background, without
a new init — unless the node now runs under `RELEASE_FIRST`: then the candidate
is retired and the node cold-inits (see
[Idempotent restart](../concepts/idempotent-restart.md#restart-mid-reinit)). `replaces` must name the same process. The constructors without
`replaces` are kept and pass `null`.

### LogLoaded
`(clock, timestamp, formatVersion, Sid sid)` — a process finished `load` and is
now Serving. For a candidate this is the durable switch: from here on its Sid is
the live one, even if the `LogDead` of the old one never lands.

### LogTrigger
`(clock, timestamp, formatVersion, List<String> processNames)` — a re-init was
requested for these processes (`engine.trigger(...)` or a watcher), recorded
atomically before it is applied. On restart, a process whose latest
`LogTrigger` (or `LogDependencyChanged`) is newer than its live
`LogInitialized` (and than its candidate, if any) is re-initialised after
warm-loading — the request is replayed.
A trigger for a `Dead` process records its `LogTrigger` the same way, before the
restart, and `trigger(Map)` writes **one** record naming every requested
process, `Dead` ones included. Trigger values are not recorded.

During a re-init (either strategy) a `LogTrigger` naming the process may also
**follow** the new version's `LogInitialized`. It is written by the engine, not
by a `trigger()` call: a request (a trigger or a dependency change) that arrived
while the new version was being made has its own record *before* that
`LogInitialized`, so a restart would take it as done. Recording it again after
the new state makes a restart replay it. One is written per re-init, however
many requests queue up.

### LogDead
`(clock, timestamp, formatVersion, Sid sid)` — the given Sid was retired
(re-init, replace, or a candidate that will never serve). Consumers must re-route
to the new live Sid. After a `KEEP_OLD` re-init it follows the new version's
`LogLoaded` and only tidies the log: the promotion already happened.

## Extended events

May be skipped by an older reader with a warning.

### LogDependencyChanged
`(clock, timestamp, formatVersion, Sid sid, String depName, long oldDepClock, long newDepClock)`
— a reactive dependency `depName` of `sid` changed Sid clock from `oldDepClock`
to `newDepClock`. Recorded for tracing and dedup.

### LogCleanedUp
`(clock, timestamp, formatVersion, Sid sid, boolean ok)` — `cleanUp` completed
(`ok=true`) or threw/timed out (`ok=false`) for `sid`.

### LogSnapshot
`(clock, timestamp, formatVersion, long checkpointClock)` — the rotation boundary
marker, the **last** event a [snapshot](../concepts/snapshots.md) writes;
`checkpointClock` is the last clock of the log the snapshot was built from.

## Pause events

The latest `LogPaused` / `LogResumed` for a process decides whether it comes up
paused after a restart. See
[pausing processes](../concepts/graph-swap.md#removing-pausing-and-resuming-processes).

### LogPaused
`(clock, timestamp, formatVersion, String processName, boolean stale, boolean forDependency)`
— the process was paused: stopped, persisted state kept. Written again with
`stale=true` when a trigger or a reactive dependency change arrives while it is
paused, meaning it must re-initialise on resume. `forDependency=true` means
nobody asked for the pause: the engine paused the process at startup only
because a dependency was paused, and it starts by itself after a restart once
no dependency is paused. A snapshot re-emits one per paused process, keeping
both flags.

### LogResumed
`(clock, timestamp, formatVersion, String processName)` — the pause is over: the
process was resumed, or removed from the graph while paused.

## Clock rewriting

When appended, an event's `clock` is advisory; the backend overwrites it with
`LogClocks.nextClock(lastEvent)` — the last event's clock + 1, or `0` in an
empty log. `LogClocks.withClock(event, clock)` rebuilds the record with the
assigned clock. All backends use them, so clocks strictly increase and are never
reused; `compact` keeps the clocks it is given (see
[Sid & clock](../concepts/sid-and-clock.md#sids-across-compaction)). The example
below is an uncompacted log, where clock and position coincide.

## A typical lifecycle in the log

```text
0  LogLeader        (instance claims leadership)
1  LogChangeGraph   (graph installed)
2  LogInitialized   A  → Sid(A,2)
3  LogLoaded        A
4  LogInitialized   B  → Sid(B,4)
5  LogLoaded        B
6  LogTrigger       [A]
7  LogInitialized   A  → Sid(A,7) replaces=Sid(A,2)   (Sid(A,2) still serving)
8  LogLoaded        Sid(A,7)            (the switch)
9  LogDead          Sid(A,2)
10 LogDependencyChanged  Sid(B,4) depName=A 2→7   (B is reactive)
11 LogCleanedUp     Sid(A,2) ok         (after its computes drained)
12 LogInitialized   B  → Sid(B,12) replaces=Sid(B,4)
13 LogLoaded        Sid(B,12)
14 LogDead          Sid(B,4)
15 LogCleanedUp     Sid(B,4) ok
```

Events 9–11 can come in another order: the cascade runs on its own thread and
the old version's cleanup waits for its computes. Under `RELEASE_FIRST` a
re-init writes `LogDead(old)` → `LogCleanedUp(old)` → `LogInitialized(new)`
(without `replaces`) → `LogLoaded(new)`. A request that arrives while the new
version is being made adds a `LogTrigger [A]` right after its `LogInitialized`
(see [LogTrigger](#logtrigger)).
