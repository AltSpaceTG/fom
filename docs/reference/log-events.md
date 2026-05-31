# Log events reference

Every durable change is a `LogEvent` (sealed interface, `extends Serializable`).
All carry `int clock()`, `long timestamp()` (epoch millis), and
`short formatVersion()`. See [The log](../concepts/the-log.md) for how they fit
together.

## Common shape

```java
public sealed interface LogEvent extends Serializable
        permits LogLeader, LogChangeGraph, LogInitialized, LogLoaded,
                LogTrigger, LogDependencyChanged, LogDead, LogCleanedUp, LogSnapshot {
    int clock();
    long timestamp();
    short formatVersion();
}
```

## Core events

Stable forever within a major version.

### LogLeader
`(clock, timestamp, formatVersion, String instanceId)` — a JVM instance claimed
leadership. The latest one names the current leader and gates all appends.

### LogChangeGraph
`(clock, timestamp, formatVersion, byte[] serializedGraph)` — a graph was
installed or swapped. `serializedGraph` is produced by
`SerDe.serializeProcessGraph`.

### LogInitialized
`(clock, timestamp, formatVersion, String processName, Map<String, byte[]> properties)`
— a process finished `init`. Defines a [Sid](../concepts/sid-and-clock.md):
`sid() == new Sid(processName, clock)`. `properties` are the persisted cells
(deep-copied on construction).

### LogLoaded
`(clock, timestamp, formatVersion, Sid sid)` — a process finished `load` and is
now Serving.

### LogTrigger
`(clock, timestamp, formatVersion, Map<String, byte[]> values)` — one or more
triggers fired (`processName → serialized trigger value`), recorded atomically.

### LogDead
`(clock, timestamp, formatVersion, Sid sid)` — the given Sid was retired
(re-init or replace). Consumers must re-route to the new live Sid.

## Extended events

May be skipped by an older reader with a warning.

### LogDependencyChanged
`(clock, timestamp, formatVersion, Sid sid, String depName, int oldDepClock, int newDepClock)`
— a reactive dependency `depName` of `sid` changed Sid clock from `oldDepClock`
to `newDepClock`. Recorded for tracing and dedup.

### LogCleanedUp
`(clock, timestamp, formatVersion, Sid sid, boolean ok)` — `cleanUp` completed
(`ok=true`) or threw/timed out (`ok=false`) for `sid`.

### LogSnapshot
`(clock, timestamp, formatVersion, int checkpointClock)` — the rotation boundary
marker emitted at the start of a [snapshot](../concepts/snapshots.md);
`checkpointClock` is the last clock included in the snapshot's source state.

## Clock rewriting

When appended, an event's `clock` is advisory; the backend overwrites it with
the next log position. `ClockRewriter.withClock(event, clock)` rebuilds the
record with the assigned clock — all backends use it, so the **first event in an
empty log is always clock 0**.

## A typical lifecycle in the log

```text
0  LogLeader        (instance claims leadership)
1  LogChangeGraph   (graph installed)
2  LogInitialized   A  → Sid(A,2)
3  LogLoaded        A
4  LogInitialized   B  → Sid(B,4)
5  LogLoaded        B
6  LogTrigger       {A: refresh}
7  LogDead          Sid(A,2)            (A re-initialising)
8  LogInitialized   A  → Sid(A,8)
9  LogLoaded        A
10 LogDependencyChanged  Sid(B,4) depName=A 2→8   (B is reactive)
11 LogDead          Sid(B,4)            (B cascades)
12 LogInitialized   B  → Sid(B,12)
13 LogLoaded        B
```
