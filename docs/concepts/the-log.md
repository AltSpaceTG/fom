# The log

The append-only log is the **source of truth**. The in-memory engine state is a
projection of it; replaying the log reconstructs the engine. Everything durable
— leadership, the graph, each process's initialisation and retirement — is an
event in the log.

## Event types

All events implement the sealed interface `LogEvent` and carry a `clock`, a
`timestamp` (epoch millis), and a `formatVersion`.

| Event | Written when | Notes |
|---|---|---|
| `LogLeader` | a JVM instance claims leadership | the latest one names the current leader |
| `LogChangeGraph` | a graph is installed or swapped | holds the serialized `Graph` bytes |
| `LogInitialized` | a process finishes `init` | holds the process name + property cells; defines a [Sid](sid-and-clock.md) |
| `LogLoaded` | a process finishes `load` and is Serving | references the Sid |
| `LogTrigger` | `engine.trigger(...)` or a watcher fires | holds serialized trigger values |
| `LogDependencyChanged` | a reactive dependency's Sid changed | tracing + dedup; *extended* event |
| `LogDead` | a Sid is retired (reinit/replace) | consumers must re-route to the new Sid |
| `LogCleanedUp` | `cleanUp` completes (or times out) | `ok` flag; *extended* event |
| `LogSnapshot` | a [snapshot](snapshots.md) starts | the rotation boundary; *extended* event |

**Core** events (`LogLeader`, `LogChangeGraph`, `LogInitialized`, `LogLoaded`,
`LogTrigger`, `LogDead`) are stable forever within a major version. **Extended**
events (`LogDependencyChanged`, `LogCleanedUp`, `LogSnapshot`) may be skipped by
older readers with a warning.

See [Log events reference](../reference/log-events.md) for the exact record
shapes.

## Leadership: one writer

Only the **leader** may append. The rule is enforced by `append(event,
leaderInstanceId)`:

- On an empty log, the first append must itself be a `LogLeader` — that claims
  leadership.
- Afterwards, an append succeeds only if the latest `LogLeader` in the log has
  `instanceId == leaderInstanceId`. A stale leader gets `Optional.empty()`.
- Writing a new `LogLeader` is a **takeover**: the new instance becomes leader
  and the old one can no longer append.

The *backend* additionally guards against two processes both believing they are
leader: the file backend takes an exclusive OS file lock; the Postgres backend
takes a `pg_advisory_lock`. See [Multi-node](../guides/multi-node.md).

## The `LogBackend` SPI

A backend is any implementation of `io.fom.log.LogBackend`:

```java
public interface LogBackend extends Closeable {
    String logId();
    int length();                                  // events have clocks [0, length())
    LogEvent get(int clock);                        // IndexOutOfBoundsException if out of range
    LogEvent[] getBetween(int fromClock, int toClock);
    Optional<LogEvent> append(LogEvent event, String leaderInstanceId);
    LogBackendReport introspect();
    SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId);
    void close();
}
```

Contract guarantees an implementation must uphold:

- **Atomic appends.** A partial append is never visible to `get`/`getBetween`.
- **Single leader.** As described above.
- **Concurrent reads.** `get`/`getBetween` are safe while an `append` is in
  flight on another thread.
- **Dense, 0-based clocks.** First event is clock 0.

The shared `LogBackendContractTest` (in `fom-core`'s test fixtures) encodes
these invariants; every backend runs against it. See
[Persistence backends](../guides/persistence-backends.md) for the three
bundled implementations and [Testing](../guides/testing.md) for reusing the
contract.

## Compaction

`compact(snapshotEvents, leader)` atomically replaces the whole log with a new,
shorter set of events (a fresh `LogLeader`, a `LogSnapshot` marker, the current
`LogChangeGraph`, and one `LogInitialized` per live Sid), archiving the old log.
This is how [snapshots](snapshots.md) keep the log bounded.
