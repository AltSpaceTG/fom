# Snapshots

The log grows with every change. A **snapshot** compacts it: it writes a fresh,
minimal log that captures only the *current* live state, and archives the old
one. Without snapshots, replay time and disk/table size grow without bound.

## What a snapshot contains

A snapshot replaces the entire log with:

1. a fresh `LogLeader` (the current leader re-claims),
2. a `LogSnapshot` marker carrying the `checkpointClock`,
3. the current `LogChangeGraph` (the serialized graph), and
4. one `LogInitialized` per **live** (non-retired) Sid.

Everything else — old leaders, superseded initialisations, dead Sids, triggers,
dependency-change records — is dropped. The result is a log that replays to
exactly the same engine state, but much shorter.

## Triggering a snapshot

### Manually

```java
SnapshotResult result = engine.snapshot().toCompletableFuture().get();
// result.logId(), result.archivedLogId(), result.checkpointClock(), result.eventsWritten()
```

`engine.snapshot()` runs on the common pool (not the engine scheduler), so it
never stalls dedup/retry/watcher timers.

### Automatically — `SnapshotPolicy`

The engine reads `EngineConfig.snapshotPolicy()`:

- **`SnapshotPolicy.Disabled.INSTANCE`** (default) — no automatic rotation.
- **`SnapshotPolicy.FixedInterval(interval, keepHistory)`** — snapshot every
  `interval`, keeping the newest `keepHistory` archives (older ones are purged).

```java
var cfg = EngineConfig.defaults().snapshotPolicy(); // Disabled by default
// build a config with rotation every 6h, keeping 7 archives:
var rotating = new EngineConfig(
    /* …timeouts… */,
    new SnapshotPolicy.FixedInterval(Duration.ofHours(6), 7));
```

(See [Configuration](../guides/configuration.md) for the full constructor, and
[`fom-config-hocon`](../guides/configuration.md#hocon) to derive a
`FixedInterval` from a Quartz cron string.)

## Custom policies

`SnapshotPolicy` is an open interface. A custom policy implements
`activate(SnapshotContext)`, which the engine calls once when it installs a
graph; the policy runs its own schedule and returns an `AutoCloseable` to stop
it. `SnapshotContext` gives it everything it needs:

```java
public interface SnapshotContext {
    CompletionStage<SnapshotResult> snapshot();   // snapshot now
    ScheduledExecutorService scheduler();          // the engine's scheduler
    LogBackend logBackend();                        // the active backend
    void purgeArchives(int keepHistory);
}
```

The `fom-log-maintenance` module (package `io.fom.maintenance`) ships two:

```java
import io.fom.maintenance.*;

// snapshot when the log exceeds maxEvents; poll length every pollInterval; keep N archives
var bySize = new SizeBasedSnapshotPolicy(/* maxEvents */ 10_000,
                                         /* pollInterval */ Duration.ofMinutes(1),
                                         /* keepHistory */ 7);

// fire whenever ANY constituent policy decides to
var both = new CompositeSnapshotPolicy(
    bySize,
    new SnapshotPolicy.FixedInterval(Duration.ofHours(24), 7));
```

- **`SizeBasedSnapshotPolicy(int maxEvents, Duration pollInterval, int keepHistory)`**
  — snapshot when the log grows beyond `maxEvents`.
- **`CompositeSnapshotPolicy(SnapshotPolicy... policies)`** — activate several;
  a snapshot fires whenever any of them does.

## Archives

Compaction archives the previous log rather than deleting it:

- the **file backend** moves it to a sibling `…​.archived.<timestamp>` file;
- the **Postgres backend** renames the table to `…_archived_<timestamp>`.

`FixedInterval.keepHistory` (and `SnapshotContext.purgeArchives`) bound how many
archives are retained.

## Relationship to leadership

A snapshot is a `compact`, which is a leader-only write. A follower cannot
snapshot. See [The log](the-log.md) and [Multi-node](../guides/multi-node.md).
