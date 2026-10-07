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
| `LogChangeGraph` | a graph is installed or swapped | the graph's structure — per node its name, dependencies and serialized `param`; factories and routes are not logged |
| `LogInitialized` | a process finishes `init` | holds the process name + property cells; defines a [Sid](sid-and-clock.md); `replaces` names the still-serving version a re-init replaces (`null` for a cold init) |
| `LogLoaded` | a process finishes `load` and is Serving | references the Sid; for a re-init it is the moment the new version becomes the live one |
| `LogTrigger` | `engine.trigger(...)` or a watcher fires | names the processes to re-initialise; a request not applied before shutdown is replayed on restart |
| `LogDependencyChanged` | a reactive dependency's Sid changed | tracing + dedup; *extended* event |
| `LogDead` | a Sid is retired (re-init, removal, replace, or a candidate that will never serve) | consumers must re-route to the new Sid |
| `LogCleanedUp` | `cleanUp` completes (or times out) | `ok` flag; *extended* event |
| `LogSnapshot` | a [snapshot](snapshots.md) starts | the rotation boundary; *extended* event |
| `LogPaused` | a process is [paused](graph-swap.md#removing-pausing-and-resuming-processes), or becomes stale while paused | name + `stale` and `forDependency` flags; the pause survives restarts |
| `LogResumed` | a paused process is resumed, or removed while paused | clears the pause |

**Core** events (`LogLeader`, `LogChangeGraph`, `LogInitialized`, `LogLoaded`,
`LogTrigger`, `LogDead`) are stable forever within a major version. **Extended**
events (`LogDependencyChanged`, `LogCleanedUp`, `LogSnapshot`) may be skipped by
older readers with a warning.

See [Log events reference](../reference/log-events.md) for the exact record
shapes.

## The events of a re-init { #reinit-events }

With the default `ReinitStrategy.KEEP_OLD`, a re-init writes nothing until the
new version's `init` succeeds, and retires the old version only after the new
one is loaded:

```text
LogTrigger / LogDependencyChanged          (the request, if any)
LogInitialized(new, replaces = old)        the candidate; old is still the live state
LogTrigger                                 (only if a request arrived meanwhile: recorded again)
LogLoaded(new)                             new becomes the live state
LogDead(old)                               hygiene: written once old stops answering new queries
LogCleanedUp(old)                          after old drained its computes and ran cleanUp
```

A request (a trigger, a dependency change) that arrives while the new version
is being made has its own record *before* the new `LogInitialized`; a restart
would take it as done. So the engine records it again with a `LogTrigger` right
after that `LogInitialized`, and a restart replays it. This holds under
`RELEASE_FIRST` too.

The replacement's appends (`LogInitialized(new)`, `LogLoaded(new)`,
`LogDead(old)`, a retired candidate's `LogDead`, that repeated `LogTrigger`) are
made by the process's own log-writer thread (`fom-log-<name>`), in order, off
the dispatcher: a slow log no longer holds up queries to the old version while
the new one is written.

A failed re-init records nothing for an init that never succeeded; a candidate
that was written but could not be loaded is retired with `LogDead(candidate)`,
and the old version stays the live one. A `LogDead(old)` that fails to append
is only logged at `WARN`: the `LogLoaded(new)` before it already made the new
version live.

With `ReinitStrategy.RELEASE_FIRST` the order is the older one: `LogDead(old)`
first, then `LogCleanedUp(old)`, then `LogInitialized(new)` (no `replaces`) and
`LogLoaded(new)`. See [Re-initialisation](process-lifecycle.md#re-initialisation).

### How a scan reads it

A startup or a [snapshot](snapshots.md) reads, per process, the **live**
`LogInitialized` (the incumbent) and at most one **candidate**:

- a `LogInitialized` whose `replaces` is the live incumbent becomes the
  candidate; any other `LogInitialized` becomes the live one and drops any
  candidate;
- a `LogLoaded` of the candidate makes it the live one;
- a `LogDead` of the candidate drops only the candidate; a `LogDead` at or after
  the incumbent's clock retires both.

So a candidate that will never serve does not need a `LogDead` of its own, and
does not always get one (a removal of a node paused mid re-init, a
`RELEASE_FIRST` node that drops a candidate): the next cold `LogInitialized` or
the incumbent's `LogDead` drops it.

A request (`LogTrigger`, `LogDependencyChanged`) newer than both the incumbent
and its candidate is a re-init still to do: the next start replays it. What a
start does with each case is in
[Idempotent restart](idempotent-restart.md#restart-mid-reinit).

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

!!! danger "Never share a log file across FUSE / NFS / bind-mount views"
    The file backend's leader lock is a POSIX lock on the inode of the log's
    `.lock` file. Views of the same file through layers that do not forward
    POSIX locks (many FUSE filesystems, some NFS setups, container bind layers
    on top of them) each see an unlocked file: **both** instances take the lock,
    both lead, and their appends interleave — silent corruption, not an error.
    Give each log file exactly one writer on one host, and use the
    [Postgres backend](../guides/persistence-backends.md) for more than one node.

## The `LogBackend` SPI

A backend is any implementation of `io.fom.log.LogBackend`:

```java
public interface LogBackend extends Closeable {
    String logId();
    int length();                                  // events sit at positions [0, length())
    LogEvent get(int position);                     // IndexOutOfBoundsException if out of range
    LogEvent[] getBetween(int fromPosition, int toPosition); // [from, to); IndexOutOfBoundsException
                                                             //   if from < 0, to > length() or from > to
    default void forEachBetween(int fromPosition, int toPosition,  // streams [from, to) in order;
                                Consumer<? super LogEvent> action); //   default: getBetween batches of 1,000
    Optional<LogEvent> append(LogEvent event, String leaderInstanceId);
    LogBackendReport introspect();
    io.fom.SnapshotResult compact(List<LogEvent> snapshotEvents,   // note: io.fom, not io.fom.log
                                  String leaderInstanceId);
    default void purgeArchives(int keepHistory) {   // keep the newest N archives
        if (keepHistory < 0) throw new IllegalArgumentException("keepHistory < 0");
    }                                               // default: validate, then do nothing
    void close();
}
```

Contract guarantees an implementation must uphold:

- **Atomic appends.** A partial append is never visible to `get`/`getBetween`.
- **Single leader.** As described above.
- **Concurrent reads.** `get`/`getBetween` are safe while an `append` is in
  flight on another thread.
- **Monotonic clocks.** `append` assigns `clock = last event's clock + 1` (`0`
  in an empty log) — use `LogClocks.nextClock(lastEvent)` and
  `LogClocks.withClock(event, clock)`. Clocks are `long`, strictly increase
  and are never reused — within one log's history: restoring an older copy
  (an archive or a backup) or truncating the log rewinds them, and the clocks of
  the discarded timeline are issued again (see
  [Sid & clock](sid-and-clock.md#clock)).
- **`compact` is leader-only.** If the log's latest `LogLeader` names an
  instance other than `leaderInstanceId`, `compact` must throw
  `io.fom.api.LeadershipLostException` and change nothing: no events replaced,
  no archive written, no temporary file left behind. Otherwise a deposed leader
  would silently re-claim the log by writing its own `LogLeader` back to the
  front — its `engine.snapshot()` would delete the new leader's `LogLeader` and
  both instances would believe they lead.
- **`compact` keeps clocks.** The supplied events keep the clocks they carry;
  if those don't strictly increase, `compact` throws `IllegalArgumentException`
  (`LogClocks.requireIncreasingClocks`). Later appends continue after the
  highest one.
- **`purgeArchives` rejects a negative `keepHistory`.** The default method
  already throws `IllegalArgumentException`; an override must keep that, and
  the contract test asserts it.
- **Invalid ranges throw, never clamp.** `getBetween(from, to)` returns the
  events at `[from, to)` and throws `IndexOutOfBoundsException` when
  `from < 0`, `to > length()` or `from > to` — it must not quietly return a
  shorter array. The `LogBackend` javadoc requires it and the contract test
  asserts it.
- **Positions ≠ clocks.** `get`/`getBetween` address events by position
  `[0, length())`; after a compaction a position and the event's clock differ.
- **Whole-log scans stream.** The engine reads whole logs through
  `forEachBetween` (startup scan, snapshot planning, `fom-log compact`), so
  the heap a scan needs follows the largest record, not the log. The default
  method reads `getBetween` batches of 1,000 events and drops each event from
  the batch as it is handed out; override it to keep fewer decoded events alive
  at a time. `FileLogBackend` decodes one event at a time;
  `PostgresLogBackend` reads windows of at most 1,000 events and 8 MiB of
  payload (a larger single row alone). It must reject invalid ranges like
  `getBetween`.
- **A closed backend refuses.** After `close()`, operations throw
  `IllegalStateException` rather than working on a released resource or
  failing with something else. The contract test's
  `close_then_operations_throw` requires it.

The shared `LogBackendContractTest` (published in `fom-test`) encodes
these invariants; every backend runs against it. See
[Persistence backends](../guides/persistence-backends.md) for the three
bundled implementations and [Testing](../guides/testing.md) for reusing the
contract.

## Compaction

`compact(snapshotEvents, leader)` atomically replaces the whole log with a new,
shorter set of events, archiving the old log. The engine's compaction plan
(`LogCompaction`) writes a `LogLeader` at clock `0`; then every live
`LogInitialized` of the current graph and every candidate still waiting to be
loaded (with its `replaces`, so a restart can finish the replacement), each
group preceded by the
`LogChangeGraph` it was written under (so there may be several, the current
graph last), all with their original clocks; then one `LogTrigger` for re-inits
requested but not yet applied (if any), one `LogPaused` per paused process and,
last, the `LogSnapshot` marker, at clocks after the old log's last clock.
This is how [snapshots](snapshots.md) keep the log bounded without changing any
[Sid](sid-and-clock.md#sids-across-compaction).

Because compaction rewrites the leading `LogLeader` too, it is a leader-only
write like `append`: on a log whose latest `LogLeader` names another instance,
`compact` refuses with
[`LeadershipLostException`](../reference/exceptions.md#leadership) and leaves the
log untouched.
