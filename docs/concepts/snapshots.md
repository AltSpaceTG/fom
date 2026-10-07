# Snapshots

The log grows with every change. A **snapshot** compacts it: it writes a fresh,
minimal log that captures only the *current* live state, and archives the old
one. Without snapshots, replay time and disk/table size grow without bound.

## What a snapshot contains

A snapshot replaces the entire log with, in order:

1. a `LogLeader` for the current leader, at clock `0`;
2. for each process in the latest graph, its **live** `LogInitialized`, if it
   has one (see [the liveness rule](idempotent-restart.md#how-it-works);
   an older `LogInitialized` superseded by a later one is never kept). A
   process caught mid [re-init](process-lifecycle.md#re-initialisation), with
   its new version written but not yet loaded, keeps both: the serving
   `LogInitialized` and the new one with its `replaces`, so a restart warm-loads
   the old version and finishes the replacement without another `init`.
   These records are
   grouped by the graph they were written under: each group is preceded by the
   `LogChangeGraph` that was current when those inits were written. A compacted
   log can therefore hold **several** `LogChangeGraph` events; the latest graph
   always comes last, so events appended after compaction fall under it. An
   older graph is kept **trimmed to the nodes whose state follows it** — the
   definitions (params) of processes no longer in the graph are not carried
   over, which is what makes an [erasure](../security.md#data-retention) complete. This
   lets a restart still compare persisted state against the definition it was
   built for. These kept events keep their **original clocks**;
3. one `LogTrigger` naming the processes whose requested re-init (a trigger or a
   reactive dependency change) had not happened yet — only if there are any;
4. one `LogPaused` per paused process, keeping whether it was paused by an
   operator or only for a dependency (`forDependency`);
5. a `LogSnapshot` marker carrying the `checkpointClock` (the last clock of the
   old log), always **last**.

The synthetic events 3–5 get fresh clocks right after the old log's last clock
(`lastClock + 1`, `+ 2`, …), and later appends continue after the marker.

Everything else — old leaders, superseded initialisations, dead Sids, loads,
already-applied triggers and dependency-change records, state of processes no
longer in the graph — is dropped. The result replays to the same engine state,
but much shorter.

!!! note "Sids survive snapshots"
    Clocks are never renumbered, so every process keeps its Sid through a
    snapshot, and a restart on the compacted log loads the same Sids. Clocks do
    get gaps, so they no longer equal log positions. See
    [Sid & clock](sid-and-clock.md#sids-across-compaction).

!!! warning "Timestamps are not ascending after a compaction"
    Kept events keep their **original timestamps** along with their original
    clocks, while the synthetic events are stamped with the wall clock of the
    compaction. The `LogLeader` at clock `0` is therefore the *newest* timestamp
    near the start of the log, immediately followed by events that are older —
    often much older. The `LogTrigger`/`LogPaused`/`LogSnapshot` at the end carry
    that same compaction time.

    So: clocks are ordered, timestamps are not. Read the log by **clock or
    position**, never by sorting on the timestamp, and do not compute an
    "elapsed" from the first event's timestamp in a compacted log.

## Triggering a snapshot

### Manually

```java
SnapshotResult result = engine.snapshot().toCompletableFuture().get();
// result.newLogId(), result.archivedLogId(), result.checkpointClock(), result.eventsCopied()
```

`engine.snapshot()` runs on a virtual thread of its own — neither on the engine
scheduler, so it never stalls dedup/retry/watcher timers, nor on the common
`ForkJoinPool`, so application code blocking that pool cannot stall a snapshot. Appends, and the log scan of
`resume`, wait while a snapshot scans and rewrites the log, so neither
races it. Calls are **not coalesced**: each `snapshot()` is its own compaction
and leaves its own archive, and concurrent calls run one after another — but
not necessarily in call order: even back-to-back `snapshot()` calls from one
thread each run on their own virtual thread and race for the log gate, so they
may complete in either order. To tell which compaction came later, compare
their `SnapshotResult.checkpointClock()` rather than relying on call order. A
backlog of them also slows `close()`: its shutdown records wait for each
snapshot holding the log.

`engine.snapshot()` does **not** purge archives — only a scheduled
[policy](#automatically--snapshotpolicy) given a finite `keepHistory` does, and
by default none is (see [Archive retention](#archive-retention)). So a caller that
rotates the log by hand, or keeps every archive, deletes archives by hand:

```java
engine.purgeArchives(7);   // delete all but the 7 newest archives
```

`keepHistory` must be `>= 0`, otherwise `IllegalArgumentException`;
`purgeArchives(0)` deletes **every** archive of this log, keeping only the live
log itself. (A scheduled policy is stricter: its `keepHistory` is either
`SnapshotPolicy.KEEP_ALL` — the default — or `>= 1`, so automatic rotation never
deletes the most recent archive, and by default deletes none.) A backend failure **fails this call**:
you asked for the archives to go, so you hear that they did not. Only the purge a
[scheduled policy](#automatically--snapshotpolicy) runs by itself — including
`SnapshotContext.purgeArchives` — keeps such a failure to the log, since a policy
on a timer should not fail over an archive left behind.

`purgeArchives` is **leader-only**: the archives belong to whoever leads the log
now, so an engine that has been deposed refuses with
[`LeadershipLostException`](../reference/exceptions.md#leadership) ("Engine
&lt;id&gt; is not the leader of &lt;logId&gt; (&lt;other&gt; is); no archive was
removed") and deletes nothing.

### Automatically — `SnapshotPolicy`

The engine reads `EngineConfig.snapshotPolicy()`:

- **`SnapshotPolicy.Disabled.INSTANCE`** (default) — no automatic rotation, and
  nothing purges archives: call `engine.purgeArchives(keepHistory)` yourself.
- **`SnapshotPolicy.FixedInterval(interval)`** — snapshot every `interval` and
  **keep every archive** (`keepHistory = SnapshotPolicy.KEEP_ALL`).
- **`SnapshotPolicy.FixedInterval(interval, keepHistory)`** — snapshot every
  `interval`, keeping the newest `keepHistory` archives (older ones are purged
  after each snapshot). `keepHistory` must be `>= 1` or `SnapshotPolicy.KEEP_ALL`.

  A sub-millisecond `interval` is treated as 1 ms. A tick that comes while the
  previous scheduled snapshot is still running is **skipped**. A snapshot slower
  than `interval` therefore does not pile further snapshots up behind it.

```java
// the default config rotates nothing: snapshotPolicy() is Disabled.INSTANCE.
// Copy it with rotation every 6h — every archive is kept:
EngineConfig rotating = EngineConfig.defaults()
        .withSnapshotPolicy(new SnapshotPolicy.FixedInterval(Duration.ofHours(6)));
// …or keep only the newest 7 archives, deleting older ones after each snapshot:
EngineConfig bounded = EngineConfig.defaults()
        .withSnapshotPolicy(new SnapshotPolicy.FixedInterval(Duration.ofHours(6), 7));
```

#### Archive retention { #archive-retention }

**By default no archive is ever deleted.** Every built-in policy has a
constructor without `keepHistory` — `FixedInterval(Duration)`,
`SizeBasedSnapshotPolicy(int, Duration)`, `CronSnapshotPolicy(String[, ZoneId])`
— that uses `SnapshotPolicy.KEEP_ALL` (`Integer.MAX_VALUE`, read "keep at most
2³¹−1", i.e. all of them). With `KEEP_ALL` the policy runs **no purge at all**
after a snapshot: the backend's archives are not even listed. In HOCON, leaving
out `log.rotate.keep-history` (or writing `keep-history = all`) means the same.

To bound retention, pass a finite `keepHistory` (`>= 1`): after each of its
snapshots the policy deletes all but that many newest archives. Or keep the
default and delete archives yourself with
[`engine.purgeArchives(n)`](#manually) (from a cron job, an admin endpoint…) — it
works the same whatever the policy.

!!! warning "Kept archives grow the disk without bound"
    Each snapshot leaves one archive of roughly the size of the log it replaced
    (see [Policies count events, not bytes](#policies-count-events-not-bytes)).
    With the default `KEEP_ALL`, a policy that snapshots every 6 hours adds four
    such archives a day, forever. Either set a `keepHistory`, call
    `engine.purgeArchives(n)` on your own schedule, or move archives to cheaper
    storage — the engine never needs them, they exist for
    [restoring](#restoring-an-archive) and auditing.

(See [Configuration](../guides/configuration.md) for the full constructor, and
[`fom-config-hocon`](../guides/configuration.md#hocon), which turns a Quartz
cron string into an `io.fom.config.CronSnapshotPolicy`.)

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

`fom-core` ships two more (package `io.fom`):

```java
import io.fom.*;

// snapshot once maxEvents events were added since the last snapshot; poll every pollInterval; keep N archives
// (omit keepHistory to keep every archive)
var bySize = new SizeBasedSnapshotPolicy(/* maxEvents */ 10_000,
                                         /* pollInterval */ Duration.ofMinutes(1),
                                         /* keepHistory */ 7);

// fire whenever ANY constituent policy decides to
var both = new CompositeSnapshotPolicy(
    bySize,
    new SnapshotPolicy.FixedInterval(Duration.ofHours(24), 7));
```

- **`SizeBasedSnapshotPolicy(int maxEvents, Duration pollInterval[, int keepHistory])`**
  — `keepHistory` defaults to `KEEP_ALL`; snapshot once the log has grown by `maxEvents` events since the previous
  snapshot (measured from the compacted length, so a small threshold cannot
  loop). Polling never blocks the engine scheduler: each poll reads the log's
  length on a virtual thread of its own, off the engine's single timer thread,
  one poll at a time. Polls are skipped while a poll or a snapshot is in flight.
- **`CompositeSnapshotPolicy(SnapshotPolicy... policies)`** — activate several;
  a snapshot fires whenever any of them does. `FixedInterval` children get
  their own periodic schedule; `Disabled` children are ignored.
  Each child purges archives to its **own** `keepHistory` after each of its own
  snapshots, and archives are not tagged with the policy that made them. So when
  several children fire, every purge by the child with the smallest
  `keepHistory` trims all archives down to that number — in practice the
  smallest `keepHistory` among the children that fire decides retention. Give
  the children the same `keepHistory`, or treat the smallest one as the real
  limit. A child with `KEEP_ALL` purges nothing itself, but does not protect
  archives from a sibling with a finite `keepHistory` either.

### Policies count events, not bytes

Every built-in policy measures the log in **events** (`maxEvents`) or in
**time**, never in bytes. With small records that makes no difference; with
large ones (a process whose state is megabytes) it does. Every log starts from
a [snapshot](#what-a-snapshot-contains), which holds one `LogInitialized` per
live process (two for one caught mid re-init) — the whole **live state** — and between rotations grows by about
`maxEvents` records on top of it. Each archive is a copy of such a log, so the
disk one log needs is roughly

```
(keepHistory + 1) × (live state + maxEvents × record size)
```

— `keepHistory = 7`, `maxEvents = 10_000` and 1 MiB records come to about
80 GB plus 8 copies of the live state, not the few megabytes the event count
suggests. With a large live state the base term dominates: 200 processes of
50 MB each are 10 GB per log, 80 GB across 7 archives and the live log, even
when few events arrive between snapshots. Size `maxEvents` and
`keepHistory` from your **largest** record, or write a
[custom policy](#custom-policies) that snapshots on the log file's size in
bytes (e.g. `Files.size` of the log path, timed with `SnapshotContext.scheduler()`
but read off that thread, as `SizeBasedSnapshotPolicy` does).

### When a scheduled snapshot fails { #when-a-scheduled-snapshot-fails }

A scheduled snapshot that fails — a full disk, a
[damaged log file](../security.md#damage-while-running) — does not stop rotation:
the **next tick retries**. A failed compaction replaces nothing and purges no
archive, so the live log stays as it was and keeps growing until a snapshot
succeeds. What gets logged depends on the policy:

- **`FixedInterval`** (as `EngineConfig.snapshotPolicy()`) — a failure that
  repeats on every tick is not logged on every tick. The logger `io.fom.Engine`
  writes
  `Engine[<id>] scheduled snapshot failed (N in a row; repeats are logged at doubling intervals): <error>`
  at **WARN** on the 1st, 2nd, 4th, 8th… failure in a row and at DEBUG
  otherwise, and `Engine[<id>] scheduled snapshot succeeded after N failure(s)`
  at **INFO** once one succeeds again.
- **`SizeBasedSnapshotPolicy`** — `SizeBasedSnapshotPolicy snapshot failed: …`
  at WARN on every failure; the threshold is still exceeded, so the next poll
  tries again.
- **`CompositeSnapshotPolicy`** — its `FixedInterval` children log
  `CompositeSnapshotPolicy fixed-interval snapshot failed: …` at WARN on every
  failure (no doubling); other children log as they do on their own.
- **`CronSnapshotPolicy`** (`fom-config-hocon`) —
  `CronSnapshotPolicy snapshot failed: …` at WARN on every failure; the next
  cron slot tries again.

The log line is the **only** signal: `engine.introspect()` and the
`EngineObserver` carry no snapshot status. Alert on it — e.g. on a WARN from
`io.fom.Engine` containing `scheduled snapshot failed`, or from the policy's
own logger.

## Archives

Compaction archives the previous log rather than deleting it:

- the **file backend** copies it to a sibling `…​.archived.<timestamp>.partial`
  file and atomically renames that to `…​.archived.<timestamp>`, so a crash
  mid-copy never leaves a truncated file that looks like a complete archive;
- the **Postgres backend** renames the table to `…_archived_<timestamp>`.

!!! warning "Free disk space for a file-backend compaction"
    The file backend writes the compacted log to a `<name>.tmp` file first, then
    **copies** the whole old log to the archive (`Files.copy`, not a rename: the
    live log must stay valid until the atomic swap), and only then replaces the
    log with the compacted file. At its peak a compaction therefore needs about
    **log size + compacted size** of free space in the log's directory, and the
    archive keeps the log size in use until `purgeArchives` removes it. A
    compaction that runs out of space fails and leaves the log as it was (the
    `.tmp` is removed at once, a leftover `.partial` copy by the next open or
    `purgeArchives`), but it cannot shrink the log
    either: on a nearly full disk, snapshot earlier (a lower `maxEvents` /
    shorter interval, so the log never grows that big), purge archives
    (`keepHistory`, `engine.purgeArchives`) or free space before it is needed.

By default archives are **kept forever** — see
[Archive retention](#archive-retention). A finite `keepHistory` on a policy (and
`SnapshotContext.purgeArchives`, or `Engine.purgeArchives` called by hand) bounds
how many are retained: after each scheduled snapshot of such a policy the engine
calls `LogBackend.purgeArchives(keepHistory)`, which deletes all but the newest
`<name>.archived.<millis>` files (file backend — only names ending in digits
count; leftover `.partial` copies are deleted) or drops all but the newest
`<table>_archived_<millis>` tables (Postgres backend; earlier versions never
purged them). The SPI default is a no-op — a
[custom backend](../guides/persistence-backends.md#writing-your-own-backend)
that archives should implement it. No backend deletes archives on its own —
`compact` only creates them (and removes leftover `.partial` / `.tmp` files of
an interrupted compaction); deletion happens only through `purgeArchives`.

A policy snapshot that is still running when `engine.close()` is called
completes, but the policy does **not** purge archives after it: once the engine
is closed the backend may be closed too, so every built-in policy
(`FixedInterval`, `SizeBasedSnapshotPolicy`, `CompositeSnapshotPolicy`,
`CronSnapshotPolicy`) skips that purge alike. The archive count can therefore
exceed `keepHistory` by one until the next scheduled snapshot's purge (after the
next start), or until you call `engine.purgeArchives(keepHistory)` yourself.

### Restoring an archive

On the file backend an archive is a byte-for-byte copy of the log as it was just
before that compaction — same header, same CRC-checked frames — so it is a valid
log file. To put it back as the live log:

1. **stop the engine** — `engine.close()` **and** the backend's own `close()`
   (the engine does not close the backend it was given), or stop the JVM — a running backend
   refuses to write to a log file replaced under it (`IllegalStateException`),
   and the `<name>.lock` must be released;
2. copy the archive over the log path (keep the archive itself if you may need
   it again): `cp app.log.archived.1727600000000 app.log`;
3. start the engine on that path as usual.

The engine then comes back to **that archive's state**: the processes warm-load
the state they had at that compaction, and everything appended to the live log
after it is gone. Clocks restart after the archive's last clock, so the clocks —
and therefore the Sids — of the discarded timeline are **issued again** for
different events. Anything that stored a Sid or clock from the discarded
timeline (an external cache key, an audit record) must not assume it still
names the same state; see
[Sid & clock](sid-and-clock.md#clock).

Restoring a Postgres archive (renaming an `…_archived_<timestamp>` table back)
is not covered here.

## Relationship to leadership

A snapshot is a `compact`, which is a leader-only write. A follower cannot
snapshot: compaction rewrites the leading `LogLeader` as well, so if the log's
latest `LogLeader` names another instance, `compact` refuses with
[`LeadershipLostException`](../reference/exceptions.md#leadership) and the log is
left untouched — no events replaced, no archive, no temporary file. Without that
check a deposed leader's `engine.snapshot()` would silently re-claim the log and
delete the new leader's `LogLeader`. See [The log](the-log.md) and
[Multi-node](../guides/multi-node.md).
