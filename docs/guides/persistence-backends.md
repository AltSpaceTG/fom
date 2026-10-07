# Persistence backends

A `LogBackend` is where the [append-only log](../concepts/the-log.md) lives.
Three are bundled; you can write your own against the SPI.

| Backend | Module | Persists? | Leadership | Use for |
|---|---|---|---|---|
| `InMemoryLogBackend` | `fom-core` | no | in-process | tests, quickstart, ephemeral state |
| `FileLogBackend` | `fom-core` | yes (file) | OS file lock | single-node durable state |
| `PostgresLogBackend` | `fom-jdbc` | yes (table) | `pg_advisory_lock` | multi-node leader coordination |

All three honour the same [`LogBackend` contract](../concepts/the-log.md#the-logbackend-spi)
(atomic append, single leader, concurrent reads, monotonic clocks kept by
`compact`) and are
verified against the shared `LogBackendContractTest`.

## InMemoryLogBackend

```java
try (var backend = new InMemoryLogBackend()) { … }
try (var backend = new InMemoryLogBackend("my-log-id")) { … }
```

Backed by a `CopyOnWriteArrayList`. Nothing survives the JVM — every start is a
cold start. Ideal for tests.

## FileLogBackend

```java
void run() throws IOException {   // the constructor is declared `throws IOException`
    try (var backend = new FileLogBackend(Path.of("/var/lib/fom/log.bin"))) { … }
}
```

`FileLogBackend(Path)` is the one place in the API with a **checked
exception**: it throws `java.io.IOException` if the file cannot be opened or
cannot be replayed — so the enclosing method has to declare or handle it
(`public static void main(String[] args) throws Exception` is enough in a small
program). A log that is **already open** — its `.lock` held by another process, or
by another backend in this JVM — is refused with an unchecked
`IllegalStateException` (`Cannot acquire leader lock on … — another process holds
it` / `… is held by another thread in this JVM`), not `IOException`. A log **file name** longer than 224 bytes (UTF-8) is
refused up front with `IllegalArgumentException`: the backend keeps sibling files
named `<name>` plus a suffix of up to 31 bytes (`.archived.<13-digit millis>.partial`;
also `.truncated.<millis>`, `.tmp`, `.lock`), and a name that fits the 255-byte
`NAME_MAX` limit but not with the suffix would open fine and then fail every
snapshot — and make a torn tail unrecoverable.

- Each event is framed `[int length][int CRC32][payload]`; the file starts with
  a 4-byte magic (`FOM\1`) so a wrong file fails loudly.
- On open, the whole file is scanned. Only an incomplete **last** frame — what a
  crash mid-append leaves behind — is cut off; the discarded bytes are saved
  next to the log as `<name>.truncated.<millis>`. A frame that runs past the end
  of the file (or is last and fails its CRC) counts as incomplete only if no
  complete, CRC-valid frame can be found after its header: an append is one
  sequential, synced write, so a crash leaves nothing intact behind a torn
  frame, whereas a damaged length field in a middle frame does. Only a frame
  that starts a structurally valid chain of frames running to the end of the
  file counts, so bytes inside the torn frame's own payload that happen to look
  like frames (serialized properties, even a whole fom log stored as one) do
  not stop the recovery. The search is linear in the size of the torn frame;
  data crafted to force excessive CRC work fails the open with
  `LogCorruptedException` instead. If the discarded bytes cannot be saved (a
  full disk, typically), the open fails with an `IOException` naming the log,
  the sibling and the cause — `could not save the torn tail of <log> to
  <log>.truncated.<millis>: ... No space left on device` — no partial
  `.truncated.*` file is left behind and the log is **not** modified; free some
  space and open it again (the "free some space" hint appears only when the cause
  is a full disk or quota; for any other cause, such as `AccessDeniedException` on
  the directory, the message says to fix the cause). The saved tail gets the
  log's POSIX permissions (and owner/group where permitted).
- A failed `append` throws a `RuntimeException` whose message names the cause
  too — `Append failed on <log>: java.io.IOException: No space left on device` —
  so it is visible where only `getMessage()` is reported (e.g. `NodeReport.lastException`).
- Any other damage — a bad frame followed by more data (including a middle
  frame whose damaged length points past the end of the file), a frame length above
  the 64 MiB payload limit, or an intact (CRC-valid) frame that cannot be
  decoded (tampering, a class rejected by the deserialization allowlist, or an
  event class this version cannot read), or an event whose clock is not greater
  than the one before it (frames from two timelines spliced together — a split
  brain, or a backup stitched onto a copy; opening it would make the engine
  re-issue clocks that already exist) — makes the constructor throw
  `io.fom.log.LogCorruptedException` and leaves the file **untouched**. It is an
  `IOException` carrying `offset()` (byte offset of the first unreadable frame),
  `readableEvents()` and `path()`. To recover, back the file up, then restore it
  or truncate it to `offset()` bytes (e.g. `truncate -s <offset> <file>`), which
  drops every event from there on. [`fom-log diagnose`](cli.md#diagnose) reports
  the same offset.
- An event the backend could not read back after a restart is refused at
  `append` with `IllegalArgumentException` instead of being written. That means
  one over the log payload limits: at most 64 MiB per event (the serialized
  frame payload, the same limit the reader enforces), arrays of at most
  10,000,000 elements (e.g. a `byte[]` property), 1,000,000 object references,
  nesting depth 64. The message names these limits. `PostgresLogBackend` runs
  the same read-back check but has no separate 64 MiB size cap. If that event is an init result (`LogInitialized`), the process goes
  `Dead` at once without retrying `init`; see
  [Process lifecycle](../concepts/process-lifecycle.md#retries-backoff-timeouts).
- I/O is not broken by thread interrupts: an interrupted caller's append still
  completes (its interrupt flag is restored afterwards). An interrupt landing in
  the middle of a write closes the channel; the backend reopens it (logged at
  `DEBUG`), removes the
  partial frame and writes the event again — the append fails only if it is
  interrupted three times in a row, and the log stays consistent either way.
- **Leadership** is an exclusive OS lock on a sibling `<name>.lock` file, taken
  at construction. The lock, like the data file, archives and `.tmp`, sits next
  to the **real** file: the path is resolved through symlinks (for a log that
  does not exist yet, through its parent directory), so a second process opening
  the same file by any path — directly or via a symlink — fails with
  `IllegalStateException`, and `logId()` reports the resolved path. Hard links
  cannot be told apart: don't hard-link a log file. As before, missing parent
  directories are created (for a symlink to a log not yet created: the
  target's). **Upgrading with a symlinked log:** the `.lock`, `.tmp`,
  `.truncated.*` and new `.archived.*` files now live next to the real file,
  so archives written earlier beside the symlink are no longer purged (move
  them next to the real file, or delete them); and don't run an old and a new
  version against the same symlinked log at once (a rolling upgrade) — the old
  one locks `<link>.lock`, the new one `<real>.lock`, and both would lead. **Don't delete the `.lock` file while an engine
  runs:** the lock is held on that file, so a second process could then create
  a new one, lock it and lead the same log unnoticed.
- **Don't delete, rename, replace or overwrite the log file while an engine
  runs.** The backend writes through the file it opened, so after an `rm`, a
  `mv` or a restore-from-backup over the path, appends would land in an orphaned
  file that the next start never sees — and that start would silently
  cold-start. Instead, before every append (and every compaction) the backend
  checks, with one `stat`, that the path still names the file it opened (same
  file key, i.e. inode). If not, the write is refused with
  `IllegalStateException: log file <path> was deleted or replaced while open
  ...; refusing to append to an orphaned file`. An overwrite **in place** keeps
  the inode (`cp backup.bin log.bin` onto an existing file, a `truncate`, another
  program appending), so the backend also checks, with one `fstat`, that the
  file still ends exactly where its own last write left it; if not, the write is
  refused with `IllegalStateException: log file <path> was modified by
  something else while open (it is <n> bytes, this backend last left it at
  <m>) ...`. An in-place overwrite with content of exactly the same length is
  not detected. Per the `LogBackend` SPI both are **transient** failures: the
  engine retries with backoff and the processes involved fail loudly once the
  phase's retry budget is spent; nothing is acknowledged meanwhile. Putting the
  original file (or its original content) back lets appends resume. On
  platforms without file keys (Windows) the file-key check is skipped; the size
  check still runs.
- **Compaction** writes the compacted log to `<name>.tmp` (fsynced), copies the
  old one to `<name>.archived.<timestamp>.partial` and atomically renames it to
  `<name>.archived.<timestamp>`, atomically replaces the live file with
  `<name>.tmp`, and fsyncs the directory so the rename is durable across power
  loss. The `.tmp` file gets the live log's POSIX permissions, and its owner and
  group where the process may change them (as root), before any data is written,
  so a `0600` log stays `0600` after a snapshot instead of taking the umask's
  defaults, and a root-run `fom-log compact` does not leave a root-owned log
  (a failed `chown` as non-root is ignored; permissions are still copied). On file
  systems without POSIX attributes this is skipped. The live file is valid at every step: a crash before the swap leaves
  the old log, one after it the new log. What a crash can leave behind is a
  full-size `<name>.tmp` and/or a `.partial` archive copy; the backend deletes
  both (a regular `<name>.tmp` and `<name>.archived.<digits>.partial`) when it
  next opens the log, after taking the lock, and logs each at `INFO`.
  `purgeArchives` deletes all but the newest `<name>.archived.<millis>` files —
  only names ending in digits count — and also deletes leftover `.partial` files.
  The stamp is the wall-clock time, but never lower than the highest existing
  archive stamp + 1, so the newest archive keeps the largest stamp even after
  the clock steps backwards. An archive stamped more than 100 years ahead of the
  clock is not believed: it is logged at `WARN` and treated as the oldest.

This backend owns its files; the path is a trusted input from the embedding
application.

### Sizing a large log

Opening a log reads and deserializes **every** frame (to verify it and rebuild
the index), and the backend keeps a per-event offset and length in the heap for
as long as it is open — about 75 bytes per event, measured. A 5 GB log of
21 million events took about 1.6 GB of heap and 4.4 minutes to open. The
[`fom-log`](cli.md) read commands open a copy of the log the same way and need
about as much heap (`JAVA_OPTS=-Xmx…`). What keeps both bounded is
[snapshots](../concepts/snapshots.md): a compaction rewrites the log down to
the live state, so open time and index size follow the state plus the events
since the last snapshot, not the whole history. If a log grows into the
millions of events, turn snapshots on (or run `fom-log compact` offline) rather
than raising the heap.

Large records cost heap, not native memory: frames are written and read in
bounded 256 KiB slices, so the JDK's per-thread temporary direct buffers
(which it caches for the life of each thread) stay small regardless of record
size, and appends and snapshots of records tens of MB in size do not need a
larger `-XX:MaxDirectMemorySize`. A frame is still indexed — and so visible —
only after all of it is written and fsynced; a crash mid-frame leaves a torn
tail that the next open truncates, as before.

### Backups

Copy a live log only with the engine stopped, or through
[`fom-log`](cli.md) (its read commands work on a private copy and never touch
the original). A plain `cp` of a file being appended to can end mid-frame; that
is only a torn tail, cut off on open. The real danger is **starting** an engine
on a copy while the original keeps running elsewhere: both then append events
with the same clocks, and nothing tells the two timelines apart until someone
splices them together (which the open then refuses, see above) or restores the
wrong one. Treat a copy as a backup, not as a second live log.

## PostgresLogBackend

`fom-jdbc`. Stores events in a single table and coordinates leadership with a
Postgres advisory lock — the basis for [multi-node](multi-node.md).

```java
import io.fom.jdbc.PostgresLogBackend;
import java.sql.SQLException;

// dataSource: a connection pool such as HikariCP
// Both constructors declare `throws SQLException`: they connect, create the
// table and take the advisory lock right there.
void serve(DataSource dataSource) throws SQLException {
    try (var backend = new PostgresLogBackend(dataSource, "weather");   // table fom_log_weather
         var engine  = new Engine(EngineConfig.defaults(), backend, serDe)) {
        …
    }
    // or specify the table name explicitly:
    // new PostgresLogBackend(dataSource, "weather", "fom_log_weather")
}
```

- **Table name.** The two-argument constructor uses `fom_log_<logId>` and
  **rejects** a `logId` containing anything other than ASCII letters, digits and
  `_` with `IllegalArgumentException` (earlier versions replaced such characters
  with `_`, so `weather-eu` and `weather_eu` silently shared one table and one
  lock). The three-argument constructor takes an explicit table name instead.
  Either way the name is case-insensitive (it is lowercased, as Postgres folds
  unquoted identifiers): `Weather` and `weather` are one log with one leader lock.
  A name containing `_archived_` (reserved for archive tables), a reserved SQL
  word, or a name already used by a non-table object (a view, a sequence, an
  index, …) is refused with `IllegalArgumentException`.
- **Table checks on open.** The table is looked up only in the schema it is
  created in (`current_schema()`, the first existing schema of `search_path`).
  A name the `search_path` resolves somewhere else first — a system catalog such
  as `pg_class` (the implicit `pg_catalog` comes first), or a same-named table
  in another schema — is refused with `IllegalArgumentException` naming where it
  resolves. A `search_path` naming no existing schema (so `current_schema()` is
  null and there is nowhere to create the table) is refused with
  `IllegalStateException` saying so; set `currentSchema` on the connection (or
  the role's `search_path`) to an existing schema. An **existing** table must look like a log table (`row_id bigint`,
  `type text`, `payload bytea`, `ts_millis bigint`); anything else, say a user's
  `customers` table, is refused with `IllegalArgumentException: table customers
  exists but is not a fom log table (missing column row_id)` instead of failing
  later on a raw SQL error. A **new** log also needs the names of its sequence
  (`<table>_rid_seq`), primary-key index (`<table>_rid_pk`) and `LogLeader`
  index (`<table>_leader_ix`) to be free: they
  share the namespace of tables, so log `cz` cannot be created while log
  `cz_rid_seq` (table `fom_log_cz_rid_seq`) exists. That is refused with an
  `IllegalArgumentException` naming the conflicting object, before anything is
  created. The same holds when the two logs are created **at the same moment**
  and the clash only shows in the `CREATE` itself (SQLState `23505` on
  `pg_class_relname_nsp_index`, or `42P07`): the loser's creation is rolled back
  and it gets the same `IllegalArgumentException` instead of a raw
  `PSQLException`.
- **Writable primary, session pooling.** The constructor refuses a hot standby
  (`pg_is_in_recovery()`) or a read-only session
  (`default_transaction_read_only = on`) with `IllegalStateException: … the
  database is a read-only server (hot standby?) …`, before taking any lock.
  Leadership is a session-level advisory lock, so **transaction or statement
  pooling (PgBouncer `pool_mode=transaction`/`statement`) is unsupported** —
  it hands the lock's server session to other clients and every node would
  lead; use a direct connection or `pool_mode=session` (in-JVM pools such as
  HikariCP are fine). The constructor refuses such a proxy with
  `IllegalStateException` (mentioning `pool_mode=session`) when it can see it —
  the server session it gets already holds this log's lock, or consecutive
  transactions report different `pg_backend_pid()`s — but a quiet pool that
  keeps handing back the same server session passes the check, so it is a
  safety net, not a guarantee. See [Multi-node](multi-node.md#choosing-the-right-backend).
- **You own the backend.** `Engine.close()` does not close the `LogBackend`.
  Close the backend yourself (try-with-resources or `backend.close()`) to release
  the advisory lock, otherwise a standby cannot take over until the connection
  dies. `close()` unlocks explicitly (a pool keeps the physical session, and a
  session-level lock with it, after `Connection.close()`), retries a failed
  unlock a few times with a short backoff, and confirms from the same session
  that the lock is gone. If it cannot confirm that — say, every statement is
  being cancelled — it logs a `WARN` and **aborts** the connection
  (`Connection.abort`) instead of returning it to the pool, so the session and
  its lock really end. A session that is already dead needs nothing.
- **Use a connection pool, with timeouts.** Every `get`/`getBetween`/`length`
  call borrows a connection from the `DataSource`. Startup reads the log in
  ranges of 1000 events (`getBetween`), not one round trip per event, but a
  non-pooling `DataSource` still opens a physical connection per call. Use a
  pool such as HikariCP and configure a connection (checkout) timeout and a
  socket timeout — for the pgjdbc driver `connectTimeout` and `socketTimeout`
  (the default `PGSimpleDataSource` has no socket timeout). Without them reads
  block for as long as Postgres is unresponsive or the pool is exhausted. Pick
  `socketTimeout` deliberately: a database stall longer than it raises a
  connection-level error (SQLState `08xxx`) that fences this instance for good
  (see [Multi-node](multi-node.md#failover)). Only statements the backend sends
  on the **leader session** can do that — a write, or the 15 s keepalive. Reads
  and `introspect()` use short-lived connections, so a health check during a
  stall cannot cost the node its leadership.
- **HikariCP: the leader connection is borrowed for good.** Because the
  leader session is held for the backend's whole lifetime, Hikari's
  `leakDetectionThreshold` reports it as a leak (an `Apparent connection leak
  detected` `WARN` with the constructor's stack) once the threshold passes —
  expected, not a bug; leave leak detection off on a pool dedicated to fom, or
  accept the one warning per backend. `maxLifetime` (and `idleTimeout`) do not
  apply to it either: Hikari retires only connections that are returned, so the
  leader session lives until `backend.close()` or until the server or network
  ends it. If something in between (a proxy, a firewall, the server's
  `idle_session_timeout`) caps session age, that ends leadership — the node is
  fenced — rather than recycling the connection.
- **Pool sizing: at least backends + 1.** Each `PostgresLogBackend` keeps
  **one pool connection for its whole lifetime** — the leader session that owns
  the advisory lock and carries every write — and borrows further, short-lived
  connections for reads (`get`/`getBetween`/`length`), `introspect()` and lock
  confirmations. It already needs a second one while the constructor runs (to
  read the log's leadership state). So size the pool to **at least the number of
  backends sharing it + 1**, plus headroom for concurrent reads and health
  checks; with `maximumPoolSize = 1` the constructor fails with
  `IllegalStateException` ("could not get a second connection … size the pool to
  at least the number of backends + 1"), the pool's own timeout as its cause, and
  releases the lock and the connection it took (the table may already have been
  created). A pool that is too small for the running node shows up as reads and
  `introspect()` waiting for the pool's checkout timeout.
- **Any default isolation level works; the backend's own writes run at
  `READ COMMITTED`.** The write fence and the append guard need every statement
  to see what was committed before it. Under `REPEATABLE READ` or
  `SERIALIZABLE` the first query of a transaction fixes its snapshot, so a
  `LogLeader` another node commits while an append waits on the fence would
  stay invisible and the deposed leader would keep appending with duplicate
  clocks. So the backend switches its leader connection to `READ COMMITTED`
  (`setTransactionIsolation`) whatever the pool (HikariCP
  `transactionIsolation`), role or database (`default_transaction_isolation`)
  default is, starts every write transaction with
  `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`, and takes the fence as its
  first statement, before any query. The connection goes back to the pool at
  the level it came with when the backend closes. Short-lived read connections
  keep the pool's level; reads need no particular one.
- **Opening a large log.** The constructor reads the last row (a primary-key
  lookup) and the latest `LogLeader` through a partial index,
  `<table>_leader_ix ON <table> (row_id) WHERE type = 'LogLeader'` — earlier
  versions had no such index and scanned the whole table for it on every open
  and leadership claim. A new table gets the index on creation; an **existing**
  table gets it on its first open by this version (`CREATE INDEX`, logged at
  `INFO`; it blocks writers, of which there are none but the lock holder, not
  readers). Building it needs ownership of the table: a role that may only
  read and write the table logs a `WARN` with the `CREATE INDEX IF NOT EXISTS …`
  statement for the owner to run, and opens without it. Compaction moves the
  index to the archive with the table (renamed `<archive>_leader_ix`) and
  creates a new one. If a server `statement_timeout` (or a `lock_timeout`
  behind another session's lock on the table) cancels any of the opening
  statements, the constructor throws an `SQLException` with the same SQLState
  (`57014` / `55P03`) whose message says the open was cancelled by the server,
  names `statement_timeout`, and gives the `CREATE INDEX` statement — instead of
  a bare `canceling statement due to statement timeout`. Raise the timeout for
  the backend's connections (e.g. `options=-c statement_timeout=60s` on the
  JDBC URL) or build the index beforehand.
- **Large events and `statement_timeout`.** A range read first lists the
  window (row ids and payload sizes — cheap, the sizes come from the TOAST
  header) and then fetches the payloads in runs of consecutive rows of at most
  8 MiB per `SELECT`; a single larger event is fetched on its own. So a log of
  large events stays readable, and the node can restart, under a server
  `statement_timeout` that each append met: earlier versions read up to 1000
  events in one `SELECT`, which could outlast the timeout (5 events of 60 MB
  under `statement_timeout = 1s`: every append succeeded, restart failed with
  `getBetween failed`). A run cancelled anyway (SQLState `57014`) is retried in
  halves, down to single rows; only a single event that cannot be read within
  the timeout still fails the read. Each run is checked to come from the same
  table (by oid) as the listing, so a compaction in between is detected and the
  read retried.
- **Reads survive one dropped connection.** `length()`, `get` and `getBetween`
  run on a pooled connection; if its server session turns out to be gone
  (SQLState class `08`, or `57P01`/`57P02`/`57P03` — `pg_terminate_backend`, a
  server restart, a proxy reset of an idle connection) the read is retried
  **once** on a fresh connection, so a single stale pooled connection no longer
  fails an engine start. A read that still fails — or cannot get a connection at
  all — throws with the cause and its SQLState in the message (e.g.
  `getBetween(0,2) failed on fom_log_x: … (SQLState 08006)`).
- **`introspect()` is bounded.** Its database work (the advisory-lock check and
  the report query) is bounded to 5 s. If Postgres does not answer in
  time, or the pool cannot hand out a connection, it logs a throttled `WARN` and
  returns the last known length — by the same rule as after `close()` below, so
  including this instance's own appends since the last `introspect()` — and the
  last event counts (length `-1` if none was ever known) instead of hanging or
  throwing. `currentLeader` is still this instance
  unless it positively lost the lock.
- **`introspect()` also answers after `close()`.** Handing leadership to a
  standby means closing the backend, so the health check has to survive it: a
  closed backend reports the last length it knew — read by `length()`,
  `getBetween` or `introspect()`, and kept up to date by its own appends and
  compactions (length `-1` if it never knew one) — and the event counts of the
  last `introspect()` (none if it never ran), with `currentLeader() == null` —
  `isLeader` is `false` — and opens no connection to do it. So after the engine
  has started (which reads the log) the length is current even if nothing ever
  called `introspect()`; the per-type counts may lag behind it. Every other call on a closed backend still fails
  with `IllegalStateException`. Processes that are `Serving` keep answering
  queries from memory in the meantime; a process caught **mid re-init** (past
  its `LogDead`, cleaning up or inside `init`) does not: every init attempt
  fails with `ERROR` "could not persist LogInitialized for init attempt
  &lt;n&gt;" plus a `WARN`, it stays `Initializing` without a Sid until the init
  budget runs out and then goes `Dead`, and queries to it time out meanwhile
  (details in [Multi-node](multi-node.md#failover)). **Expect a `WARN` per
  process when you do close the engine.** Each process's cleanup appends an informational `LogCleanedUp`, and
  on a closed backend that append fails, so a handover done in this order ends
  with one `WARN` per process at `engine.close()` saying the backend is closed.
  Nothing is lost: the event is informational, the FSM still reaches `Dead`, and
  `close()` returns normally.
- **Fencing is visible in `introspect()`.** `introspect()` checks that the
  leader session's backend still holds the advisory lock — over a short-lived
  `DataSource` connection, never over the leader session itself, so a read-only
  health check can neither break that session nor fence the node. An instance that was fenced off
  (its session lost the lock and another node may own the log) reports
  `currentLeader() == null`, so `EngineReport.isLeader()` is `false` as soon as
  it is fenced, not only after its next failed write. A fenced instance may
  still answer queries from its in-memory state — route traffic by `isLeader`.
- **The leader session is kept alive.** Every 15 s the backend touches the idle
  leader session and checks its advisory lock, so a server- or proxy-side idle
  timeout longer than that doesn't end it, and a lost lock is noticed early
  (`introspect()`/`isLeader` turn `false`). An `idle_session_timeout` or a
  proxy idle cut **below 15 s** still ends the session and fences the node.
- **The fencing error names its cause.** The `ERROR` `PostgresLogBackend[…]
  lost its advisory lock: <reason> (cause: <message>, SQLState <state>); this
  instance is fenced …` says which check decided and why: the leader session is
  gone (the failure that showed it), a separate session sees the lock no longer
  held, or the lock could not be verified for too long. When a separate session
  (a health check's probe) is what found it, the backend sends one `SELECT 1`
  on the leader session, bounded by a 2 s network timeout, to capture the real
  reason: `57P05` idle-session timeout, `57P01` an administrator's
  `pg_terminate_backend`, `08xxx` a broken socket — or that the session still
  answers, i.e. the lock was released on a live session
  (`pg_advisory_unlock_all()`, `DISCARD ALL`, a pooler resetting it). Every
  `LeadershipLostException` that follows carries the same reason in its message
  (`… leader session was lost (<reason>; cause: …)`) and the failure as its
  cause.
- **A positively lost lock fences at once; an unconfirmable one after 30 s.** A
  closed or terminated leader connection (SQLState `08xxx`/`57Pxx`), or a check
  showing the lock is not held, fences the node immediately. If the
  advisory-lock check itself fails (a cancelled statement — e.g. a flood of
  `pg_cancel_backend` — or a statement timeout), it is retried and then
  confirmed from a separate connection of the `DataSource` against the leader
  session's pid, bounded by 3 s (connection checkout included). If that fails
  too, the check is inconclusive and does not fence right away — but once checks
  have stayed inconclusive for more than 30 s with no conclusive "held" in
  between, the lock is treated as lost and the node is fenced. On an idle node
  only the 15 s keepalive checks, so that takes about 45–50 s. The `DataSource`
  must be able to hand out one extra short-lived connection for the
  confirmation.
- **A write is only applied after its own lock check succeeds.** Every write
  checks the lock inside its transaction on the leader session; an inconclusive
  check never lets it through. An append whose lock check failed is not
  applied: it fails with "could not verify the advisory lock (statement failed:
  …); the write was not applied", carrying the SQL cause, and can be retried. A
  write refused because the lock is *gone* fails with `LeadershipLostException`
  (an `IllegalStateException`) — the same type the engine uses for a refused
  append, whichever node noticed first.
- **Close a fenced backend.** Fencing stops this instance from writing; it does
  not end its session. A node fenced because the lock could not be confirmed
  may still hold the lock in Postgres, so a standby stays refused until you
  call `backend.close()` (or the connection dies). Close a fenced backend — and
  open a new one if this node should compete again.
- **Interrupts can't fence the node.** All writes on the leader session run on
  one dedicated platform thread; callers (virtual threads included) wait for it
  without reacting to interrupts (their interrupt flag is restored afterwards).
  An interrupted caller therefore can't close the leader socket.

Schema (created if absent):

```sql
CREATE SEQUENCE <table>_rid_seq;
CREATE TABLE <table> (
  row_id    BIGINT NOT NULL DEFAULT nextval('<table>_rid_seq'),
  type      TEXT   NOT NULL,
  payload   BYTEA  NOT NULL,
  ts_millis BIGINT NOT NULL,
  CONSTRAINT <table>_rid_pk PRIMARY KEY (row_id)
);
ALTER SEQUENCE <table>_rid_seq OWNED BY <table>.row_id;
CREATE INDEX <table>_leader_ix ON <table> (row_id) WHERE type = 'LogLeader';
```

- The constructor takes `pg_try_advisory_lock(key)`, where `key` is the first 8
  bytes of the SHA-256 of `schema + "." + tableName` (UTF-8)
  (the schema is `current_schema()`, so the same log id in two schemas is two
  logs with two leaders; earlier versions keyed on the table name only — see
  [Multi-node](multi-node.md#choosing-the-right-backend)) on a dedicated
  connection held until `close()`, **before** it creates the table. A
  second instance opening the same table fails fast with
  `IllegalStateException` ("advisory lock") — also when nodes start together on
  a brand-new log — and a standby that doesn't get the lock never creates the
  table. The connection (and thus the lock) is released even if construction
  fails partway.
- The sequence and the primary key are **named explicitly** (a prefix of the
  table name plus a 64-bit SHA-256-based hash of it when the suffix would not fit
  in 63 bytes) instead
  of being left to Postgres: its implicit names for a `BIGSERIAL PRIMARY KEY` are
  truncated by cutting the table name, so two logs whose table names share a long
  prefix would be handed the same sequence name and their concurrent `CREATE`s
  would collide on `pg_class_relname_nsp_index`. A table created by an earlier
  version keeps Postgres' implicit names; they are looked up, not assumed.
- The `row_id` column is an internal sequence-backed row id, used only for ordering.
  The engine-visible clock is the one stored in the event: last clock + 1 on
  append, kept as supplied on `compact`.
- **A log whose clocks go backwards is refused**, as the file backend refuses
  it. Clocks start at 0 and strictly increase in `row_id` order; rows of another
  timeline spliced in (old rows re-inserted, a restore stitched onto a copy)
  break that, and appending after them would re-issue clocks that already exist
  and make recovery mistake live state for stale. Since the clock lives in the
  payload, not in a column, the check is split: the constructor compares the
  last row's clock with the row count (old rows re-inserted at the end fail
  it) — bounded first by the `row_id` range, two primary-key lookups, which
  settles it for every log whose row ids have no gaps (every compacted log
  included); only a log with gaps (appends that failed after taking a row id)
  whose last clock falls inside them pays an exact `count(*)` — and every range read checks that the clocks of its window
  strictly increase — across chunks too, when a read continues the previous
  one, so the engine's startup scan checks the whole log. Both fail closed. The
  constructor's check refuses before anything is written (`… is spliced or
  reordered: its last row … has clock …, but the table holds … rows; it is
  refused (nothing was changed)`). The read check says `IllegalStateException:
  Log table … is spliced or reordered: row_id … (position …, …) has clock … but the
  previous row (row_id …, position …) has clock …, so the log is spliced or corrupt: reading
  stopped at row_id … and the log is refused. Stop every instance on this log and
  repair it before use`. The read
  itself writes nothing, but an engine claims leadership before its startup
  scan, so by then **its `LogLeader` has already been appended** after the bad
  rows. Stop every instance on the log, then restore the table from a backup
  or archive, or delete the rows that do not belong (and that `LogLeader`, if
  you keep the rest).
- Reads address rows by **position** (`ORDER BY row_id OFFSET ?`), so positional
  addressing survives compaction (which recreates the table). A chunked scan
  (engine start, resume, snapshot: `getBetween(0, k)`, `getBetween(k, 2k)`, …)
  does not pay that `OFFSET` per chunk: the backend remembers the `row_id` the
  previous range ended on (and the table's oid) and lists the next chunk **by
  key** (`WHERE row_id > ? ORDER BY row_id LIMIT ?`), so a full scan is linear.
  Earlier versions counted the whole table and skipped to the chunk with `OFFSET`
  on every chunk — quadratic (a million-row log took minutes to open). A read
  that does not continue from the remembered row, finds rows of another table (a
  compaction swapped it) or gets a short window falls back to the counted form
  below.
- A counted `getBetween` takes its bound check (the row count) and the rows
  themselves in **one statement**, hence under one snapshot (a keyset read needs
  no count: a full window proves the bound), so a concurrent append/compact
  can't shorten the result. Reads also **retry** when the log looks shorter than
  the range asked for: `compact` renames the live table away and creates a fresh
  one under the same name, and a read that resolved that name after the swap but
  took its snapshot before it would otherwise see a table with no rows. A reader
  therefore sees either the whole pre-compaction log or the whole post-compaction
  one, never an empty or half-written log; a genuinely out-of-range range still
  throws `IndexOutOfBoundsException`. Reads take no table lock, so a compaction
  never blocks them.
- The `DataSource` is injected by you (use a pool — see above). Identifiers are validated
  and length-bounded (Postgres' 63-byte `NAMEDATALEN`); a reserved SQL keyword
  (e.g. `user`, `order`) is rejected as a table name with
  `IllegalArgumentException`.
- `compact` renames the old table to `<table>_archived_<millis>` — together with
  its sequence, primary-key index and `LogLeader` index, so the new live table can take its own
  names and `DROP TABLE` takes them all with it; `purgeArchives`
  drops all but the newest of those tables (on the leader session, fenced like
  every other write). Earlier versions never purged Postgres archives. As in the
  file backend, the stamp is the leader's wall clock but never lower than the
  highest existing archive stamp + 1, so an archive written by an earlier
  leader whose clock ran ahead cannot outrank — and make `purgeArchives` drop —
  the newest one; a stamp more than 100 years ahead of the clock is not
  believed (logged at `WARN`, treated as the oldest). `purgeArchives` drops the
  surplus **oldest first, 50 archives per transaction**: every dropped table
  holds its locks (table, sequence, index, TOAST) until the commit, so a single
  transaction over a backlog of thousands of archives would fail with "out of
  shared memory" (`max_locks_per_transaction`) and, rolled back as a whole, never
  make progress. Batches that committed stay dropped if a later one fails.
- An archive belongs to a log by **name and mark**: `compact` sets the table
  comment `fom archive of <table>` on the archive in the same transaction as the
  rename, and `purgeArchives` only drops tables in the current schema whose name
  is exactly the archive name this log derives for their stamp **and** that carry
  this log's comment. Long table names are shortened to a prefix plus a hash, so
  a name match alone is not proof of ownership; earlier versions used a weak
  (polynomial) hash there and could derive identical archive, sequence and index
  names for two long log ids — and one log's `purgeArchives` could drop the other
  log's archives. Archives written before this version carry no comment and are
  **never purged** now: drop them by hand, or mark them
  (`COMMENT ON TABLE <archive> IS 'fom archive of <table>'`) to hand them back to
  `purgeArchives`. Marking works only for **short** table names, whose archive
  name `<table>_archived_<millis>` fits Postgres' 63-byte limit unshortened
  (table names up to about 40 bytes): for longer names the old archives carry
  the old hash in their names, which never equals the name this version
  derives, so even a tagged one is never matched — **drop those by hand**. Do
  not remove the comment from an archive you want purged.

!!! warning "Compaction renames the live table — logical replication / CDC"
    `compact` replaces the live table with a new one under the same name; a
    publication bound to the old table follows the archive. See
    [Consuming the log with CDC](#consuming-the-log-with-cdc) below.

- `append` re-checks leadership **inside its own transaction**, against the log
  rather than a cached field, behind a **write fence**: it first takes
  `LOCK TABLE <table> IN SHARE ROW EXCLUSIVE MODE` (compaction takes
  `ACCESS EXCLUSIVE`). That mode conflicts with every other session's
  `INSERT`/`UPDATE` but not with reads, so once it is granted every other write
  to the table has either committed — and is visible to the check — or waits for
  this transaction. The fence is the transaction's first statement and the
  transaction runs at `READ COMMITTED` whatever the configured default (see the
  isolation bullet above). The insert then carries a guard: no row written past the
  newest row this instance has seen — checked as
  `COALESCE((SELECT max(row_id) FROM <table>), ?) <= ?`, which Postgres answers
  with one backward step on the primary key under any plan. (Earlier versions
  wrote it `NOT EXISTS (… WHERE row_id > ?)`: once pgjdbc server-prepared the
  statement and Postgres switched to the generic plan, that became a sequential
  scan of the whole log on **every** append.) An instance whose log was taken over while
  it was running — a `LogLeader` row naming another instance appeared — gets
  `Optional.empty()` from its very next `append`, not only after a restart; rows
  someone else wrote that are no takeover are appended after, with a clock past
  theirs. Without the fence a `LogLeader` that another session had inserted but
  not yet committed was invisible to the guard: the old leader's append took a
  higher `row_id`, moved its watermark past the claim, and kept writing (with
  clocks the claim duplicated). The advisory lock already keeps nodes of this
  version apart; the fence covers writers it cannot see — a node of a version
  with a different lock key during a rolling upgrade (see
  [Upgrades](../concepts/idempotent-restart.md#upgrades)), a lock released on a
  live session, a manual `INSERT`. It waits at most 10 s for the table (a write
  that times out fails, not applied); a manual `VACUUM`/`ANALYZE` or an
  anti-wraparound autovacuum of the log table holds it off for its duration
  (ordinary autovacuum cancels itself after `deadlock_timeout`). A refusal also
  re-reads who does lead, so `introspect()`/`isLeader` stop claiming a
  leadership this instance no longer has; it can still take the log back the
  documented way, by appending its own `LogLeader`. `compact` checks the latest
  `LogLeader` under its lock as well, and refuses (`IllegalStateException`,
  nothing written) when the log holds rows this instance has not seen, since
  its snapshot was planned without them.
- **Append throughput.** Appends are **serialised**: every write of a backend
  runs on its single leader connection, one after the other, and **each event is
  its own transaction** — a lock check (`pg_locks`), the write fence
  (`LOCK TABLE`), the guarded `INSERT` with a placeholder payload, an `UPDATE` writing the serialised event into that row,
  and a `COMMIT` (one WAL flush). There is no batching or group commit across
  events, so throughput is bounded by the round-trip latency to Postgres plus
  its commit latency, not by the number of processes or threads appending.
  Every append waits for its own commit to be durable, so the ceiling is the
  commit (WAL `fsync`) latency of your disk: with `synchronous_commit = on` on
  ordinary disks expect **a few hundred appends/s** (a field test measured
  300–400/s; about 1 400/s with `synchronous_commit = off`, which trades the
  durability of the last few commits on a server crash). The ~2 000 appends/s
  (~0.5 ms each) measured against a local Postgres 16 container (Testcontainers,
  default settings) reflects cheap WAL flushes there, not a figure to plan with.
  Over a network, add your RTT per round trip (5 round trips per append). Each `UPDATE` also leaves a dead tuple
  per event for autovacuum to reclaim. Plan for this if your processes write
  many events per second: the log is meant for initialisation results and state
  transitions, not for a high-rate event stream.
- `compact` refuses with `LeadershipLostException` (an `IllegalStateException`),
  changing nothing, when the log's latest `LogLeader` names another instance: a
  deposed leader must not be able to re-claim the log by snapshotting its own
  `LogLeader` back to the front.
- `compact` takes the table's `ACCESS EXCLUSIVE` lock with `NOWAIT`, retrying
  for up to 5 s, instead of queueing for it. While another session uses the
  table (a report query, `pg_dump`), readers and `introspect()` are therefore
  not blocked behind the compaction; if the table stays busy for 5 s the
  compaction fails.
- `compact` **retries transient conflicts**: its `RENAME`/`CREATE` update
  catalog rows that a concurrent migration may be updating too without any
  table lock in common (a `GRANT … ON ALL TABLES IN SCHEMA` in an open
  transaction is the usual one), and Postgres fails the second writer with
  `XX000` "tuple concurrently updated", a deadlock (`40P01`) or a serialization
  failure (`40001`). Each rolls the whole compaction back, so it is tried again,
  up to 5 attempts with a doubling wait from 50 ms (logged at `WARN`); after
  that it fails, not applied.
- **Once it holds the table, a compaction waits for no lock longer than 1 s.**
  Right after the `ACCESS EXCLUSIVE` lock it sets `SET LOCAL lock_timeout =
  '1000ms'` for the rest of its transaction. Without it, a `RENAME` behind a
  migration's open catalog lock (that uncommitted `GRANT … ON ALL TABLES IN
  SCHEMA` again) would wait for the migration to end **while holding the
  table lock**, blocking every reader of the live log (`getBetween`,
  `introspect()`, health checks) for as long as the migration stays open. Now
  the wait fails with `55P03` (lock_not_available), the attempt rolls back and
  releases the table, and it is retried like the conflicts above. Readers are
  blocked for at most about 1 s per attempt; a migration still open after all 5
  attempts (about 6 s) makes the compaction fail, not applied — snapshot again
  later. The table lock's own `55P03` (the table stayed busy for 5 s) is not
  retried.

### No read-only mode { #no-read-only-mode }

There is no way to open a log just for reading. `PostgresLogBackend` refuses a
hot standby or a read-only session on open (`IllegalStateException`, "the
database is a read-only server (hot standby?)"), and it takes the log's
advisory lock in its constructor — so opening it on a table a running engine
uses fails fast instead of giving you a reader. `fom-log` (`inspect`,
`events`, `diagnose`, `compact`) takes `.bin` **files** only. To look at a
live Postgres log, query the table directly: `type` and `ts_millis` are plain
columns, and the `payload` decodes as described in
[the CDC section](#consuming-the-log-with-cdc) below (Java serialization of a
`LogEvent`, always under `ObjectInputFilters.logPayload()`). A query works on a
replica too.

### Consuming the log with CDC / logical decoding { #consuming-the-log-with-cdc }

The log table can be streamed out with logical replication, Debezium or another
CDC tool — say to feed an audit trail. What such a consumer sees:

- **Two changes per event.** Each append is one transaction that `INSERT`s a row
  with an **empty** `payload` (only `row_id`, `type` and `ts_millis` are real)
  and then `UPDATE`s that row with the payload. Ignore the `INSERT`s and read
  events from the `UPDATE`s (with the default `REPLICA IDENTITY`, the new row
  carries every column). A failed or refused append rolls back and emits
  nothing; `row_id`s can therefore have gaps.
- **The payload is Java serialization** of a `LogEvent` record
  (`io.fom.log.*`, from `fom-core`) — not JSON, and not your `SerDe`'s format:
  the user state inside a `LogInitialized` is the `byte[]` your `SerDe`
  produced. Decode it with `fom-core` on the classpath and, since the bytes come
  from a database, **always** with the engine's allowlist installed:
  `ObjectInputStream in = …; in.setObjectInputFilter(ObjectInputFilters.logPayload());`
  (see [Security](../security.md)). The event's clock is inside the payload; the
  `type` column is its simple class name, handy for filtering before decoding.
- **Compaction rewrites the table.** `compact` renames the live table to
  `<table>_archived_<millis>`, creates a new table under the old name and
  writes the [snapshot](../concepts/snapshots.md#what-a-snapshot-contains) into
  it (again `INSERT` + `UPDATE` per event), all in one transaction. The kept
  `LogChangeGraph`/`LogInitialized` events are **re-emitted with the clocks
  they already had**, the leader's `LogLeader` comes at clock `0`, and only the
  trailing `LogTrigger`/`LogPaused`/`LogSnapshot` get new clocks. A consumer
  therefore sees a burst of events it has (mostly) seen before, in a table with
  a new oid and `row_id`s restarting at 1. Deduplicate by clock (a clock you
  have already processed is a re-emitted copy; the `LogSnapshot` marks the end
  of the burst), and key nothing on `row_id`.
- **Publications follow the oid.** A publication created `FOR TABLE <table>` is
  bound to the table's oid, so after the first compaction it **follows the
  archive** and silently stops seeing the live log. Publish with
  `FOR TABLES IN SCHEMA <schema>` (Postgres 15+; archives are published too —
  filter them out by name downstream), or re-add the live table
  (`ALTER PUBLICATION … ADD TABLE <table>`) after every compaction. The same
  applies to grants, triggers and row-level-security policies added to the live
  table by hand: they stay on the archive.

### Required privileges { #required-privileges }

The role the `DataSource` connects as needs:

| When | Privileges |
|---|---|
| Creating a **new** log | `CREATE` on the schema (the table, its sequence, primary-key index and `LogLeader` index are created on first open) |
| First open of a table created by an earlier version | ownership of the table, to add the `LogLeader` index; without it the open logs a `WARN` and works without the index |
| Normal operation (open, read, append) | `SELECT`, `INSERT` and `UPDATE` on the log table — every append inserts a placeholder row and then `UPDATE`s its payload — plus `USAGE` on its sequence (`<table>_rid_seq`); `LOCK TABLE … IN SHARE ROW EXCLUSIVE MODE` is covered by `UPDATE` |
| `compact` / snapshots, `purgeArchives` | **ownership** of the table (and so of its sequence and index): compaction `ALTER … RENAME`s them, `COMMENT`s the archive and creates the new live table (`CREATE` on the schema); `purgeArchives` `DROP`s archives |
| Introspection | `SELECT` on `pg_locks` (granted to `PUBLIC` by default) |

No `DELETE` or `TRUNCATE` is ever needed: grant less than this and the
operation fails on its first statement. A role that can open the log but does
not own the table works until the first snapshot, which then fails with
"must be owner of table". Since a compaction creates a new live table, grants
you add on the table by hand stay on the archive (see the warning above): run
the backend as the table's owner rather than relying on per-table grants.

!!! warning "Row-level security"
    The backend assumes it sees **every** row of the log table. If row-level
    security is enabled on it (and the role is neither the owner nor
    `BYPASSRLS`, or `FORCE ROW LEVEL SECURITY` is set), its policies must let
    the role see all rows: a policy that hides some makes the backend read a
    **truncated log** — recovery cold-initialises the hidden processes, appends
    continue after the last visible clock, and snapshots archive the hidden rows
    away. Don't enable RLS on the log table; isolate tenants with separate log
    tables or schemas instead (see [Security](../security.md)).

!!! warning "Statement logging copies the log into the server log"
    Every append writes its event's serialized payload — process state, and for
    `LogChangeGraph` every node's `param` — as a bind parameter of an `UPDATE`
    (and a compaction writes the whole snapshot the same way). With
    `log_statement = 'mod'` or `'all'`, `log_min_duration_statement`, or
    `pgaudit` with `pgaudit.log_parameter = on`, Postgres writes those
    parameters (hex `bytea`) into its **server log**, whose length is limited
    only by `log_parameter_max_length` (default `-1`: unlimited). That is a
    second, unmanaged copy of the data: snapshots and `purgeArchives` do not
    reach it. For the fom role, set `log_parameter_max_length = 0` (or a small
    value) and keep `pgaudit.log_parameter` off — e.g. `ALTER ROLE fom SET
    log_parameter_max_length = 0` — or treat the server log under the same
    retention as the log table (see [Security](../security.md#data-retention)).

!!! note "Testcontainers"
    `fom-jdbc`'s integration tests run against a real Postgres via
    Testcontainers and therefore need a Docker daemon. `PostgresLogBackendTest`
    runs the shared backend contract plus advisory-lock/reopen tests.

## Writing your own backend

Implement `io.fom.log.LogBackend` and uphold the
[contract](../concepts/the-log.md#the-logbackend-spi). To get the invariants
tested for free, extend the shared `LogBackendContractTest` (published in
`fom-test`) — see [Testing](testing.md). Its
`a_deposed_leaders_append_is_refused_or_fails_and_is_never_applied` case accepts
**both** shapes of a deposed leader's append — `Optional.empty()` or a thrown
`LeadershipLostException` — and requires that nothing was written either way. In particular, get clocks right —
[Sids](../concepts/sid-and-clock.md) rely on them:

- clocks are `long`: store them in a 64-bit column/field, never an `int`;
- on `append`, assign `clock = last event's clock + 1` (`0` in an empty log)
  with `LogClocks.nextClock(lastEvent)` (returns `long`) and
  `LogClocks.withClock` — not `length()`, which differs after a compaction;
- in `compact`, keep the clocks of the supplied events and reject clocks that
  don't strictly increase (`LogClocks.requireIncreasingClocks` throws
  `IllegalArgumentException`); later appends continue after the highest;
- `get`/`getBetween` still address events by position `[0, length())`; positions
  stay `int`.

**Appends must survive interrupted callers.** The engine runs processes on
virtual threads and interrupts them (on timeouts, cancellation, shutdown), so an
append can start with the caller's interrupt flag set or be interrupted midway.
That may fail *that* append, but must never break the log for everyone else: no
closed-for-good channel or connection, no half-written record that the next
append lands behind. Clear the flag around blocking I/O and restore it
afterwards, and reopen what an interrupt closed (the file backend does both; the
Postgres one restores the flag and keeps its socket open). The contract test
checks this with `an_interrupted_caller_does_not_break_the_log` and
`interrupting_a_virtual_thread_mid_append_does_not_break_the_log`.

**How `append` reports a failure decides what the engine does with the process.**
There are exactly four outcomes:

| `append` … | The engine … |
|---|---|
| returns `Optional.empty()` | **Permanent.** Someone else is the leader: the process ends `Dead` with a `LeadershipLostException` the engine synthesises, no retry. |
| throws `io.fom.api.LeadershipLostException` | **Permanent**, exactly like the refusal above — for a backend that finds out by failing (a fenced lock, a lost lease). Your exception's own message is what the node reports. |
| throws `IllegalArgumentException` | **Permanent.** This event can never be stored (over your size limits, say): the process ends `Dead` with that exception. Name the limit in the message. |
| throws anything else | **Transient.** The attempt is retried with backoff until the phase's budget (`initTimeout` / `loadTimeout`) runs out. |

Both permanent cases make a refused init or load result end the process `Dead` at
once, instead of retrying for the whole `initTimeout`. So throw
`LeadershipLostException` **only when this instance can never write again** — a
backend that throws it for a recoverable condition ("my lease has not been
renewed *yet*") kills every process whose init or load result happens to land in
that window, where a transient exception would merely have been retried.

If your backend keeps archives on `compact` (as the file and Postgres backends
do), also implement `purgeArchives(int keepHistory)`: delete all but the newest
`keepHistory` archives. The engine calls it after each scheduled snapshot of a
policy with a finite `keepHistory` (e.g. `SnapshotPolicy.FixedInterval(interval,
keepHistory)`), from `SnapshotContext.purgeArchives` and from
`Engine.purgeArchives`; with the default `SnapshotPolicy.KEEP_ALL` it is never
called. Do not delete archives anywhere else (not in `compact`): retention is the
caller's choice. The default does nothing, so without it your
archives are never purged. Give each archive an id that is **unique per
snapshot** and orders by age — the file backend uses
`<name>.archived.<millis>`, the Postgres one `<table>_archived_<millis>` — since
that is what lets `purgeArchives` tell the newest `keepHistory` archives from
the rest and delete only the rest. Reject `keepHistory < 0` with
`IllegalArgumentException`, and treat `0` as "delete them all". See
[Snapshots](../concepts/snapshots.md#archives).
