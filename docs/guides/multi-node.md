# Multi-node & leadership

FOM is single-writer: at any time **one leader** appends to the log; other
instances are followers. Running more than one instance against the same log
requires a backend that arbitrates leadership across processes — that is
`PostgresLogBackend`.

## Leadership model

- The latest `LogLeader` event names the current leader. Only that
  `instanceId` may `append`/`compact`; a stale leader's writes return
  `Optional.empty()`. `PostgresLogBackend` verifies that inside the append
  transaction, so a takeover is noticed by the next `append` of the deposed
  instance, not only after a restart. See [The log](../concepts/the-log.md#leadership-one-writer).
- On top of the log rule, the backend prevents two instances from *both*
  believing they are leader:
    - `FileLogBackend` — an exclusive OS lock on a `.lock` file next to
      the real log file, symlinks resolved (single machine only).
    - `PostgresLogBackend` — a Postgres **advisory lock** keyed by the schema
      and table name, held on a dedicated connection until `close()`.

## Postgres setup

```java
import io.fom.jdbc.PostgresLogBackend;
import java.sql.SQLException;

// On each node, pointing at the same database + logId.
// dataSource should be a connection pool (e.g. HikariCP).
void serve(DataSource dataSource, Graph graph) throws SQLException {
    try (var backend = new PostgresLogBackend(dataSource, "weather");
         var engine  = new Engine(EngineConfig.defaults(), backend, new FurySerDe())) {
        engine.newGraph(graph);
        // … serve until shutdown …
    }   // closes the engine, then the backend — releasing the advisory lock
}
```

Both `PostgresLogBackend` constructors declare **`throws SQLException`** (it
connects, creates the table and takes the lock right there), so every snippet on
this page either declares it or catches it.

!!! warning "`Engine.close()` does not close the backend"
    The caller owns the `LogBackend`. Closing the engine leaves the backend — and
    its advisory lock — held until you call `backend.close()` (or the JVM / its
    connection dies). For a graceful failover, close the backend explicitly
    after the engine, as the try-with-resources above does.

- The **first** node to construct the backend acquires the advisory lock and
  becomes leader; it recovers state and serves.
- A **second** node constructing `PostgresLogBackend(dataSource, "weather")`
  fails fast with `IllegalStateException` ("advisory lock") — it did not become
  leader. Catch this and either retry later or run as a standby that constructs
  the backend only after the leader exits. The lock is taken before the table is
  created, so this also holds for nodes starting together on a brand-new log.
- The constructor also fails fast with `IllegalStateException` on a setup where
  the lock cannot work — see the warning below. Those messages say what is
  wrong (not "advisory lock … another instance holds it"); a standby loop that
  catches `IllegalStateException` should log the message, since retrying will
  not fix a misconfiguration.

!!! warning "Connect to a writable primary, directly or through a *session* pool"
    Leadership is a **session-level** advisory lock, so every transaction of the
    leader must run on the server session that took it.

    - **Transaction/statement pooling is not supported.** Behind PgBouncer (or a
      similar proxy) in `pool_mode=transaction` or `statement`, a session lock
      stays on whichever server connection took it and is handed to other
      clients with it: on a re-entrant `pg_try_advisory_lock` every node would
      become leader. The constructor detects it where it can and refuses with
      `IllegalStateException` (message mentions `pool_mode=session`): when the
      server session it is handed already holds this log's lock, when two
      consecutive transactions run on different server sessions
      (`pg_backend_pid()` changes), or when the lock is not visible from the
      next transaction. On a quiet pool that happens to hand back the same
      server session every time the check cannot tell, so do not rely on it —
      point the `DataSource` at Postgres directly or at a pool in
      `pool_mode=session`. (A lock left on a proxy's server connection this way
      lives until the proxy recycles that connection.) Pools inside the JVM
      (HikariCP & co.) are session pools and are fine.
    - **Read-only servers are refused.** Against a hot standby
      (`pg_is_in_recovery()`) or a read-only session
      (`default_transaction_read_only = on`) the constructor fails with
      `IllegalStateException: … the database is a read-only server (hot
      standby?) …` before taking any lock — earlier versions took the lock on an
      existing log and failed only at the first write. With several hosts in the
      JDBC URL, use `targetServerType=primary`.
    - **Any default isolation level is fine.** The pool, role or database may
      default to `REPEATABLE READ`/`SERIALIZABLE`; the backend runs its own
      write transactions at `READ COMMITTED` regardless, which leader fencing
      relies on (see [Persistence backends](persistence-backends.md)).

```java
void serveIfLeader(DataSource dataSource, Graph graph) throws SQLException {
    PostgresLogBackend backend;
    try {
        backend = new PostgresLogBackend(dataSource, "weather");
    } catch (IllegalStateException leaderHeldElsewhere) {
        // another node is leader; back off and retry, or stand by.
        return;
    }
    try (backend; var engine = new Engine(EngineConfig.defaults(), backend, new FurySerDe())) {
        engine.newGraph(graph);
        // …
    }
}
```

## Failover

When the leader closes its backend, its process exits, or its connection drops,
Postgres releases the advisory lock. (Closing only the engine is not enough — see
above.) A standby that then constructs the backend acquires the lock,
becomes leader, recovers from the log, and resumes — an
[idempotent restart](../concepts/idempotent-restart.md) on a different node.

!!! warning "A fenced leader can still answer queries"
    If the old leader's session loses the advisory lock without the process
    exiting (terminated backend, idle timeout, failover), its
    `engine.introspect()` reports `isLeader() == false` as soon as it is fenced:
    `PostgresLogBackend.introspect()` re-checks the lock (from a separate
    connection, never on the leader session). Its writes fail, but it
    may keep serving queries from its in-memory state: `engine.query(...)` still
    answers, while `engine.trigger(...)` throws `LeadershipLostException` (an
    `IllegalStateException`) synchronously ("… no longer holds the advisory lock
    … leader session was lost") because it has to append a `LogTrigger` first. Route traffic (and
    health checks) by `isLeader`. The backend touches the idle leader session every
    15 s (and checks the lock then), so idle timeouts longer than that don't
    end it and a lost lock shows up within about 15 s; an `idle_session_timeout`
    or proxy idle cut **below 15 s** still fences the node.

    A **positively** lost lock fences the leader at once: the leader connection
    is closed or terminated (SQLState `08xxx`/`57Pxx`), or a check shows the lock
    is not held. A failed check (a cancelled statement, e.g. a flood of
    `pg_cancel_backend`) is retried and then confirmed from a separate
    connection of the `DataSource` against the leader session's pid, bounded by
    3 s. If that fails too, the check is inconclusive and does not fence right
    away; once checks have stayed inconclusive for more than 30 s (no conclusive
    "held" in between), the lock is treated as lost and the node is fenced — on
    an idle node, checked only by the 15 s keepalive, after about 45–50 s. The
    `DataSource` must therefore be able to hand out one extra short-lived
    connection. An inconclusive check never lets a write through: a write is
    applied only after the lock check inside its own transaction succeeds.

    **`socketTimeout` is a fencing knob.** A database stall longer than the
    `socketTimeout` you configured makes pgjdbc fail the statement with a
    connection-level error (SQLState `08xxx`), which the backend reads as "the
    leader session is gone" and fences the node **permanently** — even if the
    database comes back and the session still holds the lock. Whether a given
    stall does that is a matter of timing: it fences only if a statement on the
    leader session (a write, or the 15 s keepalive's lock check) is in flight
    long enough inside the stall to hit the `socketTimeout`. Reads and
    `introspect()` never send anything on the leader session, so no number of
    health checks during a stall can fence a node. The same freeze can
    therefore fence one node and be ridden out by another. Set `socketTimeout`
    above the longest stall you want to survive (and below the time you are
    willing to hang), and keep it well above the 15 s keepalive interval.

    **Close a fenced backend.** Fencing stops writes but does not end the
    session: a node fenced because its lock could not be confirmed may still
    hold the lock, and a standby stays refused until the fenced node calls
    `backend.close()` (or its connection dies). Operators should close a fenced
    backend so a standby can take over. A closed backend keeps answering
    `introspect()` — the last length it knew (kept up to date by its reads and
    appends) and the event counts of its last `introspect()`, with no leader, so
    `EngineReport.isLeader()` is `false` — while `append`, `compact`, `get` and
    `length` still fail with `IllegalStateException`. A `/health` endpoint built on
    `engine.introspect()` therefore goes on reporting "not leader" after you close
    the backend instead of starting to throw. Closing the engine afterwards
    logs **one `WARN` per process**: each process's cleanup appends an
    informational `LogCleanedUp`, which fails on the closed backend and is
    reported as such. Expect that burst at shutdown whenever you close the
    backend first — nothing is lost, every process still reaches `Dead` and
    `engine.close()` returns normally. Between the two closes the engine is
    noisier still, because a closed backend's `IllegalStateException` is treated
    as transient. `trigger()` throws `IllegalStateException`, but a re-init can
    still start (a reactive cascade, an automatic retry):

    - Under `KEEP_OLD` (the default) the process keeps serving its current
      version from memory throughout. Each init attempt runs your `init` in
      full and fails to store the result: `ERROR` "could not persist
      LogInitialized for init attempt &lt;n&gt;" plus `WARN` "init attempt
      &lt;n&gt; failed", with the usual init backoff, until the init budget
      runs out. Then the re-init gives up (`WARN` "re-init gave up; keeps
      serving &lt;sid&gt;: …") and is retried after `reinitRetryBackoffMin`,
      and so on until `engine.close()`.

    Under `RELEASE_FIRST`:

    - A re-init **triggered** meanwhile (or one that had not yet written its
      `LogDead`) logs `ERROR` "could not persist LogDead for &lt;sid&gt;
      (attempt &lt;n&gt;); retrying reinit in &lt;ms&gt; ms" and keeps retrying
      with backoff (the `ERROR` on attempts 1, 2, 4, 8, …) until
      `engine.close()`. The process keeps serving its current state from memory,
      and `trigger()` throws `IllegalStateException`.
    - A re-init already **past its `LogDead`** — cleaning up the old state or
      inside `init` — has no state left to serve. Its cleanup logs `WARN`
      "could not persist LogCleanedUp", then every init attempt runs your
      `init` in full and fails to store the result: `ERROR` "could not persist
      LogInitialized for init attempt &lt;n&gt;" plus `WARN` "init attempt
      &lt;n&gt; failed", with the usual init backoff. The process stays
      `Initializing` without a Sid until the init budget
      (`EngineConfig.initTimeout`, total across retries) runs out, then
      logs `ERROR` "giving up: Init for &lt;name&gt; ran out of its … budget"
      and goes `Dead`. Queries to it wait in the meantime — so they time out
      at the query timeout, and those still waiting when it goes `Dead` fail
      with `QueryRejectedException` ("&lt;name&gt; is Dead"), as does every later
      query. A process in its load phase ends the same way (`ERROR` "could not
      persist LogLoaded …", load retries, fallback to init).

    Where you can, close the engine first and the backend right after it.

!!! note "Fenced in the middle of a re-init"
    A re-init whose write is refused because the node lost the log (its
    `LogInitialized` or `LogLoaded` append is refused or throws
    `LeadershipLostException`) gives up at once. Under `KEEP_OLD` the old version
    keeps serving, the node is reported stale, the engine logs `ERROR` "lost
    leadership during init" (or "load") and `WARN` "re-init gave up; keeps
    serving &lt;sid&gt;: …", and calls `EngineObserver.onReinitFailed` with the
    `LeadershipLostException`. It is not retried: leadership is never regained.
    The loss is reported once per serving Sid: a later re-init request for that
    process (a cascade, say) is dropped silently, without running `init` and
    without another callback, and the node stays stale. A new version that was
    already written stays in the log; the next leader loads it at its start (one
    running the node under `RELEASE_FIRST` retires it and cold-inits instead).
    Under `RELEASE_FIRST` a re-init whose `LogDead` is refused is dropped and
    the process keeps serving the state it has; one already past its `LogDead`
    has nothing left to serve and ends `Dead`
    ([Process lifecycle](../concepts/process-lifecycle.md#re-initialisation)).

!!! note "`close()` really releases the lock"
    `close()` unlocks explicitly, retries a failed unlock with a short backoff,
    and confirms from the same session that the lock is gone. If it cannot (for
    example, every statement is being cancelled), it logs a `WARN` and aborts
    the connection (`Connection.abort`) instead of returning it to the pool, so
    the pooled physical session — and its lock — ends and a standby can take
    over. Configure a connection timeout and a socket timeout on the
    `DataSource` (pgjdbc: `connectTimeout`, `socketTimeout`): without them reads
    hang while Postgres is unresponsive — but see the fencing trade-off above
    before choosing a small `socketTimeout`. `introspect()` bounds its own database
    work to 5 s and then reports the last known values.

!!! note "Recovery is leader work"
    Snapshots/compaction and graph installation are leader-only. A follower that
    becomes leader replays the log to rebuild state; processes warm-load their
    latest live `LogInitialized`.

## Database failover

The advisory lock coordinates *nodes*; it does nothing for the *database*. The
log is exactly as durable as your Postgres setup:

- With **asynchronous** streaming replication, a failover to the replica loses
  the transactions the old primary had acknowledged but not yet shipped. For fom
  that means appended events — `LogInitialized`, triggers, claims — that an
  engine already acted on (and answered queries with) are simply gone from the
  log. The new leader recovers the shorter log and reissues the lost clocks and
  `Sid`s for new events, exactly as after restoring a backup (see
  [Sid & clock](../concepts/sid-and-clock.md)). Use **synchronous** replication
  (`synchronous_commit = on` / `remote_apply` with `synchronous_standby_names`)
  if acknowledged events must survive a failover.
- An `Engine` and its `PostgresLogBackend` **cannot switch hosts**. The backend
  holds one leader session for its whole life (the advisory lock lives on it);
  when the primary goes away that session dies, the backend fences itself
  (`isLeader() == false`, writes fail with `LeadershipLostException`) and stays
  fenced — it does not reconnect. After a database failover, **close the engine
  and the backend** and open new ones against the new primary; the new backend
  takes the lock on the new server and the engine recovers from the log there.
- Point the `DataSource` at the primary role, not a host: a multi-host URL with
  `targetServerType=primary`
  (`jdbc:postgresql://db1,db2/app?targetServerType=primary`) or a virtual IP /
  DNS name that follows the primary. Then the *new* backend (and pool reconnects)
  reach the new primary without a configuration change. A backend opened on a
  read-only server is refused (see
  [Persistence backends](persistence-backends.md#postgreslogbackend)).

## Choosing the right backend

| You have… | Use |
|---|---|
| one process, ephemeral state | `InMemoryLogBackend` |
| one process, durable state | `FileLogBackend` |
| multiple processes/nodes, HA | `PostgresLogBackend` |

The advisory-lock key is a 64-bit hash of the schema (`current_schema()`,
where the table lives) and the table name, so distinct tables don't collide on
the lock — including the same `logId` in two schemas (`sa.fom_log_x` and
`sb.fom_log_x` are two logs, each with its own leader). Earlier versions hashed
the table name alone and refused the second schema's leader with "another
instance holds it". The key therefore changed: don't run nodes of the old and
new version against the same log at once (they would not see each other's
lock) — stop the old ones before starting the new ones (see also
[Upgrades](../concepts/idempotent-restart.md#upgrades)). If they do overlap, the
write fence keeps a new-version node from appending past a takeover by the old
one, but an old-version node does not fence itself the same way.

!!! warning "Lock keys and derived names changed again"
    The lock key is now the first 8 bytes of the **SHA-256** of
    `schema.table`; before, it was a polynomial string hash (`h = 31·h + c`) on
    which distinct short names collide — `public.fom_log_ar0` and
    `public.fom_log_c2n` shared one lock, so only one of the two logs could ever
    lead. The same weak hash shortened long derived names (archive tables,
    `row_id` sequences, primary keys), so two long log ids could get identical
    names. Both now use the SHA-256 hash. What that changes on upgrade:

    - **Stop every old-version node before starting a new one**: old and new
      nodes take different locks on the same log and would both lead (the write
      fence limits the damage, as above, but does not make it safe).
    - Tables whose name exceeds 55 bytes (the sequence/primary-key suffix no longer
      fits) keep their existing sequence and index — those are looked up in the
      catalog — but new tables and new archives get the new names.
    - Archives are now also tagged with a table comment
      (`fom archive of <table>`), and `purgeArchives` drops only tagged ones.
      Archives written by earlier versions are **no longer purged**; drop them
      by hand or tag them — tagging only works for short table names (up to
      about 40 bytes); archives of longer, shortened names carry the old hash
      and are never matched even when tagged, so drop those by hand (see
      [Persistence backends](persistence-backends.md#postgreslogbackend)). The default table name is `fom_log_<logId>`, and the
two-argument constructor **rejects** a `logId` with characters other than ASCII
letters, digits and `_` (`IllegalArgumentException`) — earlier versions mapped
them to `_`, so `weather-eu` and `weather_eu` silently shared one table and lock.
Pass an explicit table name to the three-argument constructor if you need one.
Table names are case-insensitive, as unquoted Postgres identifiers are: `Weather`
and `weather` are the same log with the same lock (a second leader is refused).
Names containing `_archived_` (reserved for archives), SQL reserved words, and
names already taken by a non-table object (a view, a sequence, …) are refused
with `IllegalArgumentException`, and so are an existing table that is not a
log table (missing `row_id`, …), a name the `search_path` resolves outside the
current schema (e.g. `pg_class`), and a new log whose sequence or primary-key
name another log's table already uses (log `cz` next to log `cz_rid_seq`) — see
[Persistence backends](persistence-backends.md#postgreslogbackend). If the
database sets a `statement_timeout`, logs of large events stay readable on
failover: range reads fetch at most 8 MiB of payload per statement.

## Security note

Anyone who can write to the Postgres log table can plant bytes the leader will
read. The backend's deserialization is allowlist-filtered, but you should still
restrict table privileges. See [Security](../security.md).
