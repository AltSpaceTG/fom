# Multi-node & leadership

FOM is single-writer: at any time **one leader** appends to the log; other
instances are followers. Running more than one instance against the same log
requires a backend that arbitrates leadership across processes — that is
`PostgresLogBackend`.

## Leadership model

- The latest `LogLeader` event names the current leader. Only that
  `instanceId` may `append`/`compact`; a stale leader's writes return
  `Optional.empty()`. See [The log](../concepts/the-log.md#leadership-one-writer).
- On top of the log rule, the backend prevents two instances from *both*
  believing they are leader:
    - `LocalFileLogBackend` — an exclusive OS lock on a sibling `.lock` file
      (single machine only).
    - `PostgresLogBackend` — a Postgres **advisory lock** keyed by the table
      name, held on a dedicated connection until `close()`.

## Postgres setup

```java
import io.fom.jdbc.PostgresLogBackend;

// On each node, pointing at the same database + logId:
var backend = new PostgresLogBackend(dataSource, "orders");
var engine  = new Engine(EngineConfig.defaults(), backend, new FurySerDe(), true);
engine.newGraph(graph);
```

- The **first** node to construct the backend acquires the advisory lock and
  becomes leader; it recovers state and serves.
- A **second** node constructing `PostgresLogBackend(dataSource, "orders")`
  fails fast with `IllegalStateException` ("advisory lock") — it did not become
  leader. Catch this and either retry later or run as a standby that constructs
  the backend only after the leader exits.

```java
PostgresLogBackend backend;
try {
    backend = new PostgresLogBackend(dataSource, "orders");
} catch (IllegalStateException leaderHeldElsewhere) {
    // another node is leader; back off and retry, or stand by.
    return;
}
```

## Failover

When the leader's process exits (or its connection drops), Postgres releases the
advisory lock. A standby that then constructs the backend acquires the lock,
becomes leader, recovers from the log, and resumes — an
[idempotent restart](../concepts/idempotent-restart.md) on a different node.

!!! note "Recovery is leader work"
    Snapshots/compaction and graph installation are leader-only. A follower that
    becomes leader replays the log to rebuild state; processes warm-load their
    latest live `LogInitialized`.

## Choosing the right backend

| You have… | Use |
|---|---|
| one process, ephemeral state | `InMemoryLogBackend` |
| one process, durable state | `LocalFileLogBackend` |
| multiple processes/nodes, HA | `PostgresLogBackend` |

The advisory-lock key is a 64-bit hash of the table name, so distinct `logId`s
don't collide on the lock.

## Security note

Anyone who can write to the Postgres log table can plant bytes the leader will
read. The backend's deserialization is allowlist-filtered, but you should still
restrict table privileges. See [Security](../security.md).
