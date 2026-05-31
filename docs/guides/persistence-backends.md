# Persistence backends

A `LogBackend` is where the [append-only log](../concepts/the-log.md) lives.
Three are bundled; you can write your own against the SPI.

| Backend | Module | Persists? | Leadership | Use for |
|---|---|---|---|---|
| `InMemoryLogBackend` | `fom-core` | no | in-process | tests, quickstart, ephemeral state |
| `LocalFileLogBackend` | `fom-core` | yes (file) | OS file lock | single-node durable state |
| `PostgresLogBackend` | `fom-jdbc` | yes (table) | `pg_advisory_lock` | multi-node leader coordination |

All three honour the same [`LogBackend` contract](../concepts/the-log.md#the-logbackend-spi)
(atomic append, single leader, concurrent reads, dense 0-based clocks) and are
verified against the shared `LogBackendContractTest`.

## InMemoryLogBackend

```java
try (var backend = new InMemoryLogBackend()) { … }
try (var backend = new InMemoryLogBackend("my-log-id")) { … }
```

Backed by a `CopyOnWriteArrayList`. Nothing survives the JVM — every start is a
cold start. Ideal for tests.

## LocalFileLogBackend

```java
try (var backend = new LocalFileLogBackend(Path.of("/var/lib/fom/log.bin"))) { … }
```

- Each event is framed `[int length][int CRC32][payload]`; the file starts with
  a 4-byte magic (`FOM\1`) so a wrong file fails loudly.
- On open, the whole file is scanned; the first invalid/CRC-mismatched frame
  truncates the tail (recovery from a partial write after a crash).
- **Leadership** is an exclusive OS lock on a sibling `<name>.lock` file, taken
  at construction. A second process opening the same path fails with
  `IllegalStateException`.
- **Compaction** writes a new file, copies the old one to
  `<name>.archived.<timestamp>`, atomically replaces the live file, and fsyncs
  the directory so the rename is durable across power loss.

This backend owns its files; the path is a trusted input from the embedding
application.

## PostgresLogBackend

`fom-jdbc`. Stores events in a single table and coordinates leadership with a
Postgres advisory lock — the basis for [multi-node](multi-node.md).

```java
import io.fom.jdbc.PostgresLogBackend;

var backend = new PostgresLogBackend(dataSource, "orders");
// or specify the table name explicitly:
var backend2 = new PostgresLogBackend(dataSource, "orders", "fom_log_orders");
```

Schema (created if absent):

```sql
CREATE TABLE IF NOT EXISTS <table> (
  clock     SERIAL PRIMARY KEY,
  type      TEXT   NOT NULL,
  payload   BYTEA  NOT NULL,
  ts_millis BIGINT NOT NULL
);
```

- The constructor takes `pg_try_advisory_lock(hash64(tableName))` on a dedicated
  connection held until `close()`. A second instance opening the same table
  fails fast with `IllegalStateException` ("advisory lock"). The connection (and
  thus the lock) is released even if construction fails partway.
- The internal `clock` column is a 1-based `SERIAL`, but the engine-visible
  clock honours the SPI's **0-based** contract.
- Reads address rows by **position** (`ORDER BY clock OFFSET ?`), so positional
  addressing survives compaction (which recreates the table).
- `getBetween` runs its bound check and range read in one `REPEATABLE READ`
  transaction, so a concurrent append/compact can't shorten the result.
- The `DataSource` is injected by you (use a pool). Identifiers are validated
  and length-bounded (Postgres' 63-byte `NAMEDATALEN`).

!!! note "Testcontainers"
    `fom-jdbc`'s integration tests run against a real Postgres via
    Testcontainers and therefore need a Docker daemon. `PostgresLogBackendTest`
    runs the shared backend contract plus advisory-lock/reopen tests.

## Writing your own backend

Implement `io.fom.log.LogBackend` and uphold the
[contract](../concepts/the-log.md#the-logbackend-spi). To get the invariants
tested for free, extend the shared `LogBackendContractTest` (exposed from
`fom-core`'s test fixtures) — see [Testing](testing.md).
