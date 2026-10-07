# Security

FOM persists state by serializing objects into a log and reading them back. The
read side is the sensitive part: **deserializing attacker-controlled bytes is a
classic remote-code-execution vector.** This page describes the threat model,
what the library does by default, and what you must do for a hardened
deployment.

## Threat model

The log is trusted to the extent its **storage** is trusted:

- **`FileLogBackend`** — anyone who can write the log file (or its
  directory) can plant bytes the leader will deserialize on the next
  start/replay.
- **`PostgresLogBackend`** — anyone who can `INSERT`/`UPDATE` the log table (a
  compromised DB account, SQL injection elsewhere, a malicious co-tenant) can do
  the same.
- **`fom-log` CLI** — `inspect`/`diagnose` deserialize whatever file path they
  are given.

The CRC32 in the file format detects **corruption**, not tampering — it is not a
MAC and an attacker who rewrites the file simply recomputes it.

`FileLogBackend` does not silently discard data it cannot read: only an
incomplete *last* frame (a crash mid-append) is cut off, and those bytes are
kept as `<name>.truncated.<millis>`. An intact (CRC-valid) frame that cannot be
decoded — e.g. a planted gadget class rejected by the allowlist below — or
damage in the middle of the file (including a damaged length field that makes a
middle frame look like a torn last one: if any complete, CRC-valid frame follows
it, it is damage, not a torn tail) makes the constructor throw
`LogCorruptedException` and leaves the file untouched, so accidental damage in
the middle of the file surfaces as a failed start rather than as retired state
quietly coming back. This catches **accidental** corruption, not deliberate
tampering: under the threat model above, anyone who can write the file can
truncate it or recompute CRCs and silently drop any suffix of the log, and even
accidental damage to the payload or CRC of the *last complete* frame is
indistinguishable from a torn write — that frame is cut off with a `WARN` (if it
was a `LogDead`, the retired state comes back). Protect the log with storage
access control, not with the CRC. See
[Persistence backends](guides/persistence-backends.md#filelogbackend).

### Damage while the node runs { #damage-while-running }

The open-time check is not the only one: `FileLogBackend` verifies each
frame's CRC on **every read**, so damage done to the file of a node that is
already running (a disk error, a stray write, an edit under the engine) is not
served either. A read of a damaged frame fails with

```text
Failed to read event at position <N> of <path>: java.io.IOException: frame at offset <offset> is damaged
(CRC mismatch since the file was opened); stop the engine and repair the log (see fom-log diagnose)
```

In particular a [snapshot](concepts/snapshots.md#when-a-scheduled-snapshot-fails)
reads the whole log, so it **fails** instead of re-encoding the damaged frame
into the compacted log with a fresh, valid CRC — which would have made the damage
permanent and invisible. A scheduled snapshot keeps failing on every tick until
the log is repaired, and the log keeps growing meanwhile; alert on that WARN.
What to do: stop the engine, run [`fom-log diagnose`](guides/cli.md#diagnose)
on the file to find the damaged offset, and repair it — e.g. by
[restoring an archive](concepts/snapshots.md#restoring-an-archive). As above,
this catches accidental damage only: a deliberate edit that recomputes the CRC
passes.

## What the library does by default

- **Log-payload deserialization is allowlist-filtered.** The file and Postgres
  backends only ever reconstruct their own `io.fom.*` event records plus JDK
  types and `byte[]` — your objects stay inside `byte[]` cells until your
  `SerDe` decodes them separately. Those backends install
  `ObjectInputFilters.logPayload()` on their internal `readObject`, which
  **rejects any non-`io.fom`/non-JDK class** and enforces depth/reference/size
  caps. This is automatic and not configurable.
- **`JavaSerializableSerDe` runs under a resource-limit filter.** Because it must
  accept arbitrary *user* param types, it cannot allowlist classes;
  by default it applies `ObjectInputFilters.resourceLimits()` (caps on
  deserialization depth, references, stream bytes, and arrays of at most
  10,000,000 elements) to blunt
  "deserialization bomb" payloads.
- **Type guards on decode.** Every internal `readObject` result is checked with
  `instanceof` before use, so a valid-but-wrong-type payload fails cleanly
  instead of throwing a raw `ClassCastException`.

## What you should do for production

!!! danger "Harden the serializer for untrusted bytes"
    If anyone other than your own leader can write the log storage, do **one** of:

    - Use **`FurySerDe` with class registration on**:
      `FurySerDe.strict(YourParamType.class, ...)` — Fury then refuses to
      instantiate any class you did not register. Only process `param` types go
      through the `SerDe`; see [Serialization](guides/serialization.md).
      This is the strongest, recommended option.
    - Or pass a **strict allowlist** filter to the Java serializer:
      `new JavaSerializableSerDe(myObjectInputFilter)` where `myObjectInputFilter`
      permits only your known types — or set the JVM-wide `jdk.serialFilter`.

- **Lock down the storage.** Treat the log file as a locally-owned,
  access-controlled artifact (restrictive file permissions). `FileLogBackend`
  keeps what you set: files it writes to replace or sit next to the log (the
  compacted log that replaces it on a snapshot, a saved torn tail) get the log's
  POSIX permissions and, where the process may (as root), its owner and group — so
  a `0600` log stays `0600`, and a root-run `fom-log compact` does not leave a
  root-owned log. Archives are copies with the attributes preserved. Set the
  directory's permissions too: the `.lock` file is created with the umask's
  defaults. For Postgres,
  restrict table privileges to the application role only; don't share the table
  with untrusted tenants. The privileges the backend needs (and why row-level
  security on the log table truncates the log it sees) are listed in
  [Required privileges](guides/persistence-backends.md#required-privileges).
- **Run the CLI only on trusted files.** `fom-log inspect/diagnose` deserialize
  payloads (under the allowlist filter, but still) — don't point it at files you
  don't control.
- **Validate untrusted config.** The HOCON cron string is parsed by
  `cron-utils`; don't pass cron expressions from untrusted sources without
  validation.
- **Validate untrusted paths.** `FileLogBackend` takes the `Path` from you
  as a trusted embedder input — if you ever derive it from external input,
  validate/canonicalize it yourself.

## Multi-tenant isolation

The `fom-tenant` wrapper is **fail-closed**: its default `authzPolicy` denies
everything; a process that resolves to no tenant (or ambiguously) is denied
unless explicitly declared global; global processes are query-only unless a
`globalTriggerPolicy` admits the caller (re-initialising shared data cascades
into every tenant); the default resolver is strict and never
guesses a tenant from a name with several separators; `query(caller, msg)`
rejects non-`Routable` messages (whose target tenant can't be known before
dispatch) and reads a `Routable`'s target only once; the tenant lifecycle calls
(`pauseTenant` / `resumeTenant` / `removeTenant`) require an authorized caller;
and the wrapped engine is not exposed. It is defence-in-depth *on top of* your own authentication —
it decides *which tenant* a caller may touch, not *who* the caller is. See
[Multi-tenancy](guides/multi-tenancy.md).

## Data retention { #data-retention }

The log is append-only, so nothing the engine does *deletes* state by itself:

- **Leaving processes out of the graph retires nothing.** A node that is simply
  absent from the graph installed at a restart gets no `LogDead`; install it
  again under the same name (e.g. re-create a tenant with the same id) and it
  warm-loads the old state (unless a snapshot ran in between: a snapshot keeps
  only the current graph's state). Retire it with `remove` /
  `removeTenant`, which write a `LogDead`.
- **Retiring is not deleting.** A retired (or superseded) state stays in the
  live log until the next [snapshot](concepts/snapshots.md), which drops it, and
  in the archives that snapshot leaves until `purgeArchives` removes them.

- **Params are stored in plaintext.** Every graph install persists a
  `LogChangeGraph` whose `Node.param` is the node's `param` as serialized by the
  engine's `SerDe` — readable by anyone who can read the log or its archives
  (e.g. with `fom-log`). Don't put secrets (passwords, API keys, tokens) in a
  `param`: pass a *reference* (a secret name, a vault path) and resolve it in
  `init`/`load`.

So to really delete a process's or a tenant's data: retire it
(`remove`/`removeTenant`), then `engine.snapshot()`, then
`engine.purgeArchives(0)` (or the `keepHistory` your retention policy allows).
This removes the retired processes' *definitions* (their params) as well as their
state: the new graph written by the removal no longer lists them, and a snapshot
keeps an older `LogChangeGraph` only for the processes whose state it copies,
trimmed to exactly those nodes.

`FileLogBackend` also leaves files that `purgeArchives` does not touch: when
it opens a log with a torn tail (a crash mid-write), it cuts the incomplete frame
off and saves the discarded bytes as `<log>.truncated.<millis>` next to the log.
Those bytes may hold retired data, so delete any `*.truncated.*` siblings as part
of an erasure. Backups of the log you take yourself are outside the engine's reach.

`PostgresLogBackend` writes every event's serialized payload (process state,
graph `param`s) as a statement bind parameter, so **Postgres statement logging
copies it into the server log**: `log_statement = 'mod'`/`'all'`,
`log_min_duration_statement`, or `pgaudit` with `pgaudit.log_parameter = on` log
the parameters in full unless `log_parameter_max_length` limits them (default
`-1`, unlimited). Snapshots and `purgeArchives` never reach that copy. Set
`log_parameter_max_length = 0` for the fom role (`ALTER ROLE … SET
log_parameter_max_length = 0`) and keep `pgaudit.log_parameter` off, or put the
server logs (and wherever they are shipped) under the same retention and access
rules as the log table. See
[Persistence backends](guides/persistence-backends.md#postgreslogbackend).

## Production deployment checklist

- [ ] **Serializer:** `FurySerDe.strict(...)` (registered classes) **or** a
      strict `ObjectInputFilter` on `JavaSerializableSerDe` — never the
      permissive default against untrusted bytes.
- [ ] **Storage ACLs:** log file permissions / Postgres table grants restricted
      to the app (see [Required privileges](guides/persistence-backends.md#required-privileges);
      no RLS on the log table).
- [ ] **Tenant authz:** an explicit `authzPolicy` set (the default denies all,
      so an unset policy is safe but useless); shared processes listed in
      `globalProcesses`; `globalTriggerPolicy` left at its deny-all default or
      limited to operators; a resolver that matches your naming (`registry`/`regex`
      if names or tenant ids contain `_`); the sets returned by
      `pauseTenant`/`resumeTenant`/`removeTenant` checked (empty = nothing matched).
- [ ] **Config inputs:** cron and any externally-sourced config validated.
- [ ] **CLI hygiene:** operators run `fom-log` only on trusted files.
- [ ] **Dependencies:** keep `fom-fury`, `postgresql`, `cron-utils`, etc.
      patched (e.g. an OWASP dependency check in CI).

!!! note
    Until these are in place, do not run FOM against untrusted log storage or in
    a multi-tenant production environment.
