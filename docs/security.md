# Security

FOM persists state by serializing objects into a log and reading them back. The
read side is the sensitive part: **deserializing attacker-controlled bytes is a
classic remote-code-execution vector.** This page describes the threat model,
what the library does by default, and what you must do for a hardened
deployment.

## Threat model

The log is trusted to the extent its **storage** is trusted:

- **`LocalFileLogBackend`** — anyone who can write the log file (or its
  directory) can plant bytes the leader will deserialize on the next
  start/replay.
- **`PostgresLogBackend`** — anyone who can `INSERT`/`UPDATE` the log table (a
  compromised DB account, SQL injection elsewhere, a malicious co-tenant) can do
  the same.
- **`fom-log` CLI** — `inspect`/`diagnose` deserialize whatever file path they
  are given.

The CRC32 in the file format detects **corruption**, not tampering — it is not a
MAC and an attacker who rewrites the file simply recomputes it.

## What the library does by default

- **Log-payload deserialization is allowlist-filtered.** The file and Postgres
  backends only ever reconstruct their own `io.fom.*` event records plus JDK
  types and `byte[]` — your objects stay inside `byte[]` cells until your
  `SerDe` decodes them separately. Those backends install
  `ObjectInputFilters.logPayload()` on their internal `readObject`, which
  **rejects any non-`io.fom`/non-JDK class** and enforces depth/reference/size
  caps. This is automatic and not configurable.
- **`JavaSerializableSerDe` runs under a resource-limit filter.** Because it must
  accept arbitrary *user* trigger/property values, it cannot allowlist classes;
  by default it applies `ObjectInputFilters.resourceLimits()` (caps on
  deserialization depth, references, stream bytes, array length) to blunt
  "deserialization bomb" payloads.
- **Type guards on decode.** Every internal `readObject` result is checked with
  `instanceof` before use, so a valid-but-wrong-type payload fails cleanly
  instead of throwing a raw `ClassCastException`.

## What you should do for production

!!! danger "Harden the serializer for untrusted bytes"
    If anyone other than your own leader can write the log storage, do **one** of:

    - Use **`FurySerDe` with class registration on**:
      `new FurySerDe(true)` — Fury then refuses to instantiate any class you did
      not register. This is the strongest, recommended option.
    - Or pass a **strict allowlist** filter to the Java serializer:
      `new JavaSerializableSerDe(myObjectInputFilter)` where `myObjectInputFilter`
      permits only your known types — or set the JVM-wide `jdk.serialFilter`.

- **Lock down the storage.** Treat the log file as a locally-owned,
  access-controlled artifact (restrictive file permissions). For Postgres,
  restrict table privileges to the application role only; don't share the table
  with untrusted tenants.
- **Run the CLI only on trusted files.** `fom-log inspect/diagnose` deserialize
  payloads (under the allowlist filter, but still) — don't point it at files you
  don't control.
- **Validate untrusted config.** The HOCON cron string is parsed by
  `cron-utils`; don't pass cron expressions from untrusted sources without
  validation.
- **Validate untrusted paths.** `LocalFileLogBackend` takes the `Path` from you
  as a trusted embedder input — if you ever derive it from external input,
  validate/canonicalize it yourself.

## Multi-tenant isolation

The `fom-tenant` wrapper is **fail-closed**: its default `authzPolicy` denies
everything, `query(caller, msg)` rejects non-`Routable` messages (whose target
tenant can't be known before dispatch), and `shutdownTenant` requires an
authorized caller. It is defence-in-depth *on top of* your own authentication —
it decides *which tenant* a caller may touch, not *who* the caller is. See
[Multi-tenancy](guides/multi-tenancy.md).

## Production deployment checklist

- [ ] **Serializer:** `FurySerDe(true)` (registered classes) **or** a
      strict `ObjectInputFilter` on `JavaSerializableSerDe` — never the
      permissive default against untrusted bytes.
- [ ] **Storage ACLs:** log file permissions / Postgres table grants restricted
      to the app.
- [ ] **Tenant authz:** an explicit `authzPolicy` set (the default denies all,
      so an unset policy is safe but useless).
- [ ] **Config inputs:** cron and any externally-sourced config validated.
- [ ] **CLI hygiene:** operators run `fom-log` only on trusted files.
- [ ] **Dependencies:** keep `fom-fury`, `postgresql`, `cron-utils`, etc.
      patched (e.g. an OWASP dependency check in CI).

!!! note
    Until these are in place, do not run FOM against untrusted log storage or in
    a multi-tenant production environment.
