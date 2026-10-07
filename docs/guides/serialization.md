# Serialization

The engine writes exactly one kind of user-typed value to the log through a
serializer: each node's `param`. That is the job of the **`SerDe`** SPI. Picking
and hardening a `SerDe` is a security-relevant configuration decision — read
[Security](../security.md) alongside this page.

## The `SerDe` SPI

```java
public interface SerDe {
    byte[] serializeParam(String processName, Serializable param);
    Object loadParam(String processName, byte[] bytes);
}
```

`LogChangeGraph` records each node's `param` through it, so a restart can tell
whether a node's definition changed since its state was persisted. The engine
reads a param back only for that comparison, so a `SerDe` must round-trip a
param to a value `equals` to the original (`SerDeContractTest` in `fom-test`
checks this; a schema-based `SerDe` supplies its own sample params by overriding
`sampleParams()` — see [Testing](testing.md#a-custom-serde)).

Everything else in the log is either an fom type or opaque bytes:

- **process properties** — the `Map<String, byte[]>` cells your `init` returns —
  are stored as-is; encode each cell with a `Codec` (see
  [Configuration](configuration.md#typed-property-cells));
- **trigger values** are not stored — `LogTrigger` records only which processes
  were asked to re-initialise;
- **factories and dynamic-route resolvers** are never serialized — they live in
  code.

## Choosing an implementation

### `FurySerDe`

`fom-fury` wraps [Apache Fury](https://fury.apache.org/): compact, fast, and
schema-evolution friendly (adding a nullable field to a record won't break
readers built against the old schema). Construct one per engine and share it.

```java
import io.fom.fury.FurySerDe;

var serDe = new FurySerDe();                          // permissive (default)
var strict = FurySerDe.strict(TenantParam.class);     // registration required
```

- The no-arg constructor leaves class registration **off** — zero-config, but
  Fury will instantiate any class named in the bytes.
- `FurySerDe.strict(...)` (equivalently `new FurySerDe(true, classes)`) turns
  Fury's class-registration gate **on**, its strongest defence against malicious
  bytes. You then control exactly which classes can be deserialized.

In strict mode Fury registers JDK collections, strings, boxed primitives and
`byte[]` itself. **You must register every process `param` type**
(`GraphBuilder.addWithParam`) and any non-JDK type nested inside it — nothing
else goes through the `SerDe`. Registration assigns numeric ids by order, so
pass classes in a stable order and only append new ones. `new FurySerDe(true)`
with no classes works only if no process takes a non-JDK `param`.

See [Security](../security.md) for when to choose strict mode.

### `JavaSerializableSerDe` (tests, quickstart)

`fom-core`'s built-in `SerDe` uses `ObjectOutputStream`/`ObjectInputStream`.
Params must implement `java.io.Serializable` (they have to anyway).

```java
import io.fom.serde.JavaSerializableSerDe;
import io.fom.serde.ObjectInputFilters;

var serDe = new JavaSerializableSerDe();                      // resource-limit filter
var hardened = new JavaSerializableSerDe(myObjectInputFilter); // your allowlist
```

Every `readObject()` runs under an `ObjectInputFilter`:

- the default (`ObjectInputFilters.resourceLimits()`) caps deserialization
  depth, references, stream bytes, and array length (at most 10,000,000
  elements — larger arrays in a payload are rejected) to blunt "deserialization
  bomb" payloads, while still admitting unknown user classes;
- pass your own `ObjectInputFilter` (e.g. a strict class allowlist) via the
  constructor for production, or set the JVM-wide `jdk.serialFilter`.

!!! danger "Java serialization is a known RCE sink"
    Even with the resource-limit filter, unfiltered class deserialization of
    attacker-controlled bytes is dangerous. For production handling untrusted
    log bytes, prefer `FurySerDe.strict(...)`, or pass a strict allowlist
    filter. See [Security](../security.md).

## Log-payload hardening (automatic)

The **log backends** (`FileLogBackend`, `PostgresLogBackend`) reconstruct
only their own `io.fom.*` event records plus JDK types and `byte[]` — your
params stay inside `byte[]` until the `SerDe` decodes them separately.
Those backends therefore apply a **strict allowlist** filter
(`ObjectInputFilters.logPayload()`) on their internal `readObject`, rejecting
any non-`io.fom`/non-JDK class. This is built in; you don't configure it.

The same filter caps each event's payload: at most 64 MiB per event, arrays of
at most 10,000,000 elements (e.g. a `byte[]` property), 1,000,000 object
references, and nesting depth 64 (`ObjectInputFilters.logPayloadLimits()`
describes them). Because an event over those limits could not be read back
after a restart, both backends refuse it at `append` with an
`IllegalArgumentException` naming the limits. If `init` returns properties that
large, the process goes `Dead` at once, without retrying, and
`NodeReport.lastException` shows the message. Keep bulky data outside the log
(a file or a table) and persist a reference to it.

In practice the reference limit is the one a large *number* of small properties
hits first. An init is persisted as one event whose properties are a map of
key → `byte[]`, and each property costs about **2 references** (its key
`String` and its `byte[]` value). A process whose `init` returns around
**500,000 properties** therefore reaches the 1,000,000-reference limit, however
small each one is — long before the 64 MiB cap. If a process produces that many
entries, pack them into fewer, larger properties (one encoded `byte[]` per
chunk) or, better, keep them outside the log and persist only a reference.

!!! note "CRC is not a MAC"
    The file backend frames each event with a CRC32 for *corruption* detection.
    It is not a tamper-proof signature — treat the log file as a
    locally-owned, access-controlled artifact.

## Cross-backend compatibility

All three bundled backends serialize their event payloads with Java
serialization internally, so a log written by one is readable by another. The
**`SerDe` you choose** only encodes the params inside `LogChangeGraph` — keep it
consistent across restarts of the same log.
