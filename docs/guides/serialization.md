# Serialization

The engine needs to turn user values and the graph into bytes for the log, and
back. That is the job of the **`SerDe`** SPI. Picking and hardening a `SerDe` is
the single most security-relevant configuration decision — read
[Security](../security.md) alongside this page.

## The `SerDe` SPI

```java
public interface SerDe {
    byte[] serializeTriggerValue(String processName, Object value);
    Object loadTriggerValue(String processName, byte[] bytes);
    byte[] serializeProcessProperties(Map<String, byte[]> properties);
    Map<String, byte[]> loadProcessProperties(byte[] bytes);
    byte[] serializeProcessGraph(Graph graph);
    Graph loadProcessGraph(byte[] bytes);
}
```

It serializes three things: **trigger values** (arbitrary user objects),
**process properties** (a `Map<String, byte[]>`), and the **graph** (which
embeds your factory suppliers and any dynamic-route resolvers — so those must be
serializable).

`SerDe` is engine-wide. It is distinct from the per-value `Codec` used by
`TypedKey` inside property cells (see
[Configuration](configuration.md#typed-property-cells)).

## Choosing an implementation

### `FurySerDe` (recommended for production)

`fom-fury` wraps [Apache Fury](https://fury.apache.org/): compact, fast, and
schema-evolution friendly (adding a nullable field to a record won't break
readers built against the old schema). Construct one per engine and share it.

```java
import io.fom.fury.FurySerDe;

var serDe = new FurySerDe();                 // permissive (default)
var strict = new FurySerDe(true);            // requireClassRegistration = true
```

- The no-arg constructor leaves class registration **off** — zero-config, but
  Fury will instantiate any class named in the bytes.
- `new FurySerDe(true)` turns Fury's class-registration gate **on**, its
  strongest defence against malicious bytes. You then control exactly which
  classes can be deserialized. This is opt-in because flipping the default would
  break serialization of arbitrary user trigger values.

See [Security](../security.md) for when to choose strict mode.

### `JavaSerializableSerDe` (fallback, tests, quickstart)

`fom-core`'s built-in `SerDe` uses `ObjectOutputStream`/`ObjectInputStream`. All
values must implement `java.io.Serializable`.

```java
import io.fom.serde.JavaSerializableSerDe;
import io.fom.serde.ObjectInputFilters;

var serDe = new JavaSerializableSerDe();                      // resource-limit filter
var hardened = new JavaSerializableSerDe(myObjectInputFilter); // your allowlist
```

Every `readObject()` runs under an `ObjectInputFilter`:

- the default (`ObjectInputFilters.resourceLimits()`) caps deserialization
  depth, references, stream bytes, and array length to blunt "deserialization
  bomb" payloads, while still admitting unknown user classes;
- pass your own `ObjectInputFilter` (e.g. a strict class allowlist) via the
  constructor for production, or set the JVM-wide `jdk.serialFilter`.

!!! danger "Java serialization is a known RCE sink"
    Even with the resource-limit filter, unfiltered class deserialization of
    attacker-controlled bytes is dangerous. For production handling untrusted
    log bytes, prefer `FurySerDe` with `requireClassRegistration(true)`, or pass
    a strict allowlist filter. See [Security](../security.md).

## Log-payload hardening (automatic)

The **log backends** (`LocalFileLogBackend`, `PostgresLogBackend`) reconstruct
only their own `io.fom.*` event records plus JDK types and `byte[]` — your
objects stay inside `byte[]` cells until the `SerDe` decodes them separately.
Those backends therefore apply a **strict allowlist** filter
(`ObjectInputFilters.logPayload()`) on their internal `readObject`, rejecting
any non-`io.fom`/non-JDK class. This is built in; you don't configure it.

!!! note "CRC is not a MAC"
    The file backend frames each event with a CRC32 for *corruption* detection.
    It is not a tamper-proof signature — treat the log file as a
    locally-owned, access-controlled artifact.

## Cross-backend compatibility

All three bundled backends serialize their event payloads with Java
serialization internally, so a log written by one is readable by another. The
**`SerDe` you choose** affects how *your* trigger/property/graph bytes are
encoded inside those events — keep it consistent across restarts of the same
log.
