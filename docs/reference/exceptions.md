# Exceptions

All FOM exceptions are unchecked (`RuntimeException` subclasses). They fall into
three groups: ones **you throw** from your process code to signal a phase
failure, ones **the engine throws back** to a caller, and ones from the
serialization / typed-cell helpers.

## Thrown by your process code

| Exception | Package | Throw it from | Effect |
|---|---|---|---|
| `InitializationException` | `io.fom.api` | `ProcessInitializer.init` | the engine backs off and [retries](../concepts/process-lifecycle.md) until `defaultInitTimeout` |
| `LoadException` | `io.fom.api` | `ProcessLoader.load` | retried up to `maxLoadRetries`, then falls back to `init` |

Throwing these (or any exception) from `init`/`load` drives the retry/fallback
machinery. A `compute` that fails completes the query's stage exceptionally with
your exception.

## Thrown back to a caller

| Exception | Package | When |
|---|---|---|
| `InitializationTimeoutException` | `io.fom.api` | the total `defaultInitTimeout` budget elapsed across all `init` retries; the process is now `Dead` |
| `InitInProgressException` | `io.fom.api` | a query arrived while the target was still in `NotPresent` (init hasn't produced a servable state yet) |
| `QueryException` | `io.fom.api` | routing failure — no route for the message type, a resolver returned an unknown/blank process, or a query to an undeclared dependency |
| `QueryRejectedException` | `io.fom.api` | the target is `CleaningUp`/`Dead`, or its mailbox refused the message |
| `TenantAccessDeniedException` | `io.fom.tenant` | the [tenant wrapper](../guides/multi-tenancy.md) denied the call (unauthorized, or a non-`Routable` `query`) |

These surface either synchronously (e.g. `QueryException` from a bad route) or as
the failure of the returned `CompletionStage`. When awaiting a stage with
`.get()`, they arrive wrapped in `ExecutionException` — unwrap with
`getCause()`:

```java
try {
    engine.query(msg).toCompletableFuture().get();
} catch (ExecutionException e) {
    if (e.getCause() instanceof QueryException qe) { /* bad route */ }
}
```

## Serialization & typed cells

| Exception | Package | When |
|---|---|---|
| `SerDeException` | `io.fom.serde` | a [`SerDe`](../guides/serialization.md) failed to (de)serialize — bad bytes, a filter rejection, an unexpected type |
| `CodecException` | `io.fom` | a [`Codec`](../guides/configuration.md#typed-property-cells) failed to encode/decode a typed value |
| `NoSuchPropertyException` | `io.fom` | `Properties.get(key)` for a key that isn't present in the cells |

## Other standard exceptions you may see

- **`IllegalArgumentException`** — invalid graph (cycle, missing dependency,
  route to unknown process), invalid `EngineConfig` (non-positive duration,
  `maxLoadRetries < 1`), or a bad SQL identifier in `fom-jdbc`.
- **`IllegalStateException`** — using a closed `Engine`/backend, a second
  process trying to take a backend's leadership lock, or a DI supplier resolving
  before its container was registered.
- **`IndexOutOfBoundsException`** — `LogBackend.get`/`getBetween` outside
  `[0, length())`.
