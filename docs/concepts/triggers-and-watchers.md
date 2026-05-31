# Triggers & watchers

Both are ways to make a process **re-initialise** — to pick up a change in the
outside world. A trigger is a one-shot push; a watcher is a recurring poll that
pushes when it sees a change.

## Triggers

`engine.trigger(processName, value)` records a `LogTrigger` and schedules a
re-init of the named process (subject to the [dedup window](reactive-cascade.md#the-dedup-window)).
The `value` is serialized with the engine's [SerDe](../guides/serialization.md)
and is available to the process during its re-init.

```java
engine.trigger("Inventory", new RefreshSignal("ad-hoc"));
```

Trigger several processes atomically with one log record:

```java
engine.trigger(Map.of(
    "Inventory", new RefreshSignal("nightly"),
    "Pricing",   new RefreshSignal("nightly")));
```

Re-initialising `Inventory` cascades to its reactive consumers — see
[Reactive cascade](reactive-cascade.md).

!!! note "The value must be `Serializable`"
    `trigger` takes a `Serializable` value so it can be written to the log. With
    `JavaSerializableSerDe` it must implement `java.io.Serializable`; with
    `FurySerDe` any registered/registrable class works.

## Watchers

A `ScheduledWatcher<V>` polls an external source on a fixed schedule. When its
`check` function returns a value, the engine fires a trigger for the watched
process with that value.

```java
import io.fom.ScheduledWatcher;
import java.time.Duration;

AutoCloseable handle = engine.watch(new ScheduledWatcher<>(
    "Inventory",            // process to trigger
    Long.class,             // watched value type
    0L,                     // initial value
    Duration.ZERO,          // initial delay
    Duration.ofMinutes(1),  // poll interval
    prevVersion -> fetchLatestVersion().filter(v -> v > prevVersion),  // check
    null));                 // sentinel (optional)

// later, to stop polling:
handle.close();
```

The `check` receives the **previous** value and returns an `Optional<V>`:

- **present** → the value changed; the engine updates the held value and fires a
  trigger with it.
- **empty** → no change; nothing happens this tick.

`engine.watch(...)` returns an `AutoCloseable` that cancels the schedule. A
watcher whose `check` throws is logged and skipped — one bad tick doesn't kill
the watcher.

### Watcher fields

| Field | Meaning |
|---|---|
| `processName` | process to trigger on a change |
| `valueType` | `Class<V>` of the watched value |
| `initialValue` | the value the first `check` sees as "previous" |
| `initialDelay` | delay before the first poll |
| `interval` | time between polls (must be > 0) |
| `check` | `Function<V, Optional<V>>` — returns a new value to trigger with, or empty |
| `sentinel` | optional marker value |

## When to use which

- **Trigger** — you already know something changed (a webhook, an admin action,
  another system's event).
- **Watcher** — you need to *discover* changes by polling (a version counter, a
  file's mtime, a row count).
