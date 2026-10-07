# Concepts

This section builds the mental model behind FOM. Read it roughly in order; each
page links to the ones it depends on.

<div class="grid cards" markdown>

- :material-state-machine: **[Process lifecycle](process-lifecycle.md)** —
  the per-process state machine: `NotPresent → Initializing → Loading → Serving
  → CleaningUp → Dead`, retries, timeouts, and the virtual-thread dispatcher.
- :material-identifier: **[Sid & clock](sid-and-clock.md)** — how a specific
  version of a process's state is named, and how the log assigns clocks.
- :material-database: **[The log](the-log.md)** — the append-only source of
  truth: event types, leadership, and the `LogBackend` SPI.
- :material-graph: **[Graph & routing](graph-and-routing.md)** — defining the
  process graph and how queries find their target process.
- :material-lightning-bolt: **[Reactive cascade](reactive-cascade.md)** —
  reactive vs stable dependencies and how changes propagate.
- :material-bell-ring: **[Triggers & watchers](triggers-and-watchers.md)** —
  forcing re-initialisation on demand or on a schedule.
- :material-camera: **[Snapshots](snapshots.md)** — compacting the log so it
  doesn't grow forever.
- :material-swap-horizontal: **[In-place graph swap](graph-swap.md)** —
  changing the graph of a running engine.
- :material-restore: **[Idempotent restart](idempotent-restart.md)** — why a
  JVM restart skips `init` and only runs `load`.

</div>

## The one-paragraph version

You describe a **[graph](graph-and-routing.md)** of named **processes**. Each
process has an `init` (compute persisted state, once) and a `load` (build a
live, queryable object from that state, every start). The engine drives each
process through a **[lifecycle](process-lifecycle.md)** on its own virtual
thread, writing every durable change to an **[append-only log](the-log.md)**.
A specific version of a process's state is a **[Sid](sid-and-clock.md)**. When a
producer's Sid changes, **[reactive](reactive-cascade.md)** consumers
re-initialise. You can prompt changes with
**[triggers and watchers](triggers-and-watchers.md)**, keep the log bounded with
**[snapshots](snapshots.md)**, and evolve a running system with an
**[in-place graph swap](graph-swap.md)**. Because the log is the source of
truth, a restart **[recovers without recomputing](idempotent-restart.md)**.
