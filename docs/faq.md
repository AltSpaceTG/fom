# FAQ

#### How is this different from a cache or a job scheduler?

A cache stores values you can recompute on demand; FOM stores *processes* whose
state is expensive to build and is recovered by `load` (cheap) rather than `init`
(expensive) after a restart. A job scheduler runs tasks on a clock; FOM keeps
long-lived, queryable state and re-initialises it *reactively* when its
dependencies change. The closest mental model is "a small graph of durable,
self-recovering read models".

#### Does my state have to fit in memory?

The *live* object a `load` produces lives in memory and answers queries; the
*persisted* form is the `byte[]` property cells in the log. If your state is
huge, persist a compact descriptor and have `load` build a structure that
streams or pages from elsewhere — keep `load` fast and defer heavy work to the
first `compute`.

#### What happens if `init` keeps failing?

It retries with exponential backoff until the total `defaultInitTimeout` budget
is spent, then the process goes to `Dead` with
`InitializationTimeoutException`. See
[Process lifecycle](concepts/process-lifecycle.md).

#### Can I run more than one instance?

Yes, with `PostgresLogBackend`: one instance becomes leader (advisory lock); the
others fail fast on construction and can stand by. See
[Multi-node](guides/multi-node.md). The file and in-memory backends are
single-process.

#### Why must factories be `SerializableSupplier`?

The graph — including how to build each process — is serialized into the log so
the engine can reconstruct it after a restart. Method references
(`MyInit::new`) satisfy this directly; for DI-managed objects use
[`fom-guice`/`fom-spring`](guides/dependency-injection.md).

#### Which serializer should I use?

`FurySerDe` (from `fom-fury`) for anything real — compact, fast,
schema-evolution friendly. `JavaSerializableSerDe` is fine for tests and the
quickstart. For untrusted log storage, harden the serializer — see
[Security](security.md).

#### Will the log grow forever?

Not if you enable [snapshots](concepts/snapshots.md): a snapshot compacts the
log to only the current live state and archives the rest. Use
`SnapshotPolicy.FixedInterval`, a size-based policy from `fom-log-maintenance`,
or a custom policy.

#### How do I change the graph without downtime?

Call `engine.newGraph(newGraph)` again — the engine diffs it against the running
graph and applies only the difference. See
[In-place graph swap](concepts/graph-swap.md).

#### How do I verify a warm restart actually skipped `init`?

Attach an [`EngineObserver`](guides/observability.md) and assert you see
`onLoad*` but not `onInitStarted` for each node — see
[Idempotent restart](concepts/idempotent-restart.md).

#### Is `fom-core` really dependency-free?

At runtime it depends only on `slf4j-api`. Everything else — serializers, DI,
metrics, tracing, Postgres, the Kotlin DSL — is a separate optional module.

#### What JDK do I need?

JDK 21+ at runtime (virtual threads, pattern matching). The build targets a
JDK 21 toolchain.
