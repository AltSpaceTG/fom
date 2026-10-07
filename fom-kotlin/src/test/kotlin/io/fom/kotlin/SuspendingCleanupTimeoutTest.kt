package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.Sid
import io.fom.SnapshotPolicy
import io.fom.api.Deadline
import io.fom.api.Process
import io.fom.api.ProcessContext
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.job
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.Serializable
import java.time.Duration
import java.util.Optional
import java.util.concurrent.Executor
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * A suspending `cleanUp` that never returns must not survive its budget:
 * `ProcessFSM` cancels the stage `Process.cleanUp` returned once
 * `cleanupTimeout` is up, and [SuspendingProcess] forwards that
 * cancellation to the coroutine (and to the whole retiring generation), so the
 * `finally` of `cleanUpAsync` runs and nothing of the retired generation
 * outlives a re-init or `engine.close()`.
 */
class SuspendingCleanupTimeoutTest {

    private fun config(cleanup: Duration) = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        cleanup, Duration.ofSeconds(5),
        Duration.ofMillis(1),
        Duration.ofMillis(10), Duration.ofMillis(100), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `a hanging cleanUpAsync is cancelled when the budget expires on close`() {
        val p = HangingCleanupProcess()
        val g = graph { process("Hanging", { HangingInit(p) }, { HangingInit(p) }).handles<Ping>() }

        InMemoryLogBackend().use { backend ->
            Engine(config(Duration.ofMillis(300)), backend, JavaSerializableSerDe()).use { engine ->
                engine.newGraph(g)
                assertThat(engine.query(Ping).toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo("pong")
            }
            // close() returned: the cleanup budget is over.
            assertThat(p.cleanupEntered.await(10, TimeUnit.SECONDS)).`as`("cleanUpAsync started").isTrue()
            assertThat(p.cleanupFinally.await(10, TimeUnit.SECONDS))
                .`as`("the finally of the hanging cleanUpAsync ran, i.e. it was cancelled")
                .isTrue()
            assertThat(p.liveGenerationJobs()).`as`("no coroutine of the retired generation survives").isEmpty()
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `a hanging cleanUpAsync is cancelled when a re-init retires its generation`() {
        val p = HangingCleanupProcess()
        val g = graph { process("Hanging", { HangingInit(p) }, { HangingInit(p) }).handles<Ping>() }

        InMemoryLogBackend().use { backend ->
            Engine(config(Duration.ofMillis(300)), backend, JavaSerializableSerDe()).use { engine ->
                engine.newGraph(g)
                assertThat(engine.query(Ping).toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo("pong")

                // Re-init: the current generation is retired and its cleanUpAsync hangs.
                assertThat(engine.trigger("Hanging", "v2")).isTrue()
                assertThat(p.cleanupEntered.await(10, TimeUnit.SECONDS)).isTrue()
                assertThat(p.cleanupFinally.await(10, TimeUnit.SECONDS))
                    .`as`("the retired generation's cleanup was cancelled, not left running")
                    .isTrue()

                // The instance is reused by the new load and keeps serving.
                assertThat(engine.query(Ping).toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo("pong")
                assertThat(p.inits.get()).isGreaterThanOrEqualTo(2)
            }
            assertThat(p.liveGenerationJobs()).`as`("nothing of either retired generation survives").isEmpty()
        }
    }

    /**
     * The same, with a `computeAsync` left suspended in the retiring
     * generation: cancelling the hung cleanup must take the abandoned compute
     * with it instead of leaking it past `engine.close()`.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `an abandoned compute of the retired generation does not outlive the cleanup budget`() {
        val p = HangingCleanupProcess()
        val g = graph { process("Hanging", { HangingInit(p) }, { HangingInit(p) }).handles<Ping>() }

        InMemoryLogBackend().use { backend ->
            Engine(config(Duration.ofMillis(300)), backend, JavaSerializableSerDe()).use { engine ->
                engine.newGraph(g)
                // A query whose compute never returns; its own reply is failed by the engine.
                val hung = engine.queryProcess("Hanging", Hang, Duration.ofSeconds(30)).toCompletableFuture()
                assertThat(p.computeEntered.await(10, TimeUnit.SECONDS)).isTrue()
                assertThat(hung.isDone).`as`("the compute is still suspended").isFalse()
            }
            assertThat(p.cleanupFinally.await(10, TimeUnit.SECONDS)).isTrue()
            assertThat(p.computeFinally.await(10, TimeUnit.SECONDS))
                .`as`("the abandoned computeAsync was cancelled too")
                .isTrue()
            assertThat(p.liveGenerationJobs()).isEmpty()
        }
    }

    /** Sanity check of the wrapper on its own, without an engine. */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `cancelling the stage returned by cleanUp cancels the cleanup coroutine`() {
        val p = HangingCleanupProcess()
        // one compute, so there is a real generation to retire
        p.compute(NoopContext, Ping)
        val stage = p.cleanUp(NoopProcessContext).toCompletableFuture()
        assertThat(p.cleanupEntered.await(10, TimeUnit.SECONDS)).isTrue()
        assertThat(stage.isDone).`as`("cleanUp hangs while cleanUpAsync does").isFalse()

        assertThat(stage.cancel(true)).isTrue()

        assertThat(p.cleanupFinally.await(10, TimeUnit.SECONDS)).isTrue()
        assertThat(p.liveGenerationJobs()).isEmpty()
    }

    // ───────────────── fixtures ─────────────────

    object Ping : Serializable
    object Hang : Serializable

    class HangingInit(private val process: HangingCleanupProcess) : ProcessInitializer, ProcessLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> {
            process.inits.incrementAndGet()
            return CompletableFuture.completedFuture(emptyMap())
        }

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(process)
    }

    /**
     * Its `cleanUpAsync` never returns on its own — only cancellation ends it,
     * which is what the `finally` records.
     */
    class HangingCleanupProcess : SuspendingProcess(Dispatchers.Default) {

        val inits = AtomicInteger()
        val cleanupEntered = CountDownLatch(1)
        val cleanupFinally = CountDownLatch(1)
        val computeEntered = CountDownLatch(1)
        val computeFinally = CountDownLatch(1)

        /** Every coroutine started here, so the test can assert none is left alive. */
        private val started = ConcurrentLinkedQueue<CompletableDeferred<Unit>>()

        fun liveGenerationJobs(): List<CompletableDeferred<Unit>> = started.filter { it.isActive }

        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? {
            val alive = CompletableDeferred<Unit>()
            started += alive
            @Suppress("UNUSED_VARIABLE")
            val bind = kotlin.coroutines.coroutineContext.job // keep the coroutine's job reachable
            try {
                if (query === Hang) {
                    computeEntered.countDown()
                    awaitCancellation()
                }
                return "pong"
            } finally {
                // settle `alive` first: the latch is what the test waits on, so the
                // liveness bookkeeping must already be done when it opens.
                alive.complete(Unit)
                if (query === Hang) {
                    computeFinally.countDown()
                }
            }
        }

        override suspend fun cleanUpAsync(ctx: ProcessContext) {
            val alive = CompletableDeferred<Unit>()
            started += alive
            try {
                cleanupEntered.countDown()
                awaitCancellation()
            } finally {
                alive.complete(Unit)
                cleanupFinally.countDown()
            }
        }
    }

    /**
     * The one executor both minimal contexts report. [SuspendingProcess] ties a
     * generation to its owner by executor identity (as the engine does: one
     * executor per process state machine), so compute and cleanUp contexts of the
     * same owner must return the same instance.
     */
    object InlineExecutor : Executor {
        override fun execute(command: Runnable) = command.run()
    }

    /** Minimal contexts for the engine-free sanity check. */
    object NoopContext : QueryableContext {
        override fun query(dependencyName: String, query: Any): CompletionStage<Any> =
            throw UnsupportedOperationException("no dependencies in this test")

        override fun dependencies(): List<String> = emptyList()

        override fun currentQueryDeadline(): Optional<Deadline> = Optional.empty()

        override fun sid(): Sid = Sid("Hanging", 0)

        override fun executor(): Executor = InlineExecutor
    }

    object NoopProcessContext : ProcessContext {
        override fun sid(): Sid = Sid("Hanging", 0)

        override fun executor(): Executor = InlineExecutor
    }
}
