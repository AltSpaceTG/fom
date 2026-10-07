package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.SnapshotPolicy
import io.fom.api.EngineObserver
import io.fom.api.Process
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.Serializable
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

/**
 * Which queries a [SuspendingProcess] sends are tied to the query it is computing,
 * and what cancelling a coroutine awaiting `queryAwait` does. Documented in
 * `docs/guides/kotlin-dsl.md`.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class SuspendingQueryLinkageTest {

    private fun config() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(1),
        Duration.ofSeconds(5), Duration.ofMillis(1),
        Duration.ofMillis(10), Duration.ofMillis(100), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    object Hang : Serializable
    object ViaEngine : Serializable
    object ViaCtx : Serializable

    /** processName -> parentQueryId (or NONE) of the last query sent to it; query failures by reason. */
    private class Recorder : EngineObserver {
        val parents = ConcurrentHashMap<String, Any>()
        val sentToB = CountDownLatch(1)
        val failures = ConcurrentHashMap<String, String>()
        override fun onQuerySent(processName: String, queryId: UUID, messageType: Class<*>, parentQueryId: UUID?) {
            parents[processName] = parentQueryId ?: NONE
            if (processName == "B") sentToB.countDown()
        }
        override fun onQueryFailed(processName: String, queryId: UUID, reason: String, cause: Throwable?) {
            failures[processName] = reason
        }
        companion object { val NONE = Any() }
    }

    private class Phases(private val make: () -> Process) : ProcessInitializer, ProcessLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())
        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(make())
    }

    private class Hanging(val cancelled: CountDownLatch) : SuspendingProcess() {
        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? =
            try { awaitCancellation() } finally { cancelled.countDown() }
    }

    private class Caller(context: CoroutineContext, val engine: () -> Engine) : SuspendingProcess(context) {
        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? = when (query) {
            ViaEngine -> engine().queryProcessAwait("B", Hang)
            ViaCtx -> ctx.queryAwait("B", Hang)
            else -> "?"
        }
    }

    /** Sends [how] to A (computed on [context]) and returns B's recorded parent id. */
    private fun parentOfBQuery(context: CoroutineContext, how: Any): Any? {
        val rec = Recorder()
        lateinit var engine: Engine
        val bCancelled = CountDownLatch(1)
        Engine(config(), InMemoryLogBackend(), JavaSerializableSerDe(), rec).use { e ->
            engine = e
            e.newGraph(graph {
                process("B", { Phases { Hanging(bCancelled) } }, { Phases { Hanging(bCancelled) } })
                process("A", { Phases { Caller(context) { engine } } }, { Phases { Caller(context) { engine } } },
                    dependsOn = listOf("B"))
            })
            assertThatThrownBy {
                e.queryProcess("A", how, Duration.ofMillis(300)).toCompletableFuture().get(10, TimeUnit.SECONDS)
            }.isNotNull()
            assertThat(rec.sentToB.await(5, TimeUnit.SECONDS)).isTrue()
            return rec.parents["B"]
        }
    }

    @Test
    fun `an outer-Engine query from a dispatched computeAsync is not tied to the issuing query`() {
        assertThat(parentOfBQuery(Dispatchers.Default, ViaEngine)).isSameAs(Recorder.NONE)
    }

    @Test
    fun `an outer-Engine query from Unconfined before the first suspension is tied`() {
        assertThat(parentOfBQuery(Dispatchers.Unconfined, ViaEngine)).isInstanceOf(UUID::class.java)
    }

    @Test
    fun `ctx queryAwait is always tied`() {
        assertThat(parentOfBQuery(Dispatchers.Default, ViaCtx)).isInstanceOf(UUID::class.java)
    }

    @Test
    fun `cancelling the coroutine awaiting queryProcessAwait cancels the query and its compute`() {
        val rec = Recorder()
        val bCancelled = CountDownLatch(1)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            Engine(config(), InMemoryLogBackend(), JavaSerializableSerDe(), rec).use { engine ->
                engine.newGraph(graph { process("B", { Phases { Hanging(bCancelled) } }, { Phases { Hanging(bCancelled) } }) })
                val job = scope.launch { engine.queryProcessAwait("B", Hang, Duration.ofSeconds(30)) }
                assertThat(rec.sentToB.await(5, TimeUnit.SECONDS)).isTrue()
                Thread.sleep(100)
                job.cancel()
                assertThat(bCancelled.await(5, TimeUnit.SECONDS)).`as`("B's computeAsync cancelled").isTrue()
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
                while (rec.failures["B"] == null && System.nanoTime() < deadline) Thread.sleep(10)
                assertThat(rec.failures["B"]).isEqualTo("cancelled")
            }
        } finally {
            scope.cancel()
        }
    }
}
