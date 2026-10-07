package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.Sid
import io.fom.SnapshotPolicy
import io.fom.api.EngineObserver
import io.fom.api.Process
import io.fom.api.ProcessContext
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.Serializable
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

/**
 * A compute the engine has abandoned (its query timed out) but that is stuck in
 * blocking, non-cancellable code must not hold `cleanUp` — and with it a pause,
 * a re-init or `engine.close()` — for the whole cleanup budget. A plain Java
 * `Process` is not held up either; [SuspendingProcess] must behave the same.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class SuspendingCleanupAbandonedComputeTest {

    /** Released in [tearDown] so no blocked IO thread outlives the test. */
    private val release = CountDownLatch(1)
    private val blockedEntered = CountDownLatch(1)

    @AfterEach
    fun tearDown() = release.countDown()

    private val cleanupBudget = Duration.ofSeconds(5)

    private fun config() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        cleanupBudget, Duration.ofSeconds(5),
        Duration.ofMillis(1),
        Duration.ofMillis(10), Duration.ofMillis(100), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    object Block : Serializable
    object Ping : Serializable

    private class Cleanup(val ok: Boolean, val duration: Duration)

    private class Recorder : EngineObserver {
        val cleanups = ConcurrentLinkedQueue<Cleanup>()
        override fun onCleanupCompleted(processName: String, sid: Sid, ok: Boolean, duration: Duration) {
            cleanups += Cleanup(ok, duration)
        }
    }

    private inner class Blocking(context: CoroutineContext) : SuspendingProcess(context) {
        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? {
            if (query === Block) {
                blockedEntered.countDown()
                // Blocking and deaf to coroutine cancellation, like Thread.sleep.
                release.await(30, TimeUnit.SECONDS)
            }
            return "pong"
        }
    }

    private inner class Phases(private val context: CoroutineContext) : ProcessInitializer, ProcessLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(Blocking(context))
    }

    private fun Engine.blockOneCompute() {
        assertThatThrownBy {
            queryProcess("S", Block, Duration.ofMillis(200)).toCompletableFuture().get(10, TimeUnit.SECONDS)
        }.isNotNull()
        assertThat(blockedEntered.await(5, TimeUnit.SECONDS)).isTrue()
    }

    private fun assertFastAndOk(rec: Recorder, count: Int) {
        assertThat(rec.cleanups).hasSize(count)
        rec.cleanups.forEach {
            assertThat(it.ok).`as`("cleanup ok").isTrue()
            assertThat(it.duration).`as`("cleanup not held by the abandoned compute").isLessThan(Duration.ofSeconds(2))
        }
    }

    private fun millisOf(block: () -> Unit): Long {
        val t0 = System.nanoTime()
        block()
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)
    }

    @Test
    fun `pause and close are not held by an abandoned blocked compute`() {
        val rec = Recorder()
        val phases = Phases(Dispatchers.IO)
        Engine(config(), InMemoryLogBackend(), JavaSerializableSerDe(), rec).use { engine ->
            engine.newGraph(graph { process("S", { phases }, { phases }) })
            engine.blockOneCompute()

            assertThat(millisOf { engine.pause(listOf("S")) }).isLessThan(2000)
            assertFastAndOk(rec, 1)

            engine.resume(listOf("S"))
            assertThat(engine.queryProcess("S", Ping).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("pong")
        }
        assertFastAndOk(rec, 2)
    }

    @Test
    fun `a re-init is not held by an abandoned blocked compute`() {
        val rec = Recorder()
        val phases = Phases(Dispatchers.IO)
        Engine(config(), InMemoryLogBackend(), JavaSerializableSerDe(), rec).use { engine ->
            engine.newGraph(graph { process("S", { phases }, { phases }) })
            engine.blockOneCompute()

            assertThat(engine.trigger("S", "v2")).isTrue()
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
            while (rec.cleanups.isEmpty() && System.nanoTime() < deadline) Thread.sleep(10)
            assertFastAndOk(rec, 1)
            assertThat(engine.queryProcess("S", Ping).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("pong")

            val closeMs = millisOf { engine.close() }
            assertThat(closeMs).isLessThan(2000)
        }
        assertFastAndOk(rec, 2)
    }

    @Test
    fun `close after the app cancelled its scope reports cleanup as ok`() {
        val rec = Recorder()
        val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val phases = Phases(appScope.coroutineContext)
        Engine(config(), InMemoryLogBackend(), JavaSerializableSerDe(), rec).use { engine ->
            engine.newGraph(graph { process("S", { phases }, { phases }) })
            assertThat(engine.queryProcess("S", Ping).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("pong")
            appScope.cancel()
        }
        assertFastAndOk(rec, 1)
    }

    @Test
    fun `a genuinely failing cleanup is still reported after the app cancelled its scope`() {
        val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val p = object : SuspendingProcess(appScope.coroutineContext) {
            override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? = "pong"
            override suspend fun cleanUpAsync(ctx: ProcessContext) = throw IllegalStateException("cleanup boom")
        }
        appScope.cancel()
        assertThatThrownBy {
            p.cleanUp(SuspendingCleanupTimeoutTest.NoopProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)
        }.cause().isInstanceOf(IllegalStateException::class.java).hasMessage("cleanup boom")
    }
}
