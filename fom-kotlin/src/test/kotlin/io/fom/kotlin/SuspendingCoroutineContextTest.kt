package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.SnapshotPolicy
import io.fom.api.EngineObserver
import io.fom.api.Process
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext

/**
 * Pins down two behaviours documented in the Kotlin DSL guide: how a compute's
 * *own* `withTimeout` reaches callers, and which coroutine context elements
 * cross into a [SuspendingProcess] and its suspend init/load.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SuspendingCoroutineContextTest {

    private fun cfg() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(30), Duration.ofMillis(10),
        Duration.ofMillis(10), Duration.ofMillis(50), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    class Recording : EngineObserver {
        val failureReasons = ConcurrentLinkedQueue<String>()
        override fun onQueryFailed(processName: String, queryId: UUID, reason: String, cause: Throwable?) {
            failureReasons += reason
        }
    }

    /** `"raw"` times out with `withTimeout`; `"orNull"` with `withTimeoutOrNull`; `"wrapped"` rethrows as a plain exception. */
    class Slow : SuspendingProcess() {
        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? = when (query) {
            "raw" -> withTimeout(20) { awaitCancellation() }
            "orNull" -> withTimeoutOrNull(20) { awaitCancellation() } ?: "fallback"
            "wrapped" -> try {
                withTimeout(20) { awaitCancellation() }
            } catch (e: TimeoutCancellationException) {
                throw IllegalStateException("upstream timed out", e)
            }
            else -> query
        }
    }

    private fun phasesOf(p: Process) = object : SuspendingInitializer, SuspendingLoader {
        override suspend fun initAsync(ctx: QueryableContext) = mapOf("v" to byteArrayOf(1))
        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>) = p
    }

    @Test
    fun `a compute's own withTimeout reaches the caller as a CancellationException and is counted as cancelled`() {
        val observer = Recording()
        Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe(), observer).use { engine ->
            val phases = phasesOf(Slow())
            engine.newGraph(graph { process("Slow", { phases }, { phases }) })

            val seen = AtomicReference<Throwable?>()
            val after = AtomicReference<String?>()
            runBlocking {
                // Inside launch the CancellationException ends the coroutine silently:
                // the line after queryProcessAwait never runs and nothing fails.
                val job = launch {
                    try {
                        engine.queryProcessAwait("Slow", "raw")
                        after.set("reached")
                    } catch (e: Throwable) {
                        seen.set(e)
                        throw e
                    }
                }
                job.join()
                assertThat(job.isCancelled).`as`("launch ended as cancelled, not failed").isTrue()
            }
            assertThat(seen.get()).isInstanceOf(CancellationException::class.java)
            assertThat(after.get()).isNull()
            // Query outcomes reach observers on engine threads, so only eventually.
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted { assertThat(observer.failureReasons).contains("cancelled") }

            // withTimeoutOrNull: a normal value.
            assertThat(runBlocking { engine.queryProcessAwait("Slow", "orNull") }).isEqualTo("fallback")
            // Wrapped into a non-cancellation exception: an ordinary failure.
            val wrapped = runCatching { runBlocking { engine.queryProcessAwait("Slow", "wrapped") } }.exceptionOrNull()
            assertThat(wrapped).isInstanceOf(IllegalStateException::class.java).isNotInstanceOf(CancellationException::class.java)
            // Query outcomes reach observers on engine threads, so only eventually.
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted { assertThat(observer.failureReasons).contains("exception") }
        }
    }

    private val tl = ThreadLocal<String?>()

    inner class Tagged(ctx: CoroutineContext) : SuspendingProcess(ctx) {
        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? = tl.get() ?: "none"
    }

    @Test
    fun `context elements given to the process are visible in compute, init and load, the caller's are not`() {
        val seenInInit = AtomicReference<String?>()
        val seenInLoad = AtomicReference<String?>()
        val process = Tagged(Dispatchers.Default + tl.asContextElement("process"))
        val phases = object : SuspendingInitializer, SuspendingLoader {
            override val initContext: CoroutineContext = Dispatchers.Default + tl.asContextElement("init")
            override val loadContext: CoroutineContext = Dispatchers.Default + tl.asContextElement("load")
            override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
                seenInInit.set(tl.get()); return mapOf("v" to byteArrayOf(1))
            }
            override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process {
                seenInLoad.set(tl.get()); return process
            }
        }
        val bare = Tagged(Dispatchers.Default)
        val barePhases = phasesOf(bare)
        Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            engine.newGraph(graph {
                process("Tagged", { phases }, { phases })
                process("Bare", { barePhases }, { barePhases })
            })
            runBlocking(tl.asContextElement("caller")) {
                assertThat(engine.queryProcessAwait("Tagged", "q")).isEqualTo("process")
                assertThat(engine.queryProcessAwait("Bare", "q")).isEqualTo("none")
            }
            assertThat(seenInInit.get()).isEqualTo("init")
            assertThat(seenInLoad.get()).isEqualTo("load")
        }
    }
}
