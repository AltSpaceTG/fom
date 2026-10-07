package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.SnapshotPolicy
import io.fom.api.Process
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * A caller cancelled while `newGraphAwait` / `updateGraphAwait` blocks: the install runs to its
 * end, and the caller always resumes with a `CancellationException` — even when the install then
 * fails with an ordinary exception, which is attached as suppressed rather than failing the job.
 */
class GraphUpdatesCancellationTest {

    private fun config(initTimeout: Duration) = EngineConfig(
        initTimeout, Duration.ofSeconds(5),
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(100),
        Duration.ofMillis(10), Duration.ofMillis(100), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    data class Ask(val text: String)

    /** Init signals [started], then waits for [release] and fails or succeeds. */
    class GatedInit(
        private val started: CountDownLatch,
        private val release: CompletableFuture<Boolean>,
    ) : ProcessInitializer, ProcessLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> {
            started.countDown()
            return release.thenApply { ok ->
                check(ok) { "init failed on purpose" }
                emptyMap()
            }
        }

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(Process { _, q -> CompletableFuture.completedFuture("ok:${(q as Ask).text}") })
    }

    /** Never finishes init: the install fails with the engine's init-budget timeout. */
    class HangingInit(private val started: CountDownLatch) : ProcessInitializer, ProcessLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> {
            started.countDown()
            return CompletableFuture()
        }

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(Process { _, _ -> CompletableFuture.completedFuture("never") })
    }

    private class Outcome {
        val thrown = AtomicReference<Throwable?>()
        val handlerSaw = AtomicReference<Throwable?>()
        val handler = CoroutineExceptionHandler { _, e -> handlerSaw.set(e) }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `cancelled newGraphAwait whose install then fails resumes with CancellationException`(): Unit = runBlocking {
        Engine(config(Duration.ofMillis(500)), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            val started = CountDownLatch(1)
            val out = Outcome()
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + out.handler)
            val job = scope.launch {
                try {
                    engine.newGraphAwait(graph { process("Slow", { HangingInit(started) }, { HangingInit(started) }) })
                } catch (e: Throwable) {
                    out.thrown.set(e)
                    throw e
                }
            }
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue()
            job.cancel()
            job.join()

            val thrown = out.thrown.get()
            assertThat(thrown).isInstanceOf(CancellationException::class.java)
            assertThat(thrown!!.suppressed).anySatisfy { s ->
                assertThat(s).isNotInstanceOf(CancellationException::class.java)
                assertThat(s).hasMessageContaining("Slow")
            }
            assertThat(job.isCancelled).isTrue()
            assertThat(out.handlerSaw.get()).isNull() // ended as cancelled, not failed
            scope.cancel()
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `cancelled updateGraphAwait whose swap then fails resumes with CancellationException`(): Unit = runBlocking {
        Engine(config(Duration.ofSeconds(1)), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            val baseStarted = CountDownLatch(1)
            val baseRelease = CompletableFuture.completedFuture(true)
            engine.newGraphAwait(graph { process("Base", { GatedInit(baseStarted, baseRelease) }, { GatedInit(baseStarted, baseRelease) }).handles<Ask>() })

            val started = CountDownLatch(1)
            val release = CompletableFuture<Boolean>()
            val out = Outcome()
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + out.handler)
            val job = scope.launch {
                try {
                    engine.extendGraphAwait {
                        process("Added", { GatedInit(started, release) }, { GatedInit(started, release) }, dependsOn = listOf("Base"))
                    }
                } catch (e: Throwable) {
                    out.thrown.set(e)
                    throw e
                }
            }
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue()
            job.cancel()
            assertThat(job.isCompleted).isFalse() // still waiting for the swap to end
            release.complete(false) // every init attempt now fails until the budget runs out
            job.join()

            val thrown = out.thrown.get()
            assertThat(thrown).isInstanceOf(CancellationException::class.java)
            assertThat(thrown!!.suppressed).anySatisfy { s ->
                assertThat(s).isNotInstanceOf(CancellationException::class.java)
            }
            assertThat(out.handlerSaw.get()).isNull()
            assertThat(engine.queryAwait(Ask("still"))).isEqualTo("ok:still") // the old node is unaffected
            scope.cancel()
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `cancelled newGraphAwait whose install succeeds resumes with CancellationException and the graph is installed`(): Unit = runBlocking {
        Engine(config(Duration.ofSeconds(5)), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            val started = CountDownLatch(1)
            val release = CompletableFuture<Boolean>()
            val thrown = AtomicReference<Throwable?>()
            val job = launch(Dispatchers.Default) {
                try {
                    engine.newGraphAwait(graph { process("Gated", { GatedInit(started, release) }, { GatedInit(started, release) }).handles<Ask>() })
                } catch (e: Throwable) {
                    thrown.set(e)
                    throw e
                }
            }
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue()
            job.cancel()
            release.complete(true)
            job.join()

            assertThat(thrown.get()).isInstanceOf(CancellationException::class.java)
            assertThat(thrown.get()!!.suppressed).isEmpty()
            // The install ran to its end: the outcome is visible through the engine.
            assertThat(engine.currentGraph().nodes().keys).containsExactly("Gated")
            assertThat(engine.introspect().await().graph().nodes().first { it.name() == "Gated" }.state()).isEqualTo("Serving")
            assertThat(engine.queryAwait(Ask("a"))).isEqualTo("ok:a")
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `an already cancelled caller does not start the install`(): Unit = runBlocking {
        Engine(config(Duration.ofSeconds(5)), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            val ran = AtomicBoolean(false)
            val job = launch {
                coroutineContext.job.cancel()
                engine.updateGraphAwait { ran.set(true); it }
            }
            job.join()
            assertThat(job.isCancelled).isTrue()
            assertThat(ran.get()).isFalse()
        }
    }
}
