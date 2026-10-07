package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.SnapshotPolicy
import io.fom.api.Process
import io.fom.api.ProcessContext
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.CompletableDeferred
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * One [SuspendingProcess] instance shared by *two* nodes (a DI-held or cached
 * service handed back by both loaders). Generations are bookkept per process, so
 * retiring N1's generation must not cancel a compute that is in flight in N2.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SuspendingProcessSharedInstanceTest {

    private fun cfg() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(30), Duration.ofMillis(10),
        Duration.ofMillis(10), Duration.ofMillis(50), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    /** One instance, two nodes. `"hold"` suspends until [gate] is opened. */
    class Shared : SuspendingProcess() {
        val gate = CompletableDeferred<String>()
        val holdEntered = CountDownLatch(1)
        val cleanUps = AtomicInteger()

        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any {
            if (query == "hold") {
                holdEntered.countDown()
                return "${ctx.sid().processName()}:${gate.await()}"
            }
            return "${ctx.sid().processName()}:$query"
        }

        override suspend fun cleanUpAsync(ctx: ProcessContext) {
            cleanUps.incrementAndGet()
        }
    }

    class Phases(private val shared: Shared) : SuspendingInitializer, SuspendingLoader {
        val inits = AtomicInteger()

        override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
            inits.incrementAndGet()
            return mapOf("v" to "x".toByteArray())
        }

        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process =
            shared
    }

    @Test
    fun `re-initialising one node does not cancel a compute in flight in another node sharing the instance`() {
        val shared = Shared()
        val one = Phases(shared)
        val two = Phases(shared)
        Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            engine.newGraph(
                graph {
                    process("N1", { one }, { one })
                    process("N2", { two }, { two })
                },
            )

            // Both nodes serve out of the same instance.
            assertThat(engine.queryProcess("N1", "a").toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo("N1:a")
            val held = engine.queryProcess("N2", "hold").toCompletableFuture()
            assertThat(shared.holdEntered.await(10, TimeUnit.SECONDS)).`as`("N2's compute started").isTrue()

            // Re-init N1: its generation is retired and cleaned up.
            val initsBefore = one.inits.get()
            assertThat(engine.trigger("N1", "v2")).isTrue()
            awaitUntil { one.inits.get() > initsBefore && shared.cleanUps.get() >= 1 }
            assertThat(engine.queryProcess("N1", "b").toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo("N1:b")

            // N2 was never touched: its suspended compute completes normally.
            assertThat(held.isDone).`as`("N2's compute was not cancelled by N1's cleanup").isFalse()
            shared.gate.complete("done")
            assertThat(held.get(10, TimeUnit.SECONDS)).isEqualTo("N2:done")

            // ... and N2 keeps serving afterwards.
            assertThat(engine.queryProcess("N2", "c").toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo("N2:c")
        }
    }

    /**
     * Two engines in one JVM, both running a node called `"N"` over one shared
     * instance. The process name alone cannot tell their generations apart; the
     * owning state machine (its executor) does.
     */
    @Test
    fun `re-initialising a node in one engine does not cancel a compute in flight in another engine`() {
        val shared = Shared()
        val one = Phases(shared)
        val two = Phases(shared)
        Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine1 ->
            Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine2 ->
                engine1.newGraph(graph { process("N", { one }, { one }) })
                engine2.newGraph(graph { process("N", { two }, { two }) })

                assertThat(engine1.queryProcess("N", "a").toCompletableFuture().get(10, TimeUnit.SECONDS))
                    .isEqualTo("N:a")
                val held = engine2.queryProcess("N", "hold").toCompletableFuture()
                assertThat(shared.holdEntered.await(10, TimeUnit.SECONDS)).`as`("engine 2's compute started").isTrue()

                val initsBefore = one.inits.get()
                assertThat(engine1.trigger("N", "v2")).isTrue()
                awaitUntil { one.inits.get() > initsBefore && shared.cleanUps.get() >= 1 }
                assertThat(engine1.queryProcess("N", "b").toCompletableFuture().get(10, TimeUnit.SECONDS))
                    .isEqualTo("N:b")

                assertThat(held.isDone).`as`("engine 2's compute was not cancelled by engine 1's re-init").isFalse()
                shared.gate.complete("done")
                assertThat(held.get(10, TimeUnit.SECONDS)).isEqualTo("N:done")
            }
        }
    }

    @Test
    fun `closing one engine does not cancel a compute in flight in another engine`() {
        val shared = Shared()
        val one = Phases(shared)
        val two = Phases(shared)
        Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine2 ->
            val engine1 = Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe())
            engine1.newGraph(graph { process("N", { one }, { one }) })
            engine2.newGraph(graph { process("N", { two }, { two }) })

            // Keyed by name alone, engine 2's compute would join engine 1's generation.
            assertThat(engine1.queryProcess("N", "a").toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo("N:a")
            val held = engine2.queryProcess("N", "hold").toCompletableFuture()
            assertThat(shared.holdEntered.await(10, TimeUnit.SECONDS)).`as`("engine 2's compute started").isTrue()

            engine1.close()
            assertThat(shared.cleanUps.get()).`as`("engine 1 cleaned its node up").isGreaterThanOrEqualTo(1)

            assertThat(held.isDone).`as`("engine 2's compute was not cancelled by engine 1's close()").isFalse()
            shared.gate.complete("done")
            assertThat(held.get(10, TimeUnit.SECONDS)).isEqualTo("N:done")
            assertThat(engine2.queryProcess("N", "c").toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo("N:c")
        }
    }

    private fun awaitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (!condition() && System.nanoTime() < deadline) Thread.sleep(10)
        assertThat(condition()).isTrue()
    }
}
