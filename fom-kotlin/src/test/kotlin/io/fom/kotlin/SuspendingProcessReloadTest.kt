package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.SnapshotPolicy
import io.fom.api.Process
import io.fom.api.ProcessContext
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.delay
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * A loader is free to hand the *same* [SuspendingProcess] back on every load (a
 * singleton service, say). The generation that re-init retires must not take the
 * new one's coroutines with it.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SuspendingProcessReloadTest {

    private fun cfg() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(1),
        Duration.ofSeconds(5), Duration.ofMillis(10),
        Duration.ofMillis(10), Duration.ofMillis(50), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    /** One shared instance, reused across every load of the process. */
    class Singleton : SuspendingProcess() {
        val cleanUps = AtomicInteger()

        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any {
            delay(5)
            return "ok:$query"
        }

        override suspend fun cleanUpAsync(ctx: ProcessContext) {
            cleanUps.incrementAndGet()
        }
    }

    class Phases(private val singleton: Singleton) : SuspendingInitializer, SuspendingLoader {
        val inits = AtomicInteger()

        override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
            inits.incrementAndGet()
            return mapOf("v" to "x".toByteArray())
        }

        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process =
            singleton
    }

    @Test
    fun `a reused SuspendingProcess keeps serving across trigger-driven re-inits`() {
        val singleton = Singleton()
        val phases = Phases(singleton)
        Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            engine.newGraph(graph { process("S", { phases }, { phases }) })

            assertThat(engine.queryProcess("S", "q0").toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isEqualTo("ok:q0")

            repeat(3) { round ->
                val initsBefore = phases.inits.get()
                assertThat(engine.trigger("S", "round-$round")).isTrue()
                awaitUntil { phases.inits.get() > initsBefore && singleton.cleanUps.get() > round }

                // The same instance is serving a new generation: queries must still work.
                assertThat(engine.queryProcess("S", "q$round").toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo("ok:q$round")
                val node = engine.introspect().toCompletableFuture().get(5, TimeUnit.SECONDS)
                    .graph().nodes().single { it.name() == "S" }
                assertThat(node.state()).isEqualTo("Serving")
                assertThat(node.lastException()).isNull()
            }

            // Each retired generation was cleaned up exactly once.
            assertThat(singleton.cleanUps.get()).isEqualTo(3)
        }
    }

    private fun awaitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (!condition() && System.nanoTime() < deadline) Thread.sleep(10)
        assertThat(condition()).isTrue()
    }
}
