package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.ProcessRef
import io.fom.SnapshotPolicy
import io.fom.api.Process
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.Serializable
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SuspendingPhasesTest {

    private fun cfg(initTimeout: Duration = Duration.ofSeconds(5)) = EngineConfig(
        initTimeout, Duration.ofSeconds(5), Duration.ofSeconds(1),
        Duration.ofSeconds(5), Duration.ofMillis(10),
        Duration.ofMillis(10), Duration.ofMillis(50), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    data class Prefix(val value: String) : Serializable

    class Source : SuspendingInitializer, SuspendingLoader {
        override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
            delay(10)
            return mapOf("v" to "hello".toByteArray())
        }

        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process {
            val v = String(properties.getValue("v"))
            return Process { _, _ -> CompletableFuture.completedFuture(v) }
        }
    }

    class Derived : SuspendingParamInitializer<Prefix>, SuspendingParamLoader<Prefix> {
        override suspend fun initAsync(ctx: QueryableContext, param: Prefix): Map<String, ByteArray> {
            val seen = ctx.queryAwait(ProcessRef.of("Source"), "q") as String
            return mapOf("v" to "${param.value}:$seen".toByteArray())
        }

        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>, param: Prefix): Process {
            val v = String(properties.getValue("v"))
            return Process { _, _ -> CompletableFuture.completedFuture(v) }
        }
    }

    @Test
    fun `suspend init and load run and can query dependencies`() {
        Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            engine.newGraph(graph {
                process("Source", ::Source, ::Source)
                processWithParam("Derived", ::Derived, ::Derived, Prefix("p"), dependsOn = listOf("Source"))
            })
            assertThat(engine.queryProcess("Derived", "q").toCompletableFuture().get(3, TimeUnit.SECONDS))
                .isEqualTo("p:hello")
        }
    }

    class Hanging : SuspendingInitializer, SuspendingLoader {
        override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
            try {
                awaitCancellation()
            } finally {
                CANCELLED.set(true)
            }
        }

        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process =
            Process { _, _ -> CompletableFuture.completedFuture("never") }
    }

    @Test
    fun `an init that runs out of its budget has its coroutine cancelled`() {
        CANCELLED.set(false)
        Engine(cfg(initTimeout = Duration.ofMillis(300)), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            assertThatThrownBy { engine.newGraph(graph { process("H", ::Hanging, ::Hanging) }) }
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
            while (!CANCELLED.get() && System.nanoTime() < deadline) Thread.sleep(20)
            assertThat(CANCELLED.get()).isTrue()
        }
    }

    companion object {
        val CANCELLED = AtomicBoolean()
    }
}
