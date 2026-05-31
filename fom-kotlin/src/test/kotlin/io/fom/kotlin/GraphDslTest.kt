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
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.Serializable
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit

class GraphDslTest {

    private fun fastConfig() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(100), Duration.ofMillis(100),
        Duration.ofMillis(10), Duration.ofMillis(100), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `graph dsl builds and engine queries through it`() = runTest {
        val g = graph {
            process("Echo", ::EchoInit, ::EchoInit)
                .handles<EchoQuery>()
        }

        InMemoryLogBackend().use { backend ->
            Engine(fastConfig(), backend, JavaSerializableSerDe(), true).use { engine ->
                engine.newGraph(g)
                val result: String = engine.queryAs(EchoQuery("hi"))
                assertThat(result).isEqualTo("echo:hi")
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `suspending process works through engine`() = runTest {
        val g = graph {
            process("Suspending", ::SuspendingInit, ::SuspendingInit)
                .handles<SuspendingQuery>()
        }

        InMemoryLogBackend().use { backend ->
            Engine(fastConfig(), backend, JavaSerializableSerDe(), true).use { engine ->
                engine.newGraph(g)
                val result: String = engine.queryAs(SuspendingQuery("alpha"))
                assertThat(result).isEqualTo("susp:alpha")
            }
        }
    }

    data class EchoQuery(val tag: String) : Serializable

    class EchoInit : ProcessInitializer, ProcessLoader {

        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(Process { _, q ->
                val tag = (q as EchoQuery).tag
                CompletableFuture.completedFuture<Any?>("echo:$tag")
            })
    }

    data class SuspendingQuery(val tag: String) : Serializable

    class SuspendingInit : ProcessInitializer, ProcessLoader {

        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(object : SuspendingProcess() {
                override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any =
                    "susp:" + (query as SuspendingQuery).tag
            })
    }
}
