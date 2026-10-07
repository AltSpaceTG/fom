package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.SnapshotPolicy
import io.fom.api.Process
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.log.FileLogBackend
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
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
        Duration.ofMillis(100),
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
            Engine(fastConfig(), backend, JavaSerializableSerDe()).use { engine ->
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
            Engine(fastConfig(), backend, JavaSerializableSerDe()).use { engine ->
                engine.newGraph(g)
                val result: String = engine.queryAs(SuspendingQuery("alpha"))
                assertThat(result).isEqualTo("susp:alpha")
            }
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `capturing factories work and a restart warm-loads`(@TempDir dir: Path) {
        val inits = AtomicInteger()
        val greeting = "hello"          // captured by both factories
        val g = graph {
            process("Greeter", { CountingInit(inits, greeting) }, { CountingInit(inits, greeting) })
        }
        val file = dir.resolve("fom.log")
        repeat(2) {
            FileLogBackend(file).use { backend ->
                Engine(fastConfig(), backend, JavaSerializableSerDe()).use { engine ->
                    engine.newGraph(g)
                    val result = engine.queryProcess("Greeter", "x").toCompletableFuture().get(5, TimeUnit.SECONDS)
                    assertThat(result).isEqualTo("hello:1")
                }
            }
        }
        assertThat(inits.get()).`as`("the second run warm-loads").isEqualTo(1)
    }

    class CountingInit(private val inits: AtomicInteger, private val greeting: String) : ProcessInitializer, ProcessLoader {

        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(mapOf("n" to byteArrayOf(inits.incrementAndGet().toByte())))

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> {
            val n = properties.getValue("n")[0]
            return CompletableFuture.completedFuture(Process { _, _ ->
                CompletableFuture.completedFuture<Any?>("$greeting:$n")
            })
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
