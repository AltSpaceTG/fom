package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.ProcessRef
import io.fom.SnapshotPolicy
import io.fom.api.Process
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit

class GraphUpdatesTest {

    private fun fastConfig() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(100),
        Duration.ofMillis(10), Duration.ofMillis(100), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    data class Ask(val text: String)
    data class AskViaBase(val text: String)

    /** Answers `"base:<text>"`. */
    class BaseInit : ProcessInitializer, ProcessLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(Process { _, q -> CompletableFuture.completedFuture("base:${(q as Ask).text}") })
    }

    /** Asks its dependency `Base` and wraps the answer. */
    class ReportsInit : ProcessInitializer, ProcessLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(object : SuspendingProcess() {
                override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? =
                    "report(" + ctx.queryAwait("Base", Ask((query as AskViaBase).text)) + ")"
            })
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `updateGraphAwait adds a node depending on an existing one and returns once it serves`(): Unit = runBlocking {
        Engine(fastConfig(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            engine.newGraphAwait(graph { process("Base", ::BaseInit, ::BaseInit).handles<Ask>() })

            val changed = engine.updateGraphAwait { current ->
                current.extend {
                    process("Reports", ::ReportsInit, ::ReportsInit, dependsOn = listOf("Base"))
                        .handles<AskViaBase>()
                }
            }

            assertThat(changed).isTrue()
            assertThat(engine.currentGraph().nodes().keys).containsExactly("Base", "Reports")
            assertThat(engine.introspect().await().graph().nodes().first { it.name() == "Reports" }.state()).isEqualTo("Serving")
            assertThat(engine.queryAwait(AskViaBase("x"))).isEqualTo("report(base:x)")
            assertThat(engine.queryAwait(Ask("y"))).isEqualTo("base:y") // existing route kept
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `extendGraphAwait accepts stable ref dependencies and an unchanged update returns false`(): Unit = runBlocking {
        Engine(fastConfig(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
            engine.newGraphAwait(graph { process("Base", ::BaseInit, ::BaseInit).handles<Ask>() })

            assertThat(engine.extendGraphAwait {
                process("Reports", ::ReportsInit, ::ReportsInit, stableDependsOn = listOf(ProcessRef.of("Base")))
                    .handles<AskViaBase>()
            }).isTrue()
            assertThat(engine.queryAwait(AskViaBase("z"))).isEqualTo("report(base:z)")

            assertThat(engine.updateGraphAwait { it }).isFalse()
        }
    }

    @Test
    fun `extend keeps existing nodes and routes and rejects clashes`() {
        val base = graph {
            process("Base", ::BaseInit, ::BaseInit).handles<Ask>()
        }
        val bigger = base.extend {
            process("Reports", ::ReportsInit, ::ReportsInit, dependsOn = listOf("Base")).handles<AskViaBase>()
        }
        assertThat(bigger.nodes()["Base"]).isSameAs(base.nodes()["Base"])
        assertThat(bigger.top().name()).isEqualTo("Reports")
        assertThat(bigger.typeRouting().keys).containsExactlyInAnyOrder(Ask::class.java, AskViaBase::class.java)
        assertThat(base.extend { }.top()).isSameAs(base.top())

        assertThatThrownBy { base.extend { process("Base", ::BaseInit, ::BaseInit) } }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("Duplicate")
        assertThatThrownBy { base.extend { process("Other", ::BaseInit, ::BaseInit).handles<Ask>() } }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("already routed")
        assertThatThrownBy { base.extend { process("Other", ::BaseInit, ::BaseInit, dependsOn = listOf("Nope")) } }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("Nope")
    }
}
