package io.fom.kotlin

import io.fom.ProcessRef
import io.fom.ReinitStrategy
import io.fom.api.ParamProcessInitializer
import io.fom.api.ParamProcessLoader
import io.fom.api.Process
import io.fom.api.QueryableContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

class GraphDslReinitStrategyTest {

    @Test
    fun `reinitStrategy is set per node and null follows the engine`() {
        val g = graph {
            process("Default", GraphDslTest::EchoInit, GraphDslTest::EchoInit)
            process("Big", GraphDslTest::EchoInit, GraphDslTest::EchoInit,
                dependsOn = listOf("Default"), reinitStrategy = ReinitStrategy.RELEASE_FIRST)
                .handles<GraphDslTest.EchoQuery>()
            process(ProcessRef.of("Pinned"), GraphDslTest::EchoInit, GraphDslTest::EchoInit,
                reinitStrategy = ReinitStrategy.KEEP_OLD)
            processWithParam("WithParam", ::ParamInit, ::ParamInit, "p",
                reinitStrategy = ReinitStrategy.RELEASE_FIRST)
            processWithParam(ProcessRef.of("RefWithParam"), ::ParamInit, ::ParamInit, "q",
                reinitStrategy = ReinitStrategy.RELEASE_FIRST)
            process("After", GraphDslTest::EchoInit, GraphDslTest::EchoInit)
        }

        val strategies = g.topologicalOrder().associate { it.name() to it.reinitStrategy() }
        assertThat(strategies).containsExactlyInAnyOrderEntriesOf(
            mapOf(
                "Default" to null,
                "Big" to ReinitStrategy.RELEASE_FIRST,
                "Pinned" to ReinitStrategy.KEEP_OLD,
                "WithParam" to ReinitStrategy.RELEASE_FIRST,
                "RefWithParam" to ReinitStrategy.RELEASE_FIRST,
                "After" to null,
            ),
        )
    }

    class ParamInit : ParamProcessInitializer<String>, ParamProcessLoader<String> {
        override fun init(ctx: QueryableContext, param: String): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>, param: String): CompletionStage<Process> =
            CompletableFuture.completedFuture(Process { _, _ -> CompletableFuture.completedFuture<Any?>(param) })
    }
}
