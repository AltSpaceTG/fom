package io.fom.kotlin

import io.fom.Dependency
import io.fom.Engine
import io.fom.EngineConfig
import io.fom.Graph
import io.fom.GraphBuilder
import io.fom.ProcessRef
import io.fom.SnapshotPolicy
import io.fom.api.ParamProcessInitializer
import io.fom.api.ParamProcessLoader
import io.fom.api.Process
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.Serializable
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

/**
 * The DSL takes a dependency as a [String], a [ProcessRef] or a ready-made
 * [Dependency], in any mix, whatever kind of name the node itself was declared
 * with. Every combination must produce exactly the graph the Java
 * [GraphBuilder] produces.
 */
class GraphDslDependencyKindsTest {

    private val inventory = ProcessRef.of("Inventory")
    private val caller = ProcessRef.of("Caller")
    private val prices = ProcessRef.of("Prices")
    private val audit = ProcessRef.of("Audit")

    // ───────────────── name kind × dependency kind ─────────────────

    @Test
    fun `string-named node takes ref dependencies`() {
        val g = graph {
            process("Inventory", ::LeafInit, ::LeafInit)
            process("Prices", ::LeafInit, ::LeafInit)
            process("Audit", ::LeafInit, ::LeafInit)
            process(
                "Caller", ::LeafInit, ::LeafInit,
                dependsOn = listOf(inventory, prices),
                stableDependsOn = listOf(audit),
            )
        }
        assertThat(deps(g, "Caller")).containsExactly(
            Dependency.reactive("Inventory"), Dependency.reactive("Prices"), Dependency.stable("Audit"),
        )
    }

    @Test
    fun `ref-named node takes string dependencies`() {
        val g = graph {
            process("Inventory", ::LeafInit, ::LeafInit)
            process("Audit", ::LeafInit, ::LeafInit)
            process(
                ProcessRef.of("Caller"), ::LeafInit, ::LeafInit,
                dependsOn = listOf("Inventory"),
                stableDependsOn = listOf("Audit"),
            )
        }
        assertThat(deps(g, "Caller")).containsExactly(
            Dependency.reactive("Inventory"), Dependency.stable("Audit"),
        )
    }

    @Test
    fun `one list mixes strings, refs and ready-made dependencies`() {
        val g = graph {
            process("Inventory", ::LeafInit, ::LeafInit)
            process("Prices", ::LeafInit, ::LeafInit)
            process("Audit", ::LeafInit, ::LeafInit)
            process(
                "Caller", ::LeafInit, ::LeafInit,
                dependsOn = listOf("Inventory", prices, Dependency.stable(audit)),
            )
        }
        // A Dependency element keeps its own kind, even inside `dependsOn`.
        assertThat(deps(g, "Caller")).containsExactly(
            Dependency.reactive("Inventory"), Dependency.reactive("Prices"), Dependency.stable("Audit"),
        )
    }

    @Test
    fun `stable list also accepts every kind`() {
        val g = graph {
            process("Inventory", ::LeafInit, ::LeafInit)
            process("Prices", ::LeafInit, ::LeafInit)
            process("Audit", ::LeafInit, ::LeafInit)
            process(
                caller, ::LeafInit, ::LeafInit,
                stableDependsOn = listOf("Inventory", prices, Dependency.reactive(audit)),
            )
        }
        assertThat(deps(g, "Caller")).containsExactly(
            Dependency.stable("Inventory"), Dependency.stable("Prices"), Dependency.reactive("Audit"),
        )
    }

    @Test
    fun `the all-string and all-ref forms still work unchanged`() {
        val byName = graph {
            process("Inventory", ::LeafInit, ::LeafInit)
            process("Caller", ::LeafInit, ::LeafInit, dependsOn = listOf("Inventory"))
        }
        val byRef = graph {
            process(inventory, ::LeafInit, ::LeafInit)
            process(caller, ::LeafInit, ::LeafInit, dependsOn = listOf(inventory))
        }
        assertThat(deps(byName, "Caller")).isEqualTo(deps(byRef, "Caller"))
        // …and so does the no-dependency form, i.e. the defaults are not ambiguous.
        val leafOnly = graph { process("Solo", ::LeafInit, ::LeafInit) }
        assertThat(deps(leafOnly, "Solo")).isEmpty()
    }

    // ───────────────── processWithParam ─────────────────

    @Test
    fun `processWithParam mixes name kinds too`() {
        val g = graph {
            process("Inventory", ::LeafInit, ::LeafInit)
            process("Audit", ::LeafInit, ::LeafInit)
            processWithParam(
                "Sized", ::SizedInit, ::SizedInit, Size(7),
                dependsOn = listOf(inventory),
                stableDependsOn = listOf("Audit"),
            )
            processWithParam(
                ProcessRef.of("Sized2"), ::SizedInit, ::SizedInit, Size(9),
                dependsOn = listOf("Inventory"),
                stableDependsOn = listOf(audit),
            )
        }
        assertThat(deps(g, "Sized")).containsExactly(
            Dependency.reactive("Inventory"), Dependency.stable("Audit"),
        )
        assertThat(deps(g, "Sized2")).containsExactly(
            Dependency.reactive("Inventory"), Dependency.stable("Audit"),
        )
        assertThat(g.nodes()["Sized"]!!.param()).isEqualTo(Size(7))
        assertThat(g.nodes()["Sized2"]!!.param()).isEqualTo(Size(9))
    }

    // ───────────────── wrong element type ─────────────────

    /**
     * The element type is only checked at build time — see
     * `GraphDsl.dependency`. Wrong types that *can* be rejected at compile time
     * still are: the node name accepts only [String]/[ProcessRef], and the
     * factories only the matching functional types (verified by compiling the
     * negative cases, not expressible as a test).
     */
    @Test
    fun `a dependency of an unsupported type is rejected with a clear message`() {
        assertThatThrownBy {
            graph {
                process("Inventory", ::LeafInit, ::LeafInit)
                process("Caller", ::LeafInit, ::LeafInit, dependsOn = listOf(42))
            }
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("must be a String, a ProcessRef or a Dependency")
            .hasMessageContaining("java.lang.Integer")

        assertThatThrownBy {
            graph {
                process("Inventory", ::LeafInit, ::LeafInit)
                process("Caller", ::LeafInit, ::LeafInit, stableDependsOn = listOf(StringBuilder("Inventory")))
            }
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("java.lang.StringBuilder")
    }

    // ───────────────── equivalence with the Java builder ─────────────────

    @Test
    fun `a mixed-kind DSL graph equals the equivalent Java-built graph`() {
        val fromDsl = graph {
            process("Inventory", ::LeafInit, ::LeafInit)
            process("Prices", ::LeafInit, ::LeafInit)
            process("Audit", ::LeafInit, ::LeafInit)
            process(
                "Caller", ::LeafInit, ::LeafInit,
                dependsOn = listOf(inventory, "Prices"),
                stableDependsOn = listOf(audit),
            ).handles<Ask>()
            processWithParam("Sized", ::SizedInit, ::SizedInit, Size(7), dependsOn = listOf(inventory))
        }

        val b = GraphBuilder()
        b.add("Inventory", Supplier { LeafInit() }, Supplier { LeafInit() })
        b.add("Prices", Supplier { LeafInit() }, Supplier { LeafInit() })
        b.add("Audit", Supplier { LeafInit() }, Supplier { LeafInit() })
        b.addDeps(
            "Caller", Supplier { LeafInit() }, Supplier { LeafInit() },
            Dependency.reactive(inventory), Dependency.reactive("Prices"), Dependency.stable(audit),
        )
        b.handlesFor("Caller", Ask::class.java)
        b.addWithParamDeps(
            "Sized", Supplier { SizedInit() }, Supplier { SizedInit() }, Size(7), Dependency.reactive(inventory),
        )
        val fromJava = b.build()

        assertThat(topology(fromDsl)).isEqualTo(topology(fromJava))
        assertThat(fromDsl.typeRouting()).isEqualTo(fromJava.typeRouting())
        assertThat(fromDsl.top().name()).isEqualTo(fromJava.top().name())
        assertThat(fromDsl.topologicalOrder().map { it.name() })
            .isEqualTo(fromJava.topologicalOrder().map { it.name() })
        assertThat(fromDsl.nodes()["Sized"]!!.param()).isEqualTo(fromJava.nodes()["Sized"]!!.param())
    }

    // ───────────────── it really runs ─────────────────

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `an engine serves a graph whose dependencies were declared by ref`() {
        val g = graph {
            process(inventory, ::LeafInit, ::LeafInit)
            process("Caller", ::CallerInit, ::CallerInit, stableDependsOn = listOf(inventory))
                .handles<Ask>()
        }
        InMemoryLogBackend().use { backend ->
            Engine(fastConfig(), backend, JavaSerializableSerDe()).use { engine ->
                engine.newGraph(g)
                val answer = engine.query(Ask("x")).toCompletableFuture().get(10, TimeUnit.SECONDS)
                assertThat(answer).isEqualTo("caller:leaf")
            }
        }
    }

    // ───────────────── helpers ─────────────────

    private fun deps(g: Graph, node: String): List<Dependency> = g.nodes()[node]!!.dependencies()

    private fun topology(g: Graph): Map<String, List<Dependency>> =
        g.nodes().mapValues { (_, n) -> n.dependencies() }

    private fun fastConfig() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(100),
        Duration.ofMillis(10), Duration.ofMillis(100), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    data class Ask(val tag: String) : Serializable

    class LeafInit : ProcessInitializer, ProcessLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
            CompletableFuture.completedFuture(Process { _, _ -> CompletableFuture.completedFuture<Any?>("leaf") })
    }

    class CallerInit : ProcessInitializer, SuspendingLoader {
        override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process =
            object : SuspendingProcess() {
                override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? =
                    "caller:" + ctx.queryAwait(ProcessRef.of("Inventory"), query)
            }
    }

    data class Size(val n: Int) : Serializable

    class SizedInit : ParamProcessInitializer<Size>, ParamProcessLoader<Size> {
        override fun init(ctx: QueryableContext, param: Size): CompletionStage<Map<String, ByteArray>> =
            CompletableFuture.completedFuture(emptyMap())

        override fun load(
            ctx: QueryableContext,
            properties: Map<String, ByteArray>,
            param: Size,
        ): CompletionStage<Process> =
            CompletableFuture.completedFuture(Process { _, _ -> CompletableFuture.completedFuture<Any?>(param) })
    }
}
