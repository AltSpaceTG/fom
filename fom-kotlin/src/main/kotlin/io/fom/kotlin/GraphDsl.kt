package io.fom.kotlin

import io.fom.Dependency
import io.fom.Graph
import io.fom.GraphBuilder
import io.fom.ProcessRef
import io.fom.ReinitStrategy
import io.fom.api.ParamProcessInitializer
import io.fom.api.ParamProcessLoader
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import java.io.Serializable
import java.util.function.Function
import java.util.function.Supplier

/** Keeps nested DSL blocks from reaching an outer receiver by accident. */
@DslMarker
annotation class GraphDslMarker

/**
 * Idiomatic Kotlin DSL over [GraphBuilder].
 *
 * ```kotlin
 * val graph = graph {
 *     process("Stations", ::StationsInit, ::StationsInit)
 *         .handles<GetStations>()
 *
 *     process("Forecasts", ::ForecastsInit, ::ForecastsInit, dependsOn = listOf("Stations"))
 *         .handles<GetForecastModel>()
 * }
 * ```
 *
 * Factories and route resolvers are ordinary Kotlin functions: they are never
 * written to the log, so they may capture anything.
 */
fun graph(block: GraphScope.() -> Unit): Graph {
    val builder = GraphBuilder()
    GraphScope(builder).block()
    return builder.build()
}

@GraphDslMarker
class GraphScope @PublishedApi internal constructor(
    @PublishedApi internal val builder: GraphBuilder,
) {

    /**
     * Adds a process. [dependsOn] are reactive dependencies (a new version
     * re-initialises this process); [stableDependsOn] are only queried.
     *
     * Each dependency is a [String] name, a [ProcessRef] or a [Dependency]; they
     * mix freely in one list:
     *
     * ```kotlin
     * process("Caller", ::CallerInit, ::CallerInit,
     *         dependsOn = listOf(Stations.REF, "Alerts"),
     *         stableDependsOn = listOf(ProcessRef.of("Audit")))
     * ```
     *
     * A [Dependency] keeps its own kind, so `Dependency.stable(ref)` stays stable
     * even under [dependsOn]. Anything else throws [IllegalArgumentException].
     *
     * [reinitStrategy] overrides `EngineConfig.reinitStrategy` for this process; `null`
     * keeps the engine's. Changing only the strategy does not restart the process on a
     * graph swap.
     *
     * ```kotlin
     * process("Forecasts", ::ForecastsInit, ::ForecastsInit,
     *         reinitStrategy = ReinitStrategy.RELEASE_FIRST) // too big to hold two versions
     * ```
     */
    fun process(
        name: String,
        initFactory: () -> ProcessInitializer,
        loadFactory: () -> ProcessLoader,
        dependsOn: List<Any> = emptyList(),
        stableDependsOn: List<Any> = emptyList(),
        reinitStrategy: ReinitStrategy? = null,
    ): NodeHandle {
        builder.addDeps(name, Supplier(initFactory), Supplier(loadFactory), *dependencies(dependsOn, stableDependsOn))
        reinitStrategy?.let { builder.reinitStrategy(it) }
        return NodeHandle(builder, name)
    }

    /** [process] with a `param`; dependencies and [reinitStrategy] work the same way. */
    fun <P : Serializable> processWithParam(
        name: String,
        initFactory: () -> ParamProcessInitializer<P>,
        loadFactory: () -> ParamProcessLoader<P>,
        param: P,
        dependsOn: List<Any> = emptyList(),
        stableDependsOn: List<Any> = emptyList(),
        reinitStrategy: ReinitStrategy? = null,
    ): NodeHandle {
        builder.addWithParamDeps(
            name, Supplier(initFactory), Supplier(loadFactory), param, *dependencies(dependsOn, stableDependsOn),
        )
        reinitStrategy?.let { builder.reinitStrategy(it) }
        return NodeHandle(builder, name)
    }

    /** [process] keyed by a typed [ProcessRef]; dependencies accept the same kinds. */
    fun process(
        ref: ProcessRef,
        initFactory: () -> ProcessInitializer,
        loadFactory: () -> ProcessLoader,
        dependsOn: List<Any> = emptyList(),
        stableDependsOn: List<Any> = emptyList(),
        reinitStrategy: ReinitStrategy? = null,
    ): NodeHandle = process(ref.name(), initFactory, loadFactory, dependsOn, stableDependsOn, reinitStrategy)

    /** [processWithParam] keyed by a typed [ProcessRef]; dependencies accept the same kinds. */
    fun <P : Serializable> processWithParam(
        ref: ProcessRef,
        initFactory: () -> ParamProcessInitializer<P>,
        loadFactory: () -> ParamProcessLoader<P>,
        param: P,
        dependsOn: List<Any> = emptyList(),
        stableDependsOn: List<Any> = emptyList(),
        reinitStrategy: ReinitStrategy? = null,
    ): NodeHandle =
        processWithParam(ref.name(), initFactory, loadFactory, param, dependsOn, stableDependsOn, reinitStrategy)

    private fun dependencies(reactive: List<Any>, stable: List<Any>): Array<Dependency> =
        (reactive.map { dependency(it, reactive = true) } + stable.map { dependency(it, reactive = false) })
            .toTypedArray()

    // The lists take Any: typed overloads for every String/ProcessRef mix would be
    // ambiguous once every dependency parameter has a default.
    private fun dependency(item: Any, reactive: Boolean): Dependency = when (item) {
        is Dependency -> item
        is ProcessRef -> if (reactive) Dependency.reactive(item) else Dependency.stable(item)
        is String -> if (reactive) Dependency.reactive(item) else Dependency.stable(item)
        else -> throw IllegalArgumentException(
            "a dependency must be a String, a ProcessRef or a Dependency, but was " +
                "${item::class.java.name}: $item",
        )
    }

    /** Routes queries of type [Q] to the process [resolver] names. */
    inline fun <reified Q : Any> route(noinline resolver: (Q) -> String) {
        builder.route(Q::class.java, Function<Q, String> { resolver(it) })
    }
}

@GraphDslMarker
class NodeHandle internal constructor(
    private val builder: GraphBuilder,
    private val name: String,
) {

    inline fun <reified Q : Any> handles(): NodeHandle = handles(Q::class.java)

    /** Routes the given query classes to this node. */
    fun handles(vararg classes: Class<*>): NodeHandle {
        builder.handlesFor(name, *classes)
        return this
    }
}
