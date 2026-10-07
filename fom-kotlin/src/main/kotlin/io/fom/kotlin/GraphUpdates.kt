package io.fom.kotlin

import io.fom.Engine
import io.fom.Graph
import io.fom.GraphBuilder
import io.fom.ProcessNode
import io.fom.ProcessRef
import io.fom.QueryRoute
import io.fom.api.InitializationTimeoutException
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import java.util.function.Supplier

/**
 * [Engine.newGraph] as a suspend call; the blocking install runs on [Dispatchers.IO]. Like
 * `newGraph`, it returns once every started node is `Serving`, or throws.
 *
 * Cancellation: an already-cancelled caller does not start the install. Once started, the install
 * runs to its end regardless; a caller cancelled by then resumes with a `CancellationException`,
 * with any failure (say an [InitializationTimeoutException]) attached as suppressed. Check
 * [Engine.currentGraph] or [Engine.introspect] for the outcome.
 */
suspend fun Engine.newGraphAwait(graph: Graph): Boolean =
    awaitControlPlane { newGraph(graph) }

/**
 * [Engine.updateGraph] as a suspend call: [change] maps the installed graph to the one to install,
 * with no other control-plane call in between. The blocking swap runs on [Dispatchers.IO] and
 * returns once the added and changed nodes are `Serving`, or throws.
 *
 * [change] runs under the engine's control lock: keep it quick, and never call this engine's
 * control plane from it, directly or by waiting on another thread (`runBlocking`, `withContext`).
 * That deadlocks the engine for good, `close()` included.
 *
 * Cancellation works as in [newGraphAwait].
 *
 * @return `true` if the swap changed anything
 * @throws IllegalStateException if no graph is installed yet, or the engine is closed
 */
suspend fun Engine.updateGraphAwait(change: (Graph) -> Graph): Boolean =
    awaitControlPlane { updateGraph { change(it) } }

/**
 * Runs the blocking [call] on [Dispatchers.IO] to its end, even if the caller is cancelled
 * meanwhile. A bare `withContext(Dispatchers.IO)` would resume a cancelled caller with the call's
 * own failure, so a cancelled `launch` would end as failed; here it gets a `CancellationException`
 * with that failure suppressed.
 */
private suspend fun <T> awaitControlPlane(call: () -> T): T {
    currentCoroutineContext().ensureActive()
    var outcome: Result<T>? = null
    try {
        // NonCancellable: the block is never abandoned, so its result is always recorded.
        withContext(Dispatchers.IO + NonCancellable) { outcome = runCatching(call) }
    } catch (e: CancellationException) {
        // Cancelled on the way back; the recorded outcome is handled below.
        if (outcome == null) throw e
    }
    val result = outcome!!
    try {
        currentCoroutineContext().ensureActive()
    } catch (cancellation: CancellationException) {
        result.exceptionOrNull()?.let { if (it !== cancellation) cancellation.addSuppressed(it) }
        throw cancellation
    }
    return result.getOrThrow()
}

/**
 * Adds the nodes declared in [block] to the running graph ([updateGraphAwait] with [Graph.extend]):
 *
 * ```kotlin
 * engine.extendGraphAwait {
 *     process("Summary", ::SummaryInit, ::SummaryInit, dependsOn = listOf("Stations"))
 *         .handles<GetSummary>()
 * }
 * ```
 */
suspend fun Engine.extendGraphAwait(block: GraphScope.() -> Unit): Boolean =
    updateGraphAwait { it.extend(block) }

/**
 * This graph plus the nodes and routes declared in [block], with the same DSL as [graph]. New
 * nodes may depend on existing ones by name or [ProcessRef]. Existing nodes and routes are kept;
 * the last node added becomes [Graph.top], or the current top stays if [block] adds none.
 *
 * ```kotlin
 * val bigger = engine.currentGraph().extend {
 *     process("Summary", ::SummaryInit, ::SummaryInit, dependsOn = listOf("Stations"))
 * }
 * ```
 *
 * Use it inside [Engine.updateGraph] / [updateGraphAwait] rather than on a graph read
 * earlier: a copy read with `currentGraph()` and installed later can undo a concurrent removal.
 *
 * @throws IllegalArgumentException if a node name is already in this graph, a query type is
 *         already routed here, or a dependency does not exist
 */
fun Graph.extend(block: GraphScope.() -> Unit): Graph {
    val existing = nodes().keys.toSet()
    val builder = GraphBuilder()
    // Placeholders let the builder resolve dependencies on existing nodes and reject duplicate
    // names; the real nodes replace them below.
    for (name in existing) {
        builder.add(name, PLACEHOLDER_INIT, PLACEHOLDER_LOAD)
    }
    GraphScope(builder).block()
    val added = builder.build()

    val merged = LinkedHashMap<String, ProcessNode>(nodes())
    for ((name, node) in added.nodes()) {
        if (name !in existing) merged[name] = node
    }
    val routing = LinkedHashMap<Class<*>, QueryRoute>(typeRouting())
    for ((type, route) in added.typeRouting()) {
        val previous = routing.putIfAbsent(type, route)
        require(previous == null) {
            "Query type ${type.name} is already routed in this graph (${describe(previous!!)})"
        }
    }
    val top = if (added.top().name() in existing) top() else merged.getValue(added.top().name())
    return Graph(top, merged, routing)
}

private fun describe(route: QueryRoute): String = when (route) {
    is QueryRoute.Static -> "Static('${route.processName()}')"
    is QueryRoute.Dynamic -> "Dynamic(...)"
}

private fun placeholderStarted(): Nothing =
    throw IllegalStateException("placeholder for an existing node; never started")

private val PLACEHOLDER_INIT = Supplier<ProcessInitializer> { placeholderStarted() }
private val PLACEHOLDER_LOAD = Supplier<ProcessLoader> { placeholderStarted() }
