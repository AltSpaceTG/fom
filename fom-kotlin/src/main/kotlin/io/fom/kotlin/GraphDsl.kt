package io.fom.kotlin

import io.fom.Graph
import io.fom.GraphBuilder
import io.fom.SerializableFunction
import io.fom.SerializableSupplier
import io.fom.api.ParamProcessInitializer
import io.fom.api.ParamProcessLoader
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import java.io.Serializable

/** DSL marker so nested receivers don't accidentally reach an outer scope. */
@DslMarker
annotation class GraphDslMarker

/**
 * Idiomatic Kotlin DSL over [GraphBuilder].
 *
 * ```kotlin
 * val graph = graph {
 *     process("Inventory", ::InventoryInit, ::InventoryInit)
 *         .handles<GetInventory>()
 *
 *     process("Products", ::ProductsInit, ::ProductsInit, dependsOn = listOf("Inventory"))
 *         .handles<GetProductModel>()
 * }
 * ```
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

    fun process(
        name: String,
        initFactory: () -> ProcessInitializer,
        loadFactory: () -> ProcessLoader,
        dependsOn: List<String> = emptyList(),
    ): NodeHandle {
        builder.add(
            name,
            toSerializableInit(initFactory),
            toSerializableLoad(loadFactory),
            *dependsOn.toTypedArray()
        )
        return NodeHandle(builder, name)
    }

    fun <P : Serializable> processWithParam(
        name: String,
        initFactory: () -> ParamProcessInitializer<P>,
        loadFactory: () -> ParamProcessLoader<P>,
        param: P,
        dependsOn: List<String> = emptyList(),
    ): NodeHandle {
        builder.addWithParam(
            name,
            toSerializableParamInit(initFactory),
            toSerializableParamLoad(loadFactory),
            param,
            *dependsOn.toTypedArray()
        )
        return NodeHandle(builder, name)
    }

    inline fun <reified Q : Any> route(noinline resolver: (Q) -> String) {
        builder.route(Q::class.java, SerializableFunction @JvmSerializableLambda { resolver(it) })
    }
}

@GraphDslMarker
class NodeHandle internal constructor(
    private val builder: GraphBuilder,
    private val name: String,
) {

    inline fun <reified Q : Any> handles(): NodeHandle = handles(Q::class.java)

    /** Routes the given query classes to this handle's node (by name, not insertion order). */
    fun handles(vararg classes: Class<*>): NodeHandle {
        builder.handlesFor(name, *classes)
        return this
    }
}

private fun toSerializableInit(f: () -> ProcessInitializer): SerializableSupplier<ProcessInitializer> =
    SerializableSupplier @JvmSerializableLambda { f() }

private fun toSerializableLoad(f: () -> ProcessLoader): SerializableSupplier<ProcessLoader> =
    SerializableSupplier @JvmSerializableLambda { f() }

private fun <P : Serializable> toSerializableParamInit(
    f: () -> ParamProcessInitializer<P>,
): SerializableSupplier<ParamProcessInitializer<P>> =
    SerializableSupplier @JvmSerializableLambda { f() }

private fun <P : Serializable> toSerializableParamLoad(
    f: () -> ParamProcessLoader<P>,
): SerializableSupplier<ParamProcessLoader<P>> =
    SerializableSupplier @JvmSerializableLambda { f() }
