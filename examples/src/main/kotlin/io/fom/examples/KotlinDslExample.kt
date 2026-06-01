package io.fom.examples

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.api.Process
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import io.fom.kotlin.SuspendingProcess
import io.fom.kotlin.graph
import io.fom.kotlin.queryAs
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.runBlocking
import java.io.Serializable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * The idiomatic Kotlin face of fom: the `graph { }` DSL, a [SuspendingProcess]
 * whose `compute` is a `suspend fun`, and the reified `queryAs<T>` extension.
 *
 * Run: `./gradlew :examples:kotlinDsl`
 */

data class GetGreeting(val who: String) : Serializable

class GreeterInit : ProcessInitializer, ProcessLoader {

    override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
        CompletableFuture.completedFuture(mapOf("prefix" to "Hello".toByteArray()))

    override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> {
        val prefix = String(properties["prefix"]!!)
        // The live process is a SuspendingProcess — compute is a suspend fun.
        val live = object : SuspendingProcess() {
            override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any {
                val q = query as GetGreeting
                return "$prefix, ${q.who}!"
            }
        }
        return CompletableFuture.completedFuture(live)
    }
}

fun main() = runBlocking {
    val g = graph {
        process("Greeter", ::GreeterInit, ::GreeterInit)
            .handles<GetGreeting>()
    }

    InMemoryLogBackend().use { backend ->
        Engine(EngineConfig.defaults(), backend, JavaSerializableSerDe(), true).use { engine ->
            engine.newGraph(g)
            // queryAs<R> awaits the CompletionStage and casts to R.
            val greeting = engine.queryAs<String>(GetGreeting("Kotlin"))
            println(greeting)
        }
    }
}
