package io.fom.kotlin

import io.fom.api.ParamProcessInitializer
import io.fom.api.ParamProcessLoader
import io.fom.api.Process
import io.fom.api.ProcessInitializer
import io.fom.api.ProcessLoader
import io.fom.api.QueryableContext
import kotlinx.coroutines.Dispatchers
import java.io.Serializable
import java.util.concurrent.CompletionStage
import kotlin.coroutines.CoroutineContext

/**
 * A [ProcessInitializer] written as a `suspend fun`. Each call runs [initAsync] in its own
 * coroutine on [initContext]; when the engine cancels the stage (init over budget, process
 * cancelled), the coroutine is cancelled too.
 *
 * <p>[initContext] may carry a `Job` (say `appScope.coroutineContext`): cancelling it cancels
 * the call, but a failing call never cancels it, so one failed init does not doom the retries.
 * The other suspend bridges below behave the same way.</p>
 *
 * ```kotlin
 * class Stations : SuspendingInitializer, SuspendingLoader {
 *     override suspend fun initAsync(ctx: QueryableContext) = mapOf("rows" to fetchRows())
 *     override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>) = StationsProcess(properties)
 * }
 * ```
 */
interface SuspendingInitializer : ProcessInitializer {

    /** Where [initAsync] runs; override with `Dispatchers.IO` for blocking work. */
    val initContext: CoroutineContext get() = Dispatchers.Default

    suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray>

    override fun init(ctx: QueryableContext): CompletionStage<Map<String, ByteArray>> =
        linkedFuture(initContext) { initAsync(ctx) }
}

/** A [ProcessLoader] written as a `suspend fun`; see [SuspendingInitializer]. */
interface SuspendingLoader : ProcessLoader {

    /** Where [loadAsync] runs; override with `Dispatchers.IO` for blocking work. */
    val loadContext: CoroutineContext get() = Dispatchers.Default

    suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process

    override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>): CompletionStage<Process> =
        linkedFuture(loadContext) { loadAsync(ctx, properties) }
}

/** A [ParamProcessInitializer] written as a `suspend fun`; see [SuspendingInitializer]. */
interface SuspendingParamInitializer<P : Serializable> : ParamProcessInitializer<P> {

    val initContext: CoroutineContext get() = Dispatchers.Default

    suspend fun initAsync(ctx: QueryableContext, param: P): Map<String, ByteArray>

    override fun init(ctx: QueryableContext, param: P): CompletionStage<Map<String, ByteArray>> =
        linkedFuture(initContext) { initAsync(ctx, param) }
}

/** A [ParamProcessLoader] written as a `suspend fun`; see [SuspendingInitializer]. */
interface SuspendingParamLoader<P : Serializable> : ParamProcessLoader<P> {

    val loadContext: CoroutineContext get() = Dispatchers.Default

    suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>, param: P): Process

    override fun load(ctx: QueryableContext, properties: Map<String, ByteArray>, param: P): CompletionStage<Process> =
        linkedFuture(loadContext) { loadAsync(ctx, properties, param) }
}
