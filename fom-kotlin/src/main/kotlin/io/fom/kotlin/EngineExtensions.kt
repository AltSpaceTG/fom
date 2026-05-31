package io.fom.kotlin

import io.fom.Engine
import io.fom.api.Process
import io.fom.api.ProcessContext
import io.fom.api.QueryableContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.time.Duration
import java.util.concurrent.CompletionStage
import kotlin.coroutines.CoroutineContext

/**
 * Suspend extension over [Engine.query]. The result is nullable because a
 * process is free to complete a query with `null`.
 */
suspend fun Engine.queryAwait(q: Any): Any? =
    query(q).toCompletableFuture().await()

suspend fun Engine.queryAwait(q: Any, timeout: Duration): Any? =
    query(q, timeout).toCompletableFuture().await()

/** Reified-typed suspend query — casts the result to [R]. */
suspend inline fun <reified R> Engine.queryAs(q: Any): R =
    queryAwait(q) as R

suspend inline fun <reified R> Engine.queryAs(q: Any, timeout: Duration): R =
    queryAwait(q, timeout) as R

/** Suspend extension over [Engine.queryProcess]. */
suspend fun Engine.queryProcessAwait(name: String, q: Any): Any? =
    queryProcess(name, q).toCompletableFuture().await()

/** Suspend extension over [QueryableContext.query] for use in [SuspendingProcess]. */
suspend fun QueryableContext.queryAwait(dependency: String, query: Any): Any? =
    query(dependency, query).toCompletableFuture().await()

/**
 * Base class for a [Process] written with coroutines. Subclasses implement
 * [computeAsync] as a `suspend fun`; the framework bridges to/from
 * [CompletionStage] transparently.
 *
 * <p>The instance is its own [CoroutineScope] backed by a single
 * [SupervisorJob], so every bridged coroutine is a child of one cancellable
 * scope rather than a fresh orphan scope per call. The scope is cancelled once
 * [cleanUp] completes, so abandoned compute coroutines do not outlive the
 * process.</p>
 */
abstract class SuspendingProcess(
    private val context: CoroutineContext = Dispatchers.Unconfined,
) : Process {

    // One long-lived scope (a single SupervisorJob) instead of a fresh parentless
    // CoroutineScope per compute()/cleanUp() — no Job leak on every framework call.
    private val scope = CoroutineScope(SupervisorJob() + context)

    final override fun compute(ctx: QueryableContext, query: Any): CompletionStage<*> =
        scope.future { computeAsync(ctx, query) }

    protected abstract suspend fun computeAsync(ctx: QueryableContext, query: Any): Any?

    override fun cleanUp(ctx: ProcessContext): CompletionStage<Void> =
        scope.future {
            cleanUpAsync(ctx)
            null
        }

    protected open suspend fun cleanUpAsync(ctx: ProcessContext) {
        // default: nothing
    }
}
