package io.fom.kotlin

import io.fom.Engine
import io.fom.ProcessRef
import io.fom.Sid
import io.fom.api.Process
import io.fom.api.ProcessContext
import io.fom.api.QueryableContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.job
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import kotlin.coroutines.CoroutineContext

/**
 * Suspend extension over [Engine.query]. The result is nullable because a
 * process is free to complete a query with `null`.
 */
suspend fun Engine.queryAwait(q: Any): Any? =
    query(q).toCompletableFuture().await()

suspend fun Engine.queryAwait(q: Any, timeout: Duration): Any? =
    query(q, timeout).toCompletableFuture().await()

/** [queryAwait] with the result cast to [R]. */
suspend inline fun <reified R> Engine.queryAs(q: Any): R =
    queryAwait(q) as R

suspend inline fun <reified R> Engine.queryAs(q: Any, timeout: Duration): R =
    queryAwait(q, timeout) as R

/** Suspend extension over [Engine.queryProcess]. */
suspend fun Engine.queryProcessAwait(name: String, q: Any): Any? =
    queryProcess(name, q).toCompletableFuture().await()

suspend fun Engine.queryProcessAwait(name: String, q: Any, timeout: Duration): Any? =
    queryProcess(name, q, timeout).toCompletableFuture().await()

suspend fun Engine.queryProcessAwait(ref: ProcessRef, q: Any): Any? =
    queryProcess(ref, q).toCompletableFuture().await()

suspend fun Engine.queryProcessAwait(ref: ProcessRef, q: Any, timeout: Duration): Any? =
    queryProcess(ref, q, timeout).toCompletableFuture().await()

/** Suspend extension over [QueryableContext.query], for use inside a [SuspendingProcess]. */
suspend fun QueryableContext.queryAwait(dependency: String, query: Any): Any? =
    query(dependency, query).toCompletableFuture().await()

/** [queryAwait] keyed by a typed [ProcessRef]. */
suspend fun QueryableContext.queryAwait(dependency: ProcessRef, query: Any): Any? =
    query(dependency, query).toCompletableFuture().await()

/**
 * Base class for a [Process] written with coroutines: implement [computeAsync] as a
 * `suspend fun` and the `CompletionStage` bridging is done for you.
 *
 * <p>Computes run in *generations*: one [CoroutineScope] over a [SupervisorJob] per
 * loaded version, keyed by `ctx.sid()`. A [compute] joins the generation of its
 * context's Sid, so a timed-out or cancelled query cancels only its own coroutine;
 * [cleanUp] runs [cleanUpAsync] in the generation of its context's Sid and then
 * cancels that generation alone. So `load()` may hand the same instance back for a
 * new version, and the old version's cleanup leaves the new one's computes running,
 * even when it runs after the new version started serving.</p>
 *
 * <p>Generations are also kept apart per owner, the identity of `ctx.executor()`, so
 * one instance can be shared by several nodes, even across engines, without one
 * owner's cleanup cancelling another's computes. Test doubles calling
 * [compute]/[cleanUp] by hand must report the same `executor()` instance and Sid for
 * the same version.</p>
 *
 * <p>Computes run on [Dispatchers.Default] unless [context] says otherwise; pass
 * [Dispatchers.IO] for blocking work. [context] may carry a `Job` (e.g.
 * `appScope.coroutineContext`): cancelling it cancels every generation, but retiring a
 * generation or a failing compute never cancels it, and a live generation does not
 * keep it from completing.</p>
 */
abstract class SuspendingProcess(
    private val context: CoroutineContext = Dispatchers.Default,
) : Process {

    private val lock = Any()

    private val generations = HashMap<Version, Generation>()

    /**
     * A generation's scope plus its in-flight compute futures, so retiring it can
     * settle them at once — even a compute stuck in blocking code that ignores
     * cancellation.
     */
    private class Generation(val scope: CoroutineScope) {
        private val computes: MutableSet<CompletableFuture<*>> = ConcurrentHashMap.newKeySet()

        fun track(f: CompletableFuture<*>) {
            computes += f
            f.whenComplete { _, _ -> computes -= f }
            // Retired between generationOf() and here: settle it like the others.
            if (scope.coroutineContext.job.isCancelled) f.cancel(false)
        }

        fun retire(cause: CancellationException) {
            scope.coroutineContext.job.cancel(cause)
            computes.forEach { it.cancel(false) }
        }
    }

    private fun newGeneration() = Generation(linkedSupervisorScope(context))

    /**
     * One loaded version: its Sid plus the state machine running it, identified by its
     * executor. The Sid alone is not enough: two engines in one JVM can both run
     * `"alerts"` at the same clock.
     */
    private class Version(val sid: Sid?, val executor: Executor?) {
        override fun equals(other: Any?) =
            other is Version && other.sid == sid && other.executor === executor

        override fun hashCode() = 31 * sid.hashCode() + System.identityHashCode(executor)
    }

    // The engine always supplies a sid and an executor; test doubles may not.
    private fun versionOf(ctx: ProcessContext) = Version(ctx.sid(), ctx.executor())

    private fun generationOf(version: Version): Generation = synchronized(lock) {
        generations.getOrPut(version, ::newGeneration)
    }

    private fun takeGeneration(version: Version): Generation? = synchronized(lock) {
        generations.remove(version)
    }

    final override fun compute(ctx: QueryableContext, query: Any): CompletionStage<*> {
        val generation = generationOf(versionOf(ctx))
        return generation.scope.future { computeAsync(ctx, query) }.also(generation::track)
    }

    /**
     * Runs [cleanUpAsync], then cancels the generation of `ctx.sid()`, whether cleanup
     * succeeded or not. Other versions and other owners keep running.
     *
     * The returned stage completes with cleanup's own result once the generation is
     * cancelled and its compute futures are settled; it does not wait for the
     * cancelled compute coroutines to finish. If the Job in [context] was cancelled
     * and cleanup failed only with that cancellation, the stage completes normally.
     *
     * <p>Cancelling the returned stage (the engine does so when a cleanup outlives
     * `cleanupTimeout`) cancels [cleanUpAsync] and the retiring generation. A
     * body that blocks before it first suspends, or suspends inside
     * `withContext(NonCancellable)`, keeps running past the budget and past
     * `close()`. `finally` blocks in [cleanUpAsync] always run.</p>
     */
    override fun cleanUp(ctx: ProcessContext): CompletionStage<Void> {
        // Never computed: cleanUpAsync gets a throwaway generation.
        val retiring = takeGeneration(versionOf(ctx)) ?: newGeneration()
        val result = CompletableFuture<Void>()
        // UNDISPATCHED: a dispatched coroutine can be cancelled before its body starts,
        // and then the cleanup's try/finally would never run.
        val cleaning = retiring.scope.future(start = CoroutineStart.UNDISPATCHED) { cleanUpAsync(ctx) }
        cleaning.whenComplete { _, err ->
            retiring.retire(CancellationException("SuspendingProcess cleaned up"))
            // Don't wait for the Job itself: its remaining children are computes the
            // engine has already abandoned, and one stuck in blocking code would hold
            // a re-init or close() for the whole cleanup budget.
            if (err == null || cancelledByUserJob(err)) result.complete(null)
            else result.completeExceptionally(err)
        }
        // Cancelling the plain `result` future does not reach the coroutine by itself.
        result.whenComplete { _, _ ->
            if (result.isCancelled) {
                cleaning.cancel(true)
                retiring.retire(CancellationException("SuspendingProcess cleanUp ran out of its budget"))
            }
        }
        return result
    }

    /** True when [err] is only the cancellation caused by the Job in [context]. */
    private fun cancelledByUserJob(err: Throwable): Boolean {
        val userJob = context[Job] ?: return false
        val cause = (err as? CompletionException)?.cause ?: err
        return cause is CancellationException && userJob.isCancelled
    }

    /** Called from [cleanUp] to release what the process holds. Does nothing by default. */
    protected open suspend fun cleanUpAsync(ctx: ProcessContext) {}

    /** Answers [query]; the result may be `null`. */
    protected abstract suspend fun computeAsync(ctx: QueryableContext, query: Any): Any?
}
