package io.fom.kotlin

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.future
import kotlinx.coroutines.job
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.CoroutineContext

/**
 * A scope over [context] with a fresh [SupervisorJob] in place of the context's own [Job].
 *
 * <p>`SupervisorJob() + context` would let the user's Job replace the supervisor, and fom
 * would then cancel the user's Job whenever it retires a scope. Here the user's Job is only a
 * kill switch: once it completes exceptionally, the scope is cancelled; nothing flows the other
 * way.</p>
 *
 * <p>The scope is not a structured child of the user's Job: a serving process's generation
 * stays active until fom retires it, so a child would keep `runBlocking { }` or a graceful
 * shutdown from ever completing. As a result, joining the user's Job does not wait for fom's
 * coroutines.</p>
 */
internal fun linkedSupervisorScope(context: CoroutineContext): CoroutineScope {
    val own = SupervisorJob()
    val userJob = context[Job]
    if (userJob != null) {
        val link = userJob.invokeOnCompletion { cause ->
            if (cause != null) own.cancel(CancellationException("The Job of the supplied coroutine context was cancelled", cause))
        }
        // A long-lived user Job must not collect one handler per scope.
        own.invokeOnCompletion { link.dispose() }
    }
    return CoroutineScope(context.minusKey(Job) + own)
}

/**
 * Runs [block] as a future in a one-shot [linkedSupervisorScope]; the scope's Job completes
 * when the future settles, which drops the link to the user's Job.
 */
internal fun <T> linkedFuture(context: CoroutineContext, block: suspend CoroutineScope.() -> T): CompletableFuture<T> {
    val scope = linkedSupervisorScope(context)
    val own = scope.coroutineContext.job as CompletableJob   // the SupervisorJob made above
    return scope.future(block = block).also { it.whenComplete { _, _ -> own.complete() } }
}
