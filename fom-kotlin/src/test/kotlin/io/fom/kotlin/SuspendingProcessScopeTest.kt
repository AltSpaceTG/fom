package io.fom.kotlin

import io.fom.Sid
import io.fom.api.ProcessContext
import io.fom.api.QueryableContext
import kotlinx.coroutines.awaitCancellation
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.lang.reflect.Proxy
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class SuspendingProcessScopeTest {

    private val ctx = Proxy.newProxyInstance(
        javaClass.classLoader, arrayOf(QueryableContext::class.java),
    ) { _, _, _ -> null } as QueryableContext

    private class Hanging(private val cleanupFailure: Throwable? = null) : SuspendingProcess() {
        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? {
            awaitCancellation()
        }

        override suspend fun cleanUpAsync(ctx: ProcessContext) {
            cleanupFailure?.let { throw it }
        }
    }

    @Test
    fun `cleanUp cancels suspended compute coroutines`() {
        val p = Hanging()
        val compute = p.compute(ctx, "q").toCompletableFuture()
        assertThat(compute.isDone).isFalse()

        assertThat(p.cleanUp(ctx as ProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull()

        // Scope is cancelled before the cleanUp stage completes.
        assertThat(compute.isDone).isTrue()
        assertThatThrownBy { compute.get() }.isInstanceOf(CancellationException::class.java)
    }

    @Test
    fun `failed cleanUp still cancels the scope and propagates the original exception`() {
        val boom = IllegalStateException("cleanup boom")
        val p = Hanging(boom)
        val compute = p.compute(ctx, "q").toCompletableFuture()

        val cleanup = p.cleanUp(ctx as ProcessContext).toCompletableFuture()
        assertThatThrownBy { cleanup.get(5, TimeUnit.SECONDS) }
            .isInstanceOf(ExecutionException::class.java)
            .cause().isSameAs(boom)
        assertThat(compute.isDone).isTrue()
        assertThat(compute.isCancelled || compute.isCompletedExceptionally).isTrue()
    }

    @Test
    fun `a second generation of a reused instance keeps working after the first is retired`() {
        val p = Hanging()
        val firstCompute = p.compute(ctx, "q").toCompletableFuture()

        // Retire generation 1 (what a re-init does to the outgoing generation).
        assertThat(p.cleanUp(ctx as ProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull()
        assertThatThrownBy { firstCompute.get() }.isInstanceOf(CancellationException::class.java)

        // The same instance handed back by the next load opens a fresh generation.
        val secondCompute = p.compute(ctx, "q").toCompletableFuture()
        Thread.sleep(50)
        assertThat(secondCompute.isDone).isFalse()

        // ... and that generation is cancellable in its turn.
        assertThat(p.cleanUp(ctx as ProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull()
        assertThatThrownBy { secondCompute.get() }.isInstanceOf(CancellationException::class.java)
    }

    @Test
    fun `retiring the older generation does not cancel the live one`() {
        val p = Hanging()
        val v1 = ctxAt(Sid("N", 1))
        val v2 = ctxAt(Sid("N", 2))
        // Version 1 is retired only after the reused instance is serving version 2.
        val stale = p.compute(v1, "q").toCompletableFuture()
        val live = p.compute(v2, "q").toCompletableFuture()

        assertThat(p.cleanUp(v1 as ProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull()

        assertThatThrownBy { stale.get() }.isInstanceOf(CancellationException::class.java)
        Thread.sleep(50)
        assertThat(live.isDone).isFalse()

        assertThat(p.cleanUp(v2 as ProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull()
        assertThatThrownBy { live.get() }.isInstanceOf(CancellationException::class.java)
    }

    /** A context of version [sid] with no executor. */
    private fun ctxAt(sid: Sid) = Proxy.newProxyInstance(
        javaClass.classLoader, arrayOf(QueryableContext::class.java),
    ) { _, method, _ -> if (method.name == "sid") sid else null } as QueryableContext

    @Test
    fun `no coroutine outlives the generations it was started in`() {
        val p = Hanging()
        val computes = (1..20).map { p.compute(ctx, "q").toCompletableFuture() }
        assertThat(p.cleanUp(ctx as ProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull()
        assertThat(computes).allSatisfy { assertThat(it.isDone).isTrue() }

        val reused = (1..20).map { p.compute(ctx, "q").toCompletableFuture() }
        Thread.sleep(50)
        assertThat(reused).allSatisfy { assertThat(it.isDone).isFalse() }
        assertThat(p.cleanUp(ctx as ProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull()
        assertThat(reused).allSatisfy { assertThat(it.isDone).isTrue() }
    }
}
