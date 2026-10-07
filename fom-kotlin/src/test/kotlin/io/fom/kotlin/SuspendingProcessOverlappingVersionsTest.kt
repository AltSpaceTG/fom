package io.fom.kotlin

import io.fom.Sid
import io.fom.api.Deadline
import io.fom.api.ProcessContext
import io.fom.api.QueryableContext
import kotlinx.coroutines.CompletableDeferred
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * One instance serving two versions of a node at once: the old version's cleanUp
 * runs after the new version has started serving. Only the old version's computes
 * may be cancelled.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SuspendingProcessOverlappingVersionsTest {

    class Held : SuspendingProcess() {
        val gate = CompletableDeferred<String>()
        val entered = CountDownLatch(2)

        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any {
            entered.countDown()
            return "${ctx.sid().clock()}:${gate.await()}"
        }
    }

    private class Ctx(private val sid: Sid, private val executor: Executor) : QueryableContext {
        override fun query(dependencyName: String, query: Any): CompletionStage<Any> =
            throw UnsupportedOperationException()

        override fun dependencies(): List<String> = emptyList()

        override fun currentQueryDeadline(): Optional<Deadline> = Optional.empty()

        override fun sid(): Sid = sid

        override fun executor(): Executor = executor
    }

    @Test
    fun `cleanUp of the old version cancels only its own computes`() {
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        executor.use {
            val process = Held()
            val v1 = Ctx(Sid("N", 1), executor)
            val v2 = Ctx(Sid("N", 2), executor)

            val old = process.compute(v1, "q").toCompletableFuture()
            val new = process.compute(v2, "q").toCompletableFuture()
            assertThat(process.entered.await(10, TimeUnit.SECONDS)).`as`("both computes started").isTrue()

            val cleanupCtx = object : ProcessContext {
                override fun sid() = Sid("N", 1)
                override fun executor(): Executor = executor
            }
            process.cleanUp(cleanupCtx).toCompletableFuture().get(10, TimeUnit.SECONDS)

            assertThat(old.isCancelled).`as`("Sid-1 compute cancelled by its cleanUp").isTrue()
            assertThat(new.isDone).`as`("Sid-2 compute still running").isFalse()

            // Sid 2 keeps serving: new computes join its live generation.
            val later = process.compute(v2, "q").toCompletableFuture()
            process.gate.complete("done")
            assertThat(new.get(10, TimeUnit.SECONDS)).isEqualTo("2:done")
            assertThat(later.get(10, TimeUnit.SECONDS)).isEqualTo("2:done")
        }
    }
}
