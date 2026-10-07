package io.fom.kotlin

import io.fom.Engine
import io.fom.EngineConfig
import io.fom.SnapshotPolicy
import io.fom.api.Process
import io.fom.api.ProcessContext
import io.fom.api.QueryableContext
import io.fom.log.InMemoryLogBackend
import io.fom.serde.JavaSerializableSerDe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.lang.reflect.Proxy
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

/**
 * A user may hand fom a coroutine context that carries a `Job` of their own —
 * `appScope.coroutineContext` is the natural thing to pass. fom's coroutines must
 * run under Jobs of their own, linked to that Job for cancellation only: fom never
 * cancels the user's Job (not when it retires a generation, not when a compute or
 * an init fails), while the user cancelling it still stops fom's coroutines.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SuspendingUserJobTest {

    private val ctx = Proxy.newProxyInstance(
        javaClass.classLoader, arrayOf(QueryableContext::class.java),
    ) { _, _, _ -> null } as QueryableContext

    /** A plain (non-supervisor) Job: one failing child would cancel it. */
    private val app = CoroutineScope(Job() + Dispatchers.Default)

    @AfterEach
    fun tearDown() = app.cancel()

    private class Echo(context: CoroutineContext) : SuspendingProcess(context) {
        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? = when (query) {
            "boom" -> throw IllegalStateException("compute boom")
            "hang" -> awaitCancellation()
            else -> "ok:$query"
        }
    }

    private fun Process.ask(q: Any): Any? = compute(ctx, q).toCompletableFuture().get(5, TimeUnit.SECONDS)

    @Test
    fun `retiring a generation does not cancel the user's scope`() {
        val p = Echo(app.coroutineContext)
        assertThat(p.ask("a")).isEqualTo("ok:a")

        // What a re-init does to the outgoing generation.
        assertThat(p.cleanUp(ctx as ProcessContext).toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull()

        assertThat(app.isActive).isTrue()
        // The instance handed back by the next load (here the same one) keeps serving.
        assertThat(p.ask("b")).isEqualTo("ok:b")
        assertThat(Echo(app.coroutineContext).ask("c")).isEqualTo("ok:c")
    }

    @Test
    fun `a failing compute does not break later computes of the same instance`() {
        val p = Echo(app.coroutineContext)
        assertThatThrownBy { p.ask("boom") }
            .isInstanceOf(ExecutionException::class.java)
            .cause().isInstanceOf(IllegalStateException::class.java).hasMessage("compute boom")

        assertThat(p.ask("after")).isEqualTo("ok:after")
        assertThat(app.isActive).isTrue()
    }

    @Test
    fun `cancelling the user's scope cancels fom's in-flight computes`() {
        val p = Echo(app.coroutineContext)
        val inFlight = p.compute(ctx, "hang").toCompletableFuture()
        Thread.sleep(50)
        assertThat(inFlight.isDone).isFalse()

        app.cancel()

        assertThatThrownBy { inFlight.get(5, TimeUnit.SECONDS) }.isInstanceOf(CancellationException::class.java)
        // ... and nothing fom starts on that context afterwards runs either.
        assertThatThrownBy { p.ask("late") }.isInstanceOf(CancellationException::class.java)
    }

    private class FlakyPhases(private val context: CoroutineContext) : SuspendingInitializer, SuspendingLoader {
        val initCalls = AtomicInteger()
        val loadCalls = AtomicInteger()
        override val initContext: CoroutineContext get() = context
        override val loadContext: CoroutineContext get() = context

        override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
            if (initCalls.incrementAndGet() == 1) throw IllegalStateException("transient init failure")
            return mapOf("v" to "x".toByteArray())
        }

        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process {
            if (loadCalls.incrementAndGet() == 1) throw IllegalStateException("transient load failure")
            return Echo(context)
        }
    }

    @Test
    fun `a failed init or load does not cancel the user's Job nor doom the retry`() {
        val phases = FlakyPhases(app.coroutineContext)

        assertThatThrownBy { phases.init(ctx).toCompletableFuture().get(5, TimeUnit.SECONDS) }
            .cause().hasMessage("transient init failure")
        assertThat(app.isActive).isTrue()
        val props = phases.init(ctx).toCompletableFuture().get(5, TimeUnit.SECONDS)
        assertThat(props).containsKey("v")

        assertThatThrownBy { phases.load(ctx, props).toCompletableFuture().get(5, TimeUnit.SECONDS) }
            .cause().hasMessage("transient load failure")
        assertThat(app.isActive).isTrue()
        assertThat(phases.load(ctx, props).toCompletableFuture().get(5, TimeUnit.SECONDS).ask("q")).isEqualTo("ok:q")
    }

    @Test
    fun `cancelling the user's scope cancels an in-flight init`() {
        val hanging = object : SuspendingInitializer {
            override val initContext: CoroutineContext get() = app.coroutineContext
            override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> = awaitCancellation()
        }
        val init = hanging.init(ctx).toCompletableFuture()
        Thread.sleep(50)
        assertThat(init.isDone).isFalse()

        app.cancel()

        assertThatThrownBy { init.get(5, TimeUnit.SECONDS) }.isInstanceOf(CancellationException::class.java)
    }

    // --- end to end: a trigger-driven re-init with processes built on the app scope ---

    private fun cfg() = EngineConfig(
        Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(1),
        Duration.ofSeconds(5), Duration.ofMillis(10),
        Duration.ofMillis(10), Duration.ofMillis(50), 1,
        SnapshotPolicy.Disabled.INSTANCE,
    )

    private class AppProcess(context: CoroutineContext, private val cleanUps: AtomicInteger) : SuspendingProcess(context) {
        override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any {
            delay(5)
            return "ok:$query"
        }

        override suspend fun cleanUpAsync(ctx: ProcessContext) {
            cleanUps.incrementAndGet()
        }
    }

    private class AppPhases(private val context: CoroutineContext) : SuspendingInitializer, SuspendingLoader {
        val inits = AtomicInteger()
        val cleanUps = AtomicInteger()
        override val initContext: CoroutineContext get() = context
        override val loadContext: CoroutineContext get() = context

        override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
            inits.incrementAndGet()
            return mapOf("v" to "x".toByteArray())
        }

        // A fresh instance per load, all of them on the user's context.
        override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process =
            AppProcess(context, cleanUps)
    }

    @Test
    fun `a node built on the user's scope keeps serving across re-inits and leaves that scope alive`() {
        val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val phases = AppPhases(appScope.coroutineContext)
            Engine(cfg(), InMemoryLogBackend(), JavaSerializableSerDe()).use { engine ->
                engine.newGraph(graph { process("S", { phases }, { phases }) })
                assertThat(engine.queryProcess("S", "q0").toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo("ok:q0")

                repeat(2) { round ->
                    val initsBefore = phases.inits.get()
                    assertThat(engine.trigger("S", "round-$round")).isTrue()
                    awaitUntil { phases.inits.get() > initsBefore && phases.cleanUps.get() > round }

                    assertThat(appScope.isActive).isTrue()
                    assertThat(engine.queryProcess("S", "q$round").toCompletableFuture().get(5, TimeUnit.SECONDS))
                        .isEqualTo("ok:q$round")
                }
            }
            // Closing the engine retires the last generation — still not the user's scope.
            assertThat(appScope.isActive).isTrue()
        } finally {
            appScope.cancel()
        }
    }

    private fun awaitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (!condition() && System.nanoTime() < deadline) Thread.sleep(10)
        assertThat(condition()).isTrue()
    }
}
