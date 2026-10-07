package io.fom.fsm;

import io.fom.EngineConfig;
import io.fom.ProcessNode;
import io.fom.Sid;
import io.fom.SnapshotPolicy;
import io.fom.api.Process;
import io.fom.api.ProcessContext;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogLeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Results of an abandoned init/load cycle must not be taken for the current cycle's. */
class CycleTokenTest {

    final AtomicInteger inits = new AtomicInteger();
    final AtomicInteger loads = new AtomicInteger();
    final CountDownLatch firstLoadGate = new CountDownLatch(1);
    final CountDownLatch secondLoadGate = new CountDownLatch(1);
    final CountDownLatch secondLoadEntered = new CountDownLatch(1);
    final CountDownLatch firstProcessCleaned = new CountDownLatch(1);

    final class Fixture implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            inits.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("k", new byte[]{1}));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            int call = loads.incrementAndGet();
            // Blocks inside load() itself, so only the budget watchdog can end the attempt.
            if (call == 1) {
                awaitQuietly(firstLoadGate);
            } else if (call == 2) {
                secondLoadEntered.countDown();
                awaitQuietly(secondLoadGate);
            }
            return CompletableFuture.completedFuture(new Named("load-" + call, call == 1 ? firstProcessCleaned : null));
        }
    }

    record Named(String name, CountDownLatch cleaned) implements Process {
        @Override
        public CompletionStage<?> compute(QueryableContext ctx, Object query) {
            return CompletableFuture.completedFuture(name);
        }

        @Override
        public CompletionStage<Void> cleanUp(ProcessContext ctx) {
            if (cleaned != null) cleaned.countDown();
            return CompletableFuture.completedFuture(null);
        }
    }

    @Test
    @Timeout(30)
    void a_late_load_result_of_an_abandoned_cycle_is_ignored() throws Exception {
        var backend = new InMemoryLogBackend();
        backend.append(new LogLeader(0, System.currentTimeMillis(), "i1"), "i1");
        Supplier<Fixture> factory = Fixture::new;
        var node = new ProcessNode("P", List.of(), null, factory, factory);
        // One load attempt per cycle, a short load budget: the first load times out and falls back to a new init.
        var config = new EngineConfig(Duration.ofSeconds(10), Duration.ofMillis(1500), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofMillis(100), Duration.ofMillis(10), Duration.ofMillis(50),
                1, SnapshotPolicy.Disabled.INSTANCE);
        ProcessRouter router = (n, q, d, parent) -> CompletableFuture.failedFuture(new IllegalStateException());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        var fsm = new ProcessFSM("P", node, config, backend, "i1", scheduler, router);
        try {
            fsm.spawnInit();
            assertThat(secondLoadEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(inits).hasValue(2);

            // The first cycle's load returns now: its Process is released and its late timeout
            // result reaches the dispatcher while the second cycle's load attempt 1 runs.
            firstLoadGate.countDown();
            assertThat(firstProcessCleaned.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(300);
            fsm.cancelInit(new Sid("P", Long.MAX_VALUE)).toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertThat(fsm.currentState()).isInstanceOf(State.Loading.class);
            assertThat(inits).as("no third init from a stale failure").hasValue(2);
            assertThat(loads).hasValue(2);

            secondLoadGate.countDown();
            fsm.servingReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
            Object answer = fsm.submitQuery("q", System.currentTimeMillis() + 5_000, null)
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(answer).isEqualTo("load-2");
        } finally {
            firstLoadGate.countDown();
            secondLoadGate.countDown();
            fsm.close();
            scheduler.shutdownNow();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
