package io.fom.fsm;

import io.fom.EngineConfig;
import io.fom.ProcessNode;
import io.fom.ReinitStrategy;
import io.fom.Sid;
import io.fom.SnapshotPolicy;
import io.fom.api.Process;
import io.fom.api.ProcessContext;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** The replacement track's retry and candidate handling, driven on one FSM. */
class ReplacementTrackTest {

    final AtomicInteger inits = new AtomicInteger();
    final List<String> loaded = new CopyOnWriteArrayList<>();
    volatile boolean failLaterInits;
    volatile boolean failFirstInit;

    final class Fixture implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            int n = inits.incrementAndGet();
            if ((n > 1 && failLaterInits) || (n == 1 && failFirstInit)) {
                return CompletableFuture.failedFuture(new IllegalStateException("upstream down (injected)"));
            }
            return CompletableFuture.completedFuture(props("fresh"));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            String value = new String(properties.get("v"), StandardCharsets.UTF_8);
            loaded.add(value);
            return CompletableFuture.completedFuture(new Process() {
                @Override
                public CompletionStage<?> compute(QueryableContext c, Object query) {
                    return CompletableFuture.completedFuture(value);
                }

                @Override
                public CompletionStage<Void> cleanUp(ProcessContext c) {
                    return CompletableFuture.completedFuture(null);
                }
            });
        }
    }

    static Map<String, byte[]> props(String value) {
        return Map.of("v", value.getBytes(StandardCharsets.UTF_8));
    }

    static EngineConfig cfg() {
        return new EngineConfig(Duration.ofMillis(300), Duration.ofSeconds(2), Duration.ofSeconds(2),
                Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(10), Duration.ofMillis(50),
                1, SnapshotPolicy.Disabled.INSTANCE);
    }

    ProcessFSM fsm(EngineConfig config, InMemoryLogBackend backend, ScheduledThreadPoolExecutor scheduler) {
        Supplier<Fixture> factory = Fixture::new;
        var node = new ProcessNode("P", List.of(), null, factory, factory);
        ProcessRouter router = (n, q, d, parent) -> CompletableFuture.failedFuture(new IllegalStateException());
        return new ProcessFSM("P", node, config, backend, "i1", scheduler, router);
    }

    static boolean retryQueued(ScheduledThreadPoolExecutor scheduler) {
        return scheduler.getQueue().stream().anyMatch(task -> ((Delayed) task).getDelay(TimeUnit.MINUTES) >= 1);
    }

    @Test
    @Timeout(30)
    void a_scheduled_retry_is_dropped_when_the_fsm_stops() throws Exception {
        var backend = new InMemoryLogBackend();
        backend.append(new LogLeader(0, System.currentTimeMillis(), "i1"), "i1");
        var scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setRemoveOnCancelPolicy(true);
        var config = cfg().withReinitRetryBackoff(Duration.ofMinutes(10), Duration.ofMinutes(10));
        var fsm = fsm(config, backend, scheduler);
        try {
            fsm.spawnInit();
            fsm.servingReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
            failLaterInits = true;
            fsm.submitReinit(new ReinitCause.Triggered("go"));
            await().atMost(Duration.ofSeconds(5)).until(() -> retryQueued(scheduler));
            fsm.shutdown(Duration.ofSeconds(2)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(retryQueued(scheduler)).as("the stopped FSM is no longer held by its retry").isFalse();
        } finally {
            fsm.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    @Timeout(30)
    void release_first_retires_a_persisted_candidate_instead_of_loading_it() throws Exception {
        var backend = new InMemoryLogBackend();
        backend.append(new LogLeader(0, System.currentTimeMillis(), "i1"), "i1");
        var old = (LogInitialized) backend.append(
                new LogInitialized(0, System.currentTimeMillis(), "P", props("old")), "i1").orElseThrow();
        var candidate = (LogInitialized) backend.append(
                new LogInitialized(0, System.currentTimeMillis(), "P", props("candidate"), old.sid()), "i1")
                .orElseThrow();
        var scheduler = new ScheduledThreadPoolExecutor(1);
        var fsm = fsm(cfg().withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend, scheduler);
        try {
            fsm.spawnLoad(old.clock(), old.properties());
            fsm.replaceFrom(candidate.clock(), candidate.properties());
            fsm.submitReinit(new ReinitCause.Triggered("go"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "fresh".equals(
                    fsm.submitQuery("q", System.currentTimeMillis() + 2_000, null).toCompletableFuture()
                            .get(2, TimeUnit.SECONDS)));
            Thread.sleep(300);
            assertThat(loaded).as("never loaded beside the old state").containsExactly("old", "fresh");
            assertThat(inits).as("one re-init for the candidate and the request together").hasValue(1);
            Sid retired = candidate.sid();
            assertThat(List.of(backend.getBetween(0, backend.length())))
                    .anyMatch((LogEvent ev) -> ev instanceof LogDead d && d.sid().equals(retired));
        } finally {
            fsm.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    @Timeout(30)
    void a_scheduled_init_retry_is_dropped_when_the_fsm_stops() throws Exception {
        var backend = new InMemoryLogBackend();
        backend.append(new LogLeader(0, System.currentTimeMillis(), "i1"), "i1");
        var scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setRemoveOnCancelPolicy(true);
        var config = new EngineConfig(Duration.ofMinutes(30), Duration.ofSeconds(2), Duration.ofSeconds(2),
                Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMinutes(2), Duration.ofMinutes(5),
                1, SnapshotPolicy.Disabled.INSTANCE);
        failFirstInit = true;
        var fsm = fsm(config, backend, scheduler);
        try {
            fsm.spawnInit();
            await().atMost(Duration.ofSeconds(5)).until(() -> retryQueued(scheduler) && fsm.initRetries() == 1);
            fsm.shutdown(Duration.ofSeconds(2)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(retryQueued(scheduler)).as("the stopped FSM is no longer held by its retry tick").isFalse();
        } finally {
            fsm.close();
            scheduler.shutdownNow();
        }
    }

    /** An out-of-memory error that runs out of memory again when logged (printing it fails too). */
    static final class UnprintableError extends OutOfMemoryError {
        UnprintableError() {
            super("Java heap space (injected)");
        }

        @Override
        public String toString() {
            throw new OutOfMemoryError("Java heap space (injected, while logging)");
        }
    }

    /** A reply whose every look fails with an {@link UnprintableError}. */
    static final class PoisonedReply extends CompletableFuture<Object> {
        @Override
        public boolean isDone() {
            throw new UnprintableError();
        }

        @Override
        public String toString() {
            throw new UnprintableError();
        }
    }

    @Test
    @Timeout(30)
    void the_dispatcher_survives_an_error_that_also_breaks_its_logging() throws Exception {
        var backend = new InMemoryLogBackend();
        backend.append(new LogLeader(0, System.currentTimeMillis(), "i1"), "i1");
        var scheduler = new ScheduledThreadPoolExecutor(1);
        var fsm = fsm(cfg(), backend, scheduler);
        try {
            fsm.spawnInit();
            fsm.servingReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
            fsm.submitAnnouncedQuery(new Envelope.Query(java.util.UUID.randomUUID(), "q", new PoisonedReply(),
                    System.currentTimeMillis() + 5_000));
            Object answer = fsm.submitQuery("q", System.currentTimeMillis() + 2_000, null)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(answer).as("still dispatching").isEqualTo("fresh");
        } finally {
            fsm.close();
            scheduler.shutdownNow();
        }
    }
}
