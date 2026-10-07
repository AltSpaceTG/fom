package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** A watcher's check runs off the engine scheduler, on its own executor when it has one. */
class WatcherExecutorTest {

    static final Map<String, AtomicInteger> INITS = new ConcurrentHashMap<>();

    static final class Counting implements ProcessInitializer, ProcessLoader {
        private final String name;

        Counting(String name) {
            this.name = name;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INITS.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    private static Engine engine() {
        return engine(new io.fom.api.EngineObserver() { });
    }

    private static Engine engine(io.fom.api.EngineObserver observer) {
        var cfg = new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        var engine = new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        engine.newGraph(new GraphBuilder()
                .add("A", (Supplier<ProcessInitializer>) () -> new Counting("A"),
                        (Supplier<ProcessLoader>) () -> new Counting("A"))
                .add("B", (Supplier<ProcessInitializer>) () -> new Counting("B"),
                        (Supplier<ProcessLoader>) () -> new Counting("B"))
                .build());
        return engine;
    }

    @Test
    @Timeout(20)
    @SuppressWarnings("try") // the watcher handle is held only to be closed
    void check_runs_on_the_supplied_executor() throws Exception {
        List<String> threads = new CopyOnWriteArrayList<>();
        var executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "my-watch"));
        try (var engine = engine();
             var ignored = engine.watch(new ScheduledWatcher<>("A", Long.class, 0L, Duration.ZERO,
                     Duration.ofMillis(20), prev -> {
                         threads.add(Thread.currentThread().getName());
                         return Optional.empty();
                     }, executor))) {
            await().atMost(Duration.ofSeconds(5)).until(() -> threads.size() >= 3);
        } finally {
            executor.shutdownNow();
        }
        assertThat(threads).containsOnly("my-watch");
    }

    @Test
    @Timeout(20)
    @SuppressWarnings("try") // the watcher handle is held only to be closed
    void a_blocking_check_neither_stalls_engine_timers_nor_overlaps_itself() throws Exception {
        INITS.clear();
        var blocked = new CountDownLatch(1);
        var running = new AtomicInteger();
        var maxRunning = new AtomicInteger();
        try (var engine = engine();
             var ignored = engine.watch(new ScheduledWatcher<>("A", Long.class, 0L, Duration.ZERO,
                     Duration.ofMillis(10), prev -> {
                         maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
                         blocked.countDown();
                         try {
                             Thread.sleep(1_500);
                         } catch (InterruptedException e) {
                             Thread.currentThread().interrupt();
                         } finally {
                             running.decrementAndGet();
                         }
                         return Optional.empty();
                     }, null))) {
            assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();
            long start = System.nanoTime();
            engine.trigger("B", "while-the-check-blocks");
            await().atMost(Duration.ofMillis(800)).until(() -> INITS.get("B").get() == 2);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(800));
            Thread.sleep(1_000); // let a few more ticks come due while the check still blocks
        }
        assertThat(maxRunning).hasValue(1);
    }

    @Test
    @Timeout(20)
    @SuppressWarnings("try") // the watcher handle is held only to be closed
    void an_inline_executor_does_not_bring_the_check_back_onto_the_scheduler() throws Exception {
        INITS.clear();
        var blocked = new CountDownLatch(1);
        List<String> threads = new CopyOnWriteArrayList<>();
        try (var engine = engine();
             var ignored = engine.watch(new ScheduledWatcher<>("A", Long.class, 0L, Duration.ZERO,
                     Duration.ofMillis(10), prev -> {
                         threads.add(Thread.currentThread().getName());
                         blocked.countDown();
                         try {
                             Thread.sleep(1_500);
                         } catch (InterruptedException e) {
                             Thread.currentThread().interrupt();
                         }
                         return Optional.empty();
                     }, Runnable::run))) {
            assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();
            engine.trigger("B", "while-the-inline-check-blocks");
            await().atMost(Duration.ofMillis(800)).until(() -> INITS.get("B").get() == 2);
        }
        assertThat(threads).noneMatch(name -> name.startsWith("fom-scheduler-"));
    }

    @Test
    @Timeout(20)
    @SuppressWarnings("try") // the watcher handle is held only to be closed
    void a_watcher_of_a_removed_process_reports_why_it_stopped() throws Exception {
        var stops = new CopyOnWriteArrayList<String>();
        io.fom.api.EngineObserver observer = new io.fom.api.EngineObserver() {
            @Override
            public void onWatcherStopped(String processName, io.fom.api.WatcherStopReason reason) {
                stops.add(processName + ": " + reason);
            }
        };
        var checks = new AtomicInteger();
        try (var engine = engine(observer);
             var ignored = engine.watch(new ScheduledWatcher<>("B", Long.class, 0L, Duration.ZERO,
                     Duration.ofMillis(20), prev -> {
                         checks.incrementAndGet();
                         return Optional.empty();
                     }, null))) {
            await().atMost(Duration.ofSeconds(5)).until(() -> checks.get() > 0);

            engine.remove(List.of("B"));

            await().atMost(Duration.ofSeconds(5)).until(() -> !stops.isEmpty());
            assertThat(stops).containsExactly("B: PROCESS_REMOVED");
        }
    }

    /**
     * The dispatch attempts themselves must stop: counting the checks would not tell the
     * difference, since a rejected tick never runs the check either way.
     */
    @Test
    @Timeout(20)
    @SuppressWarnings("try") // the watcher handle is held only to be closed
    void a_watcher_whose_executor_is_shut_down_stops_itself() throws Exception {
        var checks = new AtomicInteger();
        var rejections = new AtomicInteger();
        var stops = new CopyOnWriteArrayList<String>();
        var executor = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                if (isShutdown()) {
                    rejections.incrementAndGet();
                    throw new java.util.concurrent.RejectedExecutionException("shut down");
                }
                super.execute(command);
            }
        };
        io.fom.api.EngineObserver observer = new io.fom.api.EngineObserver() {
            @Override
            public void onWatcherStopped(String processName, io.fom.api.WatcherStopReason reason) {
                stops.add(processName + ": " + reason);
            }
        };
        try (var engine = engine(observer);
             var ignored = engine.watch(new ScheduledWatcher<>("A", Long.class, 0L, Duration.ZERO,
                     Duration.ofMillis(20), prev -> {
                         checks.incrementAndGet();
                         return Optional.empty();
                     }, executor))) {
            await().atMost(Duration.ofSeconds(5)).until(() -> checks.get() > 0);

            executor.shutdown();
            await().atMost(Duration.ofSeconds(5)).until(() -> rejections.get() > 0);
            Thread.sleep(300); // ~15 further ticks would each try to dispatch again
            int after = rejections.get();
            Thread.sleep(300);

            assertThat(rejections).as("the watcher stopped instead of dispatching every tick")
                    .hasValue(after);
            assertThat(after).as("it stopped on the first rejection").isEqualTo(1);
            assertThat(stops).as("a self-stop is reported, not only logged")
                    .containsExactly("A: EXECUTOR_SHUT_DOWN");
        } finally {
            executor.shutdownNow();
        }
    }
}
