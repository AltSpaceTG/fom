package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.Process;
import io.fom.api.ProcessContext;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogCleanedUp;
import io.fom.log.LogDead;
import io.fom.log.LogDependencyChanged;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLoaded;
import io.fom.log.LogTrigger;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * KEEP_OLD re-init: the old version answers until the new one is loaded, and keeps answering if
 * the new one fails. Covers the switch, the log it leaves, restarts at each crash point, retries,
 * cancellation, pause/remove/close and leadership loss mid re-init, the cascade, the observer and
 * the RELEASE_FIRST strategy.
 */
class KeepOldReinitTest {

    /** What the next init of a process stores: it then serves this value. */
    static final Map<String, String> SOURCE = new ConcurrentHashMap<>();
    /** Init blocks on this until it completes (per process); absent = no wait. */
    static final Map<String, CompletableFuture<Void>> INIT_GATE = new ConcurrentHashMap<>();
    /** Load blocks on this until it completes; absent = no wait. */
    static final Map<String, CompletableFuture<Void>> LOAD_GATE = new ConcurrentHashMap<>();
    /** Init fails with this while present. */
    static final Map<String, Supplier<Throwable>> INIT_FAILS = new ConcurrentHashMap<>();
    /** Load fails while present. */
    static final Map<String, Boolean> LOAD_FAILS = new ConcurrentHashMap<>();
    static final Map<String, AtomicInteger> INITS = new ConcurrentHashMap<>();
    static final Map<String, AtomicInteger> LOADS = new ConcurrentHashMap<>();
    /** A compute for the query "block" waits on this. */
    static volatile CountDownLatch blockCompute = new CountDownLatch(0);
    static volatile CountDownLatch computeBlocked = new CountDownLatch(1);
    /** Ordered record of what the processes did: "loaded:v", "cleanup:v", "computed:v". */
    static final List<String> EVENTS = new CopyOnWriteArrayList<>();
    /** Dependency the init of a process queries (its answer is folded into its value). */
    static final Map<String, String> QUERIES = new ConcurrentHashMap<>();
    /** A process's cleanUp sleeps this long (ms). */
    static final Map<String, Long> CLEANUP_DELAY = new ConcurrentHashMap<>();

    @BeforeEach
    void reset() {
        SOURCE.clear();
        INIT_GATE.clear();
        LOAD_GATE.clear();
        INIT_FAILS.clear();
        LOAD_FAILS.clear();
        INITS.clear();
        LOADS.clear();
        EVENTS.clear();
        QUERIES.clear();
        CLEANUP_DELAY.clear();
        blockCompute = new CountDownLatch(0);
        computeBlocked = new CountDownLatch(1);
    }

    static final class Node implements ProcessInitializer, ProcessLoader {
        private final String name;

        Node(String name) {
            this.name = name;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INITS.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            CompletableFuture<Void> gate = INIT_GATE.getOrDefault(name, CompletableFuture.completedFuture(null));
            return gate.thenCompose(ignored -> {
                Supplier<Throwable> failure = INIT_FAILS.get(name);
                if (failure != null) {
                    Throwable t = failure.get();
                    if (t instanceof Error error) throw error;
                    return CompletableFuture.failedFuture(t);
                }
                String dep = QUERIES.get(name);
                CompletionStage<Object> seen = dep == null ? CompletableFuture.completedFuture(null)
                        : ctx.query(dep, "q");
                return seen.thenApply(answer -> {
                    String value = SOURCE.getOrDefault(name, name) + (answer == null ? "" : "<" + answer + ">");
                    return Map.of("v", value.getBytes(StandardCharsets.UTF_8));
                });
            });
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            LOADS.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            String value = new String(props.get("v"), StandardCharsets.UTF_8);
            CompletableFuture<Void> gate = LOAD_GATE.getOrDefault(name, CompletableFuture.completedFuture(null));
            return gate.thenApply(ignored -> {
                if (LOAD_FAILS.containsKey(name)) throw new IllegalStateException("load fails (injected)");
                EVENTS.add("loaded:" + value);
                return new Process() {
                    @Override
                    public CompletionStage<?> compute(QueryableContext c, Object query) {
                        if ("block".equals(query)) {
                            computeBlocked.countDown();
                            return CompletableFuture.supplyAsync(() -> {
                                try {
                                    blockCompute.await(10, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                EVENTS.add("computed:" + value);
                                return value;
                            });
                        }
                        return CompletableFuture.completedFuture(value);
                    }

                    @Override
                    public CompletionStage<Void> cleanUp(ProcessContext c) {
                        EVENTS.add("cleanup:" + value);
                        long delay = CLEANUP_DELAY.getOrDefault(name, 0L);
                        if (delay > 0) {
                            return CompletableFuture.runAsync(() -> { },
                                    CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS));
                        }
                        return CompletableFuture.completedFuture(null);
                    }
                };
            });
        }
    }

    private static EngineConfig cfg() {
        return new EngineConfig(
                Duration.ofMillis(1500), Duration.ofSeconds(2), Duration.ofSeconds(2),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE)
                .withReinitRetryBackoff(Duration.ZERO, null); // no automatic retry unless a test wants one
    }

    private static GraphBuilder add(GraphBuilder b, String name, String... deps) {
        return b.add(name, (Supplier<ProcessInitializer>) () -> new Node(name),
                (Supplier<ProcessLoader>) () -> new Node(name), deps);
    }

    private static Graph graph(String... names) {
        var b = new GraphBuilder();
        for (String name : names) add(b, name);
        return b.build();
    }

    private static Engine engine(EngineConfig cfg, LogBackend backend, EngineObserver observer) {
        return new Engine(cfg, backend, new JavaSerializableSerDe(), observer);
    }

    private static Engine engine(LogBackend backend) {
        return engine(cfg(), backend, new EngineObserver() { });
    }

    private static Object ask(Engine e, String name) throws Exception {
        return e.queryProcess(name, "q").toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static EngineReport.NodeReport node(Engine e, String name) throws Exception {
        return e.introspect().toCompletableFuture().get().graph().nodes().stream()
                .filter(n -> n.name().equals(name)).findFirst().orElseThrow();
    }

    private static int inits(String name) {
        return INITS.getOrDefault(name, new AtomicInteger()).get();
    }

    private static int loads(String name) {
        return LOADS.getOrDefault(name, new AtomicInteger()).get();
    }

    private static List<LogEvent> events(LogBackend backend) {
        return List.of(backend.getBetween(0, backend.length()));
    }

    private static String replacement(Engine e, String name) throws Exception {
        String phase = node(e, name).replacement();
        return phase == null ? "none" : phase;
    }

    // ───────────────── serving during a re-init ─────────────────

    @Test
    @Timeout(30)
    void queries_during_a_reinit_are_answered_by_the_old_version() throws Exception {
        SOURCE.put("A", "v1");
        try (var e = engine(new InMemoryLogBackend())) {
            e.newGraph(graph("A"));
            SOURCE.put("A", "v2");
            INIT_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "A")));
            for (int i = 0; i < 5; i++) {
                assertThat(e.queryProcess("A", "q").toCompletableFuture().get(500, TimeUnit.MILLISECONDS))
                        .as("answered at once, by the old version").isEqualTo("v1");
            }
            LOAD_GATE.put("A", new CompletableFuture<>());
            INIT_GATE.get("A").complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
            assertThat(ask(e, "A")).as("still the old version while the new one loads").isEqualTo("v1");
            LOAD_GATE.get("A").complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
        }
    }

    @Test
    @Timeout(30)
    void a_reinit_is_visible_in_introspection() throws Exception {
        SOURCE.put("A", "v1");
        try (var e = engine(new InMemoryLogBackend())) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            INIT_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "A")));
            var during = node(e, "A");
            assertThat(during.state()).isEqualTo("Serving");
            assertThat(during.sid()).as("the version answering").isEqualTo(old);
            assertThat(during.stale()).isTrue();
            LOAD_GATE.put("A", new CompletableFuture<>());
            INIT_GATE.get("A").complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
            assertThat(node(e, "A").sid()).isEqualTo(old);
            LOAD_GATE.get("A").complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> !node(e, "A").sid().equals(old));
            var after = node(e, "A");
            assertThat(after.replacement()).isNull();
            assertThat(after.stale()).isFalse();
        }
    }

    @Test
    @Timeout(30)
    void the_switch_moves_new_queries_to_the_new_version_and_drains_old_computes() throws Exception {
        SOURCE.put("A", "v1");
        try (var e = engine(new InMemoryLogBackend())) {
            e.newGraph(graph("A"));
            blockCompute = new CountDownLatch(1);
            var inFlight = e.queryProcess("A", "block").toCompletableFuture();
            assertThat(computeBlocked.await(5, TimeUnit.SECONDS)).isTrue();

            SOURCE.put("A", "v2");
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
            assertThat(EVENTS).as("the old version is not cleaned up under a running compute")
                    .doesNotContain("cleanup:v1");

            blockCompute.countDown();
            assertThat(inFlight.get(5, TimeUnit.SECONDS)).as("the old compute finishes on the old version")
                    .isEqualTo("v1");
            await().atMost(Duration.ofSeconds(5)).until(() -> EVENTS.contains("cleanup:v1"));
            assertThat(EVENTS.indexOf("computed:v1")).isLessThan(EVENTS.indexOf("cleanup:v1"));
        }
    }

    @Test
    @Timeout(30)
    void old_version_cleanup_runs_after_the_new_version_serves() throws Exception {
        SOURCE.put("A", "v1");
        try (var e = engine(new InMemoryLogBackend())) {
            e.newGraph(graph("A"));
            SOURCE.put("A", "v2");
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> EVENTS.contains("cleanup:v1"));
            assertThat(EVENTS).containsSubsequence("loaded:v1", "loaded:v2", "cleanup:v1");
            assertThat(ask(e, "A")).isEqualTo("v2");
        }
    }

    @Test
    @Timeout(30)
    void the_log_records_the_new_version_before_retiring_the_old_one() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            SOURCE.put("A", "v2");
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> events(backend).stream()
                    .anyMatch(ev -> ev instanceof LogCleanedUp c && c.sid().equals(old)));
            Sid fresh = node(e, "A").sid();

            List<String> order = new ArrayList<>();
            for (LogEvent ev : events(backend)) {
                if (ev instanceof LogInitialized i && i.sid().equals(fresh)) {
                    assertThat(i.replaces()).as("the new version names the one it replaces").isEqualTo(old);
                    order.add("initialized(new)");
                } else if (ev instanceof LogLoaded l && l.sid().equals(fresh)) {
                    order.add("loaded(new)");
                } else if (ev instanceof LogDead d && d.sid().equals(old)) {
                    order.add("dead(old)");
                } else if (ev instanceof LogCleanedUp c && c.sid().equals(old)) {
                    order.add("cleanedUp(old)");
                }
            }
            assertThat(order).containsExactly("initialized(new)", "loaded(new)", "dead(old)", "cleanedUp(old)");
        }
    }

    // ───────────────── failures ─────────────────

    @Test
    @Timeout(30)
    void a_failed_reinit_keeps_serving_the_old_state() throws Exception {
        var failed = new CopyOnWriteArrayList<String>();
        var observer = new EngineObserver() {
            @Override
            public void onReinitFailed(String processName, Sid keptSid, Throwable cause) {
                failed.add(processName + "@" + keptSid.clock() + ": " + cause.getClass().getSimpleName());
            }
        };
        SOURCE.put("A", "v1");
        try (var e = engine(cfg(), new InMemoryLogBackend(), observer)) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            INIT_FAILS.put("A", () -> new IllegalStateException("upstream down (injected)"));
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(10)).until(() -> !failed.isEmpty());

            assertThat(failed).containsExactly("A@" + old.clock() + ": InitializationTimeoutException");
            var report = node(e, "A");
            assertThat(report.state()).isEqualTo("Serving");
            assertThat(report.sid()).isEqualTo(old);
            assertThat(report.stale()).as("the asked-for re-init has not happened").isTrue();
            assertThat(report.lastException()).contains("InitializationTimeoutException");
            assertThat(ask(e, "A")).isEqualTo("v1");
        }
    }

    @Test
    @Timeout(30)
    void an_oom_during_reinit_keeps_serving_the_old_state() throws Exception {
        SOURCE.put("A", "v1");
        var cfg = cfg().withReinitRetryBackoff(Duration.ofMillis(300), Duration.ofSeconds(1));
        try (var e = engine(cfg, new InMemoryLogBackend(), new EngineObserver() { })) {
            e.newGraph(graph("A"));
            INIT_FAILS.put("A", () -> new OutOfMemoryError("Java heap space (injected)"));
            SOURCE.put("A", "v2");
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(10)).until(() -> node(e, "A").stale()
                    && node(e, "A").replacement() == null && node(e, "A").lastException() != null);
            assertThat(node(e, "A").state()).isEqualTo("Serving");
            assertThat(ask(e, "A")).isEqualTo("v1");

            // Transient: once memory is back, the automatic retry gets there without another trigger.
            INIT_FAILS.remove("A");
            await().atMost(Duration.ofSeconds(10)).until(() -> "v2".equals(ask(e, "A")));
            assertThat(node(e, "A").stale()).isFalse();
        }
    }

    @Test
    @Timeout(30)
    void a_failed_reinit_is_retried_automatically_with_backoff() throws Exception {
        SOURCE.put("A", "v1");
        var cfg = cfg().withInitTimeout(Duration.ofMillis(300))
                .withReinitRetryBackoff(Duration.ofMillis(200), Duration.ofMillis(400));
        try (var e = engine(cfg, new InMemoryLogBackend(), new EngineObserver() { })) {
            e.newGraph(graph("A"));
            INIT_FAILS.put("A", () -> new IllegalStateException("upstream down (injected)"));
            SOURCE.put("A", "v2");
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> node(e, "A").stale() && node(e, "A").replacement() == null);
            int first = inits("A");
            await().atMost(Duration.ofSeconds(5)).until(() -> inits("A") > first); // retried by itself
            INIT_FAILS.remove("A");
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
        }
    }

    @Test
    @Timeout(30)
    void a_new_trigger_supersedes_a_scheduled_retry() throws Exception {
        SOURCE.put("A", "v1");
        var cfg = cfg().withInitTimeout(Duration.ofMillis(300))
                .withReinitRetryBackoff(Duration.ofSeconds(4), Duration.ofSeconds(4));
        try (var e = engine(cfg, new InMemoryLogBackend(), new EngineObserver() { })) {
            e.newGraph(graph("A"));
            INIT_FAILS.put("A", () -> new IllegalStateException("upstream down (injected)"));
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> node(e, "A").stale() && node(e, "A").replacement() == null);
            INIT_FAILS.remove("A");
            SOURCE.put("A", "v2");
            long before = System.nanoTime();
            e.trigger("A", "again");
            await().atMost(Duration.ofSeconds(2)).until(() -> "v2".equals(ask(e, "A")));
            assertThat(Duration.ofNanos(System.nanoTime() - before)).as("not held back by the retry's backoff")
                    .isLessThan(Duration.ofSeconds(2));
            int settled = inits("A");
            Thread.sleep(6_200); // past the superseded retry's due time (4s with jitter: at most 6s after the failure)
            assertThat(inits("A")).as("the superseded retry does not re-init again").isEqualTo(settled);
        }
    }

    @Test
    @Timeout(30)
    void a_permanent_failure_is_not_retried() throws Exception {
        SOURCE.put("A", "v1");
        var cfg = cfg().withReinitRetryBackoff(Duration.ofMillis(100), Duration.ofMillis(200));
        try (var e = engine(cfg, new InMemoryLogBackend(), new EngineObserver() { })) {
            e.newGraph(graph("A"));
            QUERIES.put("A", "Nowhere"); // an undeclared dependency: no retry can grow it
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> node(e, "A").stale() && node(e, "A").replacement() == null);
            int settled = inits("A");
            Thread.sleep(800);
            assertThat(inits("A")).isEqualTo(settled);
            assertThat(node(e, "A").lastException()).contains("UndeclaredDependencyException");
            assertThat(ask(e, "A")).isEqualTo("v1");
        }
    }

    @Test
    @Timeout(30)
    void a_failed_reinit_is_retried_at_next_start() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            INIT_FAILS.put("A", () -> new IllegalStateException("upstream down (injected)"));
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> node(e, "A").stale() && node(e, "A").replacement() == null);
        }
        INIT_FAILS.remove("A");
        SOURCE.put("A", "v2");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
        }
    }

    // ───────────────── restarts at each crash point ─────────────────

    private static Sid liveSid(InMemoryLogBackend backend, String name) {
        Sid sid = null;
        for (LogEvent ev : events(backend)) {
            if (ev instanceof LogInitialized i && i.processName().equals(name)) sid = i.sid();
        }
        return sid;
    }

    @Test
    @Timeout(30)
    void a_crash_before_the_new_version_is_written_serves_the_old_one_and_replays_the_reinit() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
        }
        // The trigger was recorded; the engine died before the new version's LogInitialized.
        String leader = backend.introspect().currentLeader();
        backend.append(new LogTrigger(0, System.currentTimeMillis(), List.of("A")), leader);

        SOURCE.put("A", "v2");
        INIT_GATE.put("A", new CompletableFuture<>());
        try (var e = engine(backend)) {
            e.newGraph(graph("A")); // returns with the old version serving
            assertThat(ask(e, "A")).isEqualTo("v1");
            INIT_GATE.get("A").complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
        }
    }

    @Test
    @Timeout(30)
    void a_crash_after_the_new_version_is_written_loads_it_without_a_new_init() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
        }
        Sid old = liveSid(backend, "A");
        String leader = backend.introspect().currentLeader();
        backend.append(new LogInitialized(0, System.currentTimeMillis(), "A",
                Map.of("v", "v2".getBytes(StandardCharsets.UTF_8)), old), leader);

        int initsBefore = inits("A");
        LOAD_GATE.put("A", new CompletableFuture<>());
        LOAD_GATE.get("A").complete(null); // the first (old) load is free
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
            assertThat(inits("A")).as("the persisted candidate is loaded, not initialised again")
                    .isEqualTo(initsBefore);
            assertThat(EVENTS).containsSubsequence("loaded:v1", "loaded:v2", "cleanup:v1");
        }
    }

    @Test
    @Timeout(30)
    void a_candidate_that_cannot_be_loaded_at_restart_is_replaced_by_a_fresh_init() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
        }
        Sid old = liveSid(backend, "A");
        String leader = backend.introspect().currentLeader();
        Sid candidate = backend.append(new LogInitialized(0, System.currentTimeMillis(), "A",
                Map.of("v", "broken".getBytes(StandardCharsets.UTF_8)), old), leader).map(ev -> ((LogInitialized) ev).sid()).orElseThrow();

        SOURCE.put("A", "v3");
        try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() {
            @Override
            public void onLoadStarted(String processName, Sid sid, int attempt) {
                if (sid.equals(candidate)) LOAD_FAILS.put("A", true);
                else LOAD_FAILS.remove("A");
            }
        })) {
            e.newGraph(graph("A"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "v3".equals(ask(e, "A")));
            assertThat(events(backend)).as("the unloadable candidate is retired")
                    .anyMatch(ev -> ev instanceof LogDead d && d.sid().equals(candidate));
        }
    }

    @Test
    @Timeout(30)
    void a_crash_after_the_new_version_loaded_warm_loads_it() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
        }
        Sid old = liveSid(backend, "A");
        String leader = backend.introspect().currentLeader();
        Sid fresh = backend.append(new LogInitialized(0, System.currentTimeMillis(), "A",
                Map.of("v", "v2".getBytes(StandardCharsets.UTF_8)), old), leader).map(ev -> ((LogInitialized) ev).sid()).orElseThrow();
        backend.append(new LogLoaded(0, System.currentTimeMillis(), fresh), leader);
        // ...and died before LogDead(old).

        int initsBefore = inits("A");
        EVENTS.clear();
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            assertThat(ask(e, "A")).isEqualTo("v2");
            assertThat(node(e, "A").sid()).isEqualTo(fresh);
            assertThat(inits("A")).isEqualTo(initsBefore);
            assertThat(EVENTS).doesNotContain("loaded:v1");
        }
    }

    @Test
    @Timeout(30)
    void a_consumer_of_a_changed_producer_serves_its_old_state_at_start_and_is_rebuilt_in_the_background()
            throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("P", "p1");
        SOURCE.put("C", "c");
        QUERIES.put("C", "P");
        java.util.function.Supplier<Graph> graph = () -> add(add(new GraphBuilder(), "P"), "C", "P").build();
        try (var e = engine(backend)) {
            e.newGraph(graph.get());
        }
        // The producer was re-initialised and the engine died before the cascade reached C.
        String leader = backend.introspect().currentLeader();
        backend.append(new LogInitialized(0, System.currentTimeMillis(), "P",
                Map.of("v", "p2".getBytes(StandardCharsets.UTF_8))), leader);

        INIT_GATE.put("C", new CompletableFuture<>());
        try (var e = engine(backend)) {
            e.newGraph(graph.get());
            assertThat(ask(e, "C")).as("served at once, from the state it has").isEqualTo("c<p1>");
            INIT_GATE.get("C").complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> "c<p2>".equals(ask(e, "C")));
        }
    }

    @Test
    @Timeout(30)
    void a_consumer_of_a_changed_producer_cold_inits_at_start_under_release_first() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("P", "p1");
        SOURCE.put("C", "c");
        QUERIES.put("C", "P");
        java.util.function.Supplier<Graph> graph = () -> add(add(new GraphBuilder(), "P"), "C", "P").build();
        try (var e = engine(backend)) {
            e.newGraph(graph.get());
        }
        String leader = backend.introspect().currentLeader();
        backend.append(new LogInitialized(0, System.currentTimeMillis(), "P",
                Map.of("v", "p2".getBytes(StandardCharsets.UTF_8))), leader);
        try (var e = engine(cfg().withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend, new EngineObserver() { })) {
            e.newGraph(graph.get());
            assertThat(ask(e, "C")).isEqualTo("c<p2>");
        }
    }

    @Test
    @Timeout(30)
    void close_mid_reinit_keeps_the_candidate_for_the_next_start() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            SOURCE.put("A", "v2");
            LOAD_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
        }
        LOAD_GATE.clear();
        int initsBefore = inits("A");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
            assertThat(inits("A")).isEqualTo(initsBefore);
        }
    }

    @Test
    @Timeout(30)
    void snapshot_mid_reinit_keeps_both_incumbent_and_candidate() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            SOURCE.put("A", "v2");
            LOAD_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
            e.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);

            var inits = events(backend).stream().filter(ev -> ev instanceof LogInitialized)
                    .map(ev -> (LogInitialized) ev).toList();
            assertThat(inits).hasSize(2);
            assertThat(inits.get(0).sid()).isEqualTo(old);
            assertThat(inits.get(1).replaces()).isEqualTo(old);
            var scan = LogCompaction.scan(backend);
            assertThat(scan.liveInits().get("A").init().sid()).isEqualTo(old);
            assertThat(scan.candidates()).containsKey("A");

            LOAD_GATE.get("A").complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
        }
    }

    // ───────────────── cancelInit ─────────────────

    @Test
    @Timeout(30)
    void cancel_init_by_name_cancels_only_the_replacement() throws Exception {
        SOURCE.put("A", "v1");
        var cfg = cfg().withReinitRetryBackoff(Duration.ofMillis(100), Duration.ofMillis(200));
        try (var e = engine(cfg, new InMemoryLogBackend(), new EngineObserver() { })) {
            e.newGraph(graph("A"));
            var gate = new CompletableFuture<Void>();
            INIT_GATE.put("A", gate);
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "A")));
            e.cancelInit("A").toCompletableFuture().get(5, TimeUnit.SECONDS);

            var report = node(e, "A");
            assertThat(report.state()).isEqualTo("Serving");
            assertThat(report.replacement()).isNull();
            assertThat(report.stale()).isTrue();
            assertThat(ask(e, "A")).isEqualTo("v1");
            assertThat(e.fsmOf("A").reinitRetryPending()).as("an operator's cancel is not retried").isFalse();
        }
    }

    @Test
    @Timeout(30)
    void cancel_init_by_sid_matches_the_candidate_and_leaves_the_incumbent_alone() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            LOAD_GATE.put("A", new CompletableFuture<>());
            SOURCE.put("A", "v2");
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));

            e.cancelInit(old).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(replacement(e, "A")).as("the incumbent's Sid cancels nothing").isEqualTo("Loading");

            Sid candidate = liveSid(backend, "A");
            assertThat(candidate).isNotEqualTo(old);
            e.cancelInit(candidate).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(replacement(e, "A")).isEqualTo("none");
            assertThat(node(e, "A").sid()).isEqualTo(old);
            assertThat(node(e, "A").stale()).isTrue();
            assertThat(ask(e, "A")).isEqualTo("v1");
            await("the unloaded candidate is retired").atMost(Duration.ofSeconds(5)).until(() ->
                    events(backend).stream().anyMatch(ev -> ev instanceof LogDead d && d.sid().equals(candidate)));
        }
    }

    // ───────────────── pause / remove / leadership ─────────────────

    @Test
    @Timeout(30)
    void removing_a_node_mid_reinit_retires_the_incumbent_and_the_candidate() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("Keep", "A"));
            Sid old = node(e, "A").sid();
            LOAD_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
            Sid candidate = liveSid(backend, "A");
            e.remove(List.of("A"));
            assertThat(events(backend)).anyMatch(ev -> ev instanceof LogDead d && d.sid().equals(old));
            assertThat(events(backend)).anyMatch(ev -> ev instanceof LogDead d && d.sid().equals(candidate));
        }
        LOAD_GATE.clear();
        int before = inits("A");
        try (var e = engine(backend)) {
            e.newGraph(graph("Keep", "A"));
            assertThat(inits("A")).as("added back with a fresh init").isEqualTo(before + 1);
        }
    }

    @Test
    @Timeout(30)
    void pause_and_resume_mid_load_finish_the_replacement_without_a_second_init() throws Exception {
        SOURCE.put("A", "v1");
        try (var e = engine(new InMemoryLogBackend())) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            SOURCE.put("A", "v2");
            LOAD_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
            e.pause(List.of("A"));
            assertThat(node(e, "A").sid()).as("the pause records the incumbent").isEqualTo(old);
            assertThat(node(e, "A").stale()).as("the candidate carries the re-init").isFalse();
            LOAD_GATE.clear();
            int before = inits("A");
            e.resume(List.of("A"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
            assertThat(inits("A")).isEqualTo(before);
        }
    }

    /** Refuses every append once {@code refuse} accepts it, like a log another instance took over. */
    static final class TakenOverBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        volatile Predicate<LogEvent> refuse = ev -> false;

        @Override
        public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
            if (refuse.test(event)) return Optional.empty();
            return delegate.append(event, leaderInstanceId);
        }

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String id) { return delegate.compact(events, id); }
        @Override public void close() { delegate.close(); }
    }

    @Test
    @Timeout(30)
    void leadership_lost_mid_reinit_keeps_serving_and_drops_later_requests() throws Exception {
        var failed = new CopyOnWriteArrayList<Throwable>();
        var observer = new EngineObserver() {
            @Override
            public void onReinitFailed(String processName, Sid keptSid, Throwable cause) {
                failed.add(cause);
            }
        };
        var backend = new TakenOverBackend();
        SOURCE.put("A", "v1");
        var cfg = cfg().withReinitRetryBackoff(Duration.ofMillis(100), Duration.ofMillis(200));
        try (var e = engine(cfg, backend, observer)) {
            e.newGraph(graph("A"));
            backend.refuse = ev -> ev instanceof LogInitialized;
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> !failed.isEmpty());
            assertThat(failed.get(0)).isInstanceOf(io.fom.api.LeadershipLostException.class);
            assertThat(ask(e, "A")).isEqualTo("v1");

            int settled = inits("A");
            e.trigger("A", "again");
            Thread.sleep(600);
            assertThat(inits("A")).as("a later request is dropped without an init").isEqualTo(settled);
            assertThat(failed).as("not retried").hasSize(1);
            assertThat(ask(e, "A")).isEqualTo("v1");
        }
    }

    // ───────────────── cascade ─────────────────

    @Test
    @Timeout(30)
    void a_consumer_mid_reinit_gets_its_dependency_change_and_reinits_again_after() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("P", "p1");
        SOURCE.put("C", "c");
        QUERIES.put("C", "P");
        try (var e = engine(backend)) {
            e.newGraph(add(add(new GraphBuilder(), "P"), "C", "P").build());
            assertThat(ask(e, "C")).isEqualTo("c<p1>");
            Sid consumerOld = node(e, "C").sid();

            var gate = new CompletableFuture<Void>();
            INIT_GATE.put("C", gate);
            e.trigger("C", "own");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "C")));
            SOURCE.put("P", "p2");
            e.trigger("P", "changed");
            await().atMost(Duration.ofSeconds(5)).until(() -> events(backend).stream().anyMatch(ev ->
                    ev instanceof LogDependencyChanged d && d.sid().equals(consumerOld)));
            int initsBefore = inits("C");
            INIT_GATE.remove("C");
            gate.complete(null); // C's own re-init finishes (it may have seen p1 or p2) ...
            // ... and the dependency change runs one more cycle after it, so C ends on p2.
            await().atMost(Duration.ofSeconds(5)).until(() -> "c<p2>".equals(ask(e, "C")));
            Settle.awaitNoReinit(e);
            assertThat(inits("C")).isGreaterThan(initsBefore);
            assertThat(ask(e, "C")).isEqualTo("c<p2>");
        }
    }

    @Test
    @Timeout(30)
    void observer_callbacks_follow_the_replacement_in_order() throws Exception {
        var calls = new CopyOnWriteArrayList<String>();
        var observer = new EngineObserver() {
            @Override public void onReinitStarted(String p, Sid s) { calls.add("reinitStarted(" + s.clock() + ")"); }
            @Override public void onInitStarted(String p, int a) { calls.add("initStarted"); }
            @Override public void onInitCompleted(String p, Sid s, Duration d) { calls.add("initCompleted(" + s.clock() + ")"); }
            @Override public void onLoadStarted(String p, Sid s, int a) { calls.add("loadStarted(" + s.clock() + ")"); }
            @Override public void onLoadCompleted(String p, Sid s, Duration d) { calls.add("loadCompleted(" + s.clock() + ")"); }
            @Override public void onSidPromotion(String p, Sid prev, Sid next) {
                calls.add("promotion(" + (prev == null ? "-" : prev.clock()) + "->" + next.clock() + ")");
            }
            @Override public void onCleanupCompleted(String p, Sid s, boolean ok, Duration d) { calls.add("cleanup(" + s.clock() + ")"); }
            @Override public void onStateTransition(String p, String from, String to) { calls.add(from + "->" + to); }
        };
        SOURCE.put("A", "v1");
        try (var e = engine(cfg(), new InMemoryLogBackend(), observer)) {
            e.newGraph(graph("A"));
            long old = node(e, "A").sid().clock();
            calls.clear();
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> calls.stream().anyMatch(c -> c.startsWith("cleanup")));
            long fresh = node(e, "A").sid().clock();
            assertThat(calls).containsExactly(
                    "reinitStarted(" + old + ")",
                    "initStarted",
                    "initCompleted(" + fresh + ")",
                    "loadStarted(" + fresh + ")",
                    "loadCompleted(" + fresh + ")",
                    "promotion(" + old + "->" + fresh + ")",
                    "cleanup(" + old + ")");
        }
    }

    // ───────────────── RELEASE_FIRST ─────────────────

    @Test
    @Timeout(30)
    void release_first_strategy_keeps_the_legacy_order() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(cfg().withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend, new EngineObserver() { })) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            SOURCE.put("A", "v2");
            INIT_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(node(e, "A").state()));
            assertThat(EVENTS).as("the old version is released first").contains("cleanup:v1");
            var waiting = e.queryProcess("A", "q").toCompletableFuture();
            Thread.sleep(200);
            assertThat(waiting).as("queries wait for the new version").isNotDone();
            INIT_GATE.get("A").complete(null);
            assertThat(waiting.get(5, TimeUnit.SECONDS)).isEqualTo("v2");

            int deadAt = -1;
            int newInitAt = -1;
            var all = events(backend);
            for (int i = 0; i < all.size(); i++) {
                if (all.get(i) instanceof LogDead d && d.sid().equals(old)) deadAt = i;
                if (all.get(i) instanceof LogInitialized init && init.clock() > old.clock()) {
                    newInitAt = i;
                    assertThat(init.replaces()).isNull();
                }
            }
            assertThat(deadAt).isNotNegative().isLessThan(newInitAt);

            // A failed RELEASE_FIRST re-init leaves the node Dead.
            INIT_GATE.clear();
            INIT_FAILS.put("A", () -> new IllegalStateException("upstream down (injected)"));
            e.trigger("A", "fail");
            await().atMost(Duration.ofSeconds(10)).until(() -> "Dead".equals(node(e, "A").state()));
        }
    }

    @Test
    @Timeout(30)
    void a_node_can_override_the_strategy_and_the_override_is_not_a_definition_change() throws Exception {
        SOURCE.put("A", "v1");
        SOURCE.put("B", "w1");
        java.util.function.Function<ReinitStrategy, Graph> graph = strategy ->
                add(add(new GraphBuilder(), "A").reinitStrategy(strategy), "B").build();
        try (var e = engine(new InMemoryLogBackend())) {
            e.newGraph(graph.apply(ReinitStrategy.RELEASE_FIRST));
            int initsBefore = inits("A");
            assertThat(e.newGraph(graph.apply(ReinitStrategy.KEEP_OLD)))
                    .as("only the strategy changed: nothing to restart").isFalse();
            assertThat(inits("A")).isEqualTo(initsBefore);
            assertThat(GraphDiff.compute(graph.apply(ReinitStrategy.RELEASE_FIRST), graph.apply(ReinitStrategy.KEEP_OLD))
                    .hasChanges()).isFalse();

            // The running node took the new strategy: a re-init keeps the old version serving.
            INIT_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "A")));
            assertThat(ask(e, "A")).isEqualTo("v1");
            INIT_GATE.get("A").complete(null);

            // And back: RELEASE_FIRST for A only, B keeps the engine's KEEP_OLD.
            e.newGraph(graph.apply(ReinitStrategy.RELEASE_FIRST));
            Settle.awaitNoReinit(e);
            INIT_GATE.put("A", new CompletableFuture<>());
            INIT_GATE.put("B", new CompletableFuture<>());
            e.trigger("A", "go");
            e.trigger("B", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(node(e, "A").state())
                    && "Initializing".equals(replacement(e, "B")));
            INIT_GATE.values().forEach(g -> g.complete(null));
        }
    }

    // ───────────────── review fixes ─────────────────

    /** Holds every append that {@code slow} accepts for {@code delayMs}, like a log under load. */
    static final class SlowBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        volatile Predicate<LogEvent> slow = ev -> false;
        final long delayMs;

        SlowBackend(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
            if (slow.test(event)) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.append(event, leaderInstanceId);
        }

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String id) { return delegate.compact(events, id); }
        @Override public void close() { delegate.close(); }
    }

    @Test
    @Timeout(60)
    void a_slow_log_does_not_hold_up_queries_during_a_reinit() throws Exception {
        var backend = new SlowBackend(1_500);
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            backend.slow = ev -> ev instanceof LogInitialized || ev instanceof LogLoaded
                    || ev instanceof LogDead || ev instanceof LogCleanedUp;
            SOURCE.put("A", "v2");
            e.trigger("A", "go");
            var answers = new java.util.LinkedHashSet<Object>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!events(backend.delegate).stream().anyMatch(ev -> ev instanceof LogCleanedUp c && c.sid().equals(old))) {
                assertThat(System.nanoTime()).as("the re-init finishes").isLessThan(deadline);
                long start = System.nanoTime();
                answers.add(e.queryProcess("A", "q").toCompletableFuture().get(400, TimeUnit.MILLISECONDS));
                assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(400));
                Thread.sleep(50);
            }
            assertThat(answers).as("answered by the old version, then by the new one").containsExactly("v1", "v2");
            Sid fresh = node(e, "A").sid();
            List<String> order = new ArrayList<>();
            for (LogEvent ev : events(backend.delegate)) {
                if (ev instanceof LogInitialized i && i.sid().equals(fresh)) order.add("initialized(new)");
                if (ev instanceof LogLoaded l && l.sid().equals(fresh)) order.add("loaded(new)");
                if (ev instanceof LogDead d && d.sid().equals(old)) order.add("dead(old)");
                if (ev instanceof LogCleanedUp c && c.sid().equals(old)) order.add("cleanedUp(old)");
            }
            assertThat(order).containsExactly("initialized(new)", "loaded(new)", "dead(old)", "cleanedUp(old)");
        }
    }

    @Test
    @Timeout(30)
    void release_first_at_restart_does_not_load_a_keep_old_candidate_beside_the_old_state() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            SOURCE.put("A", "v2");
            LOAD_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
        }
        assertThat(LogCompaction.scan(backend).candidates()).containsKey("A");
        LOAD_GATE.clear();
        EVENTS.clear();
        SOURCE.put("A", "v3");
        try (var e = engine(cfg().withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend, new EngineObserver() { })) {
            e.newGraph(graph("A"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "v3".equals(ask(e, "A")));
            assertThat(EVENTS).as("the unfinished re-init starts afresh; no two versions are loaded")
                    .containsExactly("loaded:v3");
            assertThat(LogCompaction.scan(backend).candidates()).doesNotContainKey("A");
        }
    }

    @Test
    @Timeout(30)
    void cancel_init_starts_a_request_queued_behind_the_cancelled_replacement() throws Exception {
        SOURCE.put("A", "v1");
        try (var e = engine(new InMemoryLogBackend())) {
            e.newGraph(graph("A"));
            INIT_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "A")));
            e.trigger("A", "again"); // queued behind the running replacement
            await().atMost(Duration.ofSeconds(5)).until(() -> e.fsmOf("A").reinitQueued());
            INIT_GATE.remove("A");
            SOURCE.put("A", "v2");
            e.cancelInit("A").toCompletableFuture().get(5, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
        }
    }

    @Test
    @Timeout(30)
    void cancel_init_also_cancels_a_scheduled_retry() throws Exception {
        SOURCE.put("A", "v1");
        var cfg = cfg().withInitTimeout(Duration.ofMillis(300))
                .withReinitRetryBackoff(Duration.ofMillis(800), Duration.ofMillis(800));
        try (var e = engine(cfg, new InMemoryLogBackend(), new EngineObserver() { })) {
            e.newGraph(graph("A"));
            INIT_FAILS.put("A", () -> new IllegalStateException("upstream down (injected)"));
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> node(e, "A").stale() && node(e, "A").replacement() == null);
            int settled = inits("A");
            e.cancelInit("A").toCompletableFuture().get(5, TimeUnit.SECONDS);
            INIT_FAILS.remove("A");
            assertThat(e.fsmOf("A").reinitRetryPending()).as("the scheduled retry is cancelled").isFalse();
            assertThat(inits("A")).isEqualTo(settled);
            assertThat(node(e, "A").stale()).isTrue();
            assertThat(ask(e, "A")).isEqualTo("v1");
        }
    }

    @Test
    @Timeout(30)
    void lost_leadership_is_reported_once_per_serving_version() throws Exception {
        var initFailures = new AtomicInteger();
        var observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                initFailures.incrementAndGet();
            }
        };
        var backend = new TakenOverBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(cfg(), backend, observer)) {
            e.newGraph(graph("A"));
            backend.refuse = ev -> ev instanceof LogInitialized;
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> initFailures.get() > 0);
            e.trigger("A", "again");
            Thread.sleep(500);
            assertThat(initFailures).hasValue(1);
        }
    }

    @Test
    @Timeout(30)
    void pausing_mid_reinit_reports_the_serving_state_throughout_and_a_removal_retires_it() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        var gate = new CompletableFuture<Void>();
        try (var e = engine(backend)) {
            e.newGraph(graph("Keep", "A"));
            Sid old = node(e, "A").sid();
            INIT_GATE.put("A", gate);
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "A")));
            blockCompute = new CountDownLatch(1);
            var inFlight = e.queryProcess("A", "block").toCompletableFuture(); // holds the stop in its drain
            assertThat(computeBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            var pausing = CompletableFuture.runAsync(() -> e.pause(List.of("A")));
            await().atMost(Duration.ofSeconds(5)).until(() -> "Paused".equals(node(e, "A").state()));
            assertThat(pausing).as("still stopping").isNotDone();
            assertThat(node(e, "A").sid()).as("the incumbent is still the live state").isEqualTo(old);
            blockCompute.countDown();
            pausing.get(10, TimeUnit.SECONDS);
            assertThat(inFlight.get(5, TimeUnit.SECONDS)).isEqualTo("v1");
            assertThat(node(e, "A").sid()).isEqualTo(old);
            assertThat(node(e, "A").stale()).isTrue();
            e.remove(List.of("A"));
            assertThat(events(backend)).anyMatch(ev -> ev instanceof LogDead d && d.sid().equals(old));
        }
        INIT_GATE.clear();
        gate.complete(null);
        int before = inits("A");
        try (var e = engine(backend)) {
            e.newGraph(graph("Keep", "A"));
            assertThat(inits("A")).as("added back with a fresh init").isEqualTo(before + 1);
        }
    }

    @Test
    @Timeout(30)
    void a_request_queued_during_a_replacement_survives_a_restart() throws Exception {
        var backend = new InMemoryLogBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            var gate = new CompletableFuture<Void>();
            INIT_GATE.put("A", gate);
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "A")));
            e.trigger("A", "again"); // recorded now, older than the candidate about to be written
            await().atMost(Duration.ofSeconds(5)).until(() -> e.fsmOf("A").reinitQueued());
            SOURCE.put("A", "v2");
            LOAD_GATE.put("A", new CompletableFuture<>());
            INIT_GATE.remove("A");
            gate.complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
        }
        LOAD_GATE.clear();
        SOURCE.put("A", "v3");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "v3".equals(ask(e, "A")));
        }
    }

    @Test
    @Timeout(30)
    void a_request_queued_during_a_release_first_reinit_survives_a_restart() throws Exception {
        var backend = new InMemoryLogBackend();
        var cfg = cfg().withReinitStrategy(ReinitStrategy.RELEASE_FIRST);
        SOURCE.put("A", "v1");
        try (var e = engine(cfg, backend, new EngineObserver() { })) {
            e.newGraph(graph("A"));
            var gate = new CompletableFuture<Void>();
            INIT_GATE.put("A", gate);
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(node(e, "A").state()));
            e.trigger("A", "again");
            await().atMost(Duration.ofSeconds(5)).until(() -> e.fsmOf("A").reinitQueued());
            SOURCE.put("A", "v2");
            LOAD_GATE.put("A", new CompletableFuture<>());
            INIT_GATE.remove("A");
            gate.complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(node(e, "A").state()));
        }
        LOAD_GATE.clear();
        SOURCE.put("A", "v3");
        try (var e = engine(cfg, backend, new EngineObserver() { })) {
            e.newGraph(graph("A"));
            await().atMost(Duration.ofSeconds(5)).until(() -> "v3".equals(ask(e, "A")));
        }
    }

    @Test
    @Timeout(30)
    void an_automatic_retry_follows_the_strategy_in_force_when_it_fires() throws Exception {
        SOURCE.put("A", "v1");
        var cfg = cfg().withInitTimeout(Duration.ofMillis(300))
                .withReinitRetryBackoff(Duration.ofSeconds(2), Duration.ofSeconds(2));
        java.util.function.Function<ReinitStrategy, Graph> graph = strategy ->
                add(new GraphBuilder(), "A").reinitStrategy(strategy).build();
        try (var e = engine(cfg, new InMemoryLogBackend(), new EngineObserver() { })) {
            e.newGraph(graph.apply(ReinitStrategy.KEEP_OLD));
            INIT_FAILS.put("A", () -> new IllegalStateException("upstream down (injected)"));
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> node(e, "A").stale() && node(e, "A").replacement() == null);
            e.newGraph(graph.apply(ReinitStrategy.RELEASE_FIRST));
            INIT_FAILS.remove("A");
            var gate = new CompletableFuture<Void>();
            INIT_GATE.put("A", gate);
            await().atMost(Duration.ofSeconds(6)).until(() -> "Initializing".equals(node(e, "A").state()));
            gate.complete(null);
        }
    }

    // ───────────────── log writer ─────────────────

    /** Holds the log writer's appends that {@code hold} accepts until {@link #release()}; others pass. */
    static final class HeldBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        volatile Predicate<LogEvent> hold = ev -> false;
        volatile CompletableFuture<Void> released = new CompletableFuture<>();
        final CountDownLatch entered = new CountDownLatch(1);

        void release() {
            hold = ev -> false;
            released.complete(null);
        }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
            if (Thread.currentThread().getName().startsWith("fom-log-") && hold.test(event)) {
                entered.countDown();
                released.join();
            }
            return delegate.append(event, leaderInstanceId);
        }

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String id) { return delegate.compact(events, id); }
        @Override public void close() { delegate.close(); }
    }

    private static int indexOf(List<LogEvent> log, Predicate<LogEvent> match) {
        for (int i = 0; i < log.size(); i++) {
            if (match.test(log.get(i))) return i;
        }
        return -1;
    }

    private static Sid candidateOf(LogBackend backend, String name) {
        Sid sid = null;
        for (LogEvent ev : events(backend)) {
            if (ev instanceof LogInitialized i && i.processName().equals(name) && i.replaces() != null) sid = i.sid();
        }
        return sid;
    }

    @Test
    @Timeout(30)
    void a_cancel_that_wins_the_commit_gate_leaves_no_log_loaded() throws Exception {
        var backend = new HeldBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            SOURCE.put("A", "v2");
            LOAD_GATE.put("A", new CompletableFuture<>());
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "A")));
            Sid candidate = candidateOf(backend.delegate, "A");
            // The writer is busy (recording a request queued during the load), so the commit waits behind it.
            backend.hold = ev -> ev instanceof LogTrigger;
            e.trigger("A", "again");
            assertThat(backend.entered.await(5, TimeUnit.SECONDS)).isTrue();
            LOAD_GATE.get("A").complete(null);
            await().atMost(Duration.ofSeconds(5)).until(() -> EVENTS.contains("loaded:v2"));
            e.cancelInit(candidate).toCompletableFuture().get(5, TimeUnit.SECONDS);
            backend.release();
            await().atMost(Duration.ofSeconds(5)).until(() -> events(backend.delegate).stream()
                    .anyMatch(ev -> ev instanceof LogDead d && d.sid().equals(candidate)));
            Settle.awaitNoReinit(e);
            assertThat(events(backend.delegate)).as("the abandoned attempt never becomes live in the log")
                    .noneMatch(ev -> ev instanceof LogLoaded l && l.sid().equals(candidate));
        }
    }

    @Test
    @Timeout(30)
    void a_commit_already_writing_wins_over_a_cancel() throws Exception {
        var backend = new HeldBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            SOURCE.put("A", "v2");
            backend.hold = ev -> ev instanceof LogLoaded;
            e.trigger("A", "go");
            assertThat(backend.entered.await(5, TimeUnit.SECONDS)).isTrue();
            e.cancelInit("A").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(replacement(e, "A")).as("too late to cancel").isEqualTo("Loading");
            backend.release();
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
            assertThat(node(e, "A").stale()).isFalse();
        }
    }

    @Test
    @Timeout(30)
    void a_candidate_written_after_its_replacement_was_given_up_is_retired_before_the_next_one() throws Exception {
        var backend = new HeldBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            backend.hold = ev -> ev instanceof LogInitialized;
            e.trigger("A", "go");
            assertThat(backend.entered.await(5, TimeUnit.SECONDS)).isTrue();
            e.trigger("A", "again");
            await().atMost(Duration.ofSeconds(5)).until(() -> e.fsmOf("A").reinitQueued());
            SOURCE.put("A", "v2");
            // Gives up the first replacement while its candidate is being written; the queued request
            // starts the next one at once, and its candidate queues behind the first.
            e.cancelInit("A").toCompletableFuture().get(5, TimeUnit.SECONDS);
            backend.release();
            await().atMost(Duration.ofSeconds(5)).until(() -> "v2".equals(ask(e, "A")));
            var log = events(backend.delegate);
            List<Sid> written = log.stream().filter(ev -> ev instanceof LogInitialized i && i.replaces() != null)
                    .map(ev -> ((LogInitialized) ev).sid()).toList();
            assertThat(written).hasSize(2);
            int firstDead = indexOf(log, ev -> ev instanceof LogDead d && d.sid().equals(written.get(0)));
            int second = indexOf(log, ev -> ev instanceof LogInitialized i && i.sid().equals(written.get(1)));
            assertThat(firstDead).as("the given-up candidate is retired before the next is written")
                    .isNotNegative().isLessThan(second);
        }
    }

    @Test
    @Timeout(30)
    void a_pause_returns_only_after_the_writes_of_the_stopped_version() throws Exception {
        var backend = new HeldBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            backend.hold = ev -> ev instanceof LogInitialized;
            e.trigger("A", "go");
            assertThat(backend.entered.await(5, TimeUnit.SECONDS)).isTrue();
            var pausing = CompletableFuture.runAsync(() -> e.pause(List.of("A")));
            Thread.sleep(300); // a pause that did not wait would be done by now
            assertThat(pausing).isNotDone();
            backend.release();
            pausing.get(10, TimeUnit.SECONDS);
            Sid candidate = candidateOf(backend.delegate, "A");
            assertThat(candidate).isNotNull();
            assertThat(events(backend.delegate)).as("the stopped version's candidate is already retired")
                    .anyMatch(ev -> ev instanceof LogDead d && d.sid().equals(candidate));
            assertThat(LogCompaction.scan(backend.delegate).liveInits().get("A").init().sid()).isEqualTo(old);
            e.resume(List.of("A"));
            Settle.awaitNoReinit(e);
            assertThat(ask(e, "A")).isEqualTo("v1");
        }
    }

    @Test
    @Timeout(30)
    void a_stop_while_the_switch_is_written_records_the_promoted_version() throws Exception {
        var backend = new HeldBackend();
        SOURCE.put("A", "v1");
        try (var e = engine(backend)) {
            e.newGraph(graph("Keep", "A"));
            SOURCE.put("A", "v2");
            backend.hold = ev -> ev instanceof LogLoaded;
            e.trigger("A", "go");
            assertThat(backend.entered.await(5, TimeUnit.SECONDS)).isTrue();
            Sid candidate = candidateOf(backend.delegate, "A");
            var pausing = CompletableFuture.runAsync(() -> e.pause(List.of("A")));
            Thread.sleep(200);
            backend.release();
            pausing.get(10, TimeUnit.SECONDS);
            assertThat(node(e, "A").sid()).as("the log made the candidate live").isEqualTo(candidate);
            e.remove(List.of("A"));
        }
        int before = inits("A");
        try (var e = engine(backend.delegate)) {
            e.newGraph(graph("Keep", "A"));
            assertThat(inits("A")).as("a removed process is added back with a fresh init").isEqualTo(before + 1);
        }
    }

    @Test
    @Timeout(30)
    void close_waits_for_the_log_writer() throws Exception {
        var backend = new HeldBackend();
        SOURCE.put("A", "v1");
        var e = engine(backend);
        Sid old;
        try {
            e.newGraph(graph("A"));
            old = node(e, "A").sid();
            SOURCE.put("A", "v2");
            backend.hold = ev -> ev instanceof LogDead d && d.sid().equals(old);
            e.trigger("A", "go");
            assertThat(backend.entered.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (Exception | Error t) {
            e.close();
            throw t;
        }
        var closing = CompletableFuture.runAsync(e::close);
        Thread.sleep(300);
        assertThat(closing).isNotDone();
        backend.release();
        closing.get(10, TimeUnit.SECONDS);
        assertThat(events(backend.delegate)).as("written before close returned")
                .anyMatch(ev -> ev instanceof LogDead d && d.sid().equals(old));
    }

    @Test
    @Timeout(30)
    void close_does_not_hang_on_a_hung_log_writer() throws Exception {
        var backend = new HeldBackend();
        SOURCE.put("A", "v1");
        var e = engine(backend);
        try {
            e.newGraph(graph("A"));
            Sid old = node(e, "A").sid();
            SOURCE.put("A", "v2");
            backend.hold = ev -> ev instanceof LogDead d && d.sid().equals(old);
            e.trigger("A", "go");
            assertThat(backend.entered.await(5, TimeUnit.SECONDS)).isTrue();
            long start = System.nanoTime();
            CompletableFuture.runAsync(e::close).get(15, TimeUnit.SECONDS);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(10));
        } finally {
            backend.release();
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_replacement_that_runs_out_of_budget_logs_one_warning_and_no_error() throws Exception {
        var captured = new java.io.ByteArrayOutputStream();
        var originalErr = System.err;
        SOURCE.put("A", "v1");
        try (var e = engine(new InMemoryLogBackend())) {
            e.newGraph(graph("A"));
            INIT_FAILS.put("A", () -> new IllegalStateException("upstream down (injected)"));
            System.setErr(new java.io.PrintStream(captured, true, StandardCharsets.UTF_8));
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(10)).until(() -> node(e, "A").stale() && node(e, "A").replacement() == null);
        } finally {
            System.setErr(originalErr);
        }
        String log = captured.toString(StandardCharsets.UTF_8);
        assertThat(log).doesNotContain("giving up");
        assertThat(log).contains("re-init gave up; keeps serving").contains("ran out of its");
    }

    @Test
    @Timeout(30)
    void a_reinit_waiting_out_its_dedup_window_is_reported_stale() throws Exception {
        SOURCE.put("A", "v1");
        try (var e = engine(cfg().withDedupWindow(Duration.ofSeconds(3)), new InMemoryLogBackend(),
                new EngineObserver() { })) {
            e.newGraph(graph("A"));
            e.trigger("A", "go");
            var report = node(e, "A");
            assertThat(report.replacement()).as("not started yet").isNull();
            assertThat(report.stale()).as("but already due").isTrue();
        }
    }

    @Test
    @Timeout(30)
    void close_starts_no_reinit_while_consumers_drain() throws Exception {
        SOURCE.put("P", "p1");
        SOURCE.put("C", "c");
        var cfg = cfg().withInitTimeout(Duration.ofMillis(300))
                .withReinitRetryBackoff(Duration.ofMillis(400), Duration.ofMillis(400));
        var e = engine(cfg, new InMemoryLogBackend(), new EngineObserver() { });
        try {
            e.newGraph(add(add(new GraphBuilder(), "P"), "C", "P").build());
            INIT_FAILS.put("P", () -> new IllegalStateException("upstream down (injected)"));
            e.trigger("P", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> e.fsmOf("P").reinitRetryPending());
            INIT_FAILS.remove("P");
            CLEANUP_DELAY.put("C", 1_500L); // the consumer drains while the producer's retry comes due
        } catch (Exception | Error t) {
            e.close();
            throw t;
        }
        int before = inits("P");
        e.close();
        assertThat(inits("P")).as("no re-init started after close began").isEqualTo(before);
    }
}
