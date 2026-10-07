package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.FileLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogTrigger;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import java.util.function.Supplier;

/**
 * Regression tests for engine/log consistency bugs: snapshots losing concurrent
 * appends, stale Sids after compaction, warm restart under a changed node
 * definition, a skipped reactive cascade, a failed graph swap that could not be
 * retried, and non-atomic in-memory compaction.
 */
class EngineLogConsistencyTest {

    private static EngineConfig cfg(Duration initTimeout) {
        return new EngineConfig(
                initTimeout, Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static EngineConfig cfg() {
        return cfg(Duration.ofSeconds(5));
    }

    // ───────────────── snapshots ─────────────────

    static final AtomicInteger GEN = new AtomicInteger();

    static final class GenInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            int g = GEN.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("gen", ByteBuffer.allocate(4).putInt(g).array()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            int g = ByteBuffer.wrap(properties.get("gen")).getInt();
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("gen" + g));
        }
    }

    private static Graph genGraph() {
        return new GraphBuilder()
                .add("A", (Supplier<ProcessInitializer>) GenInit::new,
                        (Supplier<ProcessLoader>) GenInit::new)
                .handles(String.class)
                .build();
    }

    private static void awaitGen(Engine e, int g) {
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            try {
                return ("gen" + g).equals(e.query("x").toCompletableFuture().get(2, TimeUnit.SECONDS));
            } catch (Exception ex) {
                return false;
            }
        });
    }

    @Test
    @Timeout(30)
    void reinit_after_compaction_survives_a_restart(@TempDir Path tmp) throws Exception {
        GEN.set(0);
        Path file = tmp.resolve("fom.log");
        try (LogBackend b = new FileLogBackend(file);
             Engine e = new Engine(cfg(), b, new JavaSerializableSerDe())) {
            e.newGraph(genGraph());
            for (int g = 2; g <= 6; g++) {
                e.trigger("A", "t" + g);
                awaitGen(e, g);
            }
            e.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
            e.trigger("A", "t7");
            awaitGen(e, 7);
        }
        int initsBefore = GEN.get();
        try (LogBackend b = new FileLogBackend(file);
             Engine e = new Engine(cfg(), b, new JavaSerializableSerDe())) {
            e.newGraph(genGraph());
            assertThat(e.query("x").toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("gen7");
        }
        assertThat(GEN.get()).as("restart must warm-load, not re-run init").isEqualTo(initsBefore);
    }

    /** Sleeps inside compact() to widen the scan→compact window a snapshot used to leave open. */
    static final class SlowCompactBackend implements LogBackend {
        final LogBackend delegate;
        final CountDownLatch compacting = new CountDownLatch(1);

        SlowCompactBackend(LogBackend delegate) {
            this.delegate = delegate;
        }

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int clock) { return delegate.get(clock); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public Optional<LogEvent> append(LogEvent event, String id) { return delegate.append(event, id); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public void close() { delegate.close(); }

        @Override
        public SnapshotResult compact(List<LogEvent> events, String id) {
            compacting.countDown();
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return delegate.compact(events, id);
        }
    }

    @Test
    @Timeout(30)
    void events_appended_during_a_snapshot_are_kept(@TempDir Path tmp) throws Exception {
        GEN.set(0);
        Path file = tmp.resolve("fom.log");
        try (var b = new SlowCompactBackend(new FileLogBackend(file));
             Engine e = new Engine(cfg(), b, new JavaSerializableSerDe())) {
            e.newGraph(genGraph());
            var snapshot = e.snapshot().toCompletableFuture();
            assertThat(b.compacting.await(5, TimeUnit.SECONDS)).isTrue();
            e.trigger("A", "during-snapshot");
            snapshot.get(5, TimeUnit.SECONDS);
            awaitGen(e, 2);

            boolean triggerKept = false;
            boolean gen2Kept = false;
            for (int i = 0; i < b.length(); i++) {
                LogEvent ev = b.get(i);
                if (ev instanceof LogTrigger) triggerKept = true;
                if (ev instanceof LogInitialized li && ByteBuffer.wrap(li.properties().get("gen")).getInt() == 2) {
                    gen2Kept = true;
                }
            }
            assertThat(triggerKept).isTrue();
            assertThat(gen2Kept).isTrue();
        }
        try (LogBackend b = new FileLogBackend(file);
             Engine e = new Engine(cfg(), b, new JavaSerializableSerDe())) {
            e.newGraph(genGraph());
            assertThat(e.query("x").toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("gen2");
        }
    }

    // ───────────────── warm restart under a changed definition ─────────────────

    record Tenant(String id) implements Serializable { }

    static final AtomicInteger TENANT_INITS = new AtomicInteger();

    static final class TenantInit implements ParamProcessInitializer<Tenant>, ParamProcessLoader<Tenant> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Tenant p) {
            TENANT_INITS.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("t", p.id().getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Tenant p) {
            String fromProps = new String(props.get("t"), StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("param=" + p.id() + ",props=" + fromProps));
        }
    }

    private static Graph tenantGraph(String tenant) {
        return new GraphBuilder()
                .addWithParam("A",
                        (Supplier<ParamProcessInitializer<Tenant>>) TenantInit::new,
                        (Supplier<ParamProcessLoader<Tenant>>) TenantInit::new,
                        new Tenant(tenant))
                .build();
    }

    @Test
    @Timeout(20)
    void restart_with_a_changed_param_cold_inits_and_an_unchanged_one_warm_loads() throws Exception {
        TENANT_INITS.set(0);
        try (var backend = new InMemoryLogBackend()) {
            try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
                e.newGraph(tenantGraph("v1"));
            }
            try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
                e.newGraph(tenantGraph("v2"));
                assertThat(e.queryProcess("A", "q").toCompletableFuture().get(3, TimeUnit.SECONDS))
                        .isEqualTo("param=v2,props=v2");
            }
            assertThat(TENANT_INITS).hasValue(2);
            try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
                e.newGraph(tenantGraph("v2"));
                assertThat(e.queryProcess("A", "q").toCompletableFuture().get(3, TimeUnit.SECONDS))
                        .isEqualTo("param=v2,props=v2");
            }
            assertThat(TENANT_INITS).as("unchanged definition warm-loads").hasValue(2);
        }
    }

    // ───────────────── reactive cascade ─────────────────

    static final AtomicInteger PRODUCER_INITS = new AtomicInteger();
    static final AtomicInteger CONSUMER_INITS = new AtomicInteger();
    static volatile CountDownLatch consumerBlocked;
    static volatile CountDownLatch consumerRelease;

    static final class ProducerInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            int n = PRODUCER_INITS.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("n", new byte[]{(byte) n}));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            int n = props.get("n")[0];
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("A" + n));
        }
    }

    static final class ConsumerInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            int n = CONSUMER_INITS.incrementAndGet();
            try {
                Object seen = ctx.query("A", "probe").toCompletableFuture().get(3, TimeUnit.SECONDS);
                if (n == 2) {
                    consumerBlocked.countDown();
                    consumerRelease.await(10, TimeUnit.SECONDS);
                }
                return CompletableFuture.completedFuture(
                        Map.of("seen", String.valueOf(seen).getBytes(StandardCharsets.UTF_8)));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            String seen = new String(props.get("seen"), StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(seen));
        }
    }

    @Test
    @Timeout(30)
    void producer_change_while_consumer_reinitialises_is_not_lost() throws Exception {
        PRODUCER_INITS.set(0);
        CONSUMER_INITS.set(0);
        consumerBlocked = new CountDownLatch(1);
        consumerRelease = new CountDownLatch(1);
        try (var backend = new InMemoryLogBackend();
             var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            e.newGraph(new GraphBuilder()
                    .add("A", (Supplier<ProcessInitializer>) ProducerInit::new,
                            (Supplier<ProcessLoader>) ProducerInit::new)
                    .add("B", (Supplier<ProcessInitializer>) ConsumerInit::new,
                            (Supplier<ProcessLoader>) ConsumerInit::new, "A")
                    .build());

            e.trigger("B", "reinit-B");
            assertThat(consumerBlocked.await(5, TimeUnit.SECONDS)).isTrue(); // B read A1 and is stuck in init
            e.trigger("A", "reinit-A");
            await().atMost(Duration.ofSeconds(5)).until(() -> PRODUCER_INITS.get() == 2);
            consumerRelease.countDown();

            // B must re-init once more and end up reflecting A2.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(e.queryProcess("B", "q").toCompletableFuture().get(2, TimeUnit.SECONDS))
                            .isEqualTo("A2"));
        }
    }

    // ───────────────── stale reactive consumers at restart ─────────────────

    record Region(String code) implements Serializable { }

    static final AtomicInteger DERIVED_INITS = new AtomicInteger();

    static final class RegionInventory implements ParamProcessInitializer<Region>, ParamProcessLoader<Region> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Region p) {
            return CompletableFuture.completedFuture(Map.of("r", p.code().getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Region p) {
            String region = new String(props.get("r"), StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(region));
        }
    }

    /** Stores whatever its dependency answered at init time. */
    static final class Derived implements ProcessInitializer, ProcessLoader {
        private final String dependency;

        Derived(String dependency) {
            this.dependency = dependency;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            DERIVED_INITS.incrementAndGet();
            try {
                Object seen = ctx.query(dependency, "q").toCompletableFuture().get(3, TimeUnit.SECONDS);
                return CompletableFuture.completedFuture(
                        Map.of("seen", String.valueOf(seen).getBytes(StandardCharsets.UTF_8)));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            String seen = new String(props.get("seen"), StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(seen));
        }
    }

    /** Inventory(region) ← Pricing ← Catalog, both reactive. */
    private static Graph regionGraph(String region) {
        return new GraphBuilder()
                .addWithParam("Inventory",
                        (Supplier<ParamProcessInitializer<Region>>) RegionInventory::new,
                        (Supplier<ParamProcessLoader<Region>>) RegionInventory::new,
                        new Region(region))
                .add("Pricing", (Supplier<ProcessInitializer>) () -> new Derived("Inventory"),
                        (Supplier<ProcessLoader>) () -> new Derived("Inventory"), "Inventory")
                .add("Catalog", (Supplier<ProcessInitializer>) () -> new Derived("Pricing"),
                        (Supplier<ProcessLoader>) () -> new Derived("Pricing"), "Pricing")
                .build();
    }

    private static Object ask(Engine e, String process) throws Exception {
        return e.queryProcess(process, "q").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(30)
    void a_producer_that_cold_inits_at_restart_re_initialises_its_reactive_consumers() throws Exception {
        DERIVED_INITS.set(0);
        try (var backend = new InMemoryLogBackend()) {
            try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
                e.newGraph(regionGraph("EU"));
                assertThat(ask(e, "Catalog")).isEqualTo("EU");
            }
            try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
                e.newGraph(regionGraph("US"));
                assertThat(ask(e, "Inventory")).isEqualTo("US");
                // KEEP_OLD: the consumers serve their old state at once and re-initialise in the background.
                Settle.awaitNoReinit(e);
                assertThat(ask(e, "Pricing")).isEqualTo("US");
                assertThat(ask(e, "Catalog")).as("transitive consumer").isEqualTo("US");
            }
            assertThat(DERIVED_INITS).hasValue(4);
            try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
                e.newGraph(regionGraph("US"));
                assertThat(ask(e, "Catalog")).isEqualTo("US");
            }
            assertThat(DERIVED_INITS).as("consistent state warm-loads").hasValue(4);
        }
    }

    @Test
    @Timeout(30)
    void a_consumer_built_before_its_producers_live_state_re_initialises_at_restart() throws Exception {
        DERIVED_INITS.set(0);
        try (var backend = new InMemoryLogBackend()) {
            try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
                e.newGraph(regionGraph("EU"));
                assertThat(ask(e, "Catalog")).isEqualTo("EU");
            }
            // The engine died right after Inventory's re-init was persisted: no
            // LogLoaded, no LogDependencyChanged for its consumers.
            String leader = backend.introspect().currentLeader();
            long inventoryClock = -1;
            for (int i = 0; i < backend.length(); i++) {
                if (backend.get(i) instanceof LogInitialized li && li.processName().equals("Inventory")) {
                    inventoryClock = li.clock();
                }
            }
            backend.append(new LogDead(0, System.currentTimeMillis(), new Sid("Inventory", inventoryClock)), leader);
            backend.append(new LogInitialized(0, System.currentTimeMillis(), "Inventory",
                    Map.of("r", "EU-2".getBytes(StandardCharsets.UTF_8))), leader);

            try (var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
                e.newGraph(regionGraph("EU"));
                assertThat(ask(e, "Inventory")).isEqualTo("EU-2");
                // KEEP_OLD: the consumers serve their old state at once and re-initialise in the background.
                Settle.awaitNoReinit(e);
                assertThat(ask(e, "Pricing")).isEqualTo("EU-2");
                assertThat(ask(e, "Catalog")).isEqualTo("EU-2");
            }
            assertThat(DERIVED_INITS).hasValue(4);
        }
    }

    // ───────────────── graph swap ─────────────────

    record Mode(String v) implements Serializable { }

    static final AtomicBoolean FAIL_INIT = new AtomicBoolean();

    static final class ModeInit implements ParamProcessInitializer<Mode>, ParamProcessLoader<Mode> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Mode m) {
            if (FAIL_INIT.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("init fails for now"));
            }
            return CompletableFuture.completedFuture(Map.of("v", m.v().getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Mode m) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(m.v()));
        }
    }

    private static Graph modeGraph(String mode, boolean withConsumer) {
        var builder = new GraphBuilder()
                .addWithParam("A",
                        (Supplier<ParamProcessInitializer<Mode>>) ModeInit::new,
                        (Supplier<ParamProcessLoader<Mode>>) ModeInit::new,
                        new Mode(mode));
        if (withConsumer) {
            builder.add("C", (Supplier<ProcessInitializer>) ProducerInit::new,
                    (Supplier<ProcessLoader>) ProducerInit::new, "A");
        }
        return builder.build();
    }

    @Test
    @Timeout(30)
    void retrying_a_failed_graph_swap_spawns_the_missing_nodes() throws Exception {
        FAIL_INIT.set(false);
        try (var backend = new InMemoryLogBackend();
             var e = new Engine(cfg(Duration.ofMillis(300)), backend, new JavaSerializableSerDe())) {
            e.newGraph(modeGraph("v1", false));

            FAIL_INIT.set(true);
            Graph next = modeGraph("v2", true);
            assertThatThrownBy(() -> e.newGraph(next)).isInstanceOf(RuntimeException.class);

            FAIL_INIT.set(false);
            assertThat(e.newGraph(next)).as("retry must re-spawn the failed/missing nodes").isTrue();
            assertThat(e.queryProcess("A", "q").toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo("v2");
            assertThat(e.queryProcess("C", "q").toCompletableFuture().get(3, TimeUnit.SECONDS)).isNotNull();
        }
    }

    // ───────────────── in-memory compaction ─────────────────

    @Test
    @Timeout(30)
    void in_memory_compaction_is_atomic_for_lock_free_readers() throws Exception {
        var backend = new InMemoryLogBackend();
        String leader = "L";
        backend.append(new LogLeader(0, 1, leader), leader);
        List<LogEvent> snapshot = new ArrayList<>();
        snapshot.add(new LogLeader(0, 1, leader));
        for (int i = 0; i < 500; i++) {
            snapshot.add(new LogInitialized(i + 1, 1, "p" + i, Map.of()));
        }
        backend.compact(snapshot, leader);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger minLength = new AtomicInteger(Integer.MAX_VALUE);
        AtomicInteger outOfBounds = new AtomicInteger();
        Thread reader = new Thread(() -> {
            while (!stop.get()) {
                int len = backend.length();
                minLength.accumulateAndGet(len, Math::min);
                try {
                    for (int i = 0; i < len; i++) backend.get(i);
                } catch (IndexOutOfBoundsException ex) {
                    outOfBounds.incrementAndGet();
                }
            }
        });
        reader.start();
        long end = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < end) {
            backend.compact(snapshot, leader);
        }
        stop.set(true);
        reader.join();

        assertThat(minLength.get()).isEqualTo(501);
        assertThat(outOfBounds.get()).isZero();
    }
}
