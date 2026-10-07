package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** In-process snapshots racing lifecycle calls, and Sids of live processes across compaction. */
class SnapshotRaceTest {

    private static EngineConfig cfg() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMillis(5),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static Graph graph() {
        return new GraphBuilder()
                .add("A", (Supplier<ProcessInitializer>) EngineLogConsistencyTest.GenInit::new,
                        (Supplier<ProcessLoader>) EngineLogConsistencyTest.GenInit::new)
                .add("Leaf", (Supplier<ProcessInitializer>) EngineLogConsistencyTest.GenInit::new,
                        (Supplier<ProcessLoader>) EngineLogConsistencyTest.GenInit::new)
                .build();
    }

    private static Sid sidOf(Engine e, String name) throws Exception {
        return e.introspect().toCompletableFuture().get(3, TimeUnit.SECONDS).graph().nodes().stream()
                .filter(n -> n.name().equals(name)).findFirst().orElseThrow().sid();
    }

    @Test
    @Timeout(60)
    void resume_racing_snapshots_never_fails() throws Exception {
        try (var backend = new InMemoryLogBackend();
             var e = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            e.newGraph(graph());
            var stop = new AtomicBoolean();
            List<Throwable> snapshotErrors = new CopyOnWriteArrayList<>();
            Thread snapshots = Thread.ofPlatform().start(() -> {
                while (!stop.get()) {
                    try {
                        e.trigger("A", "grow");
                        e.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
                    } catch (Exception ex) {
                        snapshotErrors.add(ex);
                    }
                }
            });
            try {
                for (int i = 0; i < 150; i++) {
                    e.pause(List.of("Leaf"));
                    e.resume(List.of("Leaf"));
                }
            } finally {
                stop.set(true);
                snapshots.join(10_000);
            }
            assertThat(snapshotErrors).isEmpty();
        }
    }

    @Test
    @Timeout(30)
    void a_snapshot_keeps_live_sids_and_later_sids_keep_increasing() throws Exception {
        List<Sid[]> promotions = new CopyOnWriteArrayList<>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onSidPromotion(String processName, Sid previousSid, Sid newSid) {
                if (processName.equals("A") && previousSid != null) promotions.add(new Sid[]{previousSid, newSid});
            }
        };
        try (var backend = new InMemoryLogBackend();
             var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), observer)) {
            e.newGraph(graph());
            for (int i = 0; i < 20; i++) {
                e.trigger("A", "t" + i);
                Sid before = sidOf(e, "A");
                await().atMost(Duration.ofSeconds(5)).until(() -> {
                    Sid now = sidOf(e, "A");
                    return now != null && !now.equals(before);
                });
            }
            Sid beforeSnapshot = sidOf(e, "A");
            long maxClockEver = backend.get(backend.length() - 1).clock();
            e.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(sidOf(e, "A")).as("compaction keeps Sids").isEqualTo(beforeSnapshot);
            assertThat((long) backend.length()).isLessThan(maxClockEver);

            java.util.Set<Sid> seen = new java.util.HashSet<>();
            promotions.forEach(p -> seen.add(p[1]));
            promotions.clear();
            for (int i = 0; i < 5; i++) {
                e.trigger("A", "after" + i);
                await().atMost(Duration.ofSeconds(5)).until(() -> promotions.size() > 0);
                Sid[] last = promotions.get(promotions.size() - 1);
                assertThat(last[1].clock()).as("new Sid after %s", last[0]).isGreaterThan(last[0].clock());
                assertThat(last[1].clock()).as("never reuses a clock used before the snapshot").isGreaterThan(maxClockEver);
                assertThat(seen.add(last[1])).as("Sid %s is new", last[1]).isTrue();
                promotions.clear();
                e.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
        }
    }
}
