package io.fom.micrometer;

import io.fom.Sid;
import io.fom.api.AttemptCancelledException;
import io.fom.api.LeadershipLostException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MicrometerEngineObserverTest {

    /** The registry's meters but the engine-wide {@code engine_watcher_stops_total}, primed up front. */
    private static List<io.micrometer.core.instrument.Meter> processMeters(
            io.micrometer.core.instrument.MeterRegistry registry) {
        return registry.getMeters().stream()
                .filter(m -> !m.getId().getName().equals("engine_watcher_stops_total"))
                .toList();
    }

    @Test
    void watcher_stops_are_registered_at_zero_for_every_reason_up_front() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry, Tags.of("engine", "orders"));
        for (var reason : io.fom.api.WatcherStopReason.values()) {
            String tag = reason.name().toLowerCase(java.util.Locale.ROOT);
            assertThat(registry.find("engine_watcher_stops_total").tag("engine", "orders").tag("reason", tag)
                    .counter()).as(tag).isNotNull()
                    .extracting(Counter::count).isEqualTo(0.0);
        }
        // The first stop increments the primed series rather than creating one at 1.
        observer.onWatcherStopped("P", io.fom.api.WatcherStopReason.LEADERSHIP_LOST);
        assertThat(registry.find("engine_watcher_stops_total").tag("reason", "leadership_lost").counters())
                .hasSize(1).first().extracting(Counter::count).isEqualTo(1.0);
        // Not per process: removing one leaves them.
        observer.onProcessRemoved("P");
        assertThat(registry.find("engine_watcher_stops_total").counters())
                .hasSize(io.fom.api.WatcherStopReason.values().length);
    }

    @Test
    void records_init_load_query_and_dedup_metrics() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        var sid = new Sid("Echo", 1);

        observer.onInitCompleted("Echo", sid, Duration.ofMillis(20));
        observer.onLoadCompleted("Echo", sid, Duration.ofMillis(5));
        observer.onQueryCompleted("Echo", UUID.randomUUID(), Duration.ofMillis(2));
        observer.onComputeDuration("Echo", Duration.ofMillis(1));
        observer.onDedupCollapsed("Echo", 4);
        observer.onQueryFailed("Echo", UUID.randomUUID(), "timeout", new RuntimeException("boom"));
        observer.onCleanupCompleted("Echo", sid, true, Duration.ofMillis(3));

        assertThat(registry.find("engine_process_init_duration_seconds").tag("name", "Echo").timer().count())
                .isEqualTo(1);
        assertThat(registry.find("engine_process_load_duration_seconds").tag("name", "Echo").timer().count())
                .isEqualTo(1);
        assertThat(registry.find("engine_query_duration_seconds").tag("name", "Echo").timer().count())
                .isEqualTo(1);
        assertThat(registry.find("engine_dedup_collapsed_total").tag("name", "Echo").counter().count())
                .isEqualTo(4.0);
        assertThat(registry.find("engine_query_failures_total")
                .tag("name", "Echo").tag("reason", "timeout").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("engine_process_cleanup_duration_seconds").tag("name", "Echo").timer().count())
                .isEqualTo(1);
    }

    @Test
    void lifecycle_failures_use_dedicated_meters_not_query_failures() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        var sid = new Sid("P", 1);

        observer.onInitFailed("P", 1, new RuntimeException("boom"));
        observer.onLoadFailed("P", sid, 1, new RuntimeException("boom"));
        observer.onLoadFailed("P", sid, 2, new RuntimeException("boom"));
        observer.onCleanupCompleted("P", sid, false, Duration.ofMillis(1));

        assertThat(registry.find("engine_process_init_failures_total").tag("name", "P").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("engine_process_load_failures_total").tag("name", "P").counter().count())
                .isEqualTo(2.0);
        assertThat(registry.find("engine_process_cleanup_failures_total").tag("name", "P").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("engine_query_failures_total").counters()).isEmpty();
    }

    @Test
    void cancelled_attempts_are_counted_apart_from_failures() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        var sid = new Sid("P", 1);

        // What the engine reports when a pause/removal/swap/close cuts an attempt short.
        observer.onInitFailed("P", 1, new AttemptCancelledException("P init attempt 1 cancelled"));
        observer.onLoadFailed("P", sid, 1, new java.util.concurrent.CompletionException(
                new AttemptCancelledException("P load attempt 1 cancelled")));
        observer.onInitFailed("P", 2, new IllegalStateException("real"));
        // The user's own init throwing a plain CancellationException is a failure, not a stop.
        observer.onInitFailed("P", 3, new CancellationException("my own timeout"));
        observer.onInitFailed("P", 4, new java.util.concurrent.CompletionException(
                new CancellationException("my own timeout, wrapped")));

        assertThat(registry.find("engine_process_init_cancellations_total").tag("name", "P").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("engine_process_load_cancellations_total").tag("name", "P").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("engine_process_init_failures_total").tag("name", "P").counter().count())
                .isEqualTo(3.0);
        assertThat(registry.find("engine_process_load_failures_total").counter()).isNull();
    }

    @Test
    void a_user_cancellation_that_ends_in_dead_reads_dead() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        observer.onStateTransition("P", "NotPresent", "Initializing");
        observer.onInitStarted("P", 1);
        observer.onInitFailed("P", 1, new CancellationException("my own timeout"));
        observer.onStateTransition("P", "Initializing", "Dead");
        assertThat(dead(registry, "P")).isEqualTo(1.0);
    }

    @Test
    void lifecycle_counters_are_registered_at_zero_when_a_process_is_first_seen() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry, Tags.of("engine", "e1"));
        observer.onStateTransition("P", "NotPresent", "Initializing");
        for (String name : List.of("engine_process_init_failures_total", "engine_process_init_cancellations_total",
                "engine_process_load_failures_total", "engine_process_load_cancellations_total",
                "engine_process_cleanup_failures_total", "engine_process_leadership_lost_total",
                "engine_process_reinit_failures_total", "engine_dedup_collapsed_total")) {
            assertThat(registry.find(name).tag("engine", "e1").tag("name", "P").counter().count())
                    .as(name).isEqualTo(0.0);
        }
        assertThat(dead(registry, "P")).isEqualTo(0.0);
        // A start after the transition (or instead of it) does not reset or duplicate them.
        observer.onInitFailed("P", 1, new IllegalStateException("down"));
        observer.onInitStarted("P", 2);
        assertThat(registry.find("engine_process_init_failures_total").tag("name", "P").counters()).hasSize(1);
        assertThat(registry.find("engine_process_init_failures_total").tag("name", "P").counter().count())
                .isEqualTo(1.0);

        // Removal drops the primed counters too; a late transition does not prime them again.
        observer.onProcessRemoved("P");
        observer.onStateTransition("P", "Initializing", "Dead");
        assertThat(processMeters(registry)).isEmpty();
        // Added back: primed again, from zero.
        observer.onInitStarted("P", 1);
        assertThat(registry.find("engine_process_init_failures_total").tag("name", "P").counter().count())
                .isEqualTo(0.0);
    }

    @Test
    void pausing_a_process_that_died_of_a_failure_resets_the_dead_gauge() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        observer.onStateTransition("P", "NotPresent", "Initializing");
        observer.onInitFailed("P", 1, new IllegalStateException("down"));
        observer.onStateTransition("P", "Initializing", "Dead");
        assertThat(dead(registry, "P")).isEqualTo(1.0);
        observer.onStateTransition("P", "Dead", "Paused");
        assertThat(dead(registry, "P")).isEqualTo(0.0);
    }

    private static double dead(SimpleMeterRegistry registry, String name) {
        return registry.find("engine_process_dead").tag("name", name).gauge().value();
    }

    @Test
    void dead_gauge_is_one_only_for_a_process_that_died_of_a_failure() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);

        // Init runs out of budget: the real failure is reported, then Initializing -> Dead.
        observer.onStateTransition("Bad", "NotPresent", "Initializing");
        assertThat(dead(registry, "Bad")).isEqualTo(0.0);
        observer.onInitStarted("Bad", 1);
        observer.onInitFailed("Bad", 1, new IllegalStateException("down"));
        observer.onStateTransition("Bad", "Initializing", "Dead");
        assertThat(dead(registry, "Bad")).isEqualTo(1.0);

        // Stopped mid-init (cancelInit, or a pause while a start is awaited): cancellation, then Dead.
        observer.onStateTransition("Stopped", "NotPresent", "Initializing");
        observer.onInitStarted("Stopped", 1);
        observer.onInitFailed("Stopped", 1, new AttemptCancelledException("cancelled"));
        observer.onStateTransition("Stopped", "Initializing", "Dead");
        assertThat(dead(registry, "Stopped")).isEqualTo(0.0);

        // Orderly shutdown of a serving process: through CleaningUp.
        observer.onStateTransition("Ok", "NotPresent", "Loading");
        observer.onStateTransition("Ok", "Loading", "Serving");
        observer.onStateTransition("Ok", "Serving", "CleaningUp");
        observer.onStateTransition("Ok", "CleaningUp", "Dead");
        assertThat(dead(registry, "Ok")).isEqualTo(0.0);

        // Started again (a fresh FSM for the name): back to 0.
        observer.onStateTransition("Bad", "NotPresent", "Initializing");
        assertThat(dead(registry, "Bad")).isEqualTo(0.0);
    }

    @Test
    void dead_gauge_is_dropped_on_removal_and_not_brought_back_by_late_transitions() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry, Tags.of("engine", "e1"));
        observer.onStateTransition("P", "NotPresent", "Initializing");
        observer.onInitFailed("P", 1, new IllegalStateException("down"));
        observer.onStateTransition("P", "Initializing", "Dead");
        assertThat(registry.find("engine_process_dead").tag("engine", "e1").tag("name", "P").gauge()).isNotNull();

        observer.onProcessRemoved("P");
        assertThat(registry.find("engine_process_dead").gauge()).isNull();

        observer.onStateTransition("P", "Initializing", "Dead"); // late
        observer.onInitFailed("P", 1, new AttemptCancelledException("late"));
        assertThat(processMeters(registry)).isEmpty();

        // Added back: a fresh FSM registers it again, not dead.
        observer.onStateTransition("P", "NotPresent", "Initializing");
        assertThat(dead(registry, "P")).isEqualTo(0.0);
    }

    @Test
    void meters_of_a_removed_process_are_dropped() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        observer.onQueryCompleted("Gone", UUID.randomUUID(), Duration.ofMillis(1));
        observer.onQueryFailed("Gone", UUID.randomUUID(), "timeout", null);
        observer.onQueryCompleted("Kept", UUID.randomUUID(), Duration.ofMillis(1));

        observer.onProcessRemoved("Gone");

        assertThat(registry.find("engine_query_duration_seconds").tag("name", "Gone").timer()).isNull();
        assertThat(registry.find("engine_query_failures_total").tag("name", "Gone").counter()).isNull();
        assertThat(registry.find("engine_query_duration_seconds").tag("name", "Kept").timer()).isNotNull();

        // A query that was still in flight settles after the removal: no meter comes back.
        observer.onQueryFailed("Gone", UUID.randomUUID(), "timeout", null);
        observer.onComputeDuration("Gone", Duration.ofMillis(1));
        assertThat(registry.find("engine_query_failures_total").tag("name", "Gone").counter()).isNull();
        assertThat(registry.find("engine_process_compute_duration_seconds").tag("name", "Gone").timer()).isNull();

        // Added back later: recorded again.
        observer.onInitStarted("Gone", 1);
        observer.onQueryCompleted("Gone", UUID.randomUUID(), Duration.ofMillis(1));
        assertThat(registry.find("engine_query_duration_seconds").tag("name", "Gone").timer()).isNotNull();
    }

    @Test
    void removed_names_are_bounded_even_when_never_added_back() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);

        // Tenant-shaped names, each removed once and never re-added (a field test saw 60,795 kept).
        for (int i = 0; i < 60_000; i++) {
            String name = "tenant-" + i;
            observer.onQueryCompleted(name, UUID.randomUUID(), Duration.ofMillis(1));
            observer.onProcessRemoved(name);
        }

        assertThat(observer.rememberedRemovals()).isLessThanOrEqualTo(MicrometerEngineObserver.REMOVED_CAP);
        assertThat(processMeters(registry)).isEmpty();
        // The most recent removals are still covered against late callbacks.
        observer.onQueryFailed("tenant-59999", UUID.randomUUID(), "timeout", null);
        assertThat(processMeters(registry)).isEmpty();
    }

    @Test
    void removed_names_expire_after_the_retention_but_cover_late_callbacks_until_then() {
        var registry = new SimpleMeterRegistry();
        var now = new AtomicLong();
        var observer = new MicrometerEngineObserver(registry, Duration.ofMinutes(15), 1_000, now::get);

        observer.onProcessRemoved("Old");
        now.addAndGet(Duration.ofMinutes(14).toNanos());
        // Still inside the retention: a late callback does not bring meters back.
        observer.onQueryFailed("Old", UUID.randomUUID(), "timeout", null);
        assertThat(registry.find("engine_query_failures_total").tag("name", "Old").counter()).isNull();

        now.addAndGet(Duration.ofMinutes(2).toNanos());
        observer.onProcessRemoved("New"); // expiry is swept on the next removal
        assertThat(observer.rememberedRemovals()).isEqualTo(1);
        observer.onQueryFailed("New", UUID.randomUUID(), "timeout", null);
        assertThat(registry.find("engine_query_failures_total").tag("name", "New").counter()).isNull();
    }

    @Test
    void cap_evicts_the_oldest_removal_but_not_a_newer_removal_of_the_same_name() {
        var registry = new SimpleMeterRegistry();
        var now = new AtomicLong();
        var observer = new MicrometerEngineObserver(registry, Duration.ofMinutes(15), 2, now::get);

        observer.onProcessRemoved("A");
        now.incrementAndGet();
        observer.onInitStarted("A", 1); // added back ...
        observer.onProcessRemoved("A"); // ... and removed again
        now.incrementAndGet();
        observer.onProcessRemoved("B"); // over the cap: the first removal of A is evicted
        now.incrementAndGet();

        observer.onComputeDuration("A", Duration.ofMillis(1));
        observer.onComputeDuration("B", Duration.ofMillis(1));
        assertThat(processMeters(registry)).isEmpty();

        observer.onProcessRemoved("C"); // evicts A's second removal
        assertThat(observer.rememberedRemovals()).isEqualTo(2);
        observer.onComputeDuration("A", Duration.ofMillis(1));
        assertThat(registry.find("engine_process_compute_duration_seconds").tag("name", "A").timer()).isNotNull();
    }

    @Test
    void cancellations_counted_from_cancellation_cause_or_reason() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);

        // What core actually emits for a cancelled compute stage.
        observer.onQueryFailed("P", UUID.randomUUID(), "exception", new CancellationException("deadline"));
        observer.onQueryFailed("P", UUID.randomUUID(), "cancelled", null);
        observer.onQueryFailed("P", UUID.randomUUID(), "exception", new IllegalStateException("x"));

        assertThat(registry.find("engine_query_cancellations_total").tag("name", "P").counter().count())
                .isEqualTo(2.0);
        assertThat(registry.find("engine_query_failures_total").tag("reason", "exception").counter().count())
                .isEqualTo(2.0);
        assertThat(registry.find("engine_query_failures_total").tag("reason", "cancelled").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void common_tags_are_added_to_every_meter_and_keep_two_engines_apart() {
        var registry = new SimpleMeterRegistry();
        var orders = new MicrometerEngineObserver(registry, Tags.of("engine", "orders"));
        var billing = new MicrometerEngineObserver(registry, Tags.of("engine", "billing"));

        orders.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));
        orders.onQueryFailed("P", UUID.randomUUID(), "timeout", null);
        orders.onWatcherStopped("P", io.fom.api.WatcherStopReason.values()[0]);
        billing.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));
        billing.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));

        assertThat(registry.find("engine_query_duration_seconds").tag("engine", "orders").tag("name", "P")
                .timer().count()).isEqualTo(1);
        assertThat(registry.find("engine_query_duration_seconds").tag("engine", "billing").tag("name", "P")
                .timer().count()).isEqualTo(2);
        assertThat(registry.find("engine_query_failures_total").tag("engine", "orders")
                .tag("reason", "timeout").counter()).isNotNull();
        assertThat(registry.find("engine_watcher_stops_total").tag("engine", "orders").counter()).isNotNull();
        // Every meter carries the engine tag.
        assertThat(registry.getMeters()).allSatisfy(m -> assertThat(m.getId().getTag("engine")).isNotNull());
    }

    @Test
    void removing_a_process_in_one_engine_keeps_the_other_engines_meters_of_the_same_name() {
        var registry = new SimpleMeterRegistry();
        var orders = new MicrometerEngineObserver(registry, Tags.of("engine", "orders"));
        var billing = new MicrometerEngineObserver(registry, Tags.of("engine", "billing"));
        orders.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));
        billing.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));
        billing.onQueryFailed("P", UUID.randomUUID(), "timeout", null);

        orders.onProcessRemoved("P");

        assertThat(registry.find("engine_query_duration_seconds").tag("engine", "orders").timer()).isNull();
        assertThat(registry.find("engine_query_duration_seconds").tag("engine", "billing").timer()).isNotNull();
        assertThat(registry.find("engine_query_failures_total").tag("engine", "billing").counter()).isNotNull();
        // ... and billing keeps recording into the very same meter.
        billing.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));
        assertThat(registry.find("engine_query_duration_seconds").tag("engine", "billing").timer().count())
                .isEqualTo(2);
    }

    @Test
    void removal_leaves_meters_this_observer_did_not_register() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        // Some other component registered an engine_-prefixed meter tagged with the same name.
        Counter foreign = Counter.builder("engine_custom_total").tag("name", "P").register(registry);
        observer.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));

        observer.onProcessRemoved("P");

        assertThat(registry.find("engine_query_duration_seconds").tag("name", "P").timer()).isNull();
        assertThat(registry.find("engine_custom_total").tag("name", "P").counter()).isSameAs(foreign);
    }

    @Test
    void a_removed_name_added_back_is_tracked_and_removed_again() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        observer.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));
        observer.onProcessRemoved("P");
        observer.onInitStarted("P", 1);
        observer.onQueryCompleted("P", UUID.randomUUID(), Duration.ofMillis(1));
        assertThat(registry.find("engine_query_duration_seconds").tag("name", "P").timer()).isNotNull();

        observer.onProcessRemoved("P");

        assertThat(registry.find("engine_query_duration_seconds").tag("name", "P").timer()).isNull();
    }

    @Test
    void a_new_observer_on_the_same_registry_and_tags_drives_the_dead_gauge() {
        var registry = new SimpleMeterRegistry();
        var first = new MicrometerEngineObserver(registry, Tags.of("engine", "e"));
        first.onStateTransition("P", "NotPresent", "Initializing");
        first.onInitFailed("P", 1, new IllegalStateException("down"));
        first.onStateTransition("P", "Initializing", "Dead");
        assertThat(dead(registry, "P")).isEqualTo(1.0);

        // The first engine is closed (no removal); a new engine + observer on the same registry.
        var second = new MicrometerEngineObserver(registry, Tags.of("engine", "e"));
        second.onStateTransition("P", "NotPresent", "Loading");
        second.onStateTransition("P", "Loading", "Serving");
        assertThat(dead(registry, "P")).isEqualTo(0.0);
        second.onStateTransition("P", "Serving", "Initializing");
        second.onInitFailed("P", 1, new IllegalStateException("down again"));
        second.onStateTransition("P", "Initializing", "Dead");
        assertThat(dead(registry, "P")).isEqualTo(1.0);

        second.onProcessRemoved("P");
        assertThat(registry.find("engine_process_dead").gauge()).isNull();
    }

    /** Folds tenant-qualified names ("t1/Orders", "t2/Orders") into one series per process kind. */
    private static SimpleMeterRegistry foldingTenants() {
        var registry = new SimpleMeterRegistry();
        registry.config().meterFilter(MeterFilter.replaceTagValues("name", n -> n.replaceFirst("^[^/]+/", "*/")));
        return registry;
    }

    @Test
    void removing_one_of_several_names_folded_into_one_meter_keeps_the_meter() {
        var registry = foldingTenants();
        var observer = new MicrometerEngineObserver(registry);
        observer.onStateTransition("t1/Orders", "NotPresent", "Serving");
        observer.onStateTransition("t2/Orders", "NotPresent", "Serving");
        observer.onQueryCompleted("t1/Orders", UUID.randomUUID(), Duration.ofMillis(1));
        observer.onQueryCompleted("t2/Orders", UUID.randomUUID(), Duration.ofMillis(1));
        assertThat(registry.find("engine_query_duration_seconds").tag("name", "*/Orders").timer().count())
                .isEqualTo(2);

        observer.onProcessRemoved("t1/Orders");

        assertThat(registry.find("engine_query_duration_seconds").tag("name", "*/Orders").timer()).isNotNull();
        assertThat(registry.find("engine_process_init_failures_total").tag("name", "*/Orders").counter()).isNotNull();
        assertThat(dead(registry, "*/Orders")).isEqualTo(0.0);
        observer.onQueryCompleted("t2/Orders", UUID.randomUUID(), Duration.ofMillis(1));
        assertThat(registry.find("engine_query_duration_seconds").tag("name", "*/Orders").timer().count())
                .isEqualTo(3);

        observer.onProcessRemoved("t2/Orders");

        assertThat(processMeters(registry)).isEmpty();
    }

    @Test
    void a_folded_dead_gauge_reads_one_while_any_folded_process_is_dead() {
        var registry = foldingTenants();
        var observer = new MicrometerEngineObserver(registry);
        observer.onStateTransition("t1/Orders", "NotPresent", "Serving");
        observer.onStateTransition("t2/Orders", "NotPresent", "Initializing");
        observer.onInitFailed("t2/Orders", 1, new IllegalStateException("down"));
        observer.onStateTransition("t2/Orders", "Initializing", "Dead");
        assertThat(dead(registry, "*/Orders")).isEqualTo(1.0);

        observer.onProcessRemoved("t2/Orders");
        assertThat(dead(registry, "*/Orders")).isEqualTo(0.0);

        observer.onProcessRemoved("t1/Orders");
        assertThat(registry.find("engine_process_dead").gauge()).isNull();
    }

    @Test
    void leadership_loss_is_counted_apart_from_init_and_load_failures() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        var sid = new Sid("P", 1);
        observer.onStateTransition("P", "NotPresent", "Initializing");
        assertThat(registry.find("engine_process_leadership_lost_total").tag("name", "P").counter().count())
                .isEqualTo(0.0); // primed

        // Wrapped, as a dropped re-init or a fenced append can report it.
        observer.onInitFailed("P", 1, new RuntimeException("append failed",
                new LeadershipLostException("fenced")));
        observer.onLoadFailed("P", sid, 1, new LeadershipLostException("deposed"));
        observer.onInitFailed("P", 2, new IllegalStateException("upstream down"));

        assertThat(registry.find("engine_process_leadership_lost_total").tag("name", "P").counter().count())
                .isEqualTo(2.0);
        assertThat(registry.find("engine_process_init_failures_total").tag("name", "P").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("engine_process_load_failures_total").tag("name", "P").counter().count())
                .isEqualTo(0.0);

        // A node that lost leadership still reads dead.
        observer.onInitFailed("P", 3, new LeadershipLostException("fenced"));
        observer.onStateTransition("P", "Initializing", "Dead");
        assertThat(dead(registry, "P")).isEqualTo(1.0);
    }

    @Test
    void a_new_observer_on_the_same_registry_does_not_register_the_dead_gauge_twice() {
        for (boolean registryCommonTags : new boolean[] {false, true}) {
            var registry = new SimpleMeterRegistry();
            if (registryCommonTags) registry.config().commonTags("app", "svc"); // changes the post-filter id
            String log = capturingStderr(() -> {
                var first = new MicrometerEngineObserver(registry, Tags.of("engine", "e"));
                first.onStateTransition("P", "NotPresent", "Initializing");
                first.onInitFailed("P", 1, new IllegalStateException("down"));
                first.onStateTransition("P", "Initializing", "Dead");
                for (int i = 0; i < 3; i++) { // engines re-created on the same registry and tags
                    var next = new MicrometerEngineObserver(registry, Tags.of("engine", "e"));
                    next.onStateTransition("P", "NotPresent", "Loading");
                    next.onStateTransition("P", "Loading", "Serving");
                    assertThat(dead(registry, "P")).isEqualTo(0.0);
                }
            });
            assertThat(log).as("registry common tags: " + registryCommonTags)
                    .doesNotContain("already registered");
            assertThat(registry.find("engine_process_dead").gauges()).hasSize(1);
        }
        // The capture does see the warning (at WARN once per JVM, then at DEBUG: simplelogger.properties).
        var registry = new SimpleMeterRegistry();
        Gauge.builder("g", () -> 1).register(registry);
        assertThat(capturingStderr(() -> Gauge.builder("g", () -> 1).register(registry)))
                .contains("already registered");
    }

    @Test
    void names_folded_into_one_dead_gauge_do_not_register_it_twice() {
        var registry = foldingTenants();
        var observer = new MicrometerEngineObserver(registry);
        String log = capturingStderr(() -> {
            observer.onStateTransition("t1/Orders", "NotPresent", "Serving");
            observer.onStateTransition("t2/Orders", "NotPresent", "Serving");
            observer.onStateTransition("t3/Orders", "NotPresent", "Initializing");
            observer.onInitFailed("t3/Orders", 1, new IllegalStateException("down"));
            observer.onStateTransition("t3/Orders", "Initializing", "Dead");
        });
        assertThat(log).doesNotContain("already registered");
        assertThat(registry.find("engine_process_dead").gauges()).hasSize(1);
        assertThat(dead(registry, "*/Orders")).isEqualTo(1.0);

        observer.onProcessRemoved("t3/Orders");
        assertThat(dead(registry, "*/Orders")).isEqualTo(0.0);
        observer.onProcessRemoved("t1/Orders");
        observer.onProcessRemoved("t2/Orders");
        assertThat(registry.find("engine_process_dead").gauge()).isNull();
    }

    private static String capturingStderr(Runnable body) {
        PrintStream original = System.err;
        var buffer = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            body.run();
        } finally {
            System.setErr(original);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    private static double stale(SimpleMeterRegistry registry, String name) {
        return registry.find("engine_process_stale").tag("name", name).gauge().value();
    }

    @Test
    void a_failed_reinit_counts_and_reads_stale_until_the_next_promotion() {
        var registry = new SimpleMeterRegistry();
        var clock = new AtomicLong();
        var observer = new MicrometerEngineObserver(registry, Duration.ofMinutes(1), 100, clock::get);
        var v1 = new Sid("P", 1);
        observer.onStateTransition("P", "NotPresent", "Initializing");
        assertThat(registry.find("engine_process_reinit_failures_total").tag("name", "P").counter().count())
                .isEqualTo(0.0); // primed
        assertThat(stale(registry, "P")).isEqualTo(0.0);
        observer.onSidPromotion("P", null, v1);

        observer.onReinitStarted("P", v1);
        clock.addAndGet(Duration.ofSeconds(3).toNanos());
        observer.onReinitFailed("P", v1, new IllegalStateException("upstream down"));
        assertThat(registry.find("engine_process_reinit_failures_total").tag("name", "P").counter().count())
                .isEqualTo(1.0);
        assertThat(stale(registry, "P")).isEqualTo(1.0);
        assertThat(registry.find("engine_process_reinit_duration_seconds").timer())
                .as("a failed re-init records no duration").isNull();

        // The retry succeeds: its own duration only, and no longer stale.
        observer.onReinitStarted("P", v1);
        clock.addAndGet(Duration.ofSeconds(2).toNanos());
        observer.onSidPromotion("P", v1, new Sid("P", 2));
        assertThat(stale(registry, "P")).isEqualTo(0.0);
        var timer = registry.find("engine_process_reinit_duration_seconds").tag("name", "P").timer();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(2.0);

        // A promotion without a re-init (a cold start, a RELEASE_FIRST re-init) times nothing.
        observer.onSidPromotion("P", new Sid("P", 2), new Sid("P", 3));
        assertThat(timer.count()).isEqualTo(1);
    }

    @Test
    void a_reinit_cut_short_by_a_shutdown_is_not_timed_into_the_next_promotion() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        var v1 = new Sid("P", 1);
        observer.onStateTransition("P", "NotPresent", "Serving");
        observer.onReinitStarted("P", v1);
        observer.onStateTransition("P", "Serving", "CleaningUp");
        observer.onStateTransition("P", "CleaningUp", "Dead");
        observer.onStateTransition("P", "NotPresent", "Loading");
        observer.onSidPromotion("P", v1, new Sid("P", 2));
        assertThat(registry.find("engine_process_reinit_duration_seconds").timer()).isNull();
    }

    @Test
    void stale_gauge_and_reinit_meters_are_dropped_on_removal_and_not_brought_back_late() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        var v1 = new Sid("P", 1);
        observer.onStateTransition("P", "NotPresent", "Serving");
        observer.onReinitStarted("P", v1);
        observer.onSidPromotion("P", v1, new Sid("P", 2));
        observer.onReinitFailed("P", new Sid("P", 2), new IllegalStateException("down"));
        assertThat(stale(registry, "P")).isEqualTo(1.0);

        observer.onProcessRemoved("P");
        assertThat(processMeters(registry)).isEmpty();

        observer.onReinitStarted("P", v1); // late
        observer.onReinitFailed("P", v1, new IllegalStateException("late"));
        observer.onSidPromotion("P", v1, new Sid("P", 3));
        assertThat(processMeters(registry)).isEmpty();

        // Added back: fresh, not stale.
        observer.onStateTransition("P", "NotPresent", "Initializing");
        assertThat(stale(registry, "P")).isEqualTo(0.0);
    }

    @Test
    void a_folded_stale_gauge_reads_one_while_any_folded_process_is_stale() {
        var registry = foldingTenants();
        var observer = new MicrometerEngineObserver(registry);
        String log = capturingStderr(() -> {
            observer.onStateTransition("t1/Orders", "NotPresent", "Serving");
            observer.onStateTransition("t2/Orders", "NotPresent", "Serving");
            observer.onReinitFailed("t2/Orders", new Sid("t2/Orders", 1), new IllegalStateException("down"));
        });
        assertThat(log).doesNotContain("already registered");
        assertThat(registry.find("engine_process_stale").gauges()).hasSize(1);
        assertThat(stale(registry, "*/Orders")).isEqualTo(1.0);
        assertThat(dead(registry, "*/Orders")).isEqualTo(0.0);

        observer.onProcessRemoved("t2/Orders");
        assertThat(stale(registry, "*/Orders")).isEqualTo(0.0);
        observer.onProcessRemoved("t1/Orders");
        assertThat(registry.find("engine_process_stale").gauge()).isNull();
    }

    @Test
    void a_new_observer_on_the_same_registry_and_tags_drives_the_stale_gauge() {
        var registry = new SimpleMeterRegistry();
        var first = new MicrometerEngineObserver(registry, Tags.of("engine", "e"));
        first.onStateTransition("P", "NotPresent", "Serving");
        first.onReinitFailed("P", new Sid("P", 1), new IllegalStateException("down"));
        assertThat(stale(registry, "P")).isEqualTo(1.0);

        var second = new MicrometerEngineObserver(registry, Tags.of("engine", "e"));
        second.onStateTransition("P", "NotPresent", "Loading");
        assertThat(stale(registry, "P")).isEqualTo(0.0);
        second.onReinitFailed("P", new Sid("P", 1), new IllegalStateException("down again"));
        assertThat(stale(registry, "P")).isEqualTo(1.0);
        assertThat(registry.find("engine_process_stale").gauges()).hasSize(1);
    }

    @Test
    void reserved_tag_keys_are_rejected() {
        var registry = new SimpleMeterRegistry();
        assertThatThrownBy(() -> new MicrometerEngineObserver(registry, Tags.of("name", "x")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MicrometerEngineObserver(registry, Tags.of("reason", "x")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
