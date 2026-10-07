package io.fom.test;

import io.fom.Sid;
import io.fom.api.LeadershipLostException;
import io.fom.log.LogBackend;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogLoaded;
import io.fom.log.LogSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contract for a {@link LogBackend}. Subclasses supply the factory and, if it persists, {@link #reopen}. */
public abstract class LogBackendContractTest {

    protected static final String LEADER = "instance-A";
    protected static final String OTHER_LEADER = "instance-B";

    protected LogBackend backend;

    /** Build a fresh empty backend for each test. */
    protected abstract LogBackend create();

    /** Reopens the storage behind {@code original}; {@code null} for a backend that does not persist. */
    protected LogBackend reopen(LogBackend original) {
        return null;
    }

    @BeforeEach
    void setUp() {
        backend = create();
    }

    @AfterEach
    void tearDown() {
        if (backend != null) {
            backend.close();
        }
    }

    @Test
    void empty_log_has_zero_length() {
        assertThat(backend.length()).isZero();
        assertThat(backend.introspect().length()).isZero();
    }

    @Test
    void first_append_must_be_leader_claim() {
        var rejected = backend.append(graph("g1"), LEADER);
        assertThat(rejected).isEmpty();
        assertThat(backend.length()).isZero();
    }

    @Test
    void leader_claim_succeeds_on_empty_log() {
        Optional<LogEvent> claim = backend.append(new LogLeader(0, now(), LEADER), LEADER);
        assertThat(claim).isPresent();
        assertThat(claim.get()).isInstanceOf(LogLeader.class);
        assertThat(((LogLeader) claim.get()).instanceId()).isEqualTo(LEADER);
        assertThat(backend.length()).isEqualTo(1);
    }

    @Test
    void clocks_are_sequential_starting_from_zero() {
        var leader = backend.append(new LogLeader(0, now(), LEADER), LEADER).orElseThrow();
        var a = backend.append(graph("g1"), LEADER).orElseThrow();
        var b = backend.append(graph("g2"), LEADER).orElseThrow();
        assertThat(leader.clock()).isEqualTo(0);
        assertThat(a.clock()).isEqualTo(1);
        assertThat(b.clock()).isEqualTo(2);
        assertThat(backend.length()).isEqualTo(3);
    }

    @Test
    void non_leader_append_is_rejected() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        var rejected = backend.append(graph("g1"), OTHER_LEADER);
        assertThat(rejected).isEmpty();
        assertThat(backend.length()).isEqualTo(1);
    }

    /**
     * A backend may report a takeover by refusing the append or by throwing
     * {@link LeadershipLostException}; either way the event is not applied.
     */
    @Test
    void a_deposed_leaders_append_is_refused_or_fails_and_is_never_applied() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(new LogLeader(0, now(), OTHER_LEADER), OTHER_LEADER);
        var event = graph("deposed");
        try {
            assertThat(backend.append(event, LEADER))
                    .as("a deposed leader's append must not be applied")
                    .isEmpty();
        } catch (LeadershipLostException expected) {
            // also allowed
        }
        assertThat(backend.length()).as("nothing was written").isEqualTo(2);
    }

    @Test
    void leader_takeover_via_new_log_leader_event() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        var takeover = backend.append(new LogLeader(0, now(), OTHER_LEADER), OTHER_LEADER);
        assertThat(takeover).isPresent();
        try {
            assertThat(backend.append(graph("g9"), LEADER)).isEmpty();
        } catch (LeadershipLostException expected) {
            // also allowed
        }
        assertThat(backend.append(graph("g9"), OTHER_LEADER)).isPresent();
    }

    @Test
    void get_returns_persisted_event_by_clock() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        var sid = new Sid("Foo", 1);
        backend.append(new LogInitialized(0, now(), "Foo", Map.of("k", new byte[]{42})), LEADER);
        backend.append(new LogLoaded(0, now(), sid), LEADER);

        assertThat(backend.get(0)).isInstanceOf(LogLeader.class);
        assertThat(backend.get(1)).isInstanceOf(LogInitialized.class);
        assertThat(backend.get(2)).isInstanceOf(LogLoaded.class);
    }

    @Test
    void getBetween_returns_range() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(graph("g1"), LEADER);
        backend.append(graph("g2"), LEADER);
        backend.append(graph("g3"), LEADER);

        LogEvent[] mid = backend.getBetween(1, 3);
        assertThat(mid).hasSize(2);
        assertThat(((LogChangeGraph) mid[0]).nodes().get(0).name()).isEqualTo("g1");
        assertThat(((LogChangeGraph) mid[1]).nodes().get(0).name()).isEqualTo("g2");
    }

    @Test
    void get_out_of_range_throws() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        assertThatThrownBy(() -> backend.get(5)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> backend.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> backend.getBetween(0, 5)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> backend.getBetween(-1, 0)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> backend.getBetween(2, 1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void introspect_counts_events_by_simple_class_name() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(graph("g1"), LEADER);
        backend.append(graph("g2"), LEADER);

        var report = backend.introspect();
        assertThat(report.length()).isEqualTo(3);
        assertThat(report.currentLeader()).isEqualTo(LEADER);
        assertThat(report.eventCounts())
                .containsEntry("LogLeader", 1)
                .containsEntry("LogChangeGraph", 2);
    }

    @Test
    void close_then_operations_throw() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.close();
        backend = null; // already closed
        var fresh = create();
        try {
            fresh.append(new LogLeader(0, now(), LEADER), LEADER);
            fresh.close();
            assertThatThrownBy(() -> fresh.append(graph("g1"), LEADER))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            if (!isClosed(fresh)) fresh.close();
        }
    }

    @Test
    void persistence_round_trip_if_supported() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(new LogInitialized(0, now(), "Foo", Map.of("k", new byte[]{1, 2, 3})), LEADER);
        backend.append(new LogLoaded(0, now(), new Sid("Foo", 1)), LEADER);
        int originalLength = backend.length();

        backend.close();
        LogBackend reopened = reopen(backend);
        backend = reopened;
        if (reopened == null) {
            return; // backend doesn't persist (e.g., in-memory)
        }

        assertThat(reopened.length()).isEqualTo(originalLength);
        assertThat(reopened.get(0)).isInstanceOf(LogLeader.class);
        assertThat(reopened.get(1)).isInstanceOf(LogInitialized.class);
        assertThat(((LogInitialized) reopened.get(1)).properties())
                .containsEntry("k", new byte[]{1, 2, 3});
        assertThat(reopened.get(2)).isInstanceOf(LogLoaded.class);
    }

    @Test
    void replaces_survives_append_compaction_and_reopen() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(new LogInitialized(0, now(), "Foo", Map.of()), LEADER);
        var written = backend.append(
                new LogInitialized(0, now(), "Foo", Map.of("k", new byte[]{5}), new Sid("Foo", 1)), LEADER);
        assertThat(written).get().extracting(e -> ((LogInitialized) e).replaces()).isEqualTo(new Sid("Foo", 1));
        assertThat(((LogInitialized) backend.get(1)).replaces()).isNull();
        assertThat(((LogInitialized) backend.get(2)).replaces()).isEqualTo(new Sid("Foo", 1));

        backend.compact(List.of(
                new LogLeader(0, now(), LEADER),
                new LogInitialized(1, now(), "Foo", Map.of()),
                new LogInitialized(2, now(), "Foo", Map.of("k", new byte[]{5}), new Sid("Foo", 1)),
                new LogSnapshot(3, now(), 2)), LEADER);
        assertThat(((LogInitialized) backend.get(2)).replaces()).isEqualTo(new Sid("Foo", 1));

        backend.close();
        LogBackend reopened = reopen(backend);
        backend = reopened;
        if (reopened == null) {
            return;
        }
        assertThat(((LogInitialized) reopened.get(1)).replaces()).isNull();
        assertThat(((LogInitialized) reopened.get(2)).replaces()).isEqualTo(new Sid("Foo", 1));
        assertThat(((LogInitialized) reopened.get(2)).properties()).containsEntry("k", new byte[]{5});
    }

    @Test
    void compact_keeps_clocks_the_snapshot_marker_and_the_leader_and_appends_continue_after_them() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        for (int i = 0; i < 5; i++) {
            backend.append(new LogInitialized(0, now(), "P" + i, Map.of()), LEADER);
        }
        var result = backend.compact(List.of(
                new LogLeader(0, now(), LEADER),
                new LogInitialized(5, now(), "P4", Map.of("k", new byte[]{7})),
                new LogSnapshot(6, now(), 5)), LEADER);

        assertThat(result.eventsCopied()).isEqualTo(3);
        assertThat(result.checkpointClock()).isEqualTo(5);
        assertThat(backend.length()).isEqualTo(3);
        assertThat(backend.getBetween(0, 3)).extracting(LogEvent::clock)
                .as("compaction keeps the clocks it is given").containsExactly(0L, 5L, 6L);
        assertThat(((LogInitialized) backend.get(1)).properties()).containsEntry("k", new byte[]{7});
        assertThat(((LogSnapshot) backend.get(2)).checkpointClock()).isEqualTo(5);
        assertThat(backend.introspect().currentLeader()).isEqualTo(LEADER);
        assertThat(backend.introspect().eventCounts())
                .as("introspect counts the compacted contents, not the pre-compaction ones")
                .isEqualTo(Map.of("LogLeader", 1, "LogInitialized", 1, "LogSnapshot", 1));
        assertThat(backend.append(new LogInitialized(0, now(), "Q", Map.of()), LEADER))
                .as("an append continues after the last clock, never reusing one")
                .get().extracting(LogEvent::clock).isEqualTo(7L);
        assertThat(backend.append(new LogInitialized(0, now(), "Q", Map.of()), OTHER_LEADER)).isEmpty();
    }

    /**
     * Clocks are {@code long}: a backend that narrows the next clock to {@code int}
     * would pass every small-log test and then append clock {@code -2147483648}.
     */
    @Test
    void clocks_continue_past_integer_max_value_after_compaction_and_reopen() {
        long max = Integer.MAX_VALUE;
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.compact(List.of(
                new LogLeader(0, now(), LEADER),
                new LogInitialized(max - 1, now(), "Big", Map.of("k", new byte[]{9})),
                new LogSnapshot(max, now(), max - 1)), LEADER);
        assertThat(backend.getBetween(0, 3)).extracting(LogEvent::clock).containsExactly(0L, max - 1, max);

        assertThat(backend.append(new LogInitialized(0, now(), "Q1", Map.of()), LEADER))
                .as("the clock after Integer.MAX_VALUE must not wrap").get()
                .extracting(LogEvent::clock).isEqualTo(max + 1);
        assertThat(backend.length()).isEqualTo(4);
        assertThat(backend.get(3).clock()).isEqualTo(max + 1);
        assertThat(backend.append(new LogInitialized(0, now(), "Q2", Map.of()), LEADER))
                .get().extracting(LogEvent::clock).isEqualTo(max + 2);
        assertThat(backend.length()).isEqualTo(5);
        assertThat(backend.introspect().length()).isEqualTo(5);

        backend.close();
        LogBackend reopened = reopen(backend);
        backend = reopened;
        if (reopened == null) {
            return; // backend doesn't persist (e.g., in-memory)
        }
        assertThat(reopened.length()).isEqualTo(5);
        assertThat(reopened.getBetween(0, 5)).extracting(LogEvent::clock)
                .containsExactly(0L, max - 1, max, max + 1, max + 2);
        assertThat(((LogInitialized) reopened.get(4)).processName()).isEqualTo("Q2");
        assertThat(reopened.introspect().length()).isEqualTo(5);
        assertThat(reopened.append(new LogInitialized(0, now(), "Q3", Map.of()), LEADER))
                .as("appends continue after the highest clock once reopened").get()
                .extracting(LogEvent::clock).isEqualTo(max + 3);
        assertThat(reopened.length()).isEqualTo(6);
    }

    @Test
    void compact_refuses_clocks_that_do_not_strictly_increase() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(new LogInitialized(0, now(), "P", Map.of()), LEADER);
        assertThatThrownBy(() -> backend.compact(List.of(
                new LogLeader(0, now(), LEADER),
                new LogInitialized(3, now(), "P", Map.of()),
                new LogSnapshot(3, now(), 1)), LEADER))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(backend.length()).isEqualTo(2);
    }

    @Test
    void compact_refuses_events_not_led_by_the_leader_and_leaves_the_log_alone() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(new LogInitialized(0, now(), "P", Map.of()), LEADER);
        assertThatThrownBy(() -> backend.compact(List.of(new LogSnapshot(0, now(), 1)), LEADER))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> backend.compact(List.of(new LogLeader(0, now(), OTHER_LEADER)), LEADER))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(backend.length()).isEqualTo(2);
        assertThat(backend.append(new LogInitialized(0, now(), "Q", Map.of()), LEADER)).isPresent();
    }

    /** Compaction rewrites the leading {@link LogLeader}, so a deposed leader must not be able to re-claim the log with it. */
    @Test
    void compact_by_a_deposed_leader_is_refused_and_leaves_the_log_alone() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(new LogInitialized(0, now(), "P", Map.of()), LEADER);
        backend.append(new LogLeader(0, now(), OTHER_LEADER), OTHER_LEADER); // takeover
        int lengthBefore = backend.length();
        LogEvent[] before = backend.getBetween(0, lengthBefore);

        assertThatThrownBy(() -> backend.compact(List.of(
                new LogLeader(0, now(), LEADER),
                new LogSnapshot(3, now(), 2)), LEADER))
                .as("a deposed leader must not be able to re-claim the log by compacting")
                .isInstanceOf(LeadershipLostException.class);

        assertThat(backend.length()).as("the log is untouched").isEqualTo(lengthBefore);
        assertThat(backend.getBetween(0, lengthBefore)).extracting(LogEvent::clock)
                .containsExactly(Arrays.stream(before).map(LogEvent::clock).toArray(Long[]::new));
        assertThat(backend.getBetween(0, lengthBefore)).extracting(e -> e.getClass().getSimpleName())
                .containsExactly(Arrays.stream(before)
                        .map(e -> e.getClass().getSimpleName()).toArray(String[]::new));
        assertThat(backend.introspect().currentLeader())
                .as("the takeover still stands").isEqualTo(OTHER_LEADER);
        assertThat(backend.append(new LogInitialized(0, now(), "Q", Map.of()), LEADER))
                .as("the deposed leader still cannot append").isEmpty();

        var result = backend.compact(List.of(
                new LogLeader(0, now(), OTHER_LEADER),
                new LogSnapshot(4, now(), 3)), OTHER_LEADER);
        assertThat(result.eventsCopied()).isEqualTo(2);
        assertThat(backend.length()).isEqualTo(2);
        assertThat(backend.introspect().currentLeader()).isEqualTo(OTHER_LEADER);
        assertThat(backend.append(new LogInitialized(0, now(), "R", Map.of()), OTHER_LEADER))
                .as("the new leader keeps writing after its own compaction").isPresent();
    }

    /** A takeover makes a pending compaction plan's clocks stale; the lost leadership is what gets reported. */
    @Test
    void a_deposed_leader_is_told_about_leadership_before_its_plan_is_criticised() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.append(new LogInitialized(0, now(), "p1", Map.of()), LEADER);
        backend.append(new LogLeader(0, now(), OTHER_LEADER), OTHER_LEADER); // takeover
        int lengthBefore = backend.length();

        List<LogEvent> stalePlan = List.of(
                new LogLeader(0, now(), LEADER),
                new LogInitialized(1, now(), "p1", Map.of()),
                new LogInitialized(1, now(), "p2", Map.of())); // not strictly increasing
        assertThatThrownBy(() -> backend.compact(stalePlan, LEADER))
                .as("the deposed leader must hear about the takeover, not about its clocks")
                .isInstanceOf(LeadershipLostException.class);
        assertThat(backend.length()).as("nothing was replaced").isEqualTo(lengthBefore);
    }

    @Test
    void purging_archives_never_touches_the_live_log() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.compact(List.of(new LogLeader(0, now(), LEADER), new LogSnapshot(1, now(), 0)), LEADER);
        backend.compact(List.of(new LogLeader(0, now(), LEADER), new LogSnapshot(2, now(), 1)), LEADER);
        backend.purgeArchives(0);
        assertThat(backend.length()).isEqualTo(2);
        assertThat(backend.get(0)).isInstanceOf(LogLeader.class);
        assertThat(backend.append(new LogInitialized(0, now(), "P", Map.of()), LEADER)).isPresent();
        assertThatThrownBy(() -> backend.purgeArchives(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void an_interrupted_caller_does_not_break_the_log() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        Thread.currentThread().interrupt();
        try {
            backend.append(new LogInitialized(0, now(), "Interrupted", Map.of()), LEADER);
        } catch (RuntimeException acceptable) {
            // the interrupted call itself may fail
        } finally {
            Thread.interrupted();
        }
        int length = backend.length();
        assertThat(backend.append(new LogInitialized(0, now(), "After", Map.of()), LEADER)).isPresent();
        assertThat(backend.length()).isEqualTo(length + 1);
        assertThat(((LogInitialized) backend.get(length)).processName()).isEqualTo("After");
        assertThat(backend.introspect().length()).isEqualTo(length + 1);
    }

    @Test
    void range_reads_are_safe_while_appends_run() throws Exception {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        var stop = new AtomicBoolean();
        Thread writer = Thread.ofPlatform().start(() -> {
            for (int i = 0; i < 300 && !stop.get(); i++) {
                backend.append(new LogInitialized(0, now(), "W" + i, Map.of()), LEADER);
            }
        });
        try {
            while (writer.isAlive()) {
                int length = backend.length();
                LogEvent[] events = backend.getBetween(0, length);
                assertThat(events).hasSize(length);
                assertThat(events[0]).isInstanceOf(LogLeader.class);
            }
        } finally {
            stop.set(true);
            writer.join(10_000);
        }
        assertThat(backend.getBetween(0, backend.length())).hasSize(backend.length());
    }

    @Test
    void interrupting_a_virtual_thread_mid_append_does_not_break_the_log() throws Exception {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        var stop = new AtomicBoolean();
        Thread writer = Thread.ofVirtual().start(() -> {
            while (!stop.get()) {
                try {
                    backend.append(new LogInitialized(0, now(), "W", Map.of()), LEADER);
                } catch (RuntimeException acceptable) {
                    // an append hit by the interrupt may fail; the log must survive it
                }
            }
        });
        for (int i = 0; i < 50; i++) {
            writer.interrupt();
            Thread.sleep(2);
        }
        stop.set(true);
        writer.join(10_000);
        int length = backend.length();
        assertThat(backend.append(new LogInitialized(0, now(), "After", Map.of()), LEADER))
                .as("the backend still accepts the leader's writes").isPresent();
        assertThat(backend.length()).isEqualTo(length + 1);
        assertThat(backend.introspect().currentLeader()).isEqualTo(LEADER);
    }

    protected static long now() {
        return System.currentTimeMillis();
    }

    private static LogChangeGraph graph(String nodeName) {
        return new LogChangeGraph(0, now(), List.of(new LogChangeGraph.Node(nodeName, List.of(), List.of(), null)));
    }

    /** {@link LogBackend} has no {@code isClosed}; a backend that fails {@code length()} counts as closed. */
    private static boolean isClosed(LogBackend b) {
        try {
            b.length();
            return false;
        } catch (Throwable t) {
            return true;
        }
    }

    /** Simple names of the event types a backend may store. */
    protected static final List<String> ALL_TYPES = List.of("LogLeader", "LogChangeGraph", "LogInitialized", "LogLoaded",
            "LogDead", "LogCleanedUp", "LogTrigger", "LogDependencyChanged", "LogSnapshot");
}
