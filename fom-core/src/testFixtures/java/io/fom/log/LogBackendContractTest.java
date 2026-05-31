package io.fom.log;

import io.fom.Sid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Shared invariants of any {@link LogBackend} implementation. Subclasses
 * provide the backend factory; the test methods run unchanged. Lives in
 * {@code testFixtures} so every backend module (in-memory, file, Postgres)
 * can exercise the same contract.
 */
public abstract class LogBackendContractTest {

    protected static final String LEADER = "instance-A";
    protected static final String OTHER_LEADER = "instance-B";

    protected LogBackend backend;

    /** Build a fresh empty backend for each test. */
    protected abstract LogBackend create();

    /** Reopen the same underlying storage as {@code original}. May return null for backends that don't persist. */
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
        var rejected = backend.append(new LogChangeGraph(0, now(), new byte[]{1, 2, 3}), LEADER);
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
        var a = backend.append(new LogChangeGraph(0, now(), new byte[]{1}), LEADER).orElseThrow();
        var b = backend.append(new LogChangeGraph(0, now(), new byte[]{2}), LEADER).orElseThrow();
        assertThat(leader.clock()).isEqualTo(0);
        assertThat(a.clock()).isEqualTo(1);
        assertThat(b.clock()).isEqualTo(2);
        assertThat(backend.length()).isEqualTo(3);
    }

    @Test
    void non_leader_append_is_rejected() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        var rejected = backend.append(new LogChangeGraph(0, now(), new byte[]{1}), OTHER_LEADER);
        assertThat(rejected).isEmpty();
        assertThat(backend.length()).isEqualTo(1);
    }

    @Test
    void leader_takeover_via_new_log_leader_event() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        var takeover = backend.append(new LogLeader(0, now(), OTHER_LEADER), OTHER_LEADER);
        assertThat(takeover).isPresent();
        // After takeover, the old leader cannot append:
        assertThat(backend.append(new LogChangeGraph(0, now(), new byte[]{9}), LEADER)).isEmpty();
        // The new leader can:
        assertThat(backend.append(new LogChangeGraph(0, now(), new byte[]{9}), OTHER_LEADER)).isPresent();
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
        backend.append(new LogChangeGraph(0, now(), new byte[]{1}), LEADER);
        backend.append(new LogChangeGraph(0, now(), new byte[]{2}), LEADER);
        backend.append(new LogChangeGraph(0, now(), new byte[]{3}), LEADER);

        LogEvent[] mid = backend.getBetween(1, 3);
        assertThat(mid).hasSize(2);
        assertThat(((LogChangeGraph) mid[0]).serializedGraph()).containsExactly(1);
        assertThat(((LogChangeGraph) mid[1]).serializedGraph()).containsExactly(2);
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
        backend.append(new LogChangeGraph(0, now(), new byte[]{1}), LEADER);
        backend.append(new LogChangeGraph(0, now(), new byte[]{2}), LEADER);

        var report = backend.introspect();
        assertThat(report.length()).isEqualTo(3);
        assertThat(report.currentLeader()).isEqualTo(LEADER);
        assertThat(report.eventCountsByType())
                .containsEntry("LogLeader", 1)
                .containsEntry("LogChangeGraph", 2);
    }

    @Test
    void close_then_operations_throw() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        backend.close();
        backend = null; // suppress AfterEach close
        var fresh = create();
        try {
            fresh.append(new LogLeader(0, now(), LEADER), LEADER);
            fresh.close();
            assertThatThrownBy(() -> fresh.append(new LogChangeGraph(0, now(), new byte[]{1}), LEADER))
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

    // ───────────────── helpers ─────────────────

    protected static long now() {
        return System.currentTimeMillis();
    }

    /** Heuristic since {@link LogBackend} has no isClosed; fall back to true-positive. */
    private static boolean isClosed(LogBackend b) {
        try {
            b.length();
            return false;
        } catch (Throwable t) {
            return true;
        }
    }

    /** Concrete subclass binding the contract to a specific factory + reopen function. */
    protected static final List<String> ALL_TYPES = List.of("LogLeader", "LogChangeGraph", "LogInitialized", "LogLoaded",
            "LogDead", "LogCleanedUp", "LogTrigger", "LogDependencyChanged", "LogSnapshot");
}
