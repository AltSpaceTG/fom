package io.fom;

import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogDead;
import io.fom.log.LogDependencyChanged;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogLoaded;
import io.fom.log.LogSnapshot;
import io.fom.log.LogTrigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** The scan's incumbent/candidate rules and what compaction keeps of them. Clock = log position. */
class LogCompactionScanTest {

    private static final String LEADER = "leader";

    private final InMemoryLogBackend backend = new InMemoryLogBackend();

    LogCompactionScanTest() {
        backend.append(new LogLeader(0, 0, LEADER), LEADER);
    }

    @AfterEach
    void close() {
        backend.close();
    }

    private LogEvent append(LogEvent event) {
        return backend.append(event, LEADER).orElseThrow();
    }

    private Sid init(String name) {
        return ((LogInitialized) append(new LogInitialized(0, 0, name, Map.of()))).sid();
    }

    private Sid replacement(Sid replaces) {
        return ((LogInitialized) append(new LogInitialized(0, 0, replaces.processName(), Map.of(), replaces))).sid();
    }

    private void loaded(Sid sid) {
        append(new LogLoaded(0, 0, sid));
    }

    private void dead(Sid sid) {
        append(new LogDead(0, 0, sid));
    }

    private void trigger(String name) {
        append(new LogTrigger(0, 0, List.of(name)));
    }

    private void graph(String... names) {
        var nodes = Arrays.stream(names)
                .map(n -> new LogChangeGraph.Node(n, List.of(), List.of(), new byte[0]))
                .toList();
        append(new LogChangeGraph(0, 0, nodes));
    }

    private LogCompaction.Scan scan() {
        return LogCompaction.scan(backend);
    }

    private static Map<String, Sid> sids(Map<String, LogCompaction.LiveInit> inits) {
        var out = new TreeMap<String, Sid>();
        inits.forEach((name, live) -> out.put(name, live.init().sid()));
        return out;
    }

    @Test
    void a_cold_init_is_the_incumbent() {
        Sid a = init("A");
        loaded(a);

        assertThat(sids(scan().liveInits())).containsExactly(Map.entry("A", a));
        assertThat(scan().candidates()).isEmpty();
    }

    @Test
    void an_init_replacing_the_live_incumbent_is_a_candidate_and_the_incumbent_stays_live() {
        Sid a1 = init("A");
        loaded(a1);
        Sid a2 = replacement(a1);

        var scan = scan();
        assertThat(sids(scan.liveInits())).containsExactly(Map.entry("A", a1));
        assertThat(sids(scan.candidates())).containsExactly(Map.entry("A", a2));
    }

    @Test
    void loading_the_candidate_promotes_it_and_the_old_version_retiring_later_changes_nothing() {
        Sid a1 = init("A");
        loaded(a1);
        Sid a2 = replacement(a1);
        loaded(a1);   // not the candidate: no promotion
        assertThat(sids(scan().liveInits())).containsExactly(Map.entry("A", a1));

        loaded(a2);
        assertThat(sids(scan().liveInits())).containsExactly(Map.entry("A", a2));
        assertThat(scan().candidates()).isEmpty();

        dead(a1);
        assertThat(sids(scan().liveInits())).containsExactly(Map.entry("A", a2));
    }

    @Test
    void a_retired_candidate_leaves_the_incumbent_live() {
        Sid a1 = init("A");
        Sid a2 = replacement(a1);
        dead(a2);

        assertThat(sids(scan().liveInits())).containsExactly(Map.entry("A", a1));
        assertThat(scan().candidates()).isEmpty();
    }

    @Test
    void retiring_the_incumbent_drops_the_candidate_too() {
        Sid a1 = init("A");
        replacement(a1);
        dead(a1);

        assertThat(scan().liveInits()).isEmpty();
        assertThat(scan().candidates()).isEmpty();
    }

    @Test
    void a_later_replacement_supersedes_the_earlier_candidate() {
        Sid a1 = init("A");
        replacement(a1);
        Sid a3 = replacement(a1);

        assertThat(sids(scan().liveInits())).containsExactly(Map.entry("A", a1));
        assertThat(sids(scan().candidates())).containsExactly(Map.entry("A", a3));
    }

    @Test
    void a_cold_init_replaces_incumbent_and_candidate() {
        Sid a1 = init("A");
        replacement(a1);
        Sid a3 = init("A");

        assertThat(sids(scan().liveInits())).containsExactly(Map.entry("A", a3));
        assertThat(scan().candidates()).isEmpty();
    }

    @Test
    void replacing_a_version_that_is_gone_makes_the_init_the_incumbent() {
        Sid a1 = init("A");
        dead(a1);
        Sid a3 = replacement(a1);
        // A Sid the log never had, as after a compaction dropped it.
        Sid b = replacement(new Sid("B", 99));

        assertThat(sids(scan().liveInits())).containsExactly(Map.entry("A", a3), Map.entry("B", b));
        assertThat(scan().candidates()).isEmpty();
    }

    @Test
    void a_request_is_pending_only_if_newer_than_both_incumbent_and_candidate() {
        Sid a1 = init("A");
        trigger("A");
        assertThat(scan().pendingReinit()).containsExactly("A");

        replacement(a1);
        assertThat(scan().pendingReinit()).as("the candidate answers the request").isEmpty();

        append(new LogDependencyChanged(0, 0, a1, "Dep", 0, 1));
        assertThat(scan().pendingReinit()).containsExactly("A");
    }

    @Test
    void a_request_older_than_a_retired_candidate_is_pending_again() {
        Sid a1 = init("A");
        trigger("A");
        Sid a3 = replacement(a1);
        dead(a3);

        assertThat(scan().pendingReinit()).containsExactly("A");
    }

    @Test
    void compaction_keeps_incumbent_and_candidate_and_a_restart_scan_sees_the_same() {
        graph("A", "B", "Gone");
        Sid a1 = init("A");
        Sid b1 = init("B");
        init("Gone");
        loaded(a1);
        loaded(b1);
        graph("A", "B");
        Sid a2 = replacement(a1);
        Sid b2 = replacement(b1);
        dead(b2);

        var before = scan();
        assertThat(sids(before.liveInits())).containsEntry("A", a1).containsEntry("B", b1);
        assertThat(sids(before.candidates())).containsExactly(Map.entry("A", a2));

        var plan = LogCompaction.plan(backend, LEADER, 0);
        backend.compact(plan.events(), LEADER);

        List<LogEvent> kept = List.of(backend.getBetween(0, backend.length()));
        assertThat(kept).extracting(e -> e.getClass().getSimpleName()).containsExactly(
                "LogLeader", "LogChangeGraph", "LogInitialized", "LogInitialized",
                "LogChangeGraph", "LogInitialized", "LogSnapshot");
        assertThat(((LogChangeGraph) kept.get(1)).nodes()).extracting(LogChangeGraph.Node::name)
                .as("the older graph is trimmed to the nodes kept with it").containsExactly("A", "B");
        var candidate = (LogInitialized) kept.get(5);
        assertThat(candidate.sid()).isEqualTo(a2);
        assertThat(candidate.replaces()).isEqualTo(a1);
        assertThat(kept).extracting(LogEvent::clock).isSorted();

        var after = scan();
        assertThat(sids(after.liveInits())).isEqualTo(sids(before.liveInits()).entrySet().stream()
                .filter(e -> !e.getKey().equals("Gone"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        assertThat(sids(after.candidates())).isEqualTo(sids(before.candidates()));
        assertThat(after.liveInits().get("A").graphPosition()).isEqualTo(1);
        assertThat(after.candidates().get("A").graphPosition()).isEqualTo(4);
        assertThat(kept.get(kept.size() - 1)).isInstanceOf(LogSnapshot.class);
    }
}
