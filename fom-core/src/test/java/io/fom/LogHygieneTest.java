package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The log is the source of truth, so what is <em>live</em> in it must stay a faithful picture of the
 * graph: a process has at most one un-retired state, and a process that was removed has none. Both
 * matter beyond tidiness — compaction keeps every live state for ever, and a restart warm-loads from
 * it, so a leaked record is both unbounded growth and a process coming back on state nobody meant.
 *
 * <p>This drives a small graph through a long, seeded sequence of control-plane operations and checks
 * that invariant after every step. It was written after a flapping node was found leaving one
 * un-retired {@code LogInitialized} per failed load attempt — seven live records for one process,
 * which no scenario-style test had noticed.</p>
 */
class LogHygieneTest {

    private static final List<String> NAMES = List.of("A", "B", "C");
    /** How many more loads of a process must fail; see the "flap" operation. */
    static final Map<String, Integer> FAILING_LOADS = new java.util.concurrent.ConcurrentHashMap<>();

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8})
    @Timeout(120)
    void the_log_never_holds_two_live_states_for_one_process(int seed) throws Exception {
        var random = new Random(seed);
        var backend = new InMemoryLogBackend();
        var config = new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(2), Duration.ofMillis(300),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        var engine = new Engine(config, backend, new JavaSerializableSerDe(), new EngineObserver() { });
        var inGraph = new LinkedHashSet<>(NAMES);
        var params = new HashMap<String, String>();
        NAMES.forEach(name -> params.put(name, "v0"));
        var history = new ArrayList<String>();
        try {
            engine.newGraph(graph(inGraph, params));
            history.add("newGraph" + inGraph);
            check(engine, backend, inGraph, history);

            for (int step = 0; step < 30; step++) {
                String name = NAMES.get(random.nextInt(NAMES.size()));
                String what = apply(engine, random.nextInt(8), name, inGraph, params);
                history.add(what);
                check(engine, backend, inGraph, history);
            }
        } finally {
            engine.close();
        }
        checkLog(backend, inGraph, history); // close() must not leave a second live state either
    }

    /** One step of the control plane. Operations that are illegal right now are simply skipped. */
    private static String apply(Engine engine, int operation, String name,
                                Set<String> inGraph, Map<String, String> params) {
        try {
            switch (operation) {
                case 0 -> {
                    if (!inGraph.contains(name)) return "skip trigger " + name;
                    engine.trigger(name, "go");
                    settle();
                    return "trigger " + name;
                }
                case 1 -> {
                    if (!inGraph.contains(name) || dependents(name, inGraph)) return "skip pause " + name;
                    engine.pause(List.of(name));
                    return "pause " + name;
                }
                case 2 -> {
                    if (!inGraph.contains(name)) return "skip resume " + name;
                    engine.resume(List.of(name));
                    return "resume " + name;
                }
                case 3 -> {
                    if (!inGraph.contains(name) || inGraph.size() == 1 || dependents(name, inGraph)) {
                        return "skip remove " + name;
                    }
                    engine.remove(List.of(name));
                    inGraph.remove(name); // only once the call returned: a refusal changes nothing
                    return "remove " + name;
                }
                case 4 -> {
                    if (inGraph.contains(name)) return "skip re-add " + name;
                    var next = new LinkedHashSet<>(inGraph);
                    next.add(name);
                    engine.newGraph(graph(next, params));
                    inGraph.add(name); // the model follows the engine, never leads it
                    return "re-add " + name;
                }
                case 5 -> {
                    if (!inGraph.contains(name)) return "skip re-param " + name;
                    params.put(name, params.get(name) + "+");
                    engine.newGraph(graph(inGraph, params));
                    return "re-param " + name;
                }
                case 6 -> {
                    // A flapping node: its load fails a few times, so the engine falls back to a
                    // fresh init — the path that used to leave one live state per attempt.
                    if (!inGraph.contains(name)) return "skip flap " + name;
                    FAILING_LOADS.put(name, 2); // two failed loads, then it comes up again
                    engine.trigger(name, "flap");
                    settle();
                    settle();
                    FAILING_LOADS.remove(name);
                    return "flap " + name;
                }
                default -> {
                    engine.snapshot().toCompletableFuture().join();
                    return "snapshot";
                }
            }
        } catch (RuntimeException e) {
            // A legal-looking operation the engine refuses (a dependency rule, a node mid-start):
            // the invariant must hold after a refusal too, which is the point of not rethrowing.
            return "refused(" + operation + " " + name + "): " + e.getClass().getSimpleName();
        }
    }

    /** Whether another node in the graph depends on {@code name} (pausing/removing it would be refused). */
    private static boolean dependents(String name, Set<String> inGraph) {
        return name.equals("A") && inGraph.contains("B");
    }

    private static void settle() {
        try {
            Thread.sleep(120); // let a re-init finish, so the check does not race the LogDead
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void check(Engine engine, InMemoryLogBackend backend,
                              Set<String> inGraph, List<String> history) throws Exception {
        checkLog(backend, inGraph, history);
        // And the mirror image: a process that is serving right now must have exactly one live
        // state, or the engine over-retired and a restart would cold-init what it should warm-load.
        var live = liveStates(backend);
        for (EngineReport.NodeReport node : engine.introspect().toCompletableFuture().get().graph().nodes()) {
            if (!"Serving".equals(node.state())) continue;
            assertThat(live.getOrDefault(node.name(), List.of()))
                    .as("'%s' is Serving, so exactly one live state is expected after %s",
                            node.name(), history)
                    .hasSize(1);
        }
    }

    /**
     * Unretired states per process. A replacement written beside a live state (KEEP_OLD) and not yet
     * loaded is a candidate, not a second live state — a pause keeps it so a resume can load it — but
     * a candidate superseded by another without being retired counts: it leaked.
     */
    private static Map<String, List<Sid>> liveStates(InMemoryLogBackend backend) {
        var live = new HashMap<String, List<Sid>>();
        var candidates = new HashMap<String, Sid>();
        for (LogEvent event : backend.getBetween(0, backend.length())) {
            if (event instanceof LogInitialized init) {
                String name = init.sid().processName();
                var sids = live.computeIfAbsent(name, k -> new ArrayList<>());
                if (init.replaces() != null && sids.contains(init.replaces())) {
                    Sid superseded = candidates.put(name, init.sid());
                    if (superseded != null) sids.add(superseded);
                } else {
                    sids.add(init.sid());
                }
            } else if (event instanceof io.fom.log.LogLoaded loaded) {
                String name = loaded.sid().processName();
                if (loaded.sid().equals(candidates.get(name))) {
                    live.get(name).add(candidates.remove(name));
                }
            } else if (event instanceof LogDead dead) {
                var sids = live.get(dead.sid().processName());
                if (sids != null) sids.remove(dead.sid());
                candidates.remove(dead.sid().processName(), dead.sid());
            }
        }
        return live;
    }

    private static void checkLog(InMemoryLogBackend backend,
                                 Set<String> inGraph, List<String> history) {
        for (var entry : liveStates(backend).entrySet()) {
            assertThat(entry.getValue())
                    .as("live states of '%s' after %s", entry.getKey(), history)
                    .hasSizeLessThanOrEqualTo(1);
            if (!inGraph.contains(entry.getKey())) {
                assertThat(entry.getValue())
                        .as("'%s' was removed from the graph, so nothing of it may stay live after %s",
                                entry.getKey(), history)
                        .isEmpty();
            }
        }
    }

    private static Graph graph(Set<String> inGraph, Map<String, String> params) {
        var builder = new GraphBuilder();
        for (String name : inGraph) {
            String param = params.get(name);
            if (name.equals("B") && inGraph.contains("A")) {
                builder.addWithParam("B",
                        (Supplier<io.fom.api.ParamProcessInitializer<String>>) Node::new,
                        (Supplier<io.fom.api.ParamProcessLoader<String>>) Node::new,
                        param, "A");
            } else {
                builder.addWithParam(name,
                        (Supplier<io.fom.api.ParamProcessInitializer<String>>) Node::new,
                        (Supplier<io.fom.api.ParamProcessLoader<String>>) Node::new,
                        param);
            }
        }
        return builder.build();
    }

    static final class Node implements io.fom.api.ParamProcessInitializer<String>,
            io.fom.api.ParamProcessLoader<String> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, String param) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, String param) {
            String name = ctx.sid().processName();
            if (FAILING_LOADS.computeIfPresent(name, (k, n) -> n > 1 ? n - 1 : null) != null
                    || FAILING_LOADS.remove(name) != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("load flapped (injected)"));
            }
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(param));
        }
    }
}
