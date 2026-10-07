package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Child-JVM probe for {@link GraphSwapPinningTest}. Run with a single
 * virtual-thread carrier, it performs an in-place graph swap from a virtual
 * thread. The swap blocks until the replaced node serves, which needs the node's
 * own virtual-thread dispatcher to run. If the swap held a monitor while
 * blocking, it would pin the only carrier and never finish.
 *
 * <p>Exit codes: 0 swap finished, 2 swap reported no change, 3 swap hung.</p>
 */
public final class PinningProbe {

    record Version(String value) implements Serializable { }

    static final class Node implements ParamProcessInitializer<Version>, ParamProcessLoader<Version> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Version version) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props,
                                                        Version version) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(version.value()));
        }
    }

    private static Graph graph(String version) {
        return new GraphBuilder().addWithParam("A", Node::new, Node::new, new Version(version)).build();
    }

    public static void main(String[] args) throws Exception {
        var backend = new InMemoryLogBackend();
        var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe());
        engine.newGraph(graph("v1"));

        var swapped = new CompletableFuture<Boolean>();
        Thread.ofVirtual().start(() -> {
            try {
                swapped.complete(engine.newGraph(graph("v2")));
            } catch (Throwable t) {
                swapped.completeExceptionally(t);
            }
        });
        try {
            boolean changed = swapped.get(10, TimeUnit.SECONDS);
            System.out.println("swap finished, changed=" + changed);
            Runtime.getRuntime().halt(changed ? 0 : 2);
        } catch (TimeoutException e) {
            System.out.println("swap hung: the only carrier is pinned");
            Runtime.getRuntime().halt(3);
        }
    }
}
