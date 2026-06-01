package io.fom.examples;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.SerializableSupplier;
import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.QueryableContext;
import io.fom.api.Routable;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import io.fom.tenant.TenantAccessDeniedException;
import io.fom.tenant.TenantAwareEngine;
import io.fom.tenant.TenantCaller;
import io.fom.tenant.TenantId;
import io.fom.tenant.TenantResolver;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * One process per tenant ({@code Inventory_PUB1}, {@code Inventory_PUB2}),
 * addressed by a {@link Routable} message, wrapped in a {@link TenantAwareEngine}
 * that enforces per-tenant authorization on top of the plain engine.
 *
 * <p>The wrapper is <b>fail-closed</b>: without an explicit {@code authzPolicy}
 * it denies everything. Here we allow a caller to touch only the tenants it
 * carries.</p>
 *
 * <p>Run: {@code ./gradlew :examples:multiTenant}</p>
 */
public final class MultiTenantExample {

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .addWithParam("Inventory_PUB1",
                        (SerializableSupplier<ParamProcessInitializer<Pub>>) InvInit::new,
                        (SerializableSupplier<ParamProcessLoader<Pub>>) InvInit::new,
                        new Pub("PUB1"))
                .addWithParam("Inventory_PUB2",
                        (SerializableSupplier<ParamProcessInitializer<Pub>>) InvInit::new,
                        (SerializableSupplier<ParamProcessLoader<Pub>>) InvInit::new,
                        new Pub("PUB2"))
                .build();

        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true)) {

            engine.newGraph(graph);

            var aware = TenantAwareEngine.builder(engine)
                    .tenantResolver(TenantResolver.suffixAfter("_"))                 // "Inventory_PUB1" → PUB1
                    .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant))
                    .build();

            var alice = TenantCaller.of("alice", TenantId.of("PUB1"));   // may touch PUB1 only

            // Allowed: alice → her own tenant.
            Object ok = aware.query(alice, new GetInventory("PUB1"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("alice → PUB1: " + ok);

            // Denied: alice → someone else's tenant.
            try {
                aware.query(alice, new GetInventory("PUB2"))
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
                System.out.println("alice → PUB2: UNEXPECTEDLY ALLOWED");
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TenantAccessDeniedException denied) {
                    System.out.println("alice → PUB2: denied (" + denied.getMessage() + ")");
                } else {
                    throw e;
                }
            }
        }
    }

    /** Per-tenant parameter baked into each node's identity. */
    record Pub(String id) implements Serializable { }

    /** Routable: the message names its own target process. */
    record GetInventory(String pub) implements Routable, Serializable {
        @Override public String targetProcess() { return "Inventory_" + pub; }
    }

    /** A parameterised process family — same code, different {@code Pub} per node. */
    static final class InvInit implements ParamProcessInitializer<Pub>, ParamProcessLoader<Pub> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Pub param) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties, Pub param) {
            Process live = (c, query) -> CompletableFuture.completedFuture("inventory for " + param.id());
            return CompletableFuture.completedFuture(live);
        }
    }
}
