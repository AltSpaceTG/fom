package io.fom.examples;

import io.fom.Codecs;
import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.ProcessRef;
import io.fom.Properties;
import io.fom.TypedKey;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.api.Routable;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * A two-node graph with a dependency. {@code Products} depends on
 * {@code Inventory} and queries it <em>during its own init</em> to build a
 * denormalised view — which works because the engine spawns nodes in
 * topological order (dependencies first), so Inventory is already Serving.
 *
 * <pre>
 *   Inventory  ──&gt;  Products
 * </pre>
 *
 * <p>Processes are referenced through a typed {@link ProcessRef} constant
 * ({@code InventoryInit.REF}) rather than bare string literals — dependencies,
 * queries, and routing all go through the constant, so a rename is a
 * compile-time concern, not a find-and-replace. The durable identity is still
 * the underlying name ({@code ref.name()}); {@code ProcessRef} is just the
 * type-safe handle.</p>
 *
 * <p>Run: {@code ./gradlew :examples:multiProcess}</p>
 */
public final class MultiProcessExample {

    private static final TypedKey<Long> QTY = new TypedKey<>("qty", Codecs.longCodec());
    private static final TypedKey<String> MODEL = new TypedKey<>("model", Codecs.stringCodec());

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .add(InventoryInit.REF, InventoryInit::new, InventoryInit::new)
                .handles(GetStock.class)                       // engine.query(GetStock) → Inventory
                .add(ProductsInit.REF, ProductsInit::new, ProductsInit::new,
                        InventoryInit.REF)                     // depends on Inventory (by ref, not string)
                .handles(GetProductModel.class)                // engine.query(GetProductModel) → Products
                .build();

        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true)) {

            engine.newGraph(graph);

            // Query Inventory directly (routed by message type via .handles()).
            long stock = (Long) engine.query(new GetStock("SKU-1"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("Inventory stock for SKU-1 = " + stock);

            // Query Products — its model was built from Inventory at init time.
            String model = (String) engine.query(new GetProductModel("SKU-1"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("Product model: " + model);
        }
    }

    // ── messages ──
    record GetStock(String sku) implements Routable, Serializable {
        @Override public String targetProcess() { return InventoryInit.REF.name(); }
    }

    record GetProductModel(String sku) implements Routable, Serializable {
        @Override public String targetProcess() { return ProductsInit.REF.name(); }
    }

    // ── Inventory: owns stock levels ──
    static final class InventoryInit implements ProcessInitializer, ProcessLoader {
        /** Typed handle for this process — referenced wherever the name is needed. */
        static final ProcessRef REF = ProcessRef.of("Inventory");

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            // No manual logging here — the engine logs init/load start + completion
            // (with timing) on its own slf4j logger; set the example logger to
            // `info` (src/main/resources/simplelogger.properties) to see it.
            return CompletableFuture.completedFuture(Properties.empty().put(QTY, 42L).asRaw());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            long qty = Properties.of(properties).get(QTY);
            Process live = (c, query) -> CompletableFuture.completedFuture(qty);
            return CompletableFuture.completedFuture(live);
        }
    }

    // ── Products: depends on Inventory, builds a view from it ──
    static final class ProductsInit implements ProcessInitializer, ProcessLoader {
        static final ProcessRef REF = ProcessRef.of("Products");

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            // ctx.query addresses a DECLARED dependency. Passing the ProcessRef
            // (not a string) keeps it tied to InventoryInit. Inventory is already
            // Serving because it was spawned first (topological order).
            return ctx.query(InventoryInit.REF, new GetStock("SKU-1"))
                    .thenApply(stock -> {
                        String model = "Product SKU-1 (in stock: " + stock + ")";
                        return Properties.empty().put(MODEL, model).asRaw();
                    });
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            String model = Properties.of(properties).get(MODEL);
            Process live = (c, query) -> CompletableFuture.completedFuture(model);
            return CompletableFuture.completedFuture(live);
        }
    }
}
