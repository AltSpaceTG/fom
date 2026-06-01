package io.fom.examples;

import io.fom.Codecs;
import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
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
 * <p>Run: {@code ./gradlew :examples:multiProcess}</p>
 */
public final class MultiProcessExample {

    private static final TypedKey<Long> QTY = new TypedKey<>("qty", Codecs.longCodec());
    private static final TypedKey<String> MODEL = new TypedKey<>("model", Codecs.stringCodec());

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .add("Inventory", InventoryInit::new, InventoryInit::new)
                .handles(GetStock.class)                       // engine.query(GetStock) → Inventory
                .add("Products", ProductsInit::new, ProductsInit::new, "Inventory")  // depends on Inventory
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
        @Override public String targetProcess() { return "Inventory"; }
    }

    record GetProductModel(String sku) implements Routable, Serializable {
        @Override public String targetProcess() { return "Products"; }
    }

    // ── Inventory: owns stock levels ──
    static final class InventoryInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            System.out.println("[Inventory] init");
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
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            System.out.println("[Products] init — querying its Inventory dependency");
            // ctx.query addresses a DECLARED dependency by name. Inventory is
            // already Serving because it was spawned first (topological order).
            return ctx.query("Inventory", new GetStock("SKU-1"))
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
