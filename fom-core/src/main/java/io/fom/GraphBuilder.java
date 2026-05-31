package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for a {@link Graph}. Resolves dependency names lazily; full
 * validation runs inside {@link Graph}'s compact constructor at {@link #build()}.
 *
 * <p>The most-recently-added node is the {@code top} returned in
 * {@link Graph#top()}. {@link #handles(Class[])} attaches static
 * type routing to that node; {@link #route(Class, Function)} attaches a
 * graph-level dynamic route.</p>
 */
public final class GraphBuilder {

    private final Map<String, ProcessNode> nodes = new LinkedHashMap<>();
    private final Map<Class<?>, QueryRoute> typeRouting = new HashMap<>();
    private String last;

    public GraphBuilder() {
    }

    public GraphBuilder add(String name,
                            SerializableSupplier<? extends ProcessInitializer> initFactory,
                            SerializableSupplier<? extends ProcessLoader> loadFactory,
                            String... dependencies) {
        return addInternal(name, initFactory, loadFactory, null, toReactiveDeps(dependencies));
    }

    public GraphBuilder addDeps(String name,
                                SerializableSupplier<? extends ProcessInitializer> initFactory,
                                SerializableSupplier<? extends ProcessLoader> loadFactory,
                                Dependency... dependencies) {
        return addInternal(name, initFactory, loadFactory, null, Arrays.asList(dependencies));
    }

    public <P extends Serializable> GraphBuilder addWithParam(
            String name,
            SerializableSupplier<? extends ParamProcessInitializer<P>> initFactory,
            SerializableSupplier<? extends ParamProcessLoader<P>> loadFactory,
            P param,
            String... dependencies) {
        return addInternal(name, initFactory, loadFactory, param, toReactiveDeps(dependencies));
    }

    public <P extends Serializable> GraphBuilder addWithParamDeps(
            String name,
            SerializableSupplier<? extends ParamProcessInitializer<P>> initFactory,
            SerializableSupplier<? extends ParamProcessLoader<P>> loadFactory,
            P param,
            Dependency... dependencies) {
        return addInternal(name, initFactory, loadFactory, param, Arrays.asList(dependencies));
    }

    /**
     * Register {@code Static} routes from the given query classes to the
     * most-recently-added node.
     *
     * @throws IllegalStateException if called before any {@code add(...)}
     * @throws IllegalArgumentException if any class is already registered
     */
    public GraphBuilder handles(Class<?>... queryTypes) {
        Objects.requireNonNull(queryTypes, "queryTypes");
        if (last == null) {
            throw new IllegalStateException("handles(...) called before any add(...)");
        }
        for (Class<?> type : queryTypes) {
            Objects.requireNonNull(type, "queryType");
            if (typeRouting.containsKey(type)) {
                throw new IllegalArgumentException(
                        "Query type " + type.getName() + " already routed to "
                                + describeRoute(typeRouting.get(type)));
            }
            typeRouting.put(type, new QueryRoute.Static(last));
        }
        return this;
    }

    /**
     * Register {@code Static} routes from the given query classes to the
     * explicitly named node, independent of insertion order. Lets a saved
     * node handle attach routes safely even after later nodes were added.
     *
     * @throws IllegalArgumentException if {@code name} is unknown or a class is already routed
     */
    public GraphBuilder handlesFor(String name, Class<?>... queryTypes) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(queryTypes, "queryTypes");
        if (!nodes.containsKey(name)) {
            throw new IllegalArgumentException("handlesFor(...) for unknown node: '" + name + "'");
        }
        for (Class<?> type : queryTypes) {
            Objects.requireNonNull(type, "queryType");
            if (typeRouting.containsKey(type)) {
                throw new IllegalArgumentException(
                        "Query type " + type.getName() + " already routed to "
                                + describeRoute(typeRouting.get(type)));
            }
            typeRouting.put(type, new QueryRoute.Static(name));
        }
        return this;
    }

    /**
     * Register a graph-level {@code Dynamic} route. The {@code resolver}
     * receives the query and returns the target process name; it must be
     * pure and quick.
     *
     * @throws IllegalArgumentException if the class is already registered
     */
    public <Q> GraphBuilder route(Class<Q> queryType, SerializableFunction<Q, String> resolver) {
        Objects.requireNonNull(queryType, "queryType");
        Objects.requireNonNull(resolver, "resolver");
        if (typeRouting.containsKey(queryType)) {
            throw new IllegalArgumentException(
                    "Query type " + queryType.getName() + " already routed to "
                            + describeRoute(typeRouting.get(queryType)));
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        SerializableFunction<Object, String> erased = (SerializableFunction) resolver;
        typeRouting.put(queryType, new QueryRoute.Dynamic(erased));
        return this;
    }

    private GraphBuilder addInternal(String name,
                                     SerializableSupplier<?> initFactory,
                                     SerializableSupplier<?> loadFactory,
                                     Serializable param,
                                     List<Dependency> dependencies) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(initFactory, "initFactory");
        Objects.requireNonNull(loadFactory, "loadFactory");
        Objects.requireNonNull(dependencies, "dependencies");
        if (nodes.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate process name: " + name);
        }
        nodes.put(name, new ProcessNode(name, dependencies, param, initFactory, loadFactory));
        last = name;
        return this;
    }

    /**
     * Materialise the accumulated state into a {@link Graph}. Delegates all
     * cross-node consistency checks (cycles, missing deps, type targets) to
     * {@link Graph}'s compact constructor.
     */
    public Graph build() {
        if (last == null) {
            throw new IllegalStateException("GraphBuilder is empty");
        }
        return new Graph(nodes.get(last), nodes, typeRouting);
    }

    private static List<Dependency> toReactiveDeps(String[] names) {
        var out = new ArrayList<Dependency>(names.length);
        for (String n : names) {
            out.add(Dependency.reactive(n));
        }
        return out;
    }

    private static String describeRoute(QueryRoute route) {
        return switch (route) {
            case QueryRoute.Static s -> "Static('" + s.processName() + "')";
            case QueryRoute.Dynamic ignored -> "Dynamic(...)";
        };
    }
}
