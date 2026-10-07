package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Builds a {@link Graph}. Dependencies may name nodes added later; the graph is
 * validated as a whole at {@link #build()}. The last node added becomes
 * {@link Graph#top()}.
 *
 * <p>Factories and route resolvers are never written to the log, so they may
 * capture anything.</p>
 */
public final class GraphBuilder {

    private final Map<String, ProcessNode> nodes = new LinkedHashMap<>();
    private final Map<Class<?>, QueryRoute> typeRouting = new HashMap<>();
    private String last;

    public GraphBuilder() {
    }

    public GraphBuilder add(String name,
                            Supplier<? extends ProcessInitializer> initFactory,
                            Supplier<? extends ProcessLoader> loadFactory,
                            String... dependencies) {
        return addNode(name, initFactory, loadFactory, null, reactive(dependencies));
    }

    public GraphBuilder addDeps(String name,
                                Supplier<? extends ProcessInitializer> initFactory,
                                Supplier<? extends ProcessLoader> loadFactory,
                                Dependency... dependencies) {
        return addNode(name, initFactory, loadFactory, null, Arrays.asList(dependencies));
    }

    public <P extends Serializable> GraphBuilder addWithParam(
            String name,
            Supplier<? extends ParamProcessInitializer<P>> initFactory,
            Supplier<? extends ParamProcessLoader<P>> loadFactory,
            P param,
            String... dependencies) {
        return addNode(name, initFactory, loadFactory, param, reactive(dependencies));
    }

    public <P extends Serializable> GraphBuilder addWithParamDeps(
            String name,
            Supplier<? extends ParamProcessInitializer<P>> initFactory,
            Supplier<? extends ParamProcessLoader<P>> loadFactory,
            P param,
            Dependency... dependencies) {
        return addNode(name, initFactory, loadFactory, param, Arrays.asList(dependencies));
    }

    /** {@link #add(String, Supplier, Supplier, String...)} keyed by {@link ProcessRef}. */
    public GraphBuilder add(ProcessRef ref,
                            Supplier<? extends ProcessInitializer> initFactory,
                            Supplier<? extends ProcessLoader> loadFactory,
                            ProcessRef... dependencies) {
        Objects.requireNonNull(ref, "ref");
        return addNode(ref.name(), initFactory, loadFactory, null, reactive(dependencies));
    }

    /** {@link #addWithParam} keyed by {@link ProcessRef}. */
    public <P extends Serializable> GraphBuilder addWithParam(
            ProcessRef ref,
            Supplier<? extends ParamProcessInitializer<P>> initFactory,
            Supplier<? extends ParamProcessLoader<P>> loadFactory,
            P param,
            ProcessRef... dependencies) {
        Objects.requireNonNull(ref, "ref");
        return addNode(ref.name(), initFactory, loadFactory, param, reactive(dependencies));
    }

    /** {@link #addDeps(String, Supplier, Supplier, Dependency...)} keyed by {@link ProcessRef}. */
    public GraphBuilder addDeps(ProcessRef ref,
                                Supplier<? extends ProcessInitializer> initFactory,
                                Supplier<? extends ProcessLoader> loadFactory,
                                Dependency... dependencies) {
        Objects.requireNonNull(ref, "ref");
        return addDeps(ref.name(), initFactory, loadFactory, dependencies);
    }

    /** {@link #addWithParamDeps(String, Supplier, Supplier, Serializable, Dependency...)} keyed by {@link ProcessRef}. */
    public <P extends Serializable> GraphBuilder addWithParamDeps(
            ProcessRef ref,
            Supplier<? extends ParamProcessInitializer<P>> initFactory,
            Supplier<? extends ParamProcessLoader<P>> loadFactory,
            P param,
            Dependency... dependencies) {
        Objects.requireNonNull(ref, "ref");
        return addWithParamDeps(ref.name(), initFactory, loadFactory, param, dependencies);
    }

    /**
     * Set how the most recently added node re-initialises, overriding
     * {@link EngineConfig#reinitStrategy()}. Not part of the node's definition: changing only
     * this does not make a graph swap restart the node.
     *
     * @throws IllegalStateException if called before any {@code add(...)}
     */
    public GraphBuilder reinitStrategy(ReinitStrategy strategy) {
        Objects.requireNonNull(strategy, "strategy");
        if (last == null) {
            throw new IllegalStateException("reinitStrategy(...) called before any add(...)");
        }
        nodes.put(last, nodes.get(last).withReinitStrategy(strategy));
        return this;
    }

    /**
     * Route the given query classes to the most recently added node.
     *
     * @throws IllegalStateException if called before any {@code add(...)}
     * @throws IllegalArgumentException if a class is already routed
     */
    public GraphBuilder handles(Class<?>... queryTypes) {
        Objects.requireNonNull(queryTypes, "queryTypes");
        if (last == null) {
            throw new IllegalStateException("handles(...) called before any add(...)");
        }
        return routeTo(last, queryTypes);
    }

    /**
     * Route the given query classes to the named node, whatever was added after it.
     *
     * @throws IllegalArgumentException if {@code name} is unknown or a class is already routed
     */
    public GraphBuilder handlesFor(String name, Class<?>... queryTypes) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(queryTypes, "queryTypes");
        if (!nodes.containsKey(name)) {
            throw new IllegalArgumentException("handlesFor(...) for unknown node: '" + name + "'");
        }
        return routeTo(name, queryTypes);
    }

    /** {@link #handlesFor(String, Class[])} keyed by {@link ProcessRef}. */
    public GraphBuilder handlesFor(ProcessRef ref, Class<?>... queryTypes) {
        Objects.requireNonNull(ref, "ref");
        return handlesFor(ref.name(), queryTypes);
    }

    /**
     * Route a query class to the process {@code resolver} names for each query.
     * The resolver must be pure and quick.
     *
     * @throws IllegalArgumentException if the class is already routed
     */
    public <Q> GraphBuilder route(Class<Q> queryType, Function<Q, String> resolver) {
        Objects.requireNonNull(queryType, "queryType");
        Objects.requireNonNull(resolver, "resolver");
        requireRoutable(queryType);
        requireUnrouted(queryType);
        @SuppressWarnings({"unchecked", "rawtypes"})
        Function<Object, String> erased = (Function) resolver;
        typeRouting.put(queryType, new QueryRoute.Dynamic(erased));
        return this;
    }

    /** Type routing matches a query's exact class, so a type no query can have is refused. */
    static void requireRoutable(Class<?> type) {
        if (type == Object.class) {
            throw new IllegalArgumentException("Query type java.lang.Object can never be routed:"
                    + " register each concrete class or use a Routable message");
        }
        Class<?> superclass = type.getSuperclass();
        if (!type.isEnum() && superclass != null && superclass.isEnum()) {
            throw new IllegalArgumentException("Query type " + type.getName() + " is the body of an enum"
                    + " constant; register its enum " + superclass.getName() + " instead");
        }
        // An abstract enum is fine: its constants route by the enum type (see Engine.resolveTarget).
        boolean abstractClass = !type.isArray() && !type.isEnum() && Modifier.isAbstract(type.getModifiers());
        if (type.isInterface() || type.isPrimitive() || abstractClass) {
            throw new IllegalArgumentException("Query type " + type.getName() + " can never be routed: routing"
                    + " matches a query's exact class, so register each concrete class or use a Routable message");
        }
    }

    private GraphBuilder routeTo(String processName, Class<?>[] queryTypes) {
        for (Class<?> type : queryTypes) {
            Objects.requireNonNull(type, "queryType");
            requireRoutable(type);
            requireUnrouted(type);
            typeRouting.put(type, new QueryRoute.Static(processName));
        }
        return this;
    }

    private void requireUnrouted(Class<?> type) {
        QueryRoute existing = typeRouting.get(type);
        if (existing != null) {
            throw new IllegalArgumentException("Query type " + type.getName() + " already routed to "
                    + describe(existing));
        }
    }

    private GraphBuilder addNode(String name,
                                 Supplier<?> initFactory,
                                 Supplier<?> loadFactory,
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
     * @throws IllegalStateException if no node was added
     * @throws IllegalArgumentException on a cycle, a missing dependency or a route to an unknown node
     */
    public Graph build() {
        if (last == null) {
            throw new IllegalStateException("GraphBuilder is empty");
        }
        return new Graph(nodes.get(last), nodes, typeRouting);
    }

    private static List<Dependency> reactive(String[] names) {
        var out = new ArrayList<Dependency>(names.length);
        for (String name : names) {
            out.add(Dependency.reactive(name));
        }
        return out;
    }

    private static List<Dependency> reactive(ProcessRef[] refs) {
        var out = new ArrayList<Dependency>(refs.length);
        for (ProcessRef ref : refs) {
            Objects.requireNonNull(ref, "dependency ref");
            out.add(Dependency.reactive(ref));
        }
        return out;
    }

    private static String describe(QueryRoute route) {
        return switch (route) {
            case QueryRoute.Static s -> "Static('" + s.processName() + "')";
            case QueryRoute.Dynamic ignored -> "Dynamic(...)";
        };
    }
}
