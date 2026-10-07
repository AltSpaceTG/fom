package io.fom.api;

import io.fom.ProcessRef;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * What {@code compute}, {@code init} and {@code load} get; the only way to query
 * another process.
 */
public interface QueryableContext extends ProcessContext {

    /**
     * Query a declared dependency.
     *
     * @throws UndeclaredDependencyException if {@code dependencyName} is not in {@link #dependencies()}
     */
    CompletionStage<Object> query(String dependencyName, Object query);

    /** {@link #query(String, Object)} keyed by a typed {@link ProcessRef}. */
    default CompletionStage<Object> query(ProcessRef dependency, Object query) {
        Objects.requireNonNull(dependency, "dependency");
        return query(dependency.name(), query);
    }

    /** The process's declared dependencies. */
    List<String> dependencies();

    /** The deadline of the query being answered; empty in {@code init} and {@code load}. */
    Optional<Deadline> currentQueryDeadline();
}
