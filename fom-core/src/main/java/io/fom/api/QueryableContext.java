package io.fom.api;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Context exposed to {@code Process.compute()}, {@code ProcessInitializer.init()}
 * and {@code ProcessLoader.load()} — provides the only legal entry point for
 * cross-process queries.
 */
public interface QueryableContext extends ProcessContext {

    /**
     * Issue a query to a declared dependency. The {@code dependencyName} MUST
     * appear in {@link #dependencies()}; otherwise the returned stage fails
     * synchronously with {@code QueryException("No such dependency: ...")}.
     */
    CompletionStage<Object> query(String dependencyName, Object query);

    /** Names of all declared direct dependencies of the current process. */
    List<String> dependencies();

    /**
     * Returns the deadline of the top-level query that ultimately triggered
     * this context, when this context was created for an inbound query.
     * Empty for contexts created from a watcher tick or from startup init
     * with no waiters.
     */
    Optional<Deadline> currentQueryDeadline();
}
