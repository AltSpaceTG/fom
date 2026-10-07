package io.fom.api;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * The fast restore of a process from the cells its {@link ProcessInitializer} wrote. Runs
 * on every start, so return a ready {@link Process} without heavy I/O; defer anything
 * else to the first {@code compute}. After {@code maxLoadRetries} failures the engine
 * runs {@code init} instead.
 *
 * <p>The engine cancels the returned stage when the load is no longer wanted (budget
 * spent, process cancelled, paused or removed, engine closed). Release what you hold
 * then: a {@link Process} completed after the cancellation is dropped without
 * {@code cleanUp}. An explicit stop also interrupts the calling thread; a spent budget
 * does not.</p>
 */
@FunctionalInterface
public interface ProcessLoader {

    CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) throws LoadException;
}
