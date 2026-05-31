package io.fom.api;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Fast restore of a process from persisted {@code ProcessProperties}.
 * MUST return a ready-to-serve {@link Process} without heavy I/O —
 * any "finish building" work should be deferred lazily into the first
 * {@code compute} call.
 *
 * <p>If load fails more than {@code load.max-retries}, the engine falls back
 * to {@link ProcessInitializer}.</p>
 */
@FunctionalInterface
public interface ProcessLoader {

    CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) throws LoadException;
}
