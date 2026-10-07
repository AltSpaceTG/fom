package io.fom.api;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * The heavy, one-off build of a process's state. The returned cells are written to the
 * log in a {@code LogInitialized} and handed to the {@link ProcessLoader} on every start.
 *
 * <p>Must be safe to retry: use a fresh resource per attempt, or roll back cleanly.</p>
 *
 * <p>The engine cancels the returned stage when the init is no longer wanted. An explicit
 * stop ({@code cancelInit}, a pause, a removal, a replacing graph change, {@code close()})
 * also interrupts the calling thread; a budget that runs out does not, and counts as a
 * timeout.</p>
 */
@FunctionalInterface
public interface ProcessInitializer {

    CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) throws InitializationException;
}
