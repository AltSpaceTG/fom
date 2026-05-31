package io.fom.api;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * One-shot heavy initialiser of a process state. Result is the
 * {@code ProcessProperties} payload — a {@code Map<String, byte[]>} that will
 * be written to the log inside a {@code LogInitialized} event and later
 * consumed by the corresponding {@link ProcessLoader}.
 *
 * <p>Implementations MUST:</p>
 * <ul>
 *   <li>issue all required {@code ctx.query(...)} calls upfront — dependencies
 *       that are never queried during {@code init} may be optimised away;</li>
 *   <li>be idempotent across retries: either use a {@code ResourceManager} unique
 *       resource per attempt, or be transactional with a clean rollback.</li>
 * </ul>
 */
@FunctionalInterface
public interface ProcessInitializer {

    CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) throws InitializationException;
}
