package io.fom.fsm;

import java.util.concurrent.CompletionStage;

/**
 * SPI for routing a query to a named process. Implemented by
 * {@link GraphMachine}; held by {@link ProcessFSM} for use during
 * {@code QueryableContext.query(name, msg)} dispatch.
 *
 * <p>Implementations must not block — return a {@link CompletionStage} that
 * completes when the target FSM has responded (or failed).</p>
 */
public interface ProcessRouter {

    CompletionStage<Object> route(String processName, Object query, long deadlineEpochMillis);
}
