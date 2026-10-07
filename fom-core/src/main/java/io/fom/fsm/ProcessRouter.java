package io.fom.fsm;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Sends a process's {@code ctx.query(name, msg)} to the named process.
 * Implemented by {@link GraphMachine}; must not block.
 */
public interface ProcessRouter {

    /** @param parentQueryId the query being computed that sends this one, or {@code null} */
    CompletionStage<Object> route(String processName, Object query, long deadlineEpochMillis, UUID parentQueryId);
}
