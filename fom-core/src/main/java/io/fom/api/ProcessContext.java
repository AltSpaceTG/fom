package io.fom.api;

import io.fom.Sid;

import java.util.concurrent.Executor;

/**
 * Non-querying half of the contextual API exposed to {@code Process.cleanUp()}
 * and other lifecycle hooks.
 */
public interface ProcessContext {

    /** Identifier of the current process state. */
    Sid sid();

    /**
     * Executor to be used for any asynchronous work this process spawns.
     * Default: {@code Executors.newVirtualThreadPerTaskExecutor()},
     * configurable per-process.
     */
    Executor executor();
}
