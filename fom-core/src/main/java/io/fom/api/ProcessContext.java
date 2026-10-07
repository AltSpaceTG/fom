package io.fom.api;

import io.fom.Sid;

import java.util.concurrent.Executor;

/** What {@code Process.cleanUp()} gets: the state being cleaned up and the process's executor. */
public interface ProcessContext {

    /** The current state of the process. */
    Sid sid();

    /** The process's own executor (a virtual thread per task), for asynchronous work it starts. */
    Executor executor();
}
