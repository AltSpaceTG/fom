package io.fom;

import io.fom.log.LogBackend;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

/** What a {@link SnapshotPolicy} gets from the engine to schedule itself. */
public interface SnapshotContext {

    /** Same as {@code Engine.snapshot()}. */
    CompletionStage<SnapshotResult> snapshot();

    /** The engine's single daemon timer thread: keep tasks short and never block it. */
    ScheduledExecutorService scheduler();

    /** The engine's log, for reading (length, current leader). */
    LogBackend logBackend();

    /**
     * Delete all but the newest {@code keepHistory} archives; {@link SnapshotPolicy#KEEP_ALL}
     * does nothing. Unlike {@code Engine.purgeArchives}, a failure is logged, not thrown:
     * a timer should not fail over an archive left behind.
     */
    void purgeArchives(int keepHistory);
}
