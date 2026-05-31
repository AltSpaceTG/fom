package io.fom;

import io.fom.log.LogBackend;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Facade exposed to {@link SnapshotPolicy#activate(SnapshotContext)} so a
 * user-supplied policy can schedule itself without depending on internal
 * engine fields.
 */
public interface SnapshotContext {

    /** Manual trigger — same effect as {@code Engine.snapshot()}. */
    CompletionStage<SnapshotResult> snapshot();

    /** Engine's own scheduled executor — daemon, single thread; safe for short tasks. */
    ScheduledExecutorService scheduler();

    /** Read-only access to the active log backend (length, current leader, etc.). */
    LogBackend logBackend();

    /** Purge archived files keeping the most recent {@code keepHistory}. No-op for non-file backends. */
    void purgeArchives(int keepHistory);
}
