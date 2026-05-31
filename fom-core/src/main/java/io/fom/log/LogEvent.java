package io.fom.log;

import java.io.Serializable;

/**
 * Append-only log event — the source of truth for engine state recovery.
 *
 * <p>Every event carries a monotonically increasing {@link #clock()} (its position
 * in the log), a wall-clock {@link #timestamp()} in epoch milliseconds, and a
 * {@link #formatVersion()} byte used by the backend to apply forward-compatible
 * skip-on-unknown semantics.</p>
 *
 * <p>The {@code Core} events ({@link LogLeader}, {@link LogChangeGraph},
 * {@link LogInitialized}, {@link LogLoaded}, {@link LogTrigger}, {@link LogDead})
 * are stable forever inside a major version. The {@code Extended} events
 * ({@link LogDependencyChanged}, {@link LogCleanedUp}, {@link LogSnapshot})
 * may be skipped by older readers with a warning.</p>
 */
public sealed interface LogEvent extends Serializable
        permits LogLeader,
                LogChangeGraph,
                LogInitialized,
                LogLoaded,
                LogTrigger,
                LogDependencyChanged,
                LogDead,
                LogCleanedUp,
                LogSnapshot {

    /** Monotonically increasing position of this event in the log. */
    int clock();

    /** Wall-clock timestamp, epoch millis. */
    long timestamp();

    /** Format version of this event payload. Used for forward-compat dispatch. */
    short formatVersion();
}
