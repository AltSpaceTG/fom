package io.fom.log;

import java.io.Serializable;

/**
 * An entry of the log, which is the source of truth for recovery.
 *
 * <p>Each event has a strictly increasing {@link #clock()} (assigned on append,
 * kept by compaction; not its position, see {@link LogBackend}), a wall-clock
 * {@link #timestamp()} and a {@link #formatVersion()}.</p>
 *
 * <p>The core events ({@link LogLeader}, {@link LogChangeGraph},
 * {@link LogInitialized}, {@link LogLoaded}, {@link LogTrigger}, {@link LogDead})
 * are stable within a major version. The extended ones
 * ({@link LogDependencyChanged}, {@link LogCleanedUp}, {@link LogSnapshot}) may be
 * skipped by older readers with a warning. {@link LogPaused} and {@link LogResumed}
 * keep a pause across restarts and snapshots.</p>
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
                LogSnapshot,
                LogPaused,
                LogResumed {

    /** The event's identity: the previous event's clock + 1, never reused or renumbered. */
    long clock();

    /** Wall-clock timestamp, epoch millis. */
    long timestamp();

    /** Version of this event's payload format. */
    short formatVersion();
}
