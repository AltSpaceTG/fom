package io.fom.log;

import java.util.List;

/**
 * Clock assignment shared by every {@link LogBackend}, including the ones in
 * other modules: the caller never has to guess a clock before appending.
 */
public final class LogClocks {

    private LogClocks() {
    }

    /**
     * The clock an append gets: one past the last event's, or 0 for an empty log.
     * Clocks never repeat, even across compaction gaps, unless an older copy of the
     * log is restored.
     */
    public static long nextClock(LogEvent lastEvent) {
        return lastEvent == null ? 0 : Math.addExact(lastEvent.clock(), 1);
    }

    /**
     * Checks the events handed to {@code compact()}, which keep their clocks.
     *
     * @throws IllegalArgumentException if the clocks do not strictly increase
     */
    public static void requireIncreasingClocks(List<LogEvent> events) {
        long previous = -1;
        for (LogEvent e : events) {
            if (e.clock() <= previous) {
                throw new IllegalArgumentException("Compacted events must have strictly increasing clocks; "
                        + e.getClass().getSimpleName() + " has clock " + e.clock() + " after " + previous);
            }
            previous = e.clock();
        }
    }

    /** A copy of {@code e} with its clock replaced. */
    public static LogEvent withClock(LogEvent e, long clock) {
        return switch (e) {
            case LogLeader v -> new LogLeader(clock, v.timestamp(), v.formatVersion(), v.instanceId());
            case LogChangeGraph v -> new LogChangeGraph(clock, v.timestamp(), v.formatVersion(), v.nodes());
            case LogInitialized v -> new LogInitialized(clock, v.timestamp(), v.formatVersion(), v.processName(), v.properties(),
                    v.replaces());
            case LogLoaded v -> new LogLoaded(clock, v.timestamp(), v.formatVersion(), v.sid());
            case LogTrigger v -> new LogTrigger(clock, v.timestamp(), v.formatVersion(), v.processNames());
            case LogDependencyChanged v -> new LogDependencyChanged(
                    clock, v.timestamp(), v.formatVersion(), v.sid(), v.depName(), v.oldDepClock(), v.newDepClock());
            case LogDead v -> new LogDead(clock, v.timestamp(), v.formatVersion(), v.sid());
            case LogCleanedUp v -> new LogCleanedUp(clock, v.timestamp(), v.formatVersion(), v.sid(), v.ok());
            case LogSnapshot v -> new LogSnapshot(clock, v.timestamp(), v.formatVersion(), v.checkpointClock());
            case LogPaused v -> new LogPaused(clock, v.timestamp(), v.formatVersion(), v.processName(), v.stale(),
                    v.forDependency());
            case LogResumed v -> new LogResumed(clock, v.timestamp(), v.formatVersion(), v.processName());
        };
    }
}
