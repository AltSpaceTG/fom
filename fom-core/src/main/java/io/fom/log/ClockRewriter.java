package io.fom.log;

/**
 * Rebuilds a {@link LogEvent} with the {@code clock} the backend assigned to it.
 * Used by every backend (including external ones in {@code fom-jdbc} etc.) so
 * the caller doesn't have to guess the clock before appending.
 */
public final class ClockRewriter {

    private ClockRewriter() {
    }

    public static LogEvent withClock(LogEvent e, int clock) {
        return switch (e) {
            case LogLeader v -> new LogLeader(clock, v.timestamp(), v.formatVersion(), v.instanceId());
            case LogChangeGraph v -> new LogChangeGraph(clock, v.timestamp(), v.formatVersion(), v.serializedGraph());
            case LogInitialized v -> new LogInitialized(clock, v.timestamp(), v.formatVersion(), v.processName(), v.properties());
            case LogLoaded v -> new LogLoaded(clock, v.timestamp(), v.formatVersion(), v.sid());
            case LogTrigger v -> new LogTrigger(clock, v.timestamp(), v.formatVersion(), v.values());
            case LogDependencyChanged v -> new LogDependencyChanged(
                    clock, v.timestamp(), v.formatVersion(), v.sid(), v.depName(), v.oldDepClock(), v.newDepClock());
            case LogDead v -> new LogDead(clock, v.timestamp(), v.formatVersion(), v.sid());
            case LogCleanedUp v -> new LogCleanedUp(clock, v.timestamp(), v.formatVersion(), v.sid(), v.ok());
            case LogSnapshot v -> new LogSnapshot(clock, v.timestamp(), v.formatVersion(), v.checkpointClock());
        };
    }
}
