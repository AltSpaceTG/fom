package io.fom.log;

/**
 * Marks a compacted log. {@code checkpointClock} is the clock of the last event
 * of the log the snapshot was taken from.
 *
 * <p>Extended event: older readers may skip it with a warning.</p>
 */
public record LogSnapshot(long clock, long timestamp, short formatVersion, long checkpointClock)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogSnapshot {
        if (checkpointClock < 0) {
            throw new IllegalArgumentException("checkpointClock must be >= 0, was " + checkpointClock);
        }
    }

    public LogSnapshot(long clock, long timestamp, long checkpointClock) {
        this(clock, timestamp, CURRENT_FORMAT, checkpointClock);
    }
}
