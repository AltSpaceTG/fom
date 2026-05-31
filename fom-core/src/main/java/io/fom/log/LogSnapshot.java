package io.fom.log;

/**
 * Marker emitted at the start of a snapshot — the rotation boundary.
 * After this event the backend writes {@link LogChangeGraph} and
 * {@link LogInitialized} events for every live Sid into the new file.
 *
 * <p>{@code checkpointClock} is the clock of the last event included in
 * the snapshot's source state.</p>
 *
 * <p>Extended event — older readers may skip with a warning.</p>
 */
public record LogSnapshot(int clock, long timestamp, short formatVersion, int checkpointClock)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogSnapshot {
        if (checkpointClock < 0) {
            throw new IllegalArgumentException("checkpointClock must be >= 0, was " + checkpointClock);
        }
    }

    public LogSnapshot(int clock, long timestamp, int checkpointClock) {
        this(clock, timestamp, CURRENT_FORMAT, checkpointClock);
    }
}
