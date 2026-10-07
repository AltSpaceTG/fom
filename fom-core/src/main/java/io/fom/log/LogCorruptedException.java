package io.fom.log;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A log file has damage that a crash mid-write cannot explain: an intact-looking
 * frame that cannot be read, or damage followed by more data. The backend refuses
 * to open it rather than drop the rest of the log, and leaves the file untouched.
 *
 * <p>To recover, back the file up, then restore it or truncate it to
 * {@link #offset()} bytes ({@code truncate -s <offset> <file>}), which drops
 * every event from there on.</p>
 */
public final class LogCorruptedException extends IOException {

    private static final long serialVersionUID = 1L;

    private final transient Path path;
    private final long offset;
    private final int readableEvents;

    public LogCorruptedException(Path path, long offset, int readableEvents, String reason) {
        super("Log " + path + " is corrupt at byte offset " + offset + " (" + reason + "); "
                + readableEvents + " event(s) before it are readable. The file was not modified; "
                + "restore it or truncate it to " + offset + " bytes.");
        this.path = path;
        this.offset = offset;
        this.readableEvents = readableEvents;
    }

    /** The corrupt log file. */
    public Path path() {
        return path;
    }

    /** Byte offset of the first unreadable frame. */
    public long offset() {
        return offset;
    }

    /** Number of intact events before {@link #offset()}. */
    public int readableEvents() {
        return readableEvents;
    }
}
