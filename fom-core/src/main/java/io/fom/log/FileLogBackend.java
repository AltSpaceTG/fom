package io.fom.log;

import io.fom.SnapshotResult;
import io.fom.api.LeadershipLostException;
import io.fom.serde.ObjectInputFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * Append-only log in a regular file. The file starts with the magic {@code FOM\1}; each event
 * follows as {@code [int length][int CRC32][payload]}, the payload being the Java-serialised
 * {@link LogEvent}.
 *
 * <p>Leadership is an exclusive {@link FileLock} on a sibling {@code <name>.lock} next to the
 * real file (symlinks resolved), taken in the constructor; a second opener of the same file
 * fails with {@link IllegalStateException}.</p>
 *
 * <p>On open the whole file is scanned. An incomplete last frame (a crash mid-append) is cut
 * off and saved as {@code <name>.truncated.<millis>}; any other damage fails the open with
 * {@link LogCorruptedException} and leaves the file untouched.</p>
 *
 * <p>Writes refuse, with {@link IllegalStateException}, when the path no longer names the
 * opened file or the file no longer ends where this backend left it. Per the {@link LogBackend}
 * SPI that is transient: putting the file back resumes writes.</p>
 *
 * <p>An interrupted caller's append still completes and keeps its interrupt flag. An interrupt
 * that closes the channel mid-write is recovered from by reopening, removing the partial frame
 * and writing again, up to three times.</p>
 *
 * <p><strong>Security:</strong> the CRC32 detects corruption, not forgery. Every payload is
 * deserialised under the {@link ObjectInputFilters#logPayload()} allowlist; still treat the log
 * file as a locally owned, access-controlled artifact.</p>
 */
public final class FileLogBackend implements LogBackend {

    private static final Logger logger = LoggerFactory.getLogger(FileLogBackend.class);

    private static final byte[] MAGIC = {'F', 'O', 'M', 1};
    private static final int HEADER_SIZE = 4;
    private static final int FRAME_PREFIX_SIZE = 8; // length (int) + crc (int)

    /** Every payload is an {@link ObjectOutputStream} stream, which starts with these bytes. */
    private static final byte[] STREAM_MAGIC = {(byte) 0xAC, (byte) 0xED, 0x00, 0x05};

    private static final ObjectInputFilter LOG_FILTER = ObjectInputFilters.logPayload();

    /** No readable payload is larger: the read filter caps a stream at 64 MiB. */
    private static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

    /**
     * Payloads up to this size cannot trip any read-filter cap (bytes, array length and
     * reference count are all bounded by the payload size), so only larger ones are test-read.
     */
    private static final int READ_BACK_CHECK_THRESHOLD_BYTES = 1_000_000;

    /**
     * Largest slice per {@code FileChannel.read/write}. The JDK copies a heap buffer through a
     * per-thread cached direct buffer the size of the slice; whole 50 MB frames would pin that
     * much native memory per thread.
     */
    private static final int IO_SLICE_BYTES = 256 * 1024;

    /** I/O on one frame interrupted mid-call is attempted this many times in total. */
    private static final int MAX_IO_ATTEMPTS = 3;

    /** CRC work allowed per byte of the searched region; see {@link #requireNoIntactFrameAfter}. */
    private static final long CRC_BUDGET_FACTOR = 4;

    /** Suffix of an archive copy that is still being written. */
    private static final String PARTIAL_SUFFIX = ".partial";

    /** Archive stamps further ahead of the wall clock than this are not believed. */
    private static final long MAX_ARCHIVE_STAMP_AHEAD_MILLIS = 100L * 366 * 24 * 3600 * 1000;

    /** {@code NAME_MAX} of ext4, xfs, btrfs, tmpfs, APFS and NTFS. */
    static final int MAX_FILE_NAME_BYTES = 255;

    /** {@code .archived.<13-digit millis>.partial}, the longest suffix of a sibling file. */
    static final int LONGEST_SIBLING_SUFFIX_BYTES = ".archived.".length() + 13 + PARTIAL_SUFFIX.length();

    private final Path path;
    private final Path lockPath;
    private final ChannelOpener channelOpener;
    private FileChannel dataChannel;
    private final FileChannel lockChannel;
    private final FileLock leaderLock;

    /**
     * Fair, so a reader looping over growing {@link #getBetween} ranges (which holds the lock for
     * the whole range) cannot keep barging ahead of a parked appender.
     */
    private final ReentrantLock appendLock = new ReentrantLock(true);

    // Everything below is guarded by appendLock unless volatile.
    private final List<Long> offsets = new ArrayList<>();
    private final List<Integer> payloadLengths = new ArrayList<>();
    private final Map<String, Integer> eventCounts = new LinkedHashMap<>();
    private long maxTimestampMillis = 0L;
    /** Clock of the last event, -1 while empty. */
    private long lastClock = -1;
    /** File key (inode) of the file the data channel is open on; {@code null} where the platform has none. */
    private Object openedFileKey;

    private volatile String currentLeader;
    /** Rebuilt whenever the log changes, so {@link #introspect()} takes no lock. */
    private volatile LogBackendReport report;
    private volatile boolean closed = false;
    /** A failed write could not be rolled back: the file tail is untrusted. */
    private volatile boolean broken = false;

    /**
     * Opens (or creates) the log at {@code path}, takes its lock and rebuilds the index. A torn
     * tail left by a crash is cut off; any other damage fails the open.
     *
     * @throws IOException if the file cannot be opened or replayed
     * @throws IllegalStateException if the log is already open, by another process or in this JVM
     * @throws IllegalArgumentException if the file name leaves no room for the sibling files' suffixes
     */
    public FileLogBackend(Path path) throws IOException {
        this(path, FileChannel::open);
    }

    /** Opens the data file's channel; a seam for tests that inject I/O failures. */
    @FunctionalInterface
    interface ChannelOpener {
        FileChannel open(Path path, OpenOption... options) throws IOException;
    }

    FileLogBackend(Path path, ChannelOpener channelOpener) throws IOException {
        this.channelOpener = Objects.requireNonNull(channelOpener, "channelOpener");
        requireRoomForSiblingNames(Objects.requireNonNull(path, "path"));
        // Lock, data, archives and .tmp all hang off the real file, so two spellings of one log
        // (a symlink, say) cannot both take a lock.
        this.path = realLogPath(path);
        requireRoomForSiblingNames(this.path);
        this.lockPath = this.path.resolveSibling(this.path.getFileName() + ".lock");

        FileChannel openedLockChannel = null;
        FileLock acquiredLock = null;
        FileChannel openedDataChannel = null;
        try {
            openedLockChannel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            try {
                acquiredLock = openedLockChannel.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new IllegalStateException(
                        "Lock " + lockPath + " is held by another thread in this JVM");
            }
            if (acquiredLock == null) {
                throw new IllegalStateException(
                        "Cannot acquire leader lock on " + lockPath + " — another process holds it");
            }
            removeCompactionLeftovers();

            openedDataChannel = channelOpener.open(this.path,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

            this.lockChannel = openedLockChannel;
            this.leaderLock = acquiredLock;
            this.dataChannel = openedDataChannel;
            this.openedFileKey = currentFileKey();

            initialiseFile();
            refreshReport();
        } catch (Throwable t) {
            if (openedDataChannel != null) quietly(openedDataChannel::close);
            if (acquiredLock != null) quietly(acquiredLock::release);
            if (openedLockChannel != null) quietly(openedLockChannel::close);
            if (t instanceof RuntimeException re) throw re;
            if (t instanceof IOException ioe) throw ioe;
            throw new RuntimeException(t);
        }
    }

    /**
     * Sibling files are named {@code <log name><suffix>}. A name with no room for the longest
     * suffix would open fine and then fail every snapshot, so it is refused up front.
     */
    private static void requireRoomForSiblingNames(Path path) {
        Path fileName = path.getFileName();
        if (fileName == null) return; // a root: realLogPath / the open will reject it
        int bytes = fileName.toString().getBytes(StandardCharsets.UTF_8).length;
        int max = MAX_FILE_NAME_BYTES - LONGEST_SIBLING_SUFFIX_BYTES;
        if (bytes > max) {
            throw new IllegalArgumentException("Log file name " + fileName + " is " + bytes
                    + " bytes (UTF-8); at most " + max + " are allowed so that <name>.archived.<millis>.partial"
                    + " fits the " + MAX_FILE_NAME_BYTES + "-byte NAME_MAX");
        }
    }

    /**
     * Gives {@code target} the POSIX permissions of {@code source} and, where allowed, its owner
     * and group, so a file next to the log is no more readable than the log. Failing to set the
     * permissions throws; failing to change owner or group is only logged.
     */
    private void copyPosixAccess(Path source, Path target) throws IOException {
        PosixFileAttributeView sourceView = Files.getFileAttributeView(source, PosixFileAttributeView.class);
        PosixFileAttributeView targetView = Files.getFileAttributeView(target, PosixFileAttributeView.class);
        if (sourceView == null || targetView == null) return;
        PosixFileAttributes attrs;
        try {
            attrs = sourceView.readAttributes();
        } catch (UnsupportedOperationException e) {
            return;
        }
        // Owner and group first: changing them may clear set-id bits the permissions then restore.
        try {
            if (!attrs.owner().equals(Files.getOwner(target))) targetView.setOwner(attrs.owner());
        } catch (IOException e) {
            logger.debug("FileLogBackend[{}]: could not give {} the owner {} of the log: {}",
                    path, target, attrs.owner(), e.toString());
        }
        try {
            if (!attrs.group().equals(targetView.readAttributes().group())) targetView.setGroup(attrs.group());
        } catch (IOException e) {
            logger.debug("FileLogBackend[{}]: could not give {} the group {} of the log: {}",
                    path, target, attrs.group(), e.toString());
        }
        targetView.setPermissions(attrs.permissions());
    }

    /**
     * The file a log path really names, symlinks resolved, also for a file that does not exist
     * yet (a dangling symlink is followed; the parent is created if missing). Hard links cannot
     * be told apart.
     */
    static Path realLogPath(Path path) throws IOException {
        Path p = path.toAbsolutePath();
        for (int hops = 0; hops < 40; hops++) {
            if (Files.exists(p)) {
                return p.toRealPath();
            }
            if (!Files.isSymbolicLink(p)) {
                Path parent = p.getParent();
                if (parent == null) return p;
                Files.createDirectories(parent);
                return parent.toRealPath().resolve(p.getFileName());
            }
            p = p.resolveSibling(Files.readSymbolicLink(p)); // an absolute target replaces p
        }
        throw new FileSystemException(path.toString(), null, "Too many levels of symbolic links");
    }

    /**
     * Deletes the {@code .tmp} and {@code .archived.<millis>.partial} files a crash mid-compaction
     * can leave. Compaction needs the leader lock, which we now hold, so none is in flight.
     * Best-effort: a file that cannot be removed does not fail the open.
     */
    private void removeCompactionLeftovers() {
        Path parent = path.getParent();
        if (parent == null) return;
        Pattern archiveName = archiveNamePattern();
        List<Path> leftovers = new ArrayList<>();
        leftovers.add(path.resolveSibling(path.getFileName() + ".tmp"));
        try (var stream = Files.list(parent)) {
            stream.filter(p -> {
                Matcher m = archiveName.matcher(p.getFileName().toString());
                return m.matches() && m.group(2) != null;
            }).forEach(leftovers::add);
        } catch (IOException e) {
            logger.warn("FileLogBackend[{}]: could not list {} for compaction leftovers: {}", path, parent, e.toString());
        }
        for (Path leftover : leftovers) {
            if (!Files.isRegularFile(leftover, LinkOption.NOFOLLOW_LINKS)) continue;
            try {
                long size = Files.size(leftover);
                Files.delete(leftover);
                logger.info("FileLogBackend[{}]: removed {} ({} bytes) left behind by an interrupted compaction",
                        path, leftover.getFileName(), size);
            } catch (IOException e) {
                logger.warn("FileLogBackend[{}]: could not remove compaction leftover {}: {}", path, leftover, e.toString());
            }
        }
    }

    private void initialiseFile() throws IOException {
        long size = dataChannel.size();
        if (size == 0) {
            dataChannel.write(ByteBuffer.wrap(MAGIC), 0);
            dataChannel.force(true);
            return;
        }
        if (size < HEADER_SIZE) {
            throw new IOException("Log file " + path + " too short to contain magic header");
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        readFully(header, 0);
        header.flip();
        for (byte b : MAGIC) {
            if (header.get() != b) {
                throw new IOException("Invalid magic at " + path + "; not a fom log file");
            }
        }
        scanIndex(HEADER_SIZE, size);
    }

    /**
     * Appends are sequential and synced, so a crash can only leave the last frame incomplete:
     * that is cut off. Damage anywhere else is corruption.
     */
    private void scanIndex(long startPos, long totalSize) throws IOException {
        long pos = startPos;
        while (pos < totalSize) {
            if (totalSize - pos < FRAME_PREFIX_SIZE) {
                truncateTornTail(pos, totalSize, "partial frame header");
                return;
            }
            ByteBuffer prefix = ByteBuffer.allocate(FRAME_PREFIX_SIZE);
            readFully(prefix, pos);
            prefix.flip();
            int payloadLen = prefix.getInt();
            int expectedCrc = prefix.getInt();

            if (payloadLen <= 0) {
                if (!onlyZeros(pos, totalSize)) {
                    throw corrupted(pos, "invalid frame length " + payloadLen);
                }
                truncateTornTail(pos, totalSize, "zero-filled tail");
                return;
            }
            if (payloadLen > MAX_PAYLOAD_BYTES) {
                throw corrupted(pos, "frame length " + payloadLen + " exceeds the payload limit");
            }
            long frameEnd = pos + FRAME_PREFIX_SIZE + payloadLen;
            if (frameEnd > totalSize) {
                requireNoIntactFrameAfter(pos, totalSize, "frame length " + payloadLen + " reaches past the end of the file");
                truncateTornTail(pos, totalSize, "frame extends past the end of the file");
                return;
            }
            ByteBuffer payload = ByteBuffer.allocate(payloadLen);
            readFully(payload, pos + FRAME_PREFIX_SIZE);
            if (crc32(payload.array()) != expectedCrc) {
                if (frameEnd < totalSize) {
                    throw corrupted(pos, "CRC mismatch");
                }
                requireNoIntactFrameAfter(pos, totalSize, "CRC mismatch");
                truncateTornTail(pos, totalSize, "CRC mismatch in the last frame");
                return;
            }
            Object obj;
            try {
                obj = deserialize(payload.array());
            } catch (Exception e) {
                // Intact bytes, so not a crash: a tampered frame or an event this version cannot read.
                throw corrupted(pos, "undecodable event: " + e);
            }
            if (obj instanceof LogLeader leader) {
                currentLeader = leader.instanceId();
            }
            if (obj instanceof LogEvent event) {
                // Clocks only ever grow, so one that does not means two timelines were spliced
                // together; opening it would re-issue clocks that already exist.
                if (event.clock() <= lastClock) {
                    throw corrupted(pos, "clock " + event.clock() + " of " + event.getClass().getSimpleName()
                            + " is not greater than the previous event's clock " + lastClock
                            + "; the log was spliced or reordered");
                }
                lastClock = event.clock();
                recordEvent(event);
            }
            offsets.add(pos);
            payloadLengths.add(payloadLen);
            pos = frameEnd;
        }
    }

    /**
     * The frame at {@code pos} looks torn. That is only a crash artefact if nothing intact follows
     * it; a damaged length field in a middle frame looks the same from its header alone. So the
     * bytes after it are searched for a complete, CRC-valid frame that starts a structurally valid
     * chain of frames running to EOF (see {@link FrameChains}); finding one means corruption.
     *
     * <p>The chain test is what a real later frame always passes and serialized bytes inside the
     * torn frame's own payload practically never do. Crafted data can still make many chains
     * valid, so CRC work is capped at {@link #CRC_BUDGET_FACTOR} times the region size, past which
     * the open fails closed.</p>
     */
    private void requireNoIntactFrameAfter(long pos, long totalSize, String reason) throws IOException {
        long from = pos + 1;
        int size = (int) (totalSize - from); // < 8 + MAX_PAYLOAD_BYTES: see the callers
        byte[] region = new byte[size];
        readFully(ByteBuffer.wrap(region), from);
        FrameChains chains = new FrameChains(region);
        long crcBudget = CRC_BUDGET_FACTOR * (long) size;
        for (int m = FRAME_PREFIX_SIZE; m + STREAM_MAGIC.length <= size; m++) {
            if (!streamMagicAt(region, m, STREAM_MAGIC.length)) continue;
            int frameStart = m - FRAME_PREFIX_SIZE;
            int len = intAt(region, frameStart);
            if (len < STREAM_MAGIC.length || len > MAX_PAYLOAD_BYTES || (long) m + len > size) continue;
            if (!chains.validFrom(m + len)) continue;
            crcBudget -= len;
            if (crcBudget < 0) {
                throw corrupted(pos, reason + ", and the bytes after it hold too many frame-like structures "
                        + "to tell an incomplete last write from damage; refusing rather than guessing");
            }
            CRC32 crc = new CRC32();
            crc.update(region, m, len);
            if ((int) crc.getValue() == intAt(region, frameStart + 4)) {
                throw corrupted(pos, reason + ", yet an intact frame follows at offset " + (from + frameStart)
                        + " (a damaged frame header, not an incomplete last write)");
            }
        }
    }

    private static boolean streamMagicAt(byte[] bytes, int at, int count) {
        for (int i = 0; i < count; i++) {
            if (bytes[at + i] != STREAM_MAGIC[i]) return false;
        }
        return true;
    }

    private static int intAt(byte[] bytes, int at) {
        return ((bytes[at] & 0xFF) << 24) | ((bytes[at + 1] & 0xFF) << 16) | ((bytes[at + 2] & 0xFF) << 8) | (bytes[at + 3] & 0xFF);
    }

    /**
     * Memoised "a structurally valid chain of frames starts here and runs to EOF": each header
     * has a plausible length and its payload starts with the stream magic; the last frame ends at
     * EOF, is torn by it, or is cut inside its header. Memoising keeps the search linear.
     */
    private static final class FrameChains {
        private final byte[] bytes;
        private final BitSet known = new BitSet();
        private final BitSet valid = new BitSet();

        FrameChains(byte[] bytes) {
            this.bytes = bytes;
        }

        boolean validFrom(int start) {
            int n = bytes.length;
            int[] visited = new int[16];
            int depth = 0;
            int f = start;
            boolean result;
            while (true) {
                if (n - f < FRAME_PREFIX_SIZE) { result = true; break; }   // EOF, or cut inside a header
                if (known.get(f)) { result = valid.get(f); break; }
                int len = intAt(bytes, f);
                int avail = n - f - FRAME_PREFIX_SIZE;
                if (len < STREAM_MAGIC.length || len > MAX_PAYLOAD_BYTES
                        || !streamMagicAt(bytes, f + FRAME_PREFIX_SIZE, Math.min(STREAM_MAGIC.length, avail))) {
                    result = false;
                } else if (len >= avail) {
                    result = true; // ends exactly at EOF, or torn by it
                } else {
                    if (depth == visited.length) visited = Arrays.copyOf(visited, depth * 2);
                    visited[depth++] = f;
                    f += FRAME_PREFIX_SIZE + len;
                    continue;
                }
                known.set(f);
                valid.set(f, result);
                break;
            }
            for (int i = 0; i < depth; i++) {
                known.set(visited[i]);
                valid.set(visited[i], result);
            }
            return result;
        }
    }

    private LogCorruptedException corrupted(long pos, String reason) {
        return new LogCorruptedException(path, pos, offsets.size(), reason);
    }

    private boolean onlyZeros(long from, long to) throws IOException {
        ByteBuffer chunk = ByteBuffer.allocate(64 * 1024);
        for (long pos = from; pos < to; ) {
            chunk.clear().limit((int) Math.min(chunk.capacity(), to - pos));
            readFully(chunk, pos);
            for (int i = 0; i < chunk.limit(); i++) {
                if (chunk.get(i) != 0) return false;
            }
            pos += chunk.limit();
        }
        return true;
    }

    /** Cuts off an incomplete last frame, keeping the discarded bytes in a sibling file. */
    private void truncateTornTail(long pos, long totalSize, String reason) throws IOException {
        Path saved = path.resolveSibling(path.getFileName() + ".truncated." + System.currentTimeMillis());
        long tailBytes = totalSize - pos;
        boolean created = false;
        try (FileChannel out = FileChannel.open(saved, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            created = true;
            copyPosixAccess(path, saved); // the tail holds log data
            for (long done = 0; done < tailBytes; ) {
                done += dataChannel.transferTo(pos + done, tailBytes - done, out);
            }
            out.force(true);
        } catch (IOException e) {
            // Typically a full disk. Leave no partial copy that looks like a saved tail.
            if (created) {
                try {
                    Files.deleteIfExists(saved);
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw new IOException("could not save the torn tail of " + path + " (" + tailBytes
                    + " byte(s) from offset " + pos + ", " + reason + ") to " + saved + ": " + e
                    + "; the log was not modified — " + (isOutOfSpace(e)
                            ? "free some space and open it again"
                            : "fix the cause and open it again"), e);
        }
        logger.warn("FileLogBackend[{}]: discarding an incomplete last write ({}): {} byte(s) from "
                + "offset {}, saved to {}; {} event(s) kept", path, reason, tailBytes, pos, saved, offsets.size());
        dataChannel.truncate(pos);
        dataChannel.force(true);
    }

    /** Whether {@code e} (or a cause) reports a full disk or an exhausted quota. */
    static boolean isOutOfSpace(Throwable e) {
        int depth = 0;
        for (Throwable t = e; t != null && depth++ < 16; t = t.getCause()) {
            String m = t.getMessage();
            if (m != null && (m.contains("No space left") || m.contains("Disk quota exceeded")
                    || m.contains("There is not enough space on the disk"))) {
                return true;
            }
        }
        return false;
    }

    private void readFully(ByteBuffer buf, long pos) throws IOException {
        long fileSize = dataChannel.size();
        int needed = buf.remaining();
        if (pos + needed > fileSize) {
            throw new IOException("read past EOF at " + pos + " (need " + needed + ", size " + fileSize + ")");
        }
        long offset = pos;
        int limit = buf.limit();
        try {
            while (buf.hasRemaining()) {
                buf.limit(Math.min(limit, buf.position() + IO_SLICE_BYTES));
                int n = dataChannel.read(buf, offset);
                if (n < 0) throw new IOException("unexpected EOF at " + offset);
                offset += n;
                buf.limit(limit);
            }
        } finally {
            buf.limit(limit);
        }
    }

    @Override
    public String logId() {
        return path.toString();
    }

    @Override
    public int length() {
        ensureOpen();
        appendLock.lock();
        try {
            return offsets.size();
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public LogEvent get(int position) {
        ensureOpen();
        boolean interrupted = Thread.interrupted();
        // Under the lock, so a concurrent compact() cannot swap the file under the lookup or the read.
        appendLock.lock();
        try {
            interrupted |= Thread.interrupted(); // arrived while waiting for the lock
            reopenIfClosedByInterrupt();
            if (position < 0 || position >= offsets.size()) {
                throw new IndexOutOfBoundsException("position " + position + " out of range [0, " + offsets.size() + ")");
            }
            long offset = offsets.get(position);
            int len = payloadLengths.get(position);
            ByteBuffer prefix = ByteBuffer.allocate(FRAME_PREFIX_SIZE);
            ByteBuffer payload = ByteBuffer.allocate(len);
            for (int attempt = 1; ; attempt++) {
                try {
                    requireIndexedEndForRead();
                    readFully(prefix, offset);
                    readFully(payload, offset + FRAME_PREFIX_SIZE);
                    break;
                } catch (ClosedByInterruptException e) {
                    interrupted = true;
                    Thread.interrupted();
                    reopenIfClosedByInterrupt();
                    if (attempt == MAX_IO_ATTEMPTS) throw e;
                    prefix.clear();
                    payload.clear();
                }
            }
            // Checked on every read: damage done to a running node's file must not be served,
            // nor re-encoded with a fresh CRC by a snapshot.
            if (prefix.getInt(0) != len || prefix.getInt(4) != crc32(payload.array())) {
                throw new IOException("frame at offset " + offset + " is damaged (CRC mismatch since the file was"
                        + " opened); stop the engine and repair the log (see fom-log diagnose)");
            }
            Object obj = deserialize(payload.array());
            if (!(obj instanceof LogEvent event)) {
                throw new IOException("Decoded payload at position " + position + " is not a LogEvent: "
                        + (obj == null ? "null" : obj.getClass().getName()));
            }
            return event;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to read event at position " + position + " of " + path + ": " + e, e);
        } finally {
            appendLock.unlock();
            restoreInterrupt(interrupted);
        }
    }

    /**
     * An interrupt during {@link FileChannel} I/O closes the channel for every thread. Public
     * operations run with the interrupt flag cleared, and a channel an interrupt closed anyway is
     * reopened here; the indexed part of the file is unaffected. Call under {@code appendLock}.
     */
    private void reopenIfClosedByInterrupt() {
        if (closed || dataChannel.isOpen()) return;
        try {
            dataChannel = channelOpener.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            logger.debug("FileLogBackend[{}]: reopened the data channel after an interrupted I/O call", path);
        } catch (IOException e) {
            broken = true;
            logger.error("FileLogBackend[{}]: could not reopen the data channel closed by an interrupt; "
                    + "refusing further writes until reopened", path, e);
        }
    }

    private static void restoreInterrupt(boolean interrupted) {
        if (interrupted) Thread.currentThread().interrupt();
    }

    @Override
    public LogEvent[] getBetween(int fromPosition, int toPosition) {
        ensureOpen();
        appendLock.lock();
        try {
            if (fromPosition < 0 || toPosition < fromPosition || toPosition > offsets.size()) {
                throw new IndexOutOfBoundsException(
                        "Invalid range [" + fromPosition + ", " + toPosition + ") for length " + offsets.size());
            }
            var out = new LogEvent[toPosition - fromPosition];
            for (int i = fromPosition; i < toPosition; i++) {
                out[i - fromPosition] = get(i);
            }
            return out;
        } finally {
            appendLock.unlock();
        }
    }

    /** Decodes one event at a time, so a scan needs heap for the largest record, not for a whole range. */
    @Override
    public void forEachBetween(int fromPosition, int toPosition, Consumer<? super LogEvent> action) {
        Objects.requireNonNull(action, "action");
        int length = length();
        if (fromPosition < 0 || toPosition < fromPosition || toPosition > length) {
            throw new IndexOutOfBoundsException(
                    "Invalid range [" + fromPosition + ", " + toPosition + ") for length " + length);
        }
        for (int i = fromPosition; i < toPosition; i++) {
            action.accept(get(i));
        }
    }

    @Override
    public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        ensureOpen();
        boolean interrupted = Thread.interrupted();
        appendLock.lock();
        try {
            interrupted |= Thread.interrupted(); // arrived while waiting for the lock
            ensureOpen();
            reopenIfClosedByInterrupt();
            ensureWritable();
            ensureSameFile();
            boolean isLeaderClaim = event instanceof LogLeader newLeader
                    && newLeader.instanceId().equals(leaderInstanceId);
            if (!isLeaderClaim && !leaderInstanceId.equals(currentLeader)) {
                return Optional.empty();
            }
            long clock = Math.addExact(lastClock, 1); // never wraps: clocks must not repeat
            LogEvent persisted = LogClocks.withClock(event, clock);
            ByteBuffer frame = encodeFrame(persisted);
            int payloadLength = frame.remaining() - FRAME_PREFIX_SIZE;

            long offset = -1;
            for (int attempt = 1; ; attempt++) {
                try {
                    if (offset < 0) {
                        long size = dataChannel.size(); // inside the retry: an interrupt here is retried too
                        requireIndexedEnd(size);
                        offset = size;
                    }
                    writeFully(dataChannel, frame, offset);
                    dataChannel.force(false);
                    break;
                } catch (ClosedByInterruptException e) {
                    // Undo any partial frame and retry with the flag cleared (restored on return).
                    interrupted = true;
                    if (offset >= 0) {
                        rollBackTail(offset);
                    } else {
                        Thread.interrupted();
                        reopenIfClosedByInterrupt();
                    }
                    if (broken || attempt == MAX_IO_ATTEMPTS) throw e;
                    frame.rewind();
                } catch (IOException e) {
                    if (offset >= 0) interrupted |= rollBackTail(offset);
                    throw e;
                }
            }

            offsets.add(offset);
            payloadLengths.add(payloadLength);
            lastClock = clock;
            recordEvent(persisted);
            if (persisted instanceof LogLeader claimed) {
                currentLeader = claimed.instanceId();
            }
            refreshReport();
            return Optional.of(persisted);
        } catch (IOException e) {
            // The cause goes into the message: callers often report only getMessage()
            // (NodeReport.lastException), which would hide "No space left on device".
            throw new RuntimeException("Append failed on " + path + ": " + e, e);
        } finally {
            appendLock.unlock();
            restoreInterrupt(interrupted);
        }
    }

    /** Serialises {@code event} into a whole frame, ready to write; refuses one that could not be read back. */
    private static ByteBuffer encodeFrame(LogEvent event) throws IOException {
        byte[] payload = serialize(event);
        ensureReadableAfterRestart(event, payload);
        ByteBuffer frame = ByteBuffer.allocate(FRAME_PREFIX_SIZE + payload.length);
        frame.putInt(payload.length);
        frame.putInt(crc32(payload));
        frame.put(payload);
        return frame.flip();
    }

    /**
     * A frame the read filter would reject is written fine but truncated, with everything
     * after it, on the next open. Refuse it up front instead.
     */
    private static void ensureReadableAfterRestart(LogEvent event, byte[] payload) {
        // The filter's stream-byte count misses a trailing array's contents, so the size is checked on its own.
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(event.getClass().getSimpleName() + " exceeds the log payload limits: "
                    + "it serialises to " + payload.length + " bytes, over the per-event size limit of "
                    + MAX_PAYLOAD_BYTES + " bytes (limits: " + ObjectInputFilters.logPayloadLimits()
                    + "); nothing was written");
        }
        if (payload.length <= READ_BACK_CHECK_THRESHOLD_BYTES) return;
        var rejected = new AtomicReference<ObjectInputFilter.FilterInfo>();
        try (var ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
            ois.setObjectInputFilter(info -> {
                ObjectInputFilter.Status status = LOG_FILTER.checkInput(info);
                if (status == ObjectInputFilter.Status.REJECTED) rejected.compareAndSet(null, info);
                return status;
            });
            ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            ObjectInputFilter.FilterInfo info = rejected.get();
            String which = info == null ? "it could not be decoded (" + e + ")" : violatedLimit(info);
            throw new IllegalArgumentException(event.getClass().getSimpleName() + " exceeds the log payload "
                    + "limits: " + which + " (the event serialises to " + payload.length + " bytes; limits: "
                    + ObjectInputFilters.logPayloadLimits() + "); nothing was written", e);
        }
    }

    /**
     * Which read-filter limit {@code info} broke, in words. Each measure is put to the filter on
     * its own, so the answer follows the real limits; a rejection none explains is the allowlist.
     */
    private static String violatedLimit(ObjectInputFilter.FilterInfo info) {
        if (rejectedAlone(-1, 0, 0, info.streamBytes())) {
            return "it reads past " + info.streamBytes() + " bytes of stream, over the per-event size limit";
        }
        if (rejectedAlone(info.arrayLength(), 0, 0, 0)) {
            return "it holds an array of " + String.format(Locale.ROOT, "%,d", info.arrayLength())
                    + " elements (" + (info.serialClass() == null ? "?" : info.serialClass().getTypeName())
                    + "), over the array-length limit";
        }
        if (rejectedAlone(-1, info.depth(), 0, 0)) {
            return "it nests objects " + info.depth() + " levels deep, over the nesting-depth limit";
        }
        if (rejectedAlone(-1, 0, info.references(), 0)) {
            return "it holds more than " + String.format(Locale.ROOT, "%,d", info.references() - 1)
                    + " object references, over the reference limit";
        }
        return "it contains " + (info.serialClass() == null ? "a class" : info.serialClass().getName())
                + ", which the log payload allowlist rejects";
    }

    private static boolean rejectedAlone(long arrayLength, long depth, long references, long streamBytes) {
        ObjectInputFilter.FilterInfo probe = new ObjectInputFilter.FilterInfo() {
            @Override public Class<?> serialClass() { return null; }
            @Override public long arrayLength() { return arrayLength; }
            @Override public long depth() { return depth; }
            @Override public long references() { return references; }
            @Override public long streamBytes() { return streamBytes; }
        };
        return LOG_FILTER.checkInput(probe) == ObjectInputFilter.Status.REJECTED;
    }

    /**
     * Truncates a partially written frame so the next append does not land after unindexed
     * garbage. If even that fails, the backend stops accepting writes.
     *
     * @return whether an interrupt was cleared on the way (the caller restores it)
     */
    private boolean rollBackTail(long offset) {
        boolean interrupted = false;
        for (int attempt = 1; ; attempt++) {
            // An interrupt that broke the write would also break the truncate.
            interrupted |= Thread.interrupted();
            reopenIfClosedByInterrupt();
            try {
                dataChannel.truncate(offset);
                return interrupted;
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                if (attempt < MAX_IO_ATTEMPTS) continue;
                markBroken(offset, e);
                return true;
            } catch (IOException | RuntimeException e) {
                markBroken(offset, e);
                return interrupted;
            }
        }
    }

    private void markBroken(long offset, Exception cause) {
        broken = true;
        logger.error("FileLogBackend[{}]: could not roll back a failed append at offset {}; "
                + "refusing further writes until reopened", path, offset, cause);
    }

    private void ensureWritable() {
        if (broken) {
            throw new IllegalStateException("FileLogBackend " + path
                    + " refused the write: an earlier failed write could not be rolled back; reopen the log");
        }
    }

    /** The file key of what {@link #path} names now; {@code null} where the platform has none. */
    private Object currentFileKey() throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class).fileKey();
    }

    /**
     * Refuses to write when {@link #path} no longer names the file the channel was opened on
     * (deleted, renamed, replaced by a restore or a log rotator): the events would go to an
     * orphaned inode the next start never sees. Skipped without file keys. Call under
     * {@code appendLock}.
     */
    private void ensureSameFile() {
        Object expected = openedFileKey;
        if (expected == null) return;
        Object actual;
        try {
            actual = currentFileKey();
        } catch (NoSuchFileException e) {
            actual = null;
        } catch (IOException e) {
            throw new IllegalStateException("FileLogBackend: could not check that log file " + path
                    + " is still the file this backend opened: " + e + "; refusing to write", e);
        }
        if (!expected.equals(actual)) {
            String what = actual == null ? "no longer exists" : "is now a different file";
            throw new IllegalStateException("FileLogBackend: log file " + path + " was deleted or replaced "
                    + "while open (the path " + what + "); refusing to append to an orphaned file");
        }
    }

    /** End of the last indexed frame (or of the header): where the next frame goes. Call under {@code appendLock}. */
    private long indexedEnd() {
        int n = offsets.size();
        return n == 0 ? HEADER_SIZE : offsets.get(n - 1) + FRAME_PREFIX_SIZE + payloadLengths.get(n - 1);
    }

    /**
     * Refuses to write when the file no longer ends where this backend left it: an in-place
     * overwrite, truncate or foreign append, which {@link #ensureSameFile} cannot see. An
     * overwrite of exactly the same length goes unnoticed. Call under {@code appendLock}.
     *
     * @param actual the data channel's current size
     */
    private void requireIndexedEnd(long actual) {
        requireIndexedEnd(actual, "refusing to write behind bytes it did not index");
    }

    private void requireIndexedEnd(long actual, String refusal) {
        long expected = indexedEnd();
        if (actual != expected) {
            throw new IllegalStateException("FileLogBackend: log file " + path + " was modified by something "
                    + "else while open (it is " + actual + " bytes, this backend last left it at " + expected
                    + "): overwritten, truncated or appended to in place; " + refusal);
        }
    }

    /**
     * The read-side twin of {@link #requireIndexedEnd}, so an in-place overwrite is reported as
     * such rather than as corruption. Skipped when {@code broken}: the file then legitimately ends
     * past the index. Call under {@code appendLock}.
     */
    private void requireIndexedEndForRead() throws IOException {
        if (broken) return;
        requireIndexedEnd(dataChannel.size(), "refusing to read events from bytes it did not index");
    }

    private static void writeFully(FileChannel channel, ByteBuffer buf, long pos) throws IOException {
        long offset = pos;
        int limit = buf.limit();
        try {
            while (buf.hasRemaining()) {
                buf.limit(Math.min(limit, buf.position() + IO_SLICE_BYTES));
                int n = channel.write(buf, offset);
                if (n < 0) throw new IOException("write returned -1 at " + offset);
                offset += n;
                buf.limit(limit);
            }
        } finally {
            buf.limit(limit); // the retry loops rewind() and resend the whole frame
        }
    }

    /** Best-effort fsync of a directory so a rename inside it survives power loss. */
    private static void forceDirectory(Path dir) {
        if (dir == null) return;
        try (FileChannel dirChannel = FileChannel.open(dir, StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (IOException e) {
            // Windows cannot open a directory channel; there the OS makes renames durable.
            logger.debug("FileLogBackend: directory fsync skipped: {}", e.toString());
        }
    }

    @Override
    public SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId) {
        Objects.requireNonNull(snapshotEvents, "snapshotEvents");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        if (snapshotEvents.isEmpty()
                || !(snapshotEvents.get(0) instanceof LogLeader first)
                || !first.instanceId().equals(leaderInstanceId)) {
            throw new IllegalArgumentException(
                    "snapshotEvents must start with LogLeader(" + leaderInstanceId + ")");
        }
        ensureOpen();
        // Leadership before clocks: a plan made just before a takeover has stale clocks, and the
        // caller should hear "not the leader". Checked again under the lock.
        requireStillLeader(leaderInstanceId);
        LogClocks.requireIncreasingClocks(snapshotEvents);
        boolean interrupted = Thread.interrupted();
        appendLock.lock();
        Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
        try {
            interrupted |= Thread.interrupted(); // arrived while waiting for the lock
            ensureOpen();
            reopenIfClosedByInterrupt();
            ensureWritable();
            // Compaction archives and replaces whatever `path` names: it must be the file we indexed.
            ensureSameFile();
            requireIndexedEnd(dataChannel.size());
            // A deposed leader must not snapshot itself back to the front of the log.
            // Checked before any file is created, so a refusal leaves nothing behind.
            requireStillLeader(leaderInstanceId);
            Path archivedPath = nextArchivePath();

            Files.deleteIfExists(tmpPath);
            long checkpointClock = -1;
            try (FileChannel tmpCh = FileChannel.open(tmpPath,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                // The tmp file becomes the log: give it the log's access before any data goes in.
                copyPosixAccess(path, tmpPath);
                tmpCh.write(ByteBuffer.wrap(MAGIC), 0);
                long pos = HEADER_SIZE;
                for (LogEvent e : snapshotEvents) {
                    ByteBuffer frame = encodeFrame(e); // compaction keeps the clocks it is given
                    int frameSize = frame.remaining();
                    writeFully(tmpCh, frame, pos);
                    pos += frameSize;
                    if (e instanceof LogSnapshot snap) {
                        checkpointClock = snap.checkpointClock();
                    }
                }
                tmpCh.force(true);
            }

            // Crash-safe swap: `path` stays a valid log at every instant. Copy it to the archive
            // (under a .partial name, so a crash mid-copy leaves no truncated "archive"), then
            // atomically move the tmp file over it. The live channel stays open until the archive
            // exists, so a failed copy leaves the backend usable.
            dataChannel.force(true);
            Path partial = archivedPath.resolveSibling(archivedPath.getFileName() + PARTIAL_SUFFIX);
            Files.deleteIfExists(partial);
            Files.copy(path, partial, StandardCopyOption.COPY_ATTRIBUTES);
            Files.move(partial, archivedPath, StandardCopyOption.ATOMIC_MOVE);
            dataChannel.close();
            Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(path.getParent());
            interrupted |= reopenAndReindex();

            long effectiveCheckpoint = checkpointClock < 0 ? Math.max(offsets.size() - 1, 0) : checkpointClock;
            return new SnapshotResult(path.toString(), archivedPath.toString(), effectiveCheckpoint,
                    snapshotEvents.size());
        } catch (IOException e) {
            recoverAfterFailedCompact(tmpPath);
            // The cause goes into the message: callers that print only getMessage() would see just the path.
            throw new RuntimeException("Snapshot compact failed on " + path + ": " + e, e);
        } finally {
            appendLock.unlock();
            restoreInterrupt(interrupted);
        }
    }

    private void requireStillLeader(String leaderInstanceId) {
        String leader = currentLeader;
        if (leader != null && !leader.equals(leaderInstanceId)) {
            throw new LeadershipLostException("Cannot compact " + path + ": instance "
                    + leaderInstanceId + " is no longer the leader (" + leader + " is); nothing was written");
        }
    }

    /**
     * A fresh {@code <name>.archived.<stamp>}. Stamps only grow, so the newest archive (what
     * {@link #purgeArchives} keeps) stays the latest compaction even if the wall clock steps back.
     */
    private Path nextArchivePath() throws IOException {
        long now = System.currentTimeMillis();
        long stamp = Math.max(now, highestArchiveStamp(now) + 1);
        Path archivedPath = path.resolveSibling(path.getFileName() + ".archived." + stamp);
        while (Files.exists(archivedPath)) {
            archivedPath = path.resolveSibling(path.getFileName() + ".archived." + ++stamp);
        }
        return archivedPath;
    }

    /** @return whether an interrupt was cleared on the way (the caller restores it) */
    private boolean reopenAndReindex() throws IOException {
        boolean interrupted = false;
        for (int attempt = 1; ; attempt++) {
            // A pending interrupt would fail the open or the scan of a perfectly good file.
            interrupted |= Thread.interrupted();
            try {
                dataChannel = channelOpener.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                openedFileKey = currentFileKey(); // the compacted file is a new inode
                offsets.clear();
                payloadLengths.clear();
                currentLeader = null;
                lastClock = -1;
                eventCounts.clear();
                maxTimestampMillis = 0L;
                scanIndex(HEADER_SIZE, dataChannel.size());
                refreshReport();
                return interrupted;
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                quietly(dataChannel::close);
                if (attempt == MAX_IO_ATTEMPTS) throw e;
            } catch (IOException e) {
                // Left closed so recovery knows the index is not usable.
                quietly(dataChannel::close);
                throw e;
            }
        }
    }

    /** Whatever step failed, {@code path} still holds a valid log (old or new): serve it again. */
    private void recoverAfterFailedCompact(Path tmpPath) {
        try {
            Files.deleteIfExists(tmpPath);
        } catch (IOException ignored) {
            // best-effort
        }
        if (dataChannel.isOpen()) return;
        try {
            if (reopenAndReindex()) Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            broken = true;
            logger.error("FileLogBackend[{}]: could not reopen the log after a failed compaction", path, e);
        }
    }

    /** {@code <name>.archived.<digits>} with an optional {@code .partial}; group 1 is the stamp, group 2 the suffix. */
    private Pattern archiveNamePattern() {
        return Pattern.compile(Pattern.quote(path.getFileName() + ".archived.") + "([0-9]{1,18})("
                + Pattern.quote(PARTIAL_SUFFIX) + ")?");
    }

    /**
     * A stamp absurdly far in the future (planted, or from a broken clock) counts as the oldest,
     * so it cannot push new stamps past 18 digits nor stay "newest" for ever.
     */
    private long effectiveArchiveStamp(Path file, long stamp, long now) {
        if (stamp > now + MAX_ARCHIVE_STAMP_AHEAD_MILLIS) {
            logger.warn("FileLogBackend[{}]: archive {} carries a stamp more than 100 years ahead of the clock; "
                    + "treating it as the oldest archive", path, file.getFileName());
            return -1;
        }
        return stamp;
    }

    /** The largest believable stamp among this log's archives (complete or partial), -1 when there are none. */
    private long highestArchiveStamp(long now) throws IOException {
        Pattern archiveName = archiveNamePattern();
        long highest = -1;
        try (var stream = Files.list(path.getParent())) {
            for (Path p : stream.toList()) {
                Matcher m = archiveName.matcher(p.getFileName().toString());
                if (m.matches()) highest = Math.max(highest, effectiveArchiveStamp(p, Long.parseLong(m.group(1)), now));
            }
        }
        return highest;
    }

    /** Deletes all but the newest {@code keepHistory} {@code <name>.archived.<millis>} siblings. */
    @Override
    public void purgeArchives(int keepHistory) {
        if (keepHistory < 0) throw new IllegalArgumentException("keepHistory < 0");
        Path parent = path.getParent();
        Pattern archiveName = archiveNamePattern();
        // Under the lock, so a compaction's in-flight partial copy is never removed.
        appendLock.lock();
        try {
            record Archive(Path file, long stamp) { }
            long now = System.currentTimeMillis();
            List<Archive> archives = new ArrayList<>();
            try (var stream = Files.list(parent)) {
                for (Path p : stream.toList()) {
                    Matcher m = archiveName.matcher(p.getFileName().toString());
                    if (!m.matches()) continue;
                    if (m.group(2) != null) {
                        Files.deleteIfExists(p); // left behind by a crash during compaction
                    } else {
                        archives.add(new Archive(p, effectiveArchiveStamp(p, Long.parseLong(m.group(1)), now)));
                    }
                }
            }
            archives.sort(Comparator.comparingLong(Archive::stamp));
            for (int i = 0; i < archives.size() - keepHistory; i++) {
                Files.deleteIfExists(archives.get(i).file());
            }
        } catch (IOException e) {
            throw new RuntimeException("Archive purge failed next to " + path + ": " + e, e);
        } finally {
            appendLock.unlock();
        }
    }

    /** O(1) and lock-free: the report is rebuilt whenever the log changes, so polling cannot stall appends. */
    @Override
    public LogBackendReport introspect() {
        ensureOpen();
        return report;
    }

    /** Call under {@code appendLock}. */
    private void recordEvent(LogEvent event) {
        eventCounts.merge(event.getClass().getSimpleName(), 1, Integer::sum);
        if (event.timestamp() > maxTimestampMillis) {
            maxTimestampMillis = event.timestamp();
        }
    }

    /** Call under {@code appendLock}. */
    private void refreshReport() {
        report = new LogBackendReport(path.toString(), offsets.size(), currentLeader,
                eventCounts, maxTimestampMillis);
    }

    @Override
    public void close() {
        if (closed) return;
        appendLock.lock();
        try {
            if (closed) return;
            closed = true;
        } finally {
            appendLock.unlock();
        }
        quietly(() -> dataChannel.force(true));
        quietly(dataChannel::close);
        quietly(leaderLock::release);
        quietly(lockChannel::close);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("FileLogBackend " + path + " is closed");
        }
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }

    /** Best-effort cleanup: any exception is ignored. */
    private static void quietly(IoAction action) {
        try {
            action.run();
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    private static byte[] serialize(LogEvent event) throws IOException {
        var baos = new ByteArrayOutputStream(256);
        // Not try-with-resources: on OutOfMemoryError, close() can rethrow the same shared error
        // and addSuppressed(self) would turn it into an IllegalArgumentException.
        var oos = new ObjectOutputStream(baos);
        try {
            oos.writeObject(event);
            oos.flush();
        } finally {
            try {
                oos.close(); // a ByteArrayOutputStream: nothing to release
            } catch (IOException | Error ignored) {
                // the write's own failure, if any, is what propagates
            }
        }
        return baos.toByteArray();
    }

    private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (var ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            ois.setObjectInputFilter(LOG_FILTER);
            return ois.readObject();
        }
    }
}
