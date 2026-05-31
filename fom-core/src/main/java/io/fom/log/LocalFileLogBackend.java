package io.fom.log;

import io.fom.SnapshotResult;
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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Append-only log backed by a regular file. Each event is framed as
 * {@code [int length][int CRC32][N bytes payload]}; the payload is the
 * Java-serialised {@link LogEvent}. The file begins with a 4-byte magic
 * {@code FOM\1} so that mismatched files fail loudly.
 *
 * <p>Leadership is enforced by an exclusive {@link FileLock} on a sibling
 * {@code <name>.lock} file taken at construction time. A second process
 * attempting to open the same path fails with {@link IllegalStateException}.</p>
 *
 * <p>On open, the backend scans the whole file: every event whose CRC matches
 * is indexed; the first invalid frame causes a truncate to the last valid
 * offset (corruption recovery for partial writes after crash).</p>
 *
 * <p><strong>Security:</strong> the CRC32 is a corruption check, not a MAC — it
 * does not protect against a deliberately forged file. Deserialisation of every
 * payload therefore runs under an allowlist {@link ObjectInputFilter}
 * ({@link ObjectInputFilters#logPayload()}) so a tampered file cannot drive a
 * gadget-chain through arbitrary classpath classes. Treat the log file as a
 * locally-owned, access-controlled artifact regardless.</p>
 */
public final class LocalFileLogBackend implements LogBackend {

    private static final Logger logger = LoggerFactory.getLogger(LocalFileLogBackend.class);

    private static final byte[] MAGIC = {'F', 'O', 'M', 1};
    private static final int HEADER_SIZE = 4;
    private static final int FRAME_PREFIX_SIZE = 8; // length (int) + crc (int)

    /** Allowlist filter: log payloads are only {@code io.fom.*} + JDK types; rejects gadget classes. */
    private static final ObjectInputFilter LOG_FILTER = ObjectInputFilters.logPayload();

    private final Path path;
    private final Path lockPath;
    private FileChannel dataChannel;
    private final FileChannel lockChannel;
    private final FileLock leaderLock;

    private final ReentrantLock appendLock = new ReentrantLock();
    private final List<Long> offsets = new ArrayList<>();
    private final List<Integer> payloadLengths = new ArrayList<>();

    private volatile String currentLeader;
    private volatile boolean closed = false;

    public LocalFileLogBackend(Path path) throws IOException {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath();
        this.lockPath = this.path.resolveSibling(this.path.getFileName() + ".lock");
        Path parent = this.path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

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

            openedDataChannel = FileChannel.open(this.path,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

            this.lockChannel = openedLockChannel;
            this.leaderLock = acquiredLock;
            this.dataChannel = openedDataChannel;

            initialiseFile();
        } catch (Throwable t) {
            // Roll back partial state on failure
            if (openedDataChannel != null) try { openedDataChannel.close(); } catch (Exception ignored) { }
            if (acquiredLock != null) try { acquiredLock.release(); } catch (Exception ignored) { }
            if (openedLockChannel != null) try { openedLockChannel.close(); } catch (Exception ignored) { }
            if (t instanceof RuntimeException re) throw re;
            if (t instanceof IOException ioe) throw ioe;
            throw new RuntimeException(t);
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

    private void scanIndex(long startPos, long totalSize) throws IOException {
        long pos = startPos;
        while (pos < totalSize) {
            if (totalSize - pos < FRAME_PREFIX_SIZE) {
                truncateAt(pos, "trailing partial frame header");
                return;
            }
            ByteBuffer prefix = ByteBuffer.allocate(FRAME_PREFIX_SIZE);
            readFully(prefix, pos);
            prefix.flip();
            int payloadLen = prefix.getInt();
            int expectedCrc = prefix.getInt();

            if (payloadLen <= 0 || pos + FRAME_PREFIX_SIZE + payloadLen > totalSize) {
                truncateAt(pos, "frame length out of bounds");
                return;
            }
            ByteBuffer payload = ByteBuffer.allocate(payloadLen);
            readFully(payload, pos + FRAME_PREFIX_SIZE);
            int actualCrc = crc32(payload.array());
            if (actualCrc != expectedCrc) {
                truncateAt(pos, "CRC mismatch");
                return;
            }
            try {
                Object obj = deserialize(payload.array());
                if (obj instanceof LogLeader leader) {
                    currentLeader = leader.instanceId();
                }
            } catch (Exception e) {
                truncateAt(pos, "deserialisation failed: " + e.getMessage());
                return;
            }
            offsets.add(pos);
            payloadLengths.add(payloadLen);
            pos += FRAME_PREFIX_SIZE + payloadLen;
        }
    }

    private void truncateAt(long pos, String reason) throws IOException {
        logger.warn("LocalFileLogBackend[{}]: truncating to offset {} ({}); index has {} events",
                path, pos, reason, offsets.size());
        dataChannel.truncate(pos);
        dataChannel.force(true);
    }

    private void readFully(ByteBuffer buf, long pos) throws IOException {
        long fileSize = dataChannel.size();
        int needed = buf.remaining();
        if (pos + needed > fileSize) {
            throw new IOException("read past EOF at " + pos + " (need " + needed + ", size " + fileSize + ")");
        }
        long offset = pos;
        while (buf.hasRemaining()) {
            int n = dataChannel.read(buf, offset);
            if (n < 0) throw new IOException("unexpected EOF at " + offset);
            offset += n;
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
    public LogEvent get(int clock) {
        ensureOpen();
        // Hold appendLock so a concurrent compact() (which clears the index and
        // swaps the file) cannot tear the offset/length lookup or the read.
        appendLock.lock();
        try {
            if (clock < 0 || clock >= offsets.size()) {
                throw new IndexOutOfBoundsException("clock " + clock + " out of range [0, " + offsets.size() + ")");
            }
            long offset = offsets.get(clock);
            int len = payloadLengths.get(clock);
            ByteBuffer payload = ByteBuffer.allocate(len);
            readFully(payload, offset + FRAME_PREFIX_SIZE);
            Object obj = deserialize(payload.array());
            if (!(obj instanceof LogEvent event)) {
                throw new IOException("Decoded payload at clock " + clock + " is not a LogEvent: "
                        + (obj == null ? "null" : obj.getClass().getName()));
            }
            return event;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to read event at clock " + clock, e);
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public LogEvent[] getBetween(int fromClock, int toClock) {
        ensureOpen();
        appendLock.lock();
        try {
            if (fromClock < 0 || toClock < fromClock || toClock > offsets.size()) {
                throw new IndexOutOfBoundsException(
                        "Invalid range [" + fromClock + ", " + toClock + ") for length " + offsets.size());
            }
            var out = new LogEvent[toClock - fromClock];
            for (int i = fromClock; i < toClock; i++) {
                out[i - fromClock] = get(i);
            }
            return out;
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        ensureOpen();
        appendLock.lock();
        try {
            ensureOpen();
            boolean isLeaderClaim = event instanceof LogLeader newLeader
                    && newLeader.instanceId().equals(leaderInstanceId);
            if (!isLeaderClaim) {
                String leader = currentLeader;
                if (leader == null || !leader.equals(leaderInstanceId)) {
                    return Optional.empty();
                }
            }
            int clock = offsets.size();
            LogEvent persisted = ClockRewriter.withClock(event, clock);
            byte[] payload = serialize(persisted);
            int crc = crc32(payload);

            long offset = dataChannel.size();
            ByteBuffer buf = ByteBuffer.allocate(FRAME_PREFIX_SIZE + payload.length);
            buf.putInt(payload.length);
            buf.putInt(crc);
            buf.put(payload);
            buf.flip();
            writeFully(buf, offset);
            dataChannel.force(false);

            offsets.add(offset);
            payloadLengths.add(payload.length);
            if (persisted instanceof LogLeader claimed) {
                currentLeader = claimed.instanceId();
            }
            return Optional.of(persisted);
        } catch (IOException e) {
            throw new RuntimeException("Append failed on " + path, e);
        } finally {
            appendLock.unlock();
        }
    }

    private void writeFully(ByteBuffer buf, long pos) throws IOException {
        writeFully(dataChannel, buf, pos);
    }

    private static void writeFully(FileChannel channel, ByteBuffer buf, long pos) throws IOException {
        long offset = pos;
        while (buf.hasRemaining()) {
            int n = channel.write(buf, offset);
            if (n < 0) throw new IOException("write returned -1 at " + offset);
            offset += n;
        }
    }

    /** Best-effort fsync of a directory so a rename inside it is durable across power loss. */
    private static void forceDirectory(Path dir) {
        if (dir == null) return;
        try (FileChannel dirChannel = FileChannel.open(dir, StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (IOException e) {
            // Some platforms (e.g. Windows) refuse to open a directory channel;
            // there the OS provides rename durability. Best-effort.
            logger.debug("LocalFileLogBackend: directory fsync skipped: {}", e.toString());
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
        appendLock.lock();
        try {
            ensureOpen();
            Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
            Path archivedPath = path.resolveSibling(
                    path.getFileName() + ".archived." + System.currentTimeMillis());

            Files.deleteIfExists(tmpPath);
            int checkpointClock = -1;
            int copied;

            try (FileChannel tmpCh = FileChannel.open(tmpPath,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                tmpCh.write(ByteBuffer.wrap(MAGIC), 0);
                long pos = HEADER_SIZE;
                int clock = 0;
                for (LogEvent e : snapshotEvents) {
                    LogEvent persisted = ClockRewriter.withClock(e, clock);
                    byte[] payload = serialize(persisted);
                    int crc = crc32(payload);
                    ByteBuffer buf = ByteBuffer.allocate(FRAME_PREFIX_SIZE + payload.length);
                    buf.putInt(payload.length);
                    buf.putInt(crc);
                    buf.put(payload);
                    buf.flip();
                    writeFully(tmpCh, buf, pos);
                    pos += FRAME_PREFIX_SIZE + payload.length;
                    clock++;
                    if (persisted instanceof LogSnapshot snap) {
                        checkpointClock = snap.checkpointClock();
                    }
                }
                copied = clock;
                tmpCh.force(true);
            }

            // Crash-safe swap. The live file at `path` must stay valid and replayable
            // at every instant, so COPY it to the archive first (path untouched), then
            // ATOMICALLY replace it with the compacted tmp file. A crash before the move
            // leaves the original log intact; a crash after it leaves the new log in
            // place. Finally fsync the directory so the rename is durable across power loss.
            dataChannel.force(true);
            dataChannel.close();
            Files.copy(path, archivedPath, StandardCopyOption.COPY_ATTRIBUTES);
            Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(path.getParent());
            dataChannel = FileChannel.open(path,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);

            offsets.clear();
            payloadLengths.clear();
            currentLeader = null;
            scanIndex(HEADER_SIZE, dataChannel.size());

            int effectiveCheckpoint = checkpointClock < 0 ? Math.max(offsets.size() - 1, 0) : checkpointClock;
            return new SnapshotResult(path.toString(), archivedPath.toString(), effectiveCheckpoint, copied);
        } catch (IOException e) {
            throw new RuntimeException("Snapshot compact failed on " + path, e);
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public LogBackendReport introspect() {
        ensureOpen();
        appendLock.lock();
        try {
            int len = offsets.size();
            Map<String, Integer> counts = new HashMap<>();
            long lastTs = 0L;
            for (int i = 0; i < len; i++) {
                LogEvent e = get(i);
                counts.merge(e.getClass().getSimpleName(), 1, Integer::sum);
                if (e.timestamp() > lastTs) lastTs = e.timestamp();
            }
            return new LogBackendReport(path.toString(), len, currentLeader,
                    new LinkedHashMap<>(counts), lastTs);
        } finally {
            appendLock.unlock();
        }
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
        try {
            dataChannel.force(true);
        } catch (Exception ignored) {
            // best-effort flush
        }
        try {
            dataChannel.close();
        } catch (Exception ignored) {
            // best-effort
        }
        try {
            leaderLock.release();
        } catch (Exception ignored) {
            // best-effort
        }
        try {
            lockChannel.close();
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("LocalFileLogBackend " + path + " is closed");
        }
    }

    // ───────────────── helpers ─────────────────

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    private static byte[] serialize(LogEvent event) throws IOException {
        var baos = new ByteArrayOutputStream(256);
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(event);
        }
        return baos.toByteArray();
    }

    private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        var bais = new ByteArrayInputStream(bytes);
        try (var ois = new ObjectInputStream(bais)) {
            ois.setObjectInputFilter(LOG_FILTER);
            return ois.readObject();
        }
    }
}
