package io.fom.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code introspect()} must be O(1) and must not serialise behind — or in front
 * of — an append: a {@code /debug} poller calling it in a loop used to re-read
 * and deserialise the whole log while holding the append lock, stalling every
 * trigger, init and load.
 */
class IntrospectCostTest {

    private static final String LEADER = "leader-1";

    @TempDir
    Path tmp;

    private FileLogBackend fileBackend() throws IOException {
        Path file = Files.createTempFile(tmp, "introspect-", ".bin");
        Files.delete(file); // the backend creates it
        return new FileLogBackend(file);
    }

    private static int seed(LogBackend backend, int events) {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        for (int i = 0; i < events; i++) {
            backend.append(new LogInitialized(0, now(), "P" + i, Map.of()), LEADER);
        }
        return events + 1;
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    /**
     * 20 000 calls over a multi-thousand-event log. Re-reading the log per call
     * would need tens of millions of frame reads and deserialisations (minutes);
     * an O(1) snapshot read takes milliseconds.
     */
    private static void assertIntrospectIsConstantTime(LogBackend backend, int expectedLength) {
        LogBackendReport report = backend.introspect();
        long startNanos = System.nanoTime();
        for (int i = 0; i < 20_000; i++) {
            report = backend.introspect();
        }
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        assertThat(report.length()).isEqualTo(expectedLength);
        assertThat(report.eventCounts()).containsEntry("LogLeader", 1)
                .containsEntry("LogInitialized", expectedLength - 1);
        assertThat(report.currentLeader()).isEqualTo(LEADER);
        assertThat(elapsedMillis)
                .as("20000 introspect() calls on a %d-event log took %d ms — introspect() is not O(1)",
                        expectedLength, elapsedMillis)
                .isLessThan(5_000L);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void introspect_on_a_large_file_log_is_constant_time() throws IOException {
        try (FileLogBackend backend = fileBackend()) {
            assertIntrospectIsConstantTime(backend, seed(backend, 3_000));
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void introspect_on_a_large_in_memory_log_is_constant_time() {
        var backend = new InMemoryLogBackend("counts");
        try {
            assertIntrospectIsConstantTime(backend, seed(backend, 5_000));
        } finally {
            backend.close();
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void introspect_storm_does_not_stall_appends_on_a_file_log() throws Exception {
        try (FileLogBackend backend = fileBackend()) {
            assertAppendsKeepFlowingUnderIntrospection(backend, 300, 5_000);
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void introspect_storm_does_not_stall_appends_in_memory() throws Exception {
        var backend = new InMemoryLogBackend("storm");
        try {
            // Many appends: in-memory appends are O(1) now, and 5 000 of them were over before the
            // poller had taken a thousand turns.
            assertAppendsKeepFlowingUnderIntrospection(backend, 200_000, 1_000);
        } finally {
            backend.close();
        }
    }

    /**
     * A writer appends {@code appends} events while this thread hammers
     * {@code introspect()}. Every report must be internally consistent (its
     * per-type counts sum to its own length), and the poller must get far more
     * turns than the writer — before the fix each call re-read the whole log
     * under the append lock, so the two ran in lockstep.
     */
    private static void assertAppendsKeepFlowingUnderIntrospection(LogBackend backend, int appends, long minCalls)
            throws InterruptedException {
        int start = seed(backend, 200);
        var failure = new AtomicReference<Throwable>();
        Thread writer = Thread.ofPlatform().start(() -> {
            try {
                for (int i = 0; i < appends; i++) {
                    backend.append(new LogInitialized(0, now(), "W" + i, Map.of()), LEADER);
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        });

        long calls = 0;
        long pollNanos = 0;
        int lastLength = 0;
        while (writer.isAlive()) {
            long callStart = System.nanoTime();
            LogBackendReport report = backend.introspect();
            pollNanos += System.nanoTime() - callStart;
            int counted = report.eventCounts().values().stream().mapToInt(Integer::intValue).sum();
            assertThat(counted)
                    .as("a report's per-type counts must add up to the length it reports")
                    .isEqualTo(report.length());
            assertThat(report.length()).as("length never goes backwards").isGreaterThanOrEqualTo(lastLength);
            assertThat(report.currentLeader()).isEqualTo(LEADER);
            lastLength = report.length();
            calls++;
        }
        writer.join();
        assertThat(failure.get()).isNull();

        assertThat(backend.introspect().length()).isEqualTo(start + appends);
        assertThat(calls)
                .as("only %s introspect() calls fitted alongside %s appends — introspect() is blocking them",
                        calls, appends)
                .isGreaterThan(minCalls);
        long meanNanos = pollNanos / Math.max(calls, 1);
        assertThat(meanNanos)
                .as("mean introspect() latency under concurrent appends was %s ns", meanNanos)
                .isLessThan(1_000_000L);
    }

    /**
     * The hard guarantee: while an append is stuck mid-write holding the append
     * lock, {@code introspect()} still answers immediately.
     */
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void introspect_answers_while_an_append_holds_the_append_lock() throws Exception {
        Path file = Files.createTempFile(tmp, "gated-", ".bin");
        Files.delete(file);
        var armed = new AtomicBoolean(false);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        FileLogBackend.ChannelOpener opener =
                (p, options) -> new GatedFileChannel(FileChannel.open(p, options), armed, entered, release);

        FileLogBackend backend = new FileLogBackend(file, opener);
        try {
            int length = seed(backend, 500);
            armed.set(true);
            Thread writer = Thread.ofPlatform().start(
                    () -> backend.append(new LogInitialized(0, now(), "Blocked", Map.of()), LEADER));
            try {
                assertThat(entered.await(30, TimeUnit.SECONDS))
                        .as("the gated append should be stuck mid-write").isTrue();

                var report = new AtomicReference<LogBackendReport>();
                Thread poller = Thread.ofPlatform().start(() -> report.set(backend.introspect()));
                poller.join(TimeUnit.SECONDS.toMillis(10));
                assertThat(poller.isAlive())
                        .as("introspect() waited for the in-flight append instead of reading a snapshot")
                        .isFalse();
                assertThat(report.get().length()).isEqualTo(length);
                assertThat(report.get().eventCounts())
                        .as("the blocked append is not visible until it completes")
                        .containsEntry("LogInitialized", length - 1);
            } finally {
                release.countDown();
                writer.join();
            }
            assertThat(backend.introspect().length()).isEqualTo(length + 1);
        } finally {
            release.countDown();
            backend.close();
        }
    }

    /** Delegating channel whose first positional write blocks until released. */
    private static final class GatedFileChannel extends FileChannel {

        private final FileChannel delegate;
        private final AtomicBoolean armed;
        private final CountDownLatch entered;
        private final CountDownLatch release;

        GatedFileChannel(FileChannel delegate, AtomicBoolean armed, CountDownLatch entered, CountDownLatch release) {
            this.delegate = delegate;
            this.armed = armed;
            this.entered = entered;
            this.release = release;
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            if (armed.compareAndSet(true, false)) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.write(src, position);
        }

        @Override public int read(ByteBuffer dst) throws IOException { return delegate.read(dst); }
        @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException { return delegate.read(dsts, offset, length); }
        @Override public int write(ByteBuffer src) throws IOException { return delegate.write(src); }
        @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException { return delegate.write(srcs, offset, length); }
        @Override public long position() throws IOException { return delegate.position(); }
        @Override public FileChannel position(long newPosition) throws IOException { delegate.position(newPosition); return this; }
        @Override public long size() throws IOException { return delegate.size(); }
        @Override public FileChannel truncate(long size) throws IOException { delegate.truncate(size); return this; }
        @Override public void force(boolean metaData) throws IOException { delegate.force(metaData); }
        @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException { return delegate.transferTo(position, count, target); }
        @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException { return delegate.transferFrom(src, position, count); }
        @Override public int read(ByteBuffer dst, long position) throws IOException { return delegate.read(dst, position); }
        @Override public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return delegate.map(mode, position, size); }
        @Override public FileLock lock(long position, long size, boolean shared) throws IOException { return delegate.lock(position, size, shared); }
        @Override public FileLock tryLock(long position, long size, boolean shared) throws IOException { return delegate.tryLock(position, size, shared); }
        @Override protected void implCloseChannel() throws IOException { delegate.close(); }
    }
}
