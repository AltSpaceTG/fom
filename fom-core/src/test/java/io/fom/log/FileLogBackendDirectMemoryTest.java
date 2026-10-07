package io.fom.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Large records must not cost direct (native) memory proportional to their size. The JDK
 * copies a heap buffer passed to {@code FileChannel.read/write} through a per-thread
 * temporary direct buffer of the same size and caches it for the thread's lifetime; a
 * 50 MB frame written in one call would pin ~50 MB of direct memory per thread.
 */
class FileLogBackendDirectMemoryTest {

    private static final String LEADER = "leader-1";
    private static final long ALLOWED_GROWTH = 4L * 1024 * 1024;

    @TempDir
    Path tmp;

    @Test
    void large_records_keep_direct_memory_bounded() throws Exception {
        Path file = tmp.resolve("big.bin");
        AtomicLong growth = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        // A fresh thread: its temporary direct buffer cache starts empty (and is released
        // when it exits), so earlier tests on the worker thread cannot mask a regression.
        Thread t = new Thread(() -> {
            try {
                long baseline = directMemoryUsed();
                try (var backend = new FileLogBackend(file)) {
                    assertThat(backend.append(new LogLeader(0, 1L, LEADER), LEADER)).isPresent();
                    for (int i = 0; i < 3; i++) {
                        assertThat(backend.append(new LogInitialized(0, 1L, "p" + i, bigProps(i)), LEADER))
                                .isPresent();
                    }
                    for (int i = 1; i <= 3; i++) {
                        assertThat(((LogInitialized) backend.get(i)).processName()).isEqualTo("p" + (i - 1));
                    }
                    backend.compact(List.of(new LogLeader(0, 1L, LEADER),
                            new LogInitialized(1, 1L, "p0", bigProps(0)),
                            new LogSnapshot(4, 1L, 3)), LEADER);
                    growth.set(Math.max(growth.get(), directMemoryUsed() - baseline));
                }
                try (var reopened = new FileLogBackend(file)) {
                    assertThat(reopened.length()).isEqualTo(3);
                    var init = (LogInitialized) reopened.get(1);
                    assertThat(init.processName()).isEqualTo("p0");
                    growth.set(Math.max(growth.get(), directMemoryUsed() - baseline));
                }
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        t.start();
        t.join();
        if (failure.get() != null) throw new AssertionError(failure.get());

        assertThat(growth.get())
                .as("direct memory retained after writing/reading ~45 MB records")
                .isLessThan(ALLOWED_GROWTH);
    }

    /** ~45 MB in five arrays, each under the deserialization filter's 10M-element cap. */
    private static Map<String, byte[]> bigProps(int seed) {
        Map<String, byte[]> props = new HashMap<>();
        for (int k = 0; k < 5; k++) {
            byte[] b = new byte[9_000_000];
            b[0] = (byte) seed;
            b[b.length - 1] = (byte) k;
            props.put("k" + k, b);
        }
        return props;
    }

    private static long directMemoryUsed() {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if (pool.getName().equals("direct")) return pool.getMemoryUsed();
        }
        throw new IllegalStateException("no direct buffer pool");
    }
}
