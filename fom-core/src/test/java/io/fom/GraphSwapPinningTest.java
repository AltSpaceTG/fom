package io.fom;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An in-place graph swap issued from a virtual thread must not pin its carrier:
 * with a single carrier (Java 21 does not compensate a pinned one) the swap
 * would otherwise wait forever for the new node's dispatcher. Runs
 * {@link PinningProbe} in a child JVM because the scheduler size is fixed at
 * JVM start.
 */
class GraphSwapPinningTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void graph_swap_from_a_virtual_thread_does_not_pin_the_only_carrier() throws Exception {
        String java = ProcessHandle.current().info().command().orElse("java");
        java.lang.Process child = new ProcessBuilder(
                java,
                "-Djdk.virtualThreadScheduler.parallelism=1",
                "-Djdk.virtualThreadScheduler.maxPoolSize=1",
                "-cp", System.getProperty("java.class.path"),
                PinningProbe.class.getName())
                .redirectErrorStream(true)
                .start();
        String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!child.waitFor(45, TimeUnit.SECONDS)) {
            child.destroyForcibly();
        }
        assertThat(child.exitValue()).as(output).isZero();
    }
}
