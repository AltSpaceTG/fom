package io.fom.log.cli;

import io.fom.Sid;
import io.fom.log.LocalFileLogBackend;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogLoaded;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FomLogCliTest {

    @TempDir
    Path tmp;

    @Test
    void inspect_prints_summary() throws IOException {
        Path file = tmp.resolve("log.bin");
        try (var backend = new LocalFileLogBackend(file)) {
            backend.append(new LogLeader(0, System.currentTimeMillis(), "test-instance"), "test-instance");
            backend.append(new LogInitialized(0, System.currentTimeMillis(), "Echo", Map.of("k", new byte[]{1})), "test-instance");
            backend.append(new LogLoaded(0, System.currentTimeMillis(), new Sid("Echo", 1)), "test-instance");
        }

        String out = runCaptured("inspect", file.toString());
        assertThat(out).contains("Length: 3 event(s)");
        assertThat(out).contains("Current leader: test-instance");
        assertThat(out).contains("LogLeader");
        assertThat(out).contains("LogInitialized");
        assertThat(out).contains("LogLoaded");
    }

    @Test
    void diagnose_reports_ok_for_clean_log() throws IOException {
        Path file = tmp.resolve("diag.bin");
        try (var backend = new LocalFileLogBackend(file)) {
            backend.append(new LogLeader(0, System.currentTimeMillis(), "ok"), "ok");
            backend.append(new LogInitialized(0, System.currentTimeMillis(), "P", Map.of()), "ok");
        }
        String out = runCaptured("diagnose", file.toString());
        assertThat(out).contains("Diagnose OK").contains("2 events");
    }

    @Test
    void diagnose_reports_truncation_for_corrupt_tail() throws IOException {
        Path file = tmp.resolve("corrupt.bin");
        try (var backend = new LocalFileLogBackend(file)) {
            backend.append(new LogLeader(0, System.currentTimeMillis(), "ok"), "ok");
            backend.append(new LogInitialized(0, System.currentTimeMillis(), "P", Map.of()), "ok");
        }
        // Append garbage past the framed events; LocalFileLogBackend will truncate on next open.
        Files.write(file, new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF},
                java.nio.file.StandardOpenOption.APPEND);

        String out = runCaptured("diagnose", file.toString());
        // After truncation we still see "Diagnose OK" with the surviving 2 events.
        assertThat(out).contains("any corrupt tail was truncated");
    }

    @Test
    void migrate_stub_returns_non_zero() {
        int exit = new CommandLine(new FomLogCli()).execute(
                "migrate", "--from", "1.x", "--to", "2.0", "in.bin", "out.bin");
        assertThat(exit).isEqualTo(64);
    }

    private String runCaptured(String... args) {
        var buf = new ByteArrayOutputStream();
        PrintStream old = System.out;
        System.setOut(new PrintStream(buf));
        try {
            new CommandLine(new FomLogCli()).execute(args);
        } finally {
            System.setOut(old);
        }
        return buf.toString();
    }
}
