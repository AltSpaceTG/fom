package io.fom.log.cli;

import io.fom.Sid;
import io.fom.log.FileLogBackend;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogDead;
import io.fom.log.LogDependencyChanged;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogLoaded;
import io.fom.log.LogPaused;
import io.fom.log.LogResumed;
import io.fom.log.LogTrigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class FomLogCliTest {

    @TempDir
    Path tmp;

    record Run(int exit, String out, String err) { }

    @Test
    void inspect_prints_summary() throws IOException {
        Path file = tmp.resolve("log.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, System.currentTimeMillis(), "test-instance"), "test-instance");
            backend.append(new LogInitialized(0, System.currentTimeMillis(), "Echo", Map.of("k", new byte[]{1})), "test-instance");
            backend.append(new LogLoaded(0, System.currentTimeMillis(), new Sid("Echo", 1)), "test-instance");
        }

        Run r = run("inspect", file.toString());
        assertThat(r.exit()).isZero();
        assertThat(r.out()).contains("Length: 3 event(s)");
        assertThat(r.out()).contains("Current leader: test-instance");
        assertThat(r.out()).contains("LogLeader");
        assertThat(r.out()).contains("LogInitialized");
        assertThat(r.out()).contains("LogLoaded");
        assertThat(r.out()).contains("First clock: 0 (position 0)").contains("Last clock: 2 (position 2)")
                .doesNotContain("Clock gaps");
        // a running maximum over the log, not the newest event's timestamp
        assertThat(r.out()).contains("Highest event timestamp: ").doesNotContain("Last event timestamp");
        assertThat(r.err()).doesNotContain("WARNING");
    }

    @Test
    void events_summarises_the_property_keys_of_a_large_process_unless_full() throws IOException {
        Path file = tmp.resolve("large.bin");
        Map<String, byte[]> props = new java.util.HashMap<>();
        for (int i = 0; i < 25; i++) {
            props.put(String.format("key%02d", i), new byte[]{(byte) i});
        }
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), "i"), "i");
            backend.append(new LogInitialized(0, now(), "Big", props), "i");
        }
        Run r = run("events", file.toString(), "--type", "LogInitialized");
        assertThat(r.exit()).isZero();
        assertThat(r.out()).contains("properties=25 keys [key00, key01, key02, key03, key04, key05, key06, key07, "
                + "key08, key09, ... +15 more]").doesNotContain("key10");

        Run full = run("events", file.toString(), "--type", "LogInitialized", "--full");
        assertThat(full.exit()).isZero();
        assertThat(full.out()).contains("properties=[key00, key01,").contains("key24]").doesNotContain("more]");

        assertThat(FomLogCli.Events.propertyKeys(java.util.Set.of("b", "a"), false)).isEqualTo("[a, b]");
    }

    @Test
    void diagnose_fails_a_log_spliced_from_two_timelines() throws IOException {
        Path a = tmp.resolve("a.bin");
        Path b = tmp.resolve("b.bin");
        for (Path p : List.of(a, b)) {
            try (var backend = new FileLogBackend(p)) {
                backend.append(new LogLeader(0, now(), "i"), "i");
                backend.append(new LogLoaded(0, now(), new Sid("P", 0)), "i");
            }
        }
        byte[] bytesA = Files.readAllBytes(a);
        byte[] bytesB = Files.readAllBytes(b);
        Path spliced = tmp.resolve("spliced.bin");
        byte[] joined = java.util.Arrays.copyOf(bytesA, bytesA.length + bytesB.length - 4);
        System.arraycopy(bytesB, 4, joined, bytesA.length, bytesB.length - 4);
        Files.write(spliced, joined);

        Run r = run("diagnose", spliced.toString());
        assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        assertThat(r.out()).contains("Diagnose FAIL").contains("offset " + bytesA.length)
                .contains("2 event(s) readable")
                .contains("clock 0 of LogLeader is not greater than the previous event's clock 1")
                .doesNotContain("Diagnose OK");
        assertThat(Files.readAllBytes(spliced)).isEqualTo(joined);
    }

    @Test
    void inspect_prints_dashes_for_clocks_of_an_empty_log() throws IOException {
        Path file = tmp.resolve("empty.bin");
        new FileLogBackend(file).close();
        Run r = run("inspect", file.toString());
        assertThat(r.exit()).isZero();
        assertThat(r.out()).contains("Length: 0 event(s)").contains("First clock: -").contains("Last clock: -")
                .contains("Current leader: -"); // a log nobody claimed, reported like the absent clocks
    }

    /** A log with one event of every kind worth showing, at positions == clocks 0..9. */
    private Path mixedLog() throws IOException {
        Path file = tmp.resolve("mixed.bin");
        String leader = "node-A";
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), leader), leader);                               // 0
            backend.append(graphOf("Src", "Dst"), leader);                                          // 1
            backend.append(new LogInitialized(0, now(), "Src", Map.of("k", new byte[]{1})), leader); // 2
            backend.append(new LogLoaded(0, now(), new Sid("Src", 2)), leader);                     // 3
            backend.append(new LogTrigger(0, now(), List.of("Src")), leader);                       // 4
            backend.append(new LogDependencyChanged(0, now(), new Sid("Dst", 7), "Src", 2, 9), leader); // 5
            backend.append(new LogPaused(0, now(), "Src", false, false), leader);                   // 6
            backend.append(new LogPaused(0, now(), "Dst", true, true), leader);                     // 7
            backend.append(new LogResumed(0, now(), "Src"), leader);                                // 8
            backend.append(new LogDead(0, now(), new Sid("Src", 2)), leader);                       // 9
        }
        return file;
    }

    @Test
    void events_lists_every_event_with_position_clock_type_and_key_fields() throws IOException {
        Path file = mixedLog();
        Run r = run("events", file.toString());
        assertThat(r.exit()).isZero();
        List<String> lines = r.out().lines().toList();
        assertThat(lines).hasSize(11); // header + 10 events
        assertThat(lines.get(0)).contains("POSITION").contains("CLOCK").contains("TYPE");
        assertThat(lines.get(1)).matches("0\\s+0\\s+\\S+\\s+LogLeader\\s+instance=node-A");
        assertThat(lines.get(2)).contains("LogChangeGraph").contains("2 node(s): Src, Dst");
        assertThat(lines.get(3)).matches("2\\s+2\\s+.*LogInitialized\\s+sid=Src@2 properties=\\[k]");
        assertThat(lines.get(4)).contains("LogLoaded").contains("sid=Src@2");
        assertThat(lines.get(5)).contains("LogTrigger").contains("processes=[Src]");
        assertThat(lines.get(6)).contains("LogDependencyChanged").contains("sid=Dst@7 dep=Src depClock 2 -> 9");
        assertThat(lines.get(7)).contains("LogPaused").contains("process=Src stale=false forDependency=false")
                .contains("operator pause");
        assertThat(lines.get(8)).contains("process=Dst stale=true forDependency=true")
                .contains("dependency is paused");
        assertThat(lines.get(9)).contains("LogResumed").contains("process=Src");
        assertThat(lines.get(10)).contains("LogDead").contains("sid=Src@2");
        assertThat(r.err()).doesNotContain("NOTE").doesNotContain("WARNING");
    }

    @Test
    void events_shows_which_version_a_replacement_init_replaces() throws IOException {
        Path file = tmp.resolve("replace.bin");
        String leader = "node-A";
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), leader), leader);
            backend.append(new LogInitialized(0, now(), "Src", Map.of()), leader);
            backend.append(new LogInitialized(0, now(), "Src", Map.of("k", new byte[]{1}), new Sid("Src", 1)), leader);
        }
        Run r = run("events", file.toString(), "--type", "LogInitialized");
        assertThat(r.exit()).isZero();
        List<String> lines = r.out().lines().skip(1).toList();
        assertThat(lines.get(0)).contains("sid=Src@1 properties=[]").doesNotContain("replaces");
        assertThat(lines.get(1)).contains("sid=Src@2 replaces=Src@1 properties=[k]");
    }

    @Test
    void events_filters_by_type_and_process() throws IOException {
        Path file = mixedLog();

        Run paused = run("events", file.toString(), "--type", "LogPaused");
        assertThat(paused.exit()).isZero();
        assertThat(paused.out().lines().skip(1).toList()).hasSize(2).allMatch(l -> l.contains("LogPaused"));

        Run two = run("events", file.toString(), "--type", "LogPaused,LogResumed", "--process", "Src");
        assertThat(two.out().lines().skip(1).toList()).hasSize(2)
                .anyMatch(l -> l.contains("LogPaused")).anyMatch(l -> l.contains("LogResumed"));

        Run dst = run("events", file.toString(), "--process", "Dst");
        // the dependency change of Dst and Dst's pause; not the graph listing it
        assertThat(dst.out().lines().skip(1).toList()).hasSize(2)
                .anyMatch(l -> l.contains("LogDependencyChanged")).anyMatch(l -> l.contains("LogPaused"));

        Run src = run("events", file.toString(), "--process", "Src");
        // init, load, trigger, dependency change on Src, pause, resume, dead
        assertThat(src.out().lines().skip(1).toList()).hasSize(7);

        Run none = run("events", file.toString(), "--process", "Nope");
        assertThat(none.exit()).isZero();
        assertThat(none.out().lines().toList()).hasSize(1);
        assertThat(none.err()).contains("No matching events");

        Run bad = run("events", file.toString(), "--type", "LogPausd");
        assertThat(bad.exit()).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
        assertThat(bad.err()).contains("Unknown event type 'LogPausd'").contains("LogPaused");
    }

    @Test
    void events_honours_from_and_notes_truncation() throws IOException {
        Path file = mixedLog();

        Run r = run("events", file.toString(), "--from", "3", "--limit", "2");
        assertThat(r.exit()).isZero();
        List<String> lines = r.out().lines().skip(1).toList();
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).startsWith("3 ").contains("LogLoaded");
        assertThat(lines.get(1)).startsWith("4 ").contains("LogTrigger");
        assertThat(r.err()).contains("truncated after 2 event(s)").contains("--from 5");

        // Exactly as many matches as the limit: no truncation note.
        Run exact = run("events", file.toString(), "--type", "LogPaused", "--limit", "2");
        assertThat(exact.err()).doesNotContain("truncated");

        // The default limit truncates long logs.
        Path big = tmp.resolve("long.bin");
        try (var backend = new FileLogBackend(big)) {
            backend.append(new LogLeader(0, now(), "i"), "i");
            for (int k = 0; k < FomLogCli.Events.DEFAULT_LIMIT + 5; k++) {
                backend.append(new LogTrigger(0, now(), List.of("P")), "i");
            }
        }
        Run longRun = run("events", big.toString());
        assertThat(longRun.out().lines().skip(1).toList()).hasSize(FomLogCli.Events.DEFAULT_LIMIT);
        assertThat(longRun.err()).contains("--from " + FomLogCli.Events.DEFAULT_LIMIT);

        assertThat(run("events", file.toString(), "--limit", "0").exit()).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
        assertThat(run("events", file.toString(), "--from", "-1").exit()).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
    }

    @Test
    void events_and_inspect_show_clocks_that_differ_from_positions_after_compaction() throws IOException {
        Path file = tmp.resolve("compacted.bin");
        String leader = "engine-1";
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), leader), leader);
            backend.append(graphOf("P"), leader);
            for (int gen = 0; gen < 5; gen++) {
                var init = (LogInitialized) backend.append(
                        new LogInitialized(0, now(), "P", Map.of()), leader).orElseThrow();
                backend.append(new LogLoaded(0, now(), init.sid()), leader);
                if (gen < 4) backend.append(new LogDead(0, now(), init.sid()), leader);
            }
        }
        assertThat(run("compact", file.toString()).exit()).isZero();

        List<String> clocks = new ArrayList<>();
        int len;
        try (var backend = new FileLogBackend(file)) {
            len = backend.length();
            for (int i = 0; i < len; i++) clocks.add(String.valueOf(backend.get(i).clock()));
        }
        int initPos = -1;
        for (int i = 0; i < len; i++) {
            if (!clocks.get(i).equals(String.valueOf(i))) { initPos = i; break; }
        }
        assertThat(initPos).as("some event keeps a clock different from its position: " + clocks).isNotNegative();

        Run events = run("events", file.toString());
        assertThat(events.exit()).isZero();
        assertThat(events.out().lines().skip(1 + initPos).findFirst().orElseThrow())
                .matches(initPos + "\\s+" + clocks.get(initPos) + "\\s+.*");

        Run inspect = run("inspect", file.toString());
        assertThat(inspect.out()).contains("Last clock: " + clocks.get(len - 1) + " (position " + (len - 1) + ")")
                .contains("Clock gaps:");
    }

    @Test
    void diagnose_reports_ok_for_clean_log() throws IOException {
        Path file = tmp.resolve("diag.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, System.currentTimeMillis(), "ok"), "ok");
            backend.append(new LogInitialized(0, System.currentTimeMillis(), "P", Map.of()), "ok");
        }
        Run r = run("diagnose", file.toString());
        assertThat(r.exit()).isZero();
        assertThat(r.out()).contains("Diagnose OK").contains("2 events");
    }

    @Test
    void diagnose_reports_corrupt_tail_without_touching_the_file() throws IOException {
        Path file = tmp.resolve("corrupt.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, System.currentTimeMillis(), "ok"), "ok");
            backend.append(new LogInitialized(0, System.currentTimeMillis(), "P", Map.of()), "ok");
        }
        long cleanSize = Files.size(file);
        Files.write(file, new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF},
                java.nio.file.StandardOpenOption.APPEND);
        byte[] before = Files.readAllBytes(file);

        Run r = run("diagnose", file.toString());
        assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        assertThat(r.out()).contains("Diagnose FAIL").contains("offset " + cleanSize)
                .contains("2 event(s) readable").doesNotContain("Diagnose OK");
        assertThat(Files.readAllBytes(file)).isEqualTo(before);
    }

    @Test
    void diagnose_explains_a_torn_last_frame() throws IOException {
        Path file = tmp.resolve("torn-tail.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), "ok"), "ok");
            backend.append(new LogInitialized(0, now(), "P", Map.of()), "ok");
        }
        Files.write(file, new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF},
                java.nio.file.StandardOpenOption.APPEND);
        byte[] before = Files.readAllBytes(file);

        Run r = run("diagnose", file.toString());
        assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        assertThat(r.out()).contains("Cause: torn last frame")
                .contains("crash mid-append")
                .contains("torn-tail.bin.truncated.<millis>")
                .contains("nothing else is damaged")
                .doesNotContain("damage inside the log");
        assertThat(Files.readAllBytes(file)).isEqualTo(before);
        try (var dir = Files.list(tmp)) {
            assertThat(dir.map(p -> p.getFileName().toString())).noneMatch(n -> n.contains(".truncated."));
        }
    }

    @Test
    void inspect_summarises_the_readable_prefix_of_a_torn_log_but_exits_1() throws IOException {
        Path file = tmp.resolve("torn.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), "ok"), "ok");
            backend.append(new LogInitialized(0, now(), "P", Map.of()), "ok");
        }
        Files.write(file, new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF},
                java.nio.file.StandardOpenOption.APPEND);
        byte[] before = Files.readAllBytes(file);

        Run r = run("inspect", file.toString());
        assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        assertThat(r.err()).contains("WARNING").contains("readable prefix");
        assertThat(r.out()).contains("Length: 2 event(s)");
        assertThat(Files.readAllBytes(file)).isEqualTo(before);

        // Consistent with the other read-only commands.
        assertThat(run("events", file.toString()).exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        assertThat(run("diagnose", file.toString()).exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
    }

    @Test
    void every_subcommand_prints_its_help() {
        for (String cmd : new String[]{"inspect", "events", "diagnose", "compact"}) {
            Run flag = run(cmd, "--help");
            assertThat(flag.exit()).as(cmd + " --help").isZero();
            assertThat(flag.out()).contains("Usage: fom-log " + cmd).contains("--help");
            assertThat(flag.err()).as(cmd + " --help").isEmpty();

            Run help = run("help", cmd);
            assertThat(help.exit()).as("help " + cmd).isZero();
            assertThat(help.out()).contains("Usage: fom-log " + cmd);
        }
        assertThat(run("events", "-h").out()).contains("--process");
        assertThat(run("inspect", "--version").out()).contains(FomLogCli.VERSION);
        assertThat(run("--help").out()).contains("help");
    }

    @Test
    void diagnose_middle_frame_corruption_leaves_original_intact_and_fails() throws IOException {
        Path file = tmp.resolve("middle.bin");
        List<Long> starts = new ArrayList<>();
        try (var backend = new FileLogBackend(file)) {
            starts.add(Files.size(file));
            backend.append(new LogLeader(0, System.currentTimeMillis(), "i"), "i");
            for (int k = 1; k < 20; k++) {
                starts.add(Files.size(file));
                backend.append(new LogInitialized(0, System.currentTimeMillis(), "P" + k, Map.of()), "i");
            }
        }
        long frame10 = starts.get(10);
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            long pos = frame10 + 8 + 20; // inside frame 10's payload
            raf.seek(pos);
            int b = raf.read();
            raf.seek(pos);
            raf.write(b ^ 0xFF);
        }
        byte[] before = Files.readAllBytes(file);

        Run diagnose = run("diagnose", file.toString());
        assertThat(diagnose.exit()).isNotZero();
        assertThat(diagnose.out()).contains("Diagnose FAIL").contains("offset " + frame10)
                .contains("10 event(s) readable").doesNotContain("Diagnose OK")
                .contains("damage inside the log, not a torn last frame")
                .doesNotContain("Cause: torn last frame");
        assertThat(Files.readAllBytes(file)).isEqualTo(before);

        Run inspect = run("inspect", file.toString());
        assertThat(inspect.err()).contains("WARNING").contains("offset " + frame10);
        assertThat(Files.readAllBytes(file)).isEqualTo(before);

        // The original is still a valid, fully-owned log: the corrupt frame is still there.
        assertThat(Files.size(file)).isGreaterThan(frame10);
    }

    @Test
    void events_lists_the_readable_prefix_before_mid_file_damage_but_exits_1() throws IOException {
        Path file = tmp.resolve("middle-events.bin");
        List<Long> starts = new ArrayList<>();
        try (var backend = new FileLogBackend(file)) {
            starts.add(Files.size(file));
            backend.append(new LogLeader(0, System.currentTimeMillis(), "i"), "i");
            for (int k = 1; k < 20; k++) {
                starts.add(Files.size(file));
                backend.append(new LogInitialized(0, System.currentTimeMillis(), "P" + k, Map.of()), "i");
            }
        }
        long frame10 = starts.get(10);
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            long pos = frame10 + 8 + 20; // inside frame 10's payload
            raf.seek(pos);
            int b = raf.read();
            raf.seek(pos);
            raf.write(b ^ 0xFF);
        }
        byte[] before = Files.readAllBytes(file);

        Run r = run("events", file.toString());
        assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        assertThat(r.err()).contains("WARNING").contains("offset " + frame10)
                .contains("10 event(s) readable").contains("readable prefix")
                .doesNotContain("No matching events");
        List<String> lines = r.out().lines().toList();
        assertThat(lines).hasSize(11); // header + the 10 events before the damage
        assertThat(lines.get(1)).contains("LogLeader");
        assertThat(lines.get(10)).contains("P9");
        assertThat(r.out()).doesNotContain("P10");
        // Filters work on the prefix too.
        Run one = run("events", file.toString(), "--process", "P3");
        assertThat(one.exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        assertThat(one.out().lines().toList()).hasSize(2);
        assertThat(Files.readAllBytes(file)).as("the operator's file is untouched").isEqualTo(before);
    }

    @Test
    void diagnose_cause_names_the_operators_file_not_the_cli_temp_copy() throws IOException {
        Path file = tmp.resolve("cause.bin");
        List<Long> starts = new ArrayList<>();
        try (var backend = new FileLogBackend(file)) {
            starts.add(Files.size(file));
            backend.append(new LogLeader(0, System.currentTimeMillis(), "i"), "i");
            for (int k = 1; k < 20; k++) {
                starts.add(Files.size(file));
                backend.append(new LogInitialized(0, System.currentTimeMillis(), "P" + k, Map.of()), "i");
            }
        }
        long frame10 = starts.get(10);
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            long pos = frame10 + 8 + 20; // inside frame 10's payload
            raf.seek(pos);
            int b = raf.read();
            raf.seek(pos);
            raf.write(b ^ 0xFF);
        }

        Run r = run("diagnose", file.toString());
        assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        // The only sentence with a recovery instruction must point at the operator's own file.
        assertThat(r.out()).contains("Cause: Log " + file + " is corrupt at byte offset " + frame10)
                .contains("truncate it to " + frame10 + " bytes");
        assertThat(r.out()).doesNotContain("fom-log-cli").doesNotContain("log.bin");
        assertThat(r.err()).doesNotContain("fom-log-cli");
    }

    @Test
    void a_file_that_is_not_a_fom_log_is_refused_without_naming_the_cli_temp_copy() throws IOException {
        Path random = tmp.resolve("orders.bin");
        byte[] junk = new byte[512];
        new java.util.Random(42).nextBytes(junk);
        junk[0] = 'X';                                   // definitely not the FOM magic
        Files.write(random, junk);
        Path text = tmp.resolve("notes.txt");
        Files.writeString(text, "these are notes, not a log\n");
        Path tooShort = tmp.resolve("stub.bin");
        Files.write(tooShort, new byte[]{1, 2});          // shorter than the magic header

        long tempDirsBefore = cliTempDirs();
        for (Path file : List.of(random, text, tooShort)) {
            for (String cmd : new String[]{"inspect", "events", "diagnose", "compact"}) {
                Run r = run(cmd, file.toString());
                String where = cmd + " " + file.getFileName();
                assertThat(r.exit()).as(where).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
                String all = r.out() + r.err();
                assertThat(all).as(where).contains("Failed to open " + file);
                // Nothing outside the operator's own path may name the private copy.
                String rest = all.replace(file.toString(), "<the operator's file>");
                assertThat(rest).as(where)
                        .doesNotContain(FomLogCli.TEMP_PREFIX)
                        .doesNotContain(FomLogCli.COPY_NAME)
                        .doesNotContain(System.getProperty("java.io.tmpdir"));
            }
        }
        assertThat(cliTempDirs()).as("temporary copies are cleaned up").isEqualTo(tempDirsBefore);
        assertThat(Files.readAllBytes(random)).isEqualTo(junk);   // nothing was rewritten
    }

    /** How many working copies the CLI currently has on disk. */
    private static long cliTempDirs() throws IOException {
        try (var dirs = Files.list(Path.of(System.getProperty("java.io.tmpdir")))) {
            return dirs.filter(p -> p.getFileName().toString().startsWith(FomLogCli.TEMP_PREFIX)).count();
        }
    }

    @Test
    void missing_path_is_refused_and_not_created() {
        Path typo = tmp.resolve("orders.bni");
        for (String cmd : new String[]{"inspect", "diagnose"}) {
            Run r = run(cmd, typo.toString());
            assertThat(r.exit()).as(cmd).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
            assertThat(r.err()).contains("No such file");
        }
        assertThat(Files.exists(typo)).isFalse();
        assertThat(Files.exists(tmp.resolve("orders.bni.lock"))).isFalse();
    }

    @Test
    void a_log_in_an_unsearchable_directory_is_reported_as_inaccessible_not_missing() throws IOException {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                Files.getFileStore(tmp).supportsFileAttributeView(java.nio.file.attribute.PosixFileAttributeView.class),
                "needs POSIX permissions");
        org.junit.jupiter.api.Assumptions.assumeFalse("root".equals(System.getProperty("user.name")),
                "root ignores directory permissions");
        Path dir = Files.createDirectory(tmp.resolve("locked"));
        Path file = dir.resolve("log.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
        }
        var original = Files.getPosixFilePermissions(dir);
        Files.setPosixFilePermissions(dir, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        try {
            org.junit.jupiter.api.Assumptions.assumeFalse(Files.exists(file), "directory permissions not enforced");
            for (String cmd : new String[]{"inspect", "diagnose"}) {
                Run r = run(cmd, file.toString());
                assertThat(r.exit()).as(cmd).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
                assertThat(r.err()).as(cmd).contains("Cannot access " + file).contains("permission denied")
                        .doesNotContain("No such file");
            }
        } finally {
            Files.setPosixFilePermissions(dir, original);
        }
    }

    @Test
    void inspect_works_while_log_is_held_open() throws IOException {
        Path file = tmp.resolve("live.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, System.currentTimeMillis(), "i"), "i");
            Run r = run("inspect", file.toString());
            assertThat(r.exit()).isZero();
            assertThat(r.out()).contains("Length: 1 event(s)");
        }
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    private static LogChangeGraph graphOf(String... names) {
        var nodes = new ArrayList<LogChangeGraph.Node>();
        for (String name : names) {
            nodes.add(new LogChangeGraph.Node(name, List.of(), List.of(), null));
        }
        return new LogChangeGraph(0, now(), nodes);
    }

    @Test
    void compact_keeps_live_state_and_archives_the_rest() throws IOException {
        Path file = tmp.resolve("big.bin");
        String leader = "engine-1";
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), leader), leader);
            backend.append(graphOf("P"), leader);
            for (int gen = 0; gen < 10; gen++) {   // P re-initialised ten times
                var init = (LogInitialized) backend.append(
                        new LogInitialized(0, now(), "P", Map.of("gen", new byte[]{(byte) gen})), leader).orElseThrow();
                backend.append(new LogLoaded(0, now(), init.sid()), leader);
                if (gen < 9) {
                    backend.append(new LogDead(0, now(), init.sid()), leader);
                }
            }
        }

        Run r = run("compact", file.toString());
        assertThat(r.exit()).isZero();
        assertThat(r.out()).contains("31 -> 4 event(s)").contains("archived to");

        try (var backend = new FileLogBackend(file)) {
            assertThat(backend.length()).isEqualTo(4);   // leader, snapshot marker, graph, live state
            assertThat(backend.get(1)).isInstanceOf(LogChangeGraph.class); // leader, graph, live state, snapshot marker
            assertThat(((LogInitialized) backend.get(2)).properties().get("gen")).containsExactly(9);
            assertThat(backend.introspect().currentLeader()).isEqualTo(leader);
        }
        try (var files = Files.list(tmp)) {
            assertThat(files.filter(p -> p.getFileName().toString().startsWith("big.bin.archived."))).hasSize(1);
        }
    }

    /**
     * {@code inspect} and {@code compact} on a log several times larger than the heap: the scan
     * decodes one event at a time and keeps only the latest record per process, so a small heap
     * holding the live state suffices. Forked JVM with {@code -Xmx48m}; the log holds 128 MB of
     * superseded 1 MB states, which materialising the log would need in the heap at once.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void inspect_and_compact_a_log_larger_than_the_heap() throws Exception {
        Path file = tmp.resolve("huge.bin");
        String leader = "engine-1";
        int generations = 128;
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), leader), leader);
            backend.append(graphOf("P"), leader);
            byte[] blob = new byte[1 << 20];
            for (int gen = 0; gen < generations; gen++) {
                blob[0] = (byte) gen;
                backend.append(new LogInitialized(0, now(), "P", Map.of("state", blob)), leader);
            }
        }
        assertThat(Files.size(file)).isGreaterThan(128L << 20);

        Run inspect = runForked("-Xmx48m", "inspect", file.toString());
        assertThat(inspect.exit()).as(inspect.err()).isZero();
        assertThat(inspect.out()).contains("Length: " + (generations + 2) + " event(s)");

        Run compact = runForked("-Xmx48m", "compact", file.toString());
        assertThat(compact.exit()).as(compact.err()).isZero();
        assertThat(compact.out()).contains((generations + 2) + " -> 4 event(s)");
        try (var backend = new FileLogBackend(file)) {
            assertThat(((LogInitialized) backend.get(2)).properties().get("state")[0])
                    .isEqualTo((byte) (generations - 1));
        }
    }

    /** Runs the CLI in a fresh JVM with {@code heapFlag}, on this test's class path. */
    private Run runForked(String heapFlag, String... args) throws Exception {
        String javaBin = ProcessHandle.current().info().command().orElse(
                Path.of(System.getProperty("java.home"), "bin", "java").toString());
        String classPath = System.getProperty("java.class.path", "");
        String modulePath = System.getProperty("jdk.module.path");
        if (modulePath != null && !modulePath.isEmpty()) {
            classPath = classPath.isEmpty() ? modulePath : classPath + java.io.File.pathSeparator + modulePath;
        }
        var command = new ArrayList<>(List.of(javaBin, heapFlag, "-Djava.io.tmpdir=" + tmp,
                "-cp", classPath, FomLogCli.class.getName()));
        command.addAll(List.of(args));
        Path out = tmp.resolve("fork.out");
        Path err = tmp.resolve("fork.err");
        Process process = new ProcessBuilder(command)
                .redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        if (!process.waitFor(100, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("forked fom-log " + String.join(" ", args) + " timed out");
        }
        return new Run(process.exitValue(), Files.readString(out), Files.readString(err));
    }

    @Test
    void compact_refuses_a_corrupt_log_without_touching_it() throws IOException {
        Path file = tmp.resolve("bad.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), "i"), "i");
            backend.append(graphOf("P"), "i");
        }
        Files.write(file, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, java.nio.file.StandardOpenOption.APPEND);
        byte[] before = Files.readAllBytes(file);

        Run r = run("compact", file.toString());
        assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CORRUPT);
        assertThat(r.err()).contains("Refusing to compact");
        assertThat(Files.readAllBytes(file)).isEqualTo(before);
    }

    @Test
    void compact_of_a_read_only_directory_reports_the_underlying_cause() throws IOException {
        Path dir = Files.createDirectory(tmp.resolve("locked-down"));
        Path file = dir.resolve("orders.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), "i"), "i");
            backend.append(graphOf("P"), "i");
        }
        byte[] before = Files.readAllBytes(file);

        var writable = java.nio.file.attribute.PosixFilePermissions.fromString("r-xr-xr-x");
        try {
            Files.setPosixFilePermissions(dir, writable);
        } catch (UnsupportedOperationException | IOException e) {
            return; // not a POSIX filesystem; nothing to assert here
        }
        try {
            // Running as root ignores the directory's write bit, so the compact would succeed.
            org.junit.jupiter.api.Assumptions.assumeFalse(
                    Files.isWritable(dir), "a read-only directory does not deny writes for this user (root?)");

            Run r = run("compact", file.toString());
            assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
            // The operator must see what actually failed and on which file, not just the log path.
            assertThat(r.err())
                    .contains("Cannot compact " + file)
                    .contains("AccessDeniedException")
                    .contains(file + ".tmp");
            // No path of the private working copy may appear.
            String rest = r.err().replace(file.toString(), "<the operator's file>");
            assertThat(rest)
                    .doesNotContain(FomLogCli.TEMP_PREFIX)
                    .doesNotContain(FomLogCli.COPY_NAME)
                    .doesNotContain(System.getProperty("java.io.tmpdir"));
            assertThat(Files.readAllBytes(file)).isEqualTo(before);
        } finally {
            Files.setPosixFilePermissions(dir, java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    @Test
    void compact_refuses_a_log_held_open_and_a_missing_path() throws IOException {
        Path file = tmp.resolve("held.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), "i"), "i");
            backend.append(graphOf("P"), "i");

            Run r = run("compact", file.toString());
            assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
            assertThat(r.err()).contains("Cannot compact");
            assertThat(backend.length()).isEqualTo(2);
        }
        assertThat(run("compact", tmp.resolve("nope.bin").toString()).exit()).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
    }

    @Test
    void read_commands_say_a_zero_byte_file_is_empty_rather_than_a_log_of_zero_events() throws IOException {
        Path file = Files.createFile(tmp.resolve("zero-read.bin"));
        Run diagnose = run("diagnose", file.toString());
        assertThat(diagnose.exit()).isZero();
        assertThat(diagnose.out()).contains("Diagnose OK: " + FomLogCli.EMPTY_FILE)
                .doesNotContain("all 0 events");
        Run inspect = run("inspect", file.toString());
        assertThat(inspect.exit()).isZero();
        assertThat(inspect.out()).contains("Note: " + FomLogCli.EMPTY_FILE).contains("Length: 0 event(s)");
        Run events = run("events", file.toString());
        assertThat(events.exit()).isZero();
        assertThat(events.err()).contains("No events: " + FomLogCli.EMPTY_FILE);
        assertThat(Files.size(file)).as("read commands work on a copy").isZero();
        assertThat(FomLogCli.EMPTY_FILE).contains("0 bytes, no header yet").contains("new, empty log");
    }

    @Test
    void compact_refuses_an_empty_file_without_modifying_it_or_creating_a_lock() throws IOException {
        Path file = Files.createFile(tmp.resolve("zero.bin"));
        Run r = run("compact", file.toString());
        assertThat(r.exit()).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
        assertThat(r.err()).contains("the file is empty (0 bytes); nothing to compact; it was not modified")
                .doesNotContain("not a fom log");
        assertThat(Files.size(file)).isZero();
        try (var files = Files.list(tmp)) {
            assertThat(files.map(p -> p.getFileName().toString())).containsExactly("zero.bin");
        }
    }

    @Test
    void a_temp_copy_that_cannot_be_made_is_reported_as_such_not_as_a_fault_of_the_log() throws IOException {
        Path file = tmp.resolve("fine.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), "i"), "i");
        }
        Path noRoom = Files.createDirectory(tmp.resolve("no-room"));
        Files.setPosixFilePermissions(noRoom, java.nio.file.attribute.PosixFilePermissions.fromString("r-xr-xr-x"));
        String oldTmp = System.getProperty("java.io.tmpdir");
        try {
            org.junit.jupiter.api.Assumptions.assumeFalse(Files.isWritable(noRoom), "running as root");
            System.setProperty("java.io.tmpdir", noRoom.toString());
            for (String command : List.of("inspect", "events", "diagnose", "compact")) {
                Run r = run(command, file.toString());
                assertThat(r.exit()).as(command).isEqualTo(FomLogCli.EXIT_CANNOT_OPEN);
                assertThat(r.err()).as(command)
                        .contains("could not make the temporary working copy of the log in " + noRoom)
                        .contains("AccessDeniedException")
                        .contains("the log itself was not modified")
                        .contains("the directory is not writable: point the JVM's java.io.tmpdir elsewhere")
                        .doesNotContain("bytes free")
                        .doesNotContain("free space there")
                        .doesNotContain(FomLogCli.TEMP_PREFIX);
            }
        } finally {
            System.setProperty("java.io.tmpdir", oldTmp);
            Files.setPosixFilePermissions(noRoom, java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    private Path oneEventLog(String name) throws IOException {
        Path file = tmp.resolve(name);
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), "i"), "i");
        }
        return file;
    }

    @Test
    void a_failed_write_of_the_temp_copy_blames_the_temp_directory() throws IOException {
        Path file = oneEventLog("readable.bin");
        Path root = Files.createDirectory(tmp.resolve("full-disk"));
        var e = org.junit.jupiter.api.Assertions.assertThrows(FomLogCli.LogOpenException.class, () ->
                FomLogCli.LogCopy.open(file, root, Files::newInputStream, copy -> new java.io.OutputStream() {
                    @Override public void write(int b) throws IOException {
                        throw new IOException("No space left on device");
                    }
                }));
        assertThat(FomLogCli.describe(e))
                .contains("could not make the temporary working copy of the log in " + root)
                .contains("No space left on device")
                .contains(Files.size(file) + " bytes free")
                .contains("free space there")
                .doesNotContain("not writable")
                .doesNotContain(FomLogCli.TEMP_PREFIX);
        try (var left = Files.list(root)) {
            assertThat(left).as("the private temp directory is removed").isEmpty();
        }
    }

    @Test
    void a_read_only_temp_directory_gets_the_not_writable_hint_not_the_free_space_one() throws IOException {
        Path file = oneEventLog("ro.bin");
        Path root = Files.createDirectory(tmp.resolve("read-only"));
        var e = org.junit.jupiter.api.Assertions.assertThrows(FomLogCli.LogOpenException.class, () ->
                FomLogCli.LogCopy.open(file, root, Files::newInputStream, copy -> {
                    throw new java.nio.file.FileSystemException(copy.toString(), null, "Read-only file system");
                }));
        assertThat(FomLogCli.describe(e))
                .contains("could not make the temporary working copy of the log in " + root)
                .contains("FileSystemException: Read-only file system")
                .contains("the log itself was not modified")
                .contains("the directory is not writable: point the JVM's java.io.tmpdir elsewhere")
                .doesNotContain("bytes free")
                .doesNotContain("free space there")
                .doesNotContain(FomLogCli.TEMP_PREFIX);
        try (var left = Files.list(root)) {
            assertThat(left).as("the private temp directory is removed").isEmpty();
        }
    }

    @Test
    void a_failed_read_of_the_log_is_not_blamed_on_the_temp_directory() throws IOException {
        Path file = oneEventLog("eio.bin");
        Path root = Files.createDirectory(tmp.resolve("roomy"));
        var e = org.junit.jupiter.api.Assertions.assertThrows(FomLogCli.LogOpenException.class, () ->
                FomLogCli.LogCopy.open(file, root, f -> new java.io.InputStream() {
                    @Override public int read() throws IOException {
                        throw new IOException("Input/output error");
                    }
                }, copy -> Files.newOutputStream(copy)));
        assertThat(FomLogCli.describe(e))
                .contains("Input/output error")
                .doesNotContain("temporary working copy")
                .doesNotContain(FomLogCli.TEMP_PREFIX);
        try (var left = Files.list(root)) {
            assertThat(left).isEmpty();
        }
    }

    private Run run(String... args) {
        var out = new ByteArrayOutputStream();
        var err = new ByteArrayOutputStream();
        PrintStream oldOut = System.out;
        PrintStream oldErr = System.err;
        System.setOut(new PrintStream(out));
        System.setErr(new PrintStream(err));
        int exit;
        try {
            exit = new CommandLine(new FomLogCli()).execute(args);
        } finally {
            System.setOut(oldOut);
            System.setErr(oldErr);
        }
        return new Run(exit, out.toString(), err.toString());
    }
}
