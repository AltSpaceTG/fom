package io.fom.log.cli;

import io.fom.LogCompaction;
import io.fom.SnapshotResult;
import io.fom.Sid;
import io.fom.log.FileLogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogCleanedUp;
import io.fom.log.LogCorruptedException;
import io.fom.log.LogDead;
import io.fom.log.LogDependencyChanged;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogLoaded;
import io.fom.log.LogPaused;
import io.fom.log.LogResumed;
import io.fom.log.LogSnapshot;
import io.fom.log.LogTrigger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serial;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@code fom-log} CLI: {@code inspect}, {@code events}, {@code diagnose} and {@code compact}.
 *
 * <p>Opening a {@link FileLogBackend} takes the leader lock and cuts off a torn last frame,
 * so the read commands open a temporary copy instead and never touch the user's file.
 * {@code compact} rewrites the file, but only after the same check on a copy passes.</p>
 */
@Command(name = "fom-log",
        mixinStandardHelpOptions = true,
        version = FomLogCli.VERSION,
        subcommands = {
                FomLogCli.Inspect.class,
                FomLogCli.Events.class,
                FomLogCli.Diagnose.class,
                FomLogCli.Compact.class,
                CommandLine.HelpCommand.class
        },
        description = "Inspect, diagnose and compact fom log files.")
public final class FomLogCli implements Runnable {

    static final String VERSION = "fom-log 0.1.0";

    /** Exit code: the file is missing, unreadable or not a fom log. */
    static final int EXIT_CANNOT_OPEN = 2;
    /** Exit code: the log is corrupt or ends in an incomplete frame. */
    static final int EXIT_CORRUPT = 1;

    private static final String ENGINE_REFUSES =
            ". An engine refuses to open this log; run 'fom-log diagnose' for details.";

    public static void main(String[] args) {
        int exit = new CommandLine(new FomLogCli()).execute(args);
        System.exit(exit);
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    /**
     * A read-only view of a log, opened from a copy in a private temp directory.
     * {@code backend} is {@code null} when the copy was refused as corrupt.
     */
    static final class LogCopy implements AutoCloseable {
        final Path original;
        final Path tempDir;
        final Path copy;
        final long originalSize;
        final long sizeAfterOpen;
        final FileLogBackend backend;
        final LogCorruptedException corruption;

        /** Opened by {@link #readablePrefix()}; closed with this view. */
        private FileLogBackend prefix;

        private LogCopy(Path original, Path tempDir, Path copy, long originalSize, long sizeAfterOpen,
                        FileLogBackend backend, LogCorruptedException corruption) {
            this.original = original;
            this.tempDir = tempDir;
            this.copy = copy;
            this.originalSize = originalSize;
            this.sizeAfterOpen = sizeAfterOpen;
            this.backend = backend;
            this.corruption = corruption;
        }

        static LogCopy open(Path original) throws IOException {
            return open(original, Path.of(System.getProperty("java.io.tmpdir")));
        }

        static LogCopy open(Path original, Path tempRoot) throws IOException {
            return open(original, tempRoot, Files::newInputStream,
                    copy -> Files.newOutputStream(copy, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        }

        /** Opens one side of the copy; lets tests inject I/O failures. */
        @FunctionalInterface
        interface StreamOpener<T> {
            T open(Path file) throws IOException;
        }

        static LogCopy open(Path original, Path tempRoot, StreamOpener<InputStream> source,
                            StreamOpener<OutputStream> target) throws IOException {
            Path tempDir;
            try {
                tempDir = Files.createTempDirectory(tempRoot, TEMP_PREFIX);
            } catch (IOException | RuntimeException e) {
                throw tempCopyFailed(original, tempRoot, e);
            }
            try {
                Path copy = tempDir.resolve(COPY_NAME);
                try {
                    copyLog(original, copy, source, target);
                } catch (CopyWriteFailed e) {
                    // Blame the temp directory, not the log (a read failure falls through below).
                    IOException cause = e.cause;
                    try {
                        deleteRecursively(tempDir);
                    } catch (IOException | RuntimeException suppressed) {
                        cause.addSuppressed(suppressed);
                    }
                    throw tempCopyFailed(original, tempRoot, cause);
                }
                long before = Files.size(copy);
                try {
                    var backend = new FileLogBackend(copy);
                    return new LogCopy(original, tempDir, copy, before, Files.size(copy), backend, null);
                } catch (LogCorruptedException e) {
                    return new LogCopy(original, tempDir, copy, before, before, null, e);
                }
            } catch (LogOpenException e) {
                throw e;
            } catch (IOException | RuntimeException e) {
                // No LogCopy exists yet to sanitise later, so the copy's path is removed here.
                try {
                    deleteRecursively(tempDir);
                } catch (IOException | RuntimeException suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw new LogOpenException(sanitise(causeChain(e), tempDir, original), e);
            }
        }

        /** Carries a failure on the copy's (write) side out of {@link #copyLog}. */
        private static final class CopyWriteFailed extends Exception {
            @Serial private static final long serialVersionUID = 1L;
            final transient IOException cause;

            CopyWriteFailed(IOException cause) {
                super(cause);
                this.cause = cause;
            }
        }

        /**
         * Copies the log. A read failure propagates as a plain {@link IOException} (the log's
         * fault); a failure to create, write or close the copy becomes {@link CopyWriteFailed}.
         */
        private static void copyLog(Path original, Path copy, StreamOpener<InputStream> source,
                                    StreamOpener<OutputStream> target) throws IOException, CopyWriteFailed {
            try (InputStream in = source.open(original)) {
                OutputStream out;
                try {
                    out = target.open(copy);
                } catch (IOException e) {
                    throw new CopyWriteFailed(e);
                }
                boolean copied = false;
                try {
                    byte[] buf = new byte[64 * 1024];
                    for (int n; (n = in.read(buf)) >= 0; ) {
                        try {
                            out.write(buf, 0, n);
                        } catch (IOException e) {
                            throw new CopyWriteFailed(e);
                        }
                    }
                    copied = true;
                } finally {
                    try {
                        out.close();
                    } catch (IOException e) {
                        if (copied) throw new CopyWriteFailed(e);
                    }
                }
            }
        }

        /**
         * The working copy could not be made. Names the temp root, never the private
         * directory inside it, and says how much space the copy needs when the disk is full.
         */
        private static LogOpenException tempCopyFailed(Path original, Path tempRoot, Exception e) {
            // Drop the file name: it is the private copy, not the operator's log.
            String reason = e instanceof FileSystemException fse
                    ? fse.getClass().getSimpleName() + (fse.getReason() != null ? ": " + fse.getReason() : "")
                    : e.getClass().getSimpleName() + ": " + e.getMessage();
            reason = TEMP_PATH_TOKEN.matcher(reason).replaceAll("<temporary copy>");
            String advice;
            if (isOutOfSpace(e)) {
                String size;
                try {
                    size = Files.size(original) + " bytes";
                } catch (IOException | RuntimeException ignored) {
                    size = "the log's size";
                }
                advice = "Read commands copy the whole log there first and need " + size + " free in that "
                        + "directory: free space there, or point the JVM's java.io.tmpdir elsewhere";
            } else {
                advice = "Read commands copy the whole log there first, and the directory is not writable: "
                        + "point the JVM's java.io.tmpdir elsewhere";
            }
            return new LogOpenException("could not make the temporary working copy of the log in " + tempRoot
                    + " (" + reason + "); the log itself was not modified. " + advice
                    + " (e.g. JAVA_OPTS=-Djava.io.tmpdir=/path)", e);
        }

        /** Whether {@code e} or a cause reports a full disk or quota; a copy of fom-core's package-private check. */
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

        /**
         * The events before the damage: {@link #backend} if the log opened, otherwise the copy
         * cut at the corrupt frame's offset, as the engine's recovery instruction would leave it.
         * {@code null} if even that cannot be opened.
         */
        FileLogBackend readablePrefix() {
            if (backend != null) return backend;
            if (prefix != null) return prefix;
            if (corruption == null) return null;
            try (var channel = FileChannel.open(copy, StandardOpenOption.WRITE)) {
                channel.truncate(corruption.offset());
            } catch (IOException | RuntimeException e) {
                return null;
            }
            try {
                prefix = new FileLogBackend(copy);
                return prefix;
            } catch (IOException | RuntimeException e) {
                return null;
            }
        }

        /** True when the log has a corrupt or partial frame. */
        boolean damaged() {
            // An empty file gains a magic header on open; that is growth, not corruption.
            return corruption != null || sizeAfterOpen < originalSize;
        }

        String corruptionSummary() {
            long offset = corruption != null ? corruption.offset() : sizeAfterOpen;
            int readable = corruption != null ? corruption.readableEvents() : backend.length();
            return "log is corrupt at byte offset " + offset + " of " + originalSize
                    + ": " + readable + " event(s) readable before it, "
                    + (originalSize - offset) + " byte(s) from that offset on are unreadable"
                    + " (the original file " + original + " was not modified)";
        }

        /** {@code message} with the copy's path replaced by the operator's file; see {@link #sanitise}. */
        String forOperator(Object message) {
            return sanitise(String.valueOf(message), tempDir, original);
        }

        @Override
        public void close() throws IOException {
            try {
                if (backend != null) backend.close();
            } finally {
                try {
                    if (prefix != null) prefix.close();
                } finally {
                    deleteRecursively(tempDir);
                }
            }
        }

        private static void deleteRecursively(Path dir) throws IOException {
            if (!Files.exists(dir)) return;
            try (Stream<Path> walk = Files.walk(dir)) {
                for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(p);
                }
            }
        }
    }

    /** Prefix of the private temporary directory that holds the working copy of a log. */
    static final String TEMP_PREFIX = "fom-log-cli";

    /** File name of the working copy inside that directory. */
    static final String COPY_NAME = "log.bin";

    /** Any path-like token naming the temporary directory. */
    private static final Pattern TEMP_PATH_TOKEN =
            Pattern.compile("[^\\s\"']*" + Pattern.quote(TEMP_PREFIX) + "[^\\s\"']*");

    /**
     * The working copy could not be opened. The message is ready for the operator and names
     * their file, never the temporary copy; print it with {@link #describe(Throwable)}.
     */
    static final class LogOpenException extends IOException {
        @Serial private static final long serialVersionUID = 1L;

        LogOpenException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Replaces the temporary copy's path in {@code text} with the operator's file, so messages
     * built against the copy (such as a {@link LogCorruptedException}'s recovery instruction)
     * name their file. Offsets still hold: the copy is byte-for-byte identical.
     */
    static String sanitise(String text, Path tempDir, Path original) {
        Path copy = tempDir.resolve(COPY_NAME);
        String user = original.toString();
        String result = text
                .replace(copy.toAbsolutePath().toString(), user)
                .replace(copy.toString(), user)
                .replace(tempDir.toAbsolutePath().toString(), user)
                .replace(tempDir.toString(), user);
        return sanitise(result, original);
    }

    /**
     * Replaces any remaining path carrying {@link #TEMP_PREFIX} (a symlinked temp directory,
     * {@code log.bin.lock}) with the operator's file. Also used where there is no copy, as in
     * {@code compact}.
     */
    static String sanitise(String text, Path original) {
        return TEMP_PATH_TOKEN.matcher(text).replaceAll(Matcher.quoteReplacement(original.toString()));
    }

    /**
     * A throwable as printed for the operator: the whole cause chain, so a wrapper never hides
     * the real failure. A {@link LogOpenException} is its message alone, since its cause names
     * the temporary copy.
     */
    static String describe(Throwable t) {
        return t instanceof LogOpenException ? String.valueOf(t.getMessage()) : causeChain(t);
    }

    /** {@code t} and its causes on one line, unsanitised. */
    static String causeChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable c = t; c != null && seen.add(c); c = c.getCause()) {
            if (!sb.isEmpty()) {
                sb.append("; caused by: ");
            }
            sb.append(c);
            if (c instanceof LogOpenException) break; // its cause names the temporary copy
        }
        return sb.toString();
    }

    /** How every command describes a 0-byte file. */
    static final String EMPTY_FILE = "the file is empty (0 bytes, no header yet); "
            + "an engine would treat it as a new, empty log";

    /** Validates the path; prints an error and returns false when it cannot be a log file. */
    static boolean checkReadableFile(Path path) {
        if (Files.notExists(path)) {
            System.err.println("No such file: " + path);
            return false;
        }
        if (!Files.exists(path)) {
            // Neither present nor provably absent: usually an unsearchable parent directory.
            System.err.println("Cannot access " + path + " (permission denied?)");
            return false;
        }
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            System.err.println("Not a readable regular file: " + path);
            return false;
        }
        return true;
    }

    private static int failedToOpen(Path path, Throwable t) {
        System.err.println("Failed to open " + path + ": " + describe(t));
        return EXIT_CANNOT_OPEN;
    }

    @Command(name = "inspect", mixinStandardHelpOptions = true, version = VERSION,
            description = "Show summary + event counts by type.")
    static final class Inspect implements Callable<Integer> {

        @Parameters(index = "0", description = "Path to the .bin log file.")
        Path path;

        @Override
        public Integer call() {
            if (!checkReadableFile(path)) {
                return EXIT_CANNOT_OPEN;
            }
            try (var view = LogCopy.open(path)) {
                if (view.backend == null) {
                    System.err.println("WARNING: " + view.corruptionSummary() + ENGINE_REFUSES);
                    return EXIT_CORRUPT;
                }
                if (view.damaged()) {
                    System.err.println("WARNING: " + view.corruptionSummary()
                            + ". Run 'fom-log diagnose' for details; the summary below covers only the readable prefix.");
                }
                LogBackendReport report = view.backend.introspect();
                System.out.println("Log: " + path);
                if (view.originalSize == 0) {
                    System.out.println("Note: " + EMPTY_FILE + ".");
                }
                System.out.println("Length: " + report.length() + " event(s)");
                System.out.println("Current leader: "
                        + (report.currentLeader() != null ? report.currentLeader() : "-"));
                int len = view.backend.length();
                if (len == 0) {
                    System.out.println("First clock: -");
                    System.out.println("Last clock: -");
                } else {
                    long first = view.backend.get(0).clock();
                    long last = view.backend.get(len - 1).clock();
                    System.out.println("First clock: " + first + " (position 0)");
                    System.out.println("Last clock: " + last + " (position " + (len - 1) + ")");
                    long missing = last - first + 1 - len;
                    if (missing > 0) {
                        System.out.println("Clock gaps: " + missing
                                + " clock(s) between first and last are absent (compacted away)");
                    }
                }
                System.out.println("Highest event timestamp: " + report.maxTimestampMillis());
                System.out.println("Counts by type:");
                for (Map.Entry<String, Integer> e : report.eventCounts().entrySet()) {
                    System.out.printf("  %-24s  %d%n", e.getKey(), e.getValue());
                }
                return view.damaged() ? EXIT_CORRUPT : 0;
            } catch (Throwable t) {
                return failedToOpen(path, t);
            }
        }
    }

    @Command(name = "diagnose", mixinStandardHelpOptions = true, version = VERSION,
            description = "Verify CRCs frame-by-frame; report any corruption.")
    static final class Diagnose implements Callable<Integer> {

        @Parameters(index = "0", description = "Path to the .bin log file.")
        Path path;

        @Override
        public Integer call() {
            if (!checkReadableFile(path)) {
                return EXIT_CANNOT_OPEN;
            }
            try (var view = LogCopy.open(path)) {
                System.out.println("Opened: " + path + " (" + view.originalSize + " bytes, read via a temporary copy)");
                if (view.backend == null) {
                    System.out.println("Diagnose FAIL: " + view.corruptionSummary() + ".");
                    System.out.println("Cause: " + view.forOperator(view.corruption.getMessage()));
                    System.out.println("This is damage inside the log, not a torn last frame: an engine"
                            + " refuses to open it and leaves the file untouched.");
                    return EXIT_CORRUPT;
                }
                var backend = view.backend;
                int len = backend.length();
                System.out.println("Readable events: " + len);
                int faulty = 0;
                String lastGoodClock = null;
                for (int i = 0; i < len; i++) { // a position: clocks differ after compaction
                    try {
                        LogEvent e = backend.get(i);
                        if (e == null) {
                            System.err.println("position=" + i + afterClock(lastGoodClock) + " failed: no event");
                            faulty++;
                        } else {
                            lastGoodClock = String.valueOf(e.clock());
                        }
                    } catch (Throwable t) {
                        System.err.println("position=" + i + afterClock(lastGoodClock)
                                + " failed: " + view.forOperator(t));
                        faulty++;
                    }
                }
                boolean truncated = view.damaged();
                if (truncated) {
                    System.out.println("Diagnose FAIL: " + view.corruptionSummary() + ".");
                    System.out.println("Cause: torn last frame — the final " + (view.originalSize - view.sizeAfterOpen)
                            + " byte(s) are an incomplete append, which is what a crash mid-append leaves"
                            + " behind. The next engine open cuts them off and saves them next to the log as "
                            + path.getFileName() + ".truncated.<millis>; all " + len
                            + " event(s) before it are intact and nothing else is damaged.");
                }
                if (faulty > 0) {
                    System.out.println("Diagnose FAIL: " + faulty + " corrupt events.");
                }
                if (truncated || faulty > 0) {
                    return EXIT_CORRUPT;
                }
                if (view.originalSize == 0) {
                    System.out.println("Diagnose OK: " + EMPTY_FILE + "; nothing to check.");
                } else {
                    System.out.println("Diagnose OK: all " + len + " events round-tripped.");
                }
                return 0;
            } catch (Throwable t) {
                return failedToOpen(path, t);
            }
        }

        /** The clock of the last readable event before a failed one; the failed one's is unknown. */
        private static String afterClock(String lastGoodClock) {
            return lastGoodClock == null ? "" : " (after event with clock=" + lastGoodClock + ")";
        }
    }

    @Command(name = "events", mixinStandardHelpOptions = true, version = VERSION,
            description = "List events one per line: position, clock, timestamp, type and key fields. "
                    + "Positions and clocks differ after a compaction.")
    static final class Events implements Callable<Integer> {

        static final int DEFAULT_LIMIT = 1000;

        private static final String ROW = "%-8d  %-10s  %-24s  %-20s  %s%n";

        @Parameters(index = "0", description = "Path to the .bin log file.")
        Path path;

        @Option(names = "--from", paramLabel = "POSITION",
                description = "First position to scan (default: 0).")
        int from = 0;

        @Option(names = "--limit", paramLabel = "N",
                description = "Maximum number of events to print (default: " + DEFAULT_LIMIT + ").")
        int limit = DEFAULT_LIMIT;

        @Option(names = "--type", paramLabel = "TYPE", split = ",",
                description = "Only events of these types, e.g. LogPaused,LogResumed (repeatable).")
        List<String> types = new ArrayList<>();

        @Option(names = "--process", paramLabel = "NAME",
                description = "Only events about this process: its init/load/dead/cleanup, pauses, "
                        + "triggers naming it, and dependency changes of it or of its dependency NAME.")
        String process;

        @Option(names = "--full",
                description = "Print every property key of LogInitialized events (default: the count and the first "
                        + PROPERTY_KEYS_SHOWN + " keys).")
        boolean full;

        /** Property keys of a LogInitialized shown before the rest are summarised. */
        static final int PROPERTY_KEYS_SHOWN = 10;

        @Override
        public Integer call() {
            if (!optionsValid()) {
                return EXIT_CANNOT_OPEN;
            }
            Set<String> wanted = new TreeSet<>(types);
            if (!checkReadableFile(path)) {
                return EXIT_CANNOT_OPEN;
            }
            try (var view = LogCopy.open(path)) {
                var backend = view.readablePrefix();
                if (backend == null) {
                    System.err.println("WARNING: " + view.corruptionSummary() + ENGINE_REFUSES);
                    return EXIT_CORRUPT;
                }
                if (view.backend == null) {
                    System.err.println("WARNING: " + view.corruptionSummary() + ENGINE_REFUSES
                            + " The listing below covers only the readable prefix.");
                } else if (view.damaged()) {
                    System.err.println("WARNING: " + view.corruptionSummary()
                            + ". Run 'fom-log diagnose' for details; the listing below covers only the readable prefix.");
                }
                int len = backend.length();
                int printed = 0;
                int unreadable = 0;
                System.out.printf("%-8s  %-10s  %-24s  %-20s  %s%n", "POSITION", "CLOCK", "TIMESTAMP", "TYPE", "DETAILS");
                for (int i = from; i < len; i++) {
                    LogEvent e;
                    try {
                        e = backend.get(i);
                    } catch (RuntimeException ex) {
                        if (wanted.isEmpty() && process == null) {
                            if (printed == limit) {
                                printTruncated(printed, i);
                                break;
                            }
                            System.out.printf(ROW, i, "?", "?", "?", "unreadable: " + view.forOperator(causeChain(ex)));
                            printed++;
                        }
                        unreadable++;
                        continue;
                    }
                    if (!wanted.isEmpty() && !wanted.contains(e.getClass().getSimpleName())) continue;
                    if (process != null && !concerns(e, process)) continue;
                    if (printed == limit) {
                        printTruncated(printed, i);
                        break;
                    }
                    System.out.printf(ROW, i, e.clock(), Instant.ofEpochMilli(e.timestamp()),
                            e.getClass().getSimpleName(), details(e, full));
                    printed++;
                }
                if (view.originalSize == 0) {
                    System.err.println("No events: " + EMPTY_FILE + ".");
                } else if (printed == 0) {
                    System.err.println("No matching events (log length " + len + ", scanned from position " + from + ").");
                }
                if (unreadable > 0) {
                    System.err.println("WARNING: " + unreadable + " event(s) could not be read; run 'fom-log diagnose'.");
                    return EXIT_CORRUPT;
                }
                return view.damaged() ? EXIT_CORRUPT : 0;
            } catch (Throwable t) {
                return failedToOpen(path, t);
            }
        }

        private boolean optionsValid() {
            if (from < 0) {
                System.err.println("--from must be >= 0, got " + from);
                return false;
            }
            if (limit <= 0) {
                System.err.println("--limit must be > 0, got " + limit);
                return false;
            }
            Set<String> known = Arrays.stream(LogEvent.class.getPermittedSubclasses())
                    .map(Class::getSimpleName)
                    .collect(Collectors.toCollection(TreeSet::new));
            for (String t : new TreeSet<>(types)) {
                if (!known.contains(t)) {
                    System.err.println("Unknown event type '" + t + "'. Known types: " + String.join(", ", known));
                    return false;
                }
            }
            return true;
        }

        private static void printTruncated(int printed, int nextPosition) {
            System.err.println("NOTE: output truncated after " + printed + " event(s); more match from position "
                    + nextPosition + " (continue with --from " + nextPosition + ", or raise --limit).");
        }

        static boolean concerns(LogEvent e, String name) {
            return switch (e) {
                case LogInitialized x -> x.processName().equals(name);
                case LogLoaded x -> x.sid().processName().equals(name);
                case LogDead x -> x.sid().processName().equals(name);
                case LogCleanedUp x -> x.sid().processName().equals(name);
                case LogPaused x -> x.processName().equals(name);
                case LogResumed x -> x.processName().equals(name);
                case LogTrigger x -> x.processNames().contains(name);
                case LogDependencyChanged x -> x.sid().processName().equals(name) || x.depName().equals(name);
                default -> false; // leader, graph, snapshot markers are not about one process
            };
        }

        /** @param full print every property key of a {@link LogInitialized}, not a bounded summary */
        static String details(LogEvent e, boolean full) {
            return switch (e) {
                case LogLeader x -> "instance=" + x.instanceId();
                case LogChangeGraph x -> x.nodes().size() + " node(s): "
                        + x.nodes().stream().map(LogChangeGraph.Node::name).collect(Collectors.joining(", "));
                case LogInitialized x -> "sid=" + sid(x.sid())
                        + (x.replaces() == null ? "" : " replaces=" + sid(x.replaces()))
                        + " properties=" + propertyKeys(x.properties().keySet(), full);
                case LogLoaded x -> "sid=" + sid(x.sid());
                case LogDead x -> "sid=" + sid(x.sid());
                case LogCleanedUp x -> "sid=" + sid(x.sid()) + " ok=" + x.ok();
                case LogTrigger x -> "processes=" + x.processNames();
                case LogDependencyChanged x -> "sid=" + sid(x.sid()) + " dep=" + x.depName()
                        + " depClock " + x.oldDepClock() + " -> " + x.newDepClock();
                case LogSnapshot x -> "checkpointClock=" + x.checkpointClock();
                case LogPaused x -> "process=" + x.processName() + " stale=" + x.stale()
                        + " forDependency=" + x.forDependency()
                        + (x.forDependency() ? " (paused because a dependency is paused)" : " (operator pause)");
                case LogResumed x -> "process=" + x.processName();
                default -> e.toString();
            };
        }

        /**
         * {@code [a, b]} for a few keys; for more, the count and the first {@link #PROPERTY_KEYS_SHOWN}
         * sorted keys, since a large process can have millions.
         */
        static String propertyKeys(Set<String> keys, boolean full) {
            if (full || keys.size() <= PROPERTY_KEYS_SHOWN) {
                return new TreeSet<>(keys).toString();
            }
            List<String> first = keys.stream().sorted().limit(PROPERTY_KEYS_SHOWN).toList();
            return keys.size() + " keys [" + String.join(", ", first) + ", ... +"
                    + (keys.size() - PROPERTY_KEYS_SHOWN) + " more]";
        }

        private static String sid(Sid sid) {
            return sid.processName() + "@" + sid.clock();
        }
    }

    @Command(name = "compact", mixinStandardHelpOptions = true, version = VERSION,
            description = "Compact a log offline: keep the current graph, live process state and pauses; "
                    + "archive the previous contents next to the file. Refuses a corrupt log "
                    + "or one that a running engine holds open.")
    static final class Compact implements Callable<Integer> {

        @Parameters(index = "0", description = "Path to the .bin log file.")
        Path path;

        @Override
        public Integer call() {
            if (!checkReadableFile(path)) {
                return EXIT_CANNOT_OPEN;
            }
            // Opening an empty file would write a header into it and create a .lock sibling.
            try {
                if (Files.size(path) == 0) {
                    System.err.println("Refusing to compact " + path + ": the file is empty (0 bytes); "
                            + "nothing to compact; it was not modified");
                    return EXIT_CANNOT_OPEN;
                }
            } catch (IOException e) {
                return failedToOpen(path, e);
            }
            // Opening the real file would cut off a torn last frame, so check a copy first.
            try (var view = LogCopy.open(path)) {
                if (view.damaged()) {
                    System.err.println("Refusing to compact: " + view.corruptionSummary()
                            + ". Run 'fom-log diagnose' first.");
                    return EXIT_CORRUPT;
                }
            } catch (Throwable t) {
                return failedToOpen(path, t);
            }
            try (var backend = new FileLogBackend(path)) {
                int before = backend.length();
                SnapshotResult result = LogCompaction.compact(backend);
                System.out.println("Compacted " + path + ": " + before + " -> " + backend.length() + " event(s)");
                System.out.println("Previous contents archived to " + result.archivedLogId());
                return 0;
            } catch (Throwable t) {
                System.err.println("Cannot compact " + path + ": " + sanitise(describe(t), path));
                return EXIT_CANNOT_OPEN;
            }
        }
    }
}
