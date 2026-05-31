package io.fom.log.cli;

import io.fom.log.LocalFileLogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogEvent;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Entry point for the {@code fom-log} CLI. Subcommands:
 * <ul>
 *   <li>{@code inspect &lt;path&gt;} — summary + counts by type + leader + last event timestamp</li>
 *   <li>{@code diagnose &lt;path&gt;} — walk the file, verifying CRCs; report truncation point if any</li>
 *   <li>{@code migrate &lt;from-version&gt; &lt;to-version&gt; &lt;in&gt; &lt;out&gt;} — stub until v2.0 exists</li>
 * </ul>
 */
@Command(name = "fom-log",
        mixinStandardHelpOptions = true,
        version = "fom-log 0.1.0",
        subcommands = {
                FomLogCli.Inspect.class,
                FomLogCli.Diagnose.class,
                FomLogCli.Migrate.class
        },
        description = "Inspect, diagnose and migrate fom log files.")
public final class FomLogCli implements Runnable {

    public static void main(String[] args) {
        int exit = new CommandLine(new FomLogCli()).execute(args);
        System.exit(exit);
    }

    @Override
    public void run() {
        // No-op: CommandLine prints usage when no subcommand is given.
        CommandLine.usage(this, System.out);
    }

    @Command(name = "inspect", description = "Show summary + event counts by type.")
    static final class Inspect implements Callable<Integer> {

        @Parameters(index = "0", description = "Path to the .bin log file.")
        Path path;

        @Override
        public Integer call() throws IOException {
            try (var backend = new LocalFileLogBackend(path)) {
                LogBackendReport report = backend.introspect();
                System.out.println("Log: " + report.logId());
                System.out.println("Length: " + report.length() + " event(s)");
                System.out.println("Current leader: " + report.currentLeader());
                System.out.println("Last event timestamp: " + report.lastTimestampMillis());
                System.out.println("Counts by type:");
                for (Map.Entry<String, Integer> e : report.eventCountsByType().entrySet()) {
                    System.out.printf("  %-24s  %d%n", e.getKey(), e.getValue());
                }
            }
            return 0;
        }
    }

    @Command(name = "diagnose", description = "Verify CRCs frame-by-frame; report any truncation.")
    static final class Diagnose implements Callable<Integer> {

        @Parameters(index = "0", description = "Path to the .bin log file.")
        Path path;

        @Override
        public Integer call() {
            try (var backend = new LocalFileLogBackend(path)) {
                int len = backend.length();
                System.out.println("Opened: " + backend.logId());
                System.out.println("Length after open (any corrupt tail was truncated): " + len);
                int faulty = 0;
                for (int i = 0; i < len; i++) {
                    try {
                        LogEvent e = backend.get(i);
                        if (e == null) faulty++;
                    } catch (Throwable t) {
                        System.err.println("clock=" + i + " failed: " + t);
                        faulty++;
                    }
                }
                if (faulty == 0) {
                    System.out.println("Diagnose OK: all " + len + " events round-tripped.");
                    return 0;
                }
                System.out.println("Diagnose FAIL: " + faulty + " corrupt events.");
                return 1;
            } catch (Throwable t) {
                System.err.println("Failed to open " + path + ": " + t);
                return 2;
            }
        }
    }

    @Command(name = "migrate",
            description = "Cross-major-version migration. Stub until v2.0 exists.")
    static final class Migrate implements Callable<Integer> {

        @CommandLine.Option(names = "--from", required = true, description = "Source format major (e.g. 1.x).")
        String from;

        @CommandLine.Option(names = "--to", required = true, description = "Target format major (e.g. 2.0).")
        String to;

        @Parameters(index = "0", description = "Input log file.")
        Path input;

        @Parameters(index = "1", description = "Output log file.")
        Path output;

        @Override
        public Integer call() {
            System.err.println("migrate: " + from + " → " + to + " is not implemented yet.");
            System.err.println("The current format is the only released major; no migration target exists.");
            return 64;
        }
    }
}
