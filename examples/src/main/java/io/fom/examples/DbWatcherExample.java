package io.fom.examples;

import io.fom.Codecs;
import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.Properties;
import io.fom.ScheduledWatcher;
import io.fom.TypedKey;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Re-initialise a process when a value changes in a database.
 *
 * <p>A {@link ScheduledWatcher} polls {@code observations_version.version}; when
 * it grows, the watcher triggers {@code Observations}, whose init re-reads the
 * row. The live state follows the database without a JVM restart.</p>
 *
 * <pre>
 *   external writer ──UPDATE──▶ observations_version
 *                                     ▲ poll (SELECT)
 *                              ScheduledWatcher ──trigger──▶ Observations re-init
 * </pre>
 *
 * <p>Needs Postgres: pass a JDBC URL (+ optional user/password) as arguments or
 * via {@code FOM_PG_URL} / {@code FOM_PG_USER} / {@code FOM_PG_PASSWORD}.
 * Without a URL it prints instructions and exits.</p>
 *
 * <pre>
 *   docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=test --name fom-pg postgres:16-alpine
 *   ./gradlew :examples:dbWatcher \
 *     -PexampleArgs="jdbc:postgresql://localhost:5432/postgres postgres test"
 * </pre>
 */
public final class DbWatcherExample {

    static final AtomicInteger INITS = new AtomicInteger();

    private static final TypedKey<Long> VERSION = new TypedKey<>("version", Codecs.longCodec());

    /** Read by the process's init. A real app would capture it in the factory lambda instead. */
    static volatile DataSource dataSource;

    public static void main(String[] args) throws Exception {
        String url = arg(args, 0, System.getenv("FOM_PG_URL"));
        String user = arg(args, 1, System.getenv().getOrDefault("FOM_PG_USER", "postgres"));
        String password = arg(args, 2, System.getenv().getOrDefault("FOM_PG_PASSWORD", "test"));

        if (url == null || url.isBlank()) {
            System.out.println("""
                    No Postgres URL provided — this example needs a running database.

                    Start one:
                      docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=test \\
                        --name fom-pg postgres:16-alpine

                    Then run:
                      ./gradlew :examples:dbWatcher \\
                        -PexampleArgs="jdbc:postgresql://localhost:5432/postgres postgres test"
                    """);
            return;
        }

        var ds = new PGSimpleDataSource();
        ds.setUrl(url);
        ds.setUser(user);
        ds.setPassword(password);
        dataSource = ds;

        resetVersionTo(ds, 1);   // same starting point on every run

        var graph = new GraphBuilder()
                .add("Observations", ObservationsInit::new, ObservationsInit::new)
                .build();

        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {

            engine.newGraph(graph);
            printServed(servedVersion(engine));

            // check() runs on the engine's scheduler thread, so keep it a quick indexed read.
            try (var watch = engine.watch(new ScheduledWatcher<>(
                    "Observations",
                    Long.class,
                    readVersion(ds),             // don't fire on the first tick
                    Duration.ZERO,
                    Duration.ofMillis(200),
                    previous -> {
                        long now = readVersion(ds);
                        return now > previous ? Optional.of(now) : Optional.empty();
                    },
                    null))) {

                for (long next = 2; next <= 3; next++) {
                    System.out.println("External writer sets version = " + next + " in the database...");
                    setVersion(ds, next);
                    printServed(awaitServedVersion(engine, next, Duration.ofSeconds(10)));
                }
            }
        } finally {
            dropTable(ds);
        }
    }

    private static void printServed(long version) {
        System.out.println("Observations serving version " + version + " (init calls = " + INITS.get() + ")");
    }

    private static long servedVersion(Engine engine) throws Exception {
        return (Long) engine.queryProcess("Observations", new GetVersion())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static long awaitServedVersion(Engine engine, long target, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        long served = servedVersion(engine);
        while (served != target && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            served = servedVersion(engine);
        }
        return served;
    }

    record GetVersion() implements Serializable { }

    /** init reads the current version from the database; load serves it. */
    static final class ObservationsInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INITS.incrementAndGet();
            // Blocking JDBC is fine here: init runs on a worker virtual thread.
            long version = readVersion(dataSource);
            return CompletableFuture.completedFuture(Properties.empty().put(VERSION, version).asRaw());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            long version = Properties.of(properties).get(VERSION);
            Process live = (c, query) -> CompletableFuture.completedFuture(version);
            return CompletableFuture.completedFuture(live);
        }
    }

    private static void resetVersionTo(DataSource ds, long version) throws SQLException {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS observations_version "
                    + "(id INT PRIMARY KEY, version BIGINT NOT NULL)");
            st.execute("INSERT INTO observations_version (id, version) VALUES (1, " + version + ") "
                    + "ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version");
        }
    }

    private static long readVersion(DataSource ds) {
        try (Connection c = ds.getConnection();
             var st = c.prepareStatement("SELECT version FROM observations_version WHERE id = 1");
             var rs = st.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new RuntimeException("Could not read observations_version", e);
        }
    }

    private static void setVersion(DataSource ds, long version) throws SQLException {
        try (Connection c = ds.getConnection();
             var st = c.prepareStatement("UPDATE observations_version SET version = ? WHERE id = 1")) {
            st.setLong(1, version);
            st.executeUpdate();
        }
    }

    private static void dropTable(DataSource ds) {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS observations_version");
        } catch (SQLException ignored) {
            // best-effort cleanup
        }
    }

    private static String arg(String[] args, int i, String fallback) {
        return i < args.length ? args[i] : fallback;
    }
}
