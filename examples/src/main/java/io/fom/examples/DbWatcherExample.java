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
 * Re-initialise a process when a value changes <b>in a database</b>.
 *
 * <p>The external source of truth is a Postgres row
 * ({@code catalog_version.version}). A {@link ScheduledWatcher} polls it; when
 * the version grows, the watcher fires a trigger and the {@code Catalog}
 * process re-runs its {@code init}, which re-reads the row — so the live state
 * follows the database without restarting the JVM.</p>
 *
 * <pre>
 *   external writer ──UPDATE──▶ catalog_version
 *                                     ▲ poll (SELECT)
 *                              ScheduledWatcher ──trigger──▶ Catalog re-init ──▶ re-reads row
 * </pre>
 *
 * <p>Needs a running Postgres. Pass a JDBC URL (+ optional user/password) as
 * arguments or via {@code FOM_PG_URL} / {@code FOM_PG_USER} / {@code FOM_PG_PASSWORD};
 * with nothing provided it prints instructions and exits.</p>
 *
 * <pre>
 *   docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=test --name fom-pg postgres:16-alpine
 *   ./gradlew :examples:dbWatcher \
 *     -PexampleArgs="jdbc:postgresql://localhost:5432/postgres postgres test"
 * </pre>
 */
public final class DbWatcherExample {

    /** Counts Catalog re-initialisations so we can show the watcher caused one. */
    static final AtomicInteger CATALOG_INITS = new AtomicInteger();

    private static final TypedKey<Long> VERSION = new TypedKey<>("version", Codecs.longCodec());

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

        // The Catalog process reads its version from the DB, so the DataSource
        // must be visible to it. The example is single-JVM, so a static handoff
        // is enough; a real app would inject it (see fom-guice / fom-spring).
        Db.dataSource = ds;

        resetVersionTo(ds, 1);   // deterministic starting point for repeat runs

        var graph = new GraphBuilder()
                .add("Catalog", CatalogInit::new, CatalogInit::new)
                .build();

        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true)) {

            engine.newGraph(graph);
            System.out.println("Catalog serving version = " + currentServedVersion(engine)
                    + " (init calls = " + CATALOG_INITS.get() + ")");

            // Register the watcher. It polls the DB row every 200 ms and fires a
            // trigger when the version is higher than the last value it saw.
            //
            // NOTE: the check runs on the engine scheduler thread, so keep it a
            // quick, indexed read. For a heavy poll, do the expensive part
            // elsewhere and let check() only compare a cheap version/etag.
            try (AutoCloseable watcherHandle = engine.watch(new ScheduledWatcher<>(
                    "Catalog",
                    Long.class,
                    readVersion(ds),                 // initial value: don't fire on the first tick
                    Duration.ZERO,                   // initial delay
                    Duration.ofMillis(200),          // poll interval
                    previous -> {                    // check: emit the new version, or empty if unchanged
                        long now = readVersion(ds);
                        return now > previous ? Optional.of(now) : Optional.empty();
                    },
                    null))) {

                // ── Simulate an external writer bumping the DB row ──
                System.out.println("External writer sets version = 2 in the database...");
                setVersion(ds, 2);

                long served = awaitServedVersion(engine, 2, Duration.ofSeconds(10));
                System.out.println("Catalog re-initialised → now serving version = " + served
                        + " (init calls = " + CATALOG_INITS.get() + ")");

                // ── And once more, to make the point ──
                System.out.println("External writer sets version = 3 in the database...");
                setVersion(ds, 3);
                served = awaitServedVersion(engine, 3, Duration.ofSeconds(10));
                System.out.println("Catalog re-initialised → now serving version = " + served
                        + " (init calls = " + CATALOG_INITS.get() + ")");
            }
        } finally {
            dropTable(ds);
        }
    }

    private static long currentServedVersion(Engine engine) throws Exception {
        return (Long) engine.queryProcess("Catalog", new GetVersion())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    /** Poll the Catalog until it serves {@code target} (proves the cascade happened). */
    private static long awaitServedVersion(Engine engine, long target, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        long served = currentServedVersion(engine);
        while (served != target && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            served = currentServedVersion(engine);
        }
        return served;
    }

    // ───────────────── the process ─────────────────

    record GetVersion() implements Serializable { }

    /** init reads the current version from the DB; load serves it. */
    static final class CatalogInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            CATALOG_INITS.incrementAndGet();
            // Blocking JDBC here is fine: init runs on a worker virtual thread,
            // not the dispatcher.
            long version = readVersion(Db.dataSource);
            return CompletableFuture.completedFuture(
                    Properties.empty().put(VERSION, version).asRaw());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            long version = Properties.of(properties).get(VERSION);
            Process live = (c, query) -> CompletableFuture.completedFuture(version);
            return CompletableFuture.completedFuture(live);
        }
    }

    // ───────────────── tiny JDBC helpers ─────────────────

    /** Static handoff of the DataSource to the process (single-JVM example only). */
    static final class Db {
        static volatile DataSource dataSource;
        private Db() { }
    }

    private static void resetVersionTo(DataSource ds, long version) throws SQLException {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS catalog_version "
                    + "(id INT PRIMARY KEY, version BIGINT NOT NULL)");
            st.execute("INSERT INTO catalog_version (id, version) VALUES (1, " + version + ") "
                    + "ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version");
        }
    }

    private static long readVersion(DataSource ds) {
        try (Connection c = ds.getConnection();
             var st = c.prepareStatement("SELECT version FROM catalog_version WHERE id = 1");
             var rs = st.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new RuntimeException("failed to read catalog_version", e);
        }
    }

    private static void setVersion(DataSource ds, long version) throws SQLException {
        try (Connection c = ds.getConnection();
             var st = c.prepareStatement("UPDATE catalog_version SET version = ? WHERE id = 1")) {
            st.setLong(1, version);
            st.executeUpdate();
        }
    }

    private static void dropTable(DataSource ds) {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS catalog_version");
        } catch (SQLException ignored) {
            // best-effort cleanup
        }
    }

    private static String arg(String[] args, int i, String fallback) {
        return i < args.length ? args[i] : fallback;
    }
}
