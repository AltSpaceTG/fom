package io.fom.jdbc;

import io.fom.api.LeadershipLostException;
import io.fom.log.LogBackend;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The write fence and the append guard must hold whatever isolation level the
 * {@link DataSource} hands connections out with. Under {@code REPEATABLE READ} or
 * {@code SERIALIZABLE} the first query of a transaction fixes its snapshot; if that query ran
 * before the fence, a {@code LogLeader} another node committed while the write waited on the
 * fence would stay invisible to the guard and the deposed leader would keep appending. The
 * backend forces {@code READ COMMITTED} on its own transactions and takes the fence first; these
 * tests re-run the round-26 races with the pool default, the role default and the database
 * default set to the stricter levels.
 */
@Testcontainers
class PostgresIsolationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("fom")
                    .withUsername("test")
                    .withPassword("test");

    /** Where the stricter isolation level comes from. */
    enum Setting {
        POOL_REPEATABLE_READ, POOL_SERIALIZABLE, ROLE_REPEATABLE_READ, ROLE_SERIALIZABLE, DATABASE_SERIALIZABLE
    }

    @BeforeAll
    static void setUpRolesAndDatabase() throws SQLException {
        try (Connection c = plain("fom", postgres.getUsername(), postgres.getPassword());
             var st = c.createStatement()) {
            st.execute("CREATE ROLE rr_user LOGIN SUPERUSER PASSWORD 'rr'");
            st.execute("ALTER ROLE rr_user SET default_transaction_isolation = 'repeatable read'");
            st.execute("CREATE ROLE ser_user LOGIN SUPERUSER PASSWORD 'ser'");
            st.execute("ALTER ROLE ser_user SET default_transaction_isolation = 'serializable'");
            st.execute("CREATE DATABASE ser_db");
            st.execute("ALTER DATABASE ser_db SET default_transaction_isolation = 'serializable'");
        }
    }

    private static Connection plain(String db, String user, String password) throws SQLException {
        return source(db, user, password).getConnection();
    }

    private static PGSimpleDataSource source(String db, String user, String password) {
        var ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{postgres.getHost()});
        ds.setPortNumbers(new int[]{postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)});
        ds.setDatabaseName(db);
        ds.setUser(user);
        ds.setPassword(password);
        return ds;
    }

    /** Isolation level each connection had when it was handed back ("closed") to the pool. */
    private final ConcurrentLinkedQueue<Integer> isolationOnReturn = new ConcurrentLinkedQueue<>();

    private DataSource dataSource(Setting setting) {
        return switch (setting) {
            case POOL_REPEATABLE_READ -> poolDefault(source("fom", "test", "test"),
                    Connection.TRANSACTION_REPEATABLE_READ);
            case POOL_SERIALIZABLE -> poolDefault(source("fom", "test", "test"), Connection.TRANSACTION_SERIALIZABLE);
            case ROLE_REPEATABLE_READ -> source("fom", "rr_user", "rr");
            case ROLE_SERIALIZABLE -> source("fom", "ser_user", "ser");
            case DATABASE_SERIALIZABLE -> source("ser_db", "test", "test");
        };
    }

    /**
     * What a pool configured with a default isolation (HikariCP {@code transactionIsolation})
     * does: every connection it hands out starts at that level. Records the level each
     * connection has when it is closed.
     */
    private DataSource poolDefault(DataSource delegate, int isolation) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    Object result;
                    try {
                        result = dsMethod.invoke(delegate, dsArgs);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (!(result instanceof Connection conn)) return result;
                    conn.setTransactionIsolation(isolation);
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                                if ("close".equals(method.getName()) && !conn.isClosed()) {
                                    isolationOnReturn.add(conn.getTransactionIsolation());
                                }
                                try {
                                    return method.invoke(conn, args);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                            });
                });
    }

    /** A node keyed differently: only the write fence keeps it and {@code a} apart. */
    private static PostgresLogBackend otherKeyed(DataSource ds, String id) throws SQLException {
        return new PostgresLogBackend(ds, id, "fom_log_" + id,
                PostgresLogBackend.CONFIRM_TIMEOUT_MILLIS, PostgresLogBackend.INCONCLUSIVE_LIMIT_MILLIS,
                UUID.randomUUID().getMostSignificantBits());
    }

    /** Parks a write on the leader thread between its INSERT and its COMMIT until released. */
    private static final class Gate implements Runnable {
        final CountDownLatch reached = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void run() {
            reached.countDown();
            try {
                if (!release.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("gate never released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Whether some session is waiting for a lock on {@code table} (in the database {@code setting} uses). */
    private static boolean someoneWaitsToLock(Setting setting, String table) throws SQLException {
        String db = setting == Setting.DATABASE_SERIALIZABLE ? "ser_db" : "fom";
        try (var c = plain(db, "test", "test"); var st = c.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = to_regclass(?) AND NOT granted)")) {
            st.setString(1, table);
            try (var rs = st.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private static void assertClocksAreZeroToLength(LogBackend b) {
        List<Long> clocks = java.util.Arrays.stream(b.getBetween(0, b.length())).map(LogEvent::clock).toList();
        assertThat(clocks).as("clocks: unique, gap-free and increasing in log order")
                .isEqualTo(LongStream.range(0, clocks.size()).boxed().toList());
    }

    private static String id(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    @ParameterizedTest
    @EnumSource(Setting.class)
    void an_append_racing_an_uncommitted_takeover_is_refused(Setting setting) throws Exception {
        DataSource ds = dataSource(setting);
        String id = id("iso_race");
        String table = "fom_log_" + id;
        try (var a = new PostgresLogBackend(ds, id); var b = otherKeyed(ds, id)) {
            assertThat(a.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(a.append(new LogInitialized(0, 2L, "P", Map.of()), "A")).isPresent();

            Gate gate = new Gate();
            b.beforeCommitHook = gate;
            var claim = CompletableFuture.supplyAsync(() -> b.append(new LogLeader(0, 3L, "B"), "B"));
            assertThat(gate.reached.await(10, TimeUnit.SECONDS)).as("B's claim is inserted, not committed").isTrue();
            b.beforeCommitHook = null;

            var append = CompletableFuture.supplyAsync(() -> a.append(new LogInitialized(0, 4L, "Q", Map.of()), "A"));
            await().atMost(Duration.ofSeconds(10)).until(() -> append.isDone() || someoneWaitsToLock(setting, table));
            gate.release.countDown();

            assertThat(claim.get(10, TimeUnit.SECONDS)).get().extracting(LogEvent::clock).isEqualTo(2L);
            assertThat(append.get(10, TimeUnit.SECONDS))
                    .as("the deposed leader must see the claim it waited for (%s)", setting)
                    .isEmpty();
            assertThat(a.introspect().currentLeader()).isEqualTo("B");
            assertThat(a.append(new LogInitialized(0, 5L, "R", Map.of()), "A")).as("and stays refused").isEmpty();
            assertThat(b.append(new LogInitialized(0, 6L, "S", Map.of()), "B")).isPresent();

            assertThat(a.length()).isEqualTo(4);
            assertThat(((LogLeader) a.get(2)).instanceId()).isEqualTo("B");
            assertClocksAreZeroToLength(a);
        }
        assertIsolationRestored(setting);
    }

    @ParameterizedTest
    @EnumSource(Setting.class)
    void a_takeover_racing_an_uncommitted_append_continues_after_it(Setting setting) throws Exception {
        DataSource ds = dataSource(setting);
        String id = id("iso_racec");
        String table = "fom_log_" + id;
        try (var a = new PostgresLogBackend(ds, id); var b = otherKeyed(ds, id)) {
            assertThat(a.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(a.append(new LogInitialized(0, 2L, "P", Map.of()), "A")).isPresent();

            Gate gate = new Gate();
            a.beforeCommitHook = gate;
            var append = CompletableFuture.supplyAsync(() -> a.append(new LogInitialized(0, 3L, "Q", Map.of()), "A"));
            assertThat(gate.reached.await(10, TimeUnit.SECONDS)).isTrue();
            a.beforeCommitHook = null;

            var claim = CompletableFuture.supplyAsync(() -> b.append(new LogLeader(0, 4L, "B"), "B"));
            await().atMost(Duration.ofSeconds(10)).until(() -> claim.isDone() || someoneWaitsToLock(setting, table));
            gate.release.countDown();

            assertThat(append.get(10, TimeUnit.SECONDS)).get().extracting(LogEvent::clock).isEqualTo(2L);
            assertThat(claim.get(10, TimeUnit.SECONDS)).get().extracting(LogEvent::clock)
                    .as("the claim continues after the append it waited for (%s)", setting).isEqualTo(3L);
            assertThat(a.append(new LogInitialized(0, 5L, "R", Map.of()), "A")).isEmpty();
            assertThat(a.introspect().currentLeader()).isEqualTo("B");

            assertThat(a.length()).isEqualTo(4);
            assertClocksAreZeroToLength(a);
        }
        assertIsolationRestored(setting);
    }

    /**
     * A compaction racing a claim still in flight: it retries its lock until the claim commits,
     * and must then see it — not snapshot its own leadership back over it.
     */
    @ParameterizedTest
    @EnumSource(Setting.class)
    void a_compaction_racing_an_uncommitted_takeover_is_refused(Setting setting) throws Exception {
        DataSource ds = dataSource(setting);
        String id = id("iso_cmp");
        try (var a = new PostgresLogBackend(ds, id); var b = otherKeyed(ds, id)) {
            assertThat(a.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(a.append(new LogInitialized(0, 2L, "P", Map.of()), "A")).isPresent();

            Gate gate = new Gate();
            b.beforeCommitHook = gate;
            var claim = CompletableFuture.supplyAsync(() -> b.append(new LogLeader(0, 3L, "B"), "B"));
            assertThat(gate.reached.await(10, TimeUnit.SECONDS)).isTrue();
            b.beforeCommitHook = null;

            var compact = CompletableFuture.supplyAsync(() -> a.compact(List.of(new LogLeader(1L, 4L, "A")), "A"));
            Thread.sleep(500); // the compaction is retrying its NOWAIT lock behind the claim
            gate.release.countDown();

            assertThat(claim.get(10, TimeUnit.SECONDS)).get().extracting(LogEvent::clock).isEqualTo(2L);
            assertThatThrownBy(() -> compact.get(10, TimeUnit.SECONDS))
                    .as("the compaction must see the claim it waited for (%s)", setting)
                    .hasCauseInstanceOf(LeadershipLostException.class);
            assertThat(a.length()).isEqualTo(3);
            assertThat(((LogLeader) a.get(2)).instanceId()).isEqualTo("B");
            assertClocksAreZeroToLength(a);
        }
        assertIsolationRestored(setting);
    }

    /** Compaction and the appends after it work, and keep clocks increasing, under every setting. */
    @ParameterizedTest
    @EnumSource(Setting.class)
    void compaction_works_and_clocks_keep_increasing(Setting setting) throws Exception {
        DataSource ds = dataSource(setting);
        String id = id("iso_cmpok");
        try (var a = new PostgresLogBackend(ds, id)) {
            assertThat(a.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(a.append(new LogInitialized(0, 2L, "P", Map.of()), "A")).isPresent();
            assertThat(a.append(new LogInitialized(0, 3L, "Q", Map.of()), "A")).isPresent();
            a.compact(List.of(new LogLeader(3L, 4L, "A")), "A");
            assertThat(a.append(new LogInitialized(0, 5L, "R", Map.of()), "A")).get()
                    .extracting(LogEvent::clock).isEqualTo(4L);
            assertThat(a.length()).isEqualTo(2);
            assertThat(a.introspect().currentLeader()).isEqualTo("A");
        }
        try (var reopened = new PostgresLogBackend(ds, id)) {
            assertThat(reopened.append(new LogLeader(0, 6L, "A2"), "A2")).get()
                    .extracting(LogEvent::clock).isEqualTo(5L);
        }
        assertIsolationRestored(setting);
    }

    /** The leader connection goes back to the pool at the pool's own level, not at READ COMMITTED. */
    private void assertIsolationRestored(Setting setting) {
        int expected = switch (setting) {
            case POOL_REPEATABLE_READ -> Connection.TRANSACTION_REPEATABLE_READ;
            case POOL_SERIALIZABLE -> Connection.TRANSACTION_SERIALIZABLE;
            default -> -1;
        };
        if (expected < 0) return;
        assertThat(isolationOnReturn).as("every connection returned at the pool's level").isNotEmpty()
                .allMatch(level -> level == expected);
    }
}
