package io.fom.jdbc;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.SnapshotResult;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.serde.JavaSerializableSerDe;
import io.fom.api.LeadershipLostException;
import io.fom.log.LogBackend;
import io.fom.test.LogBackendContractTest;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Runs the shared {@link LogBackendContractTest} against a real Postgres
 * (Testcontainers) plus a few Postgres-specific tests (advisory-lock leadership,
 * reopen). The contract enforces the SPI invariants — including 0-based clocks
 * and out-of-range {@code IndexOutOfBoundsException} — that the prior hand-rolled
 * tests did not cover.
 */
@Testcontainers
class PostgresLogBackendTest extends LogBackendContractTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("fom")
                    .withUsername("test")
                    .withPassword("test");

    private static DataSource dataSource;
    private String logId;

    @BeforeAll
    static void setupDataSource() {
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        dataSource = ds;
    }

    @Override
    protected LogBackend create() {
        logId = "ct_" + UUID.randomUUID().toString().replace("-", "");
        try {
            return new PostgresLogBackend(dataSource, logId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected LogBackend reopen(LogBackend original) {
        try {
            return new PostgresLogBackend(dataSource, logId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // ───────────────── Postgres-specific tests ─────────────────

    @Test
    void event_the_reader_would_reject_is_refused_and_the_log_stays_usable() {
        backend.append(new LogLeader(0, now(), LEADER), LEADER);
        // Over the read filter's 10M array-length cap: could be written but never read.
        assertThatThrownBy(() -> backend.append(
                new LogInitialized(0, now(), "x", Map.of("k", new byte[10_000_001])), LEADER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload limits");
        assertThat(backend.length()).isEqualTo(1);
        assertThat(backend.append(new LogInitialized(0, now(), "y", Map.of("k", new byte[]{1})), LEADER)).isPresent();
        assertThat(backend.get(1)).isInstanceOf(LogInitialized.class);
    }

    @Test
    void second_open_while_first_holds_advisory_lock_fails_fast() {
        // backend (from the contract's setUp) already holds the advisory lock on logId.
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, logId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advisory lock");
    }

    @Test
    void reads_back_persisted_events_after_reopen() throws SQLException {
        backend.append(new LogLeader(0, 1000L, "instance-A"), "instance-A");
        backend.append(new LogInitialized(0, 1001L, "Foo",
                Map.of("k1", new byte[]{1, 2}, "k2", new byte[]{3})), "instance-A");

        assertThat(backend.length()).isEqualTo(2);
        backend.close();
        try (var reopened = new PostgresLogBackend(dataSource, logId)) {
            assertThat(reopened.length()).isEqualTo(2);
            assertThat(reopened.get(1)).isInstanceOf(LogInitialized.class);
        }
        backend = null; // already closed; suppress the contract tearDown double-close
    }

    // ───────────────── regression: a log whose clocks go backwards is refused ─────────────────

    /** Copies the payloads of rows {@code [from, to)} (by position) to the end of the table, as a spliced restore would. */
    private static void reinsertRows(String table, int from, int to) throws SQLException {
        try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                "INSERT INTO " + table + " (type, payload, ts_millis)"
                        + " SELECT type, payload, ts_millis FROM " + table
                        + " ORDER BY row_id OFFSET ? LIMIT ?")) {
            st.setInt(1, from);
            st.setInt(2, to - from);
            st.executeUpdate();
        }
    }

    @Test
    void a_log_with_old_rows_reinserted_at_the_end_is_refused_at_open() throws Exception {
        String id = "splice_" + uuid();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            for (int i = 0; i < 4; i++) {
                b.append(new LogInitialized(0, 2L + i, "P" + i, Map.of("k", new byte[]{(byte) i})), "A");
            }
        }
        // Clocks 0..4, then 1..3 again: the last clock (3) would make the next append re-issue 4.
        reinsertRows("fom_log_" + id, 1, 4);
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("spliced")
                .hasMessageContaining("has clock 3")
                .hasMessageContaining("holds 8 rows");
        // Refused cleanly: the advisory lock was released, the table left as it was.
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id)).hasMessageContaining("spliced");
    }

    @Test
    void a_splice_the_open_check_cannot_see_is_refused_by_the_scan_naming_the_rows() throws Exception {
        String id = "splice_scan_" + uuid();
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
        }
        // Clocks 0, 1, 9, 5: the last clock (5) covers the length (4), so only a scan sees it.
        plantRow(table, new LogInitialized(9L, 9L, "X", Map.of()));
        plantRow(table, new LogInitialized(5L, 5L, "Y", Map.of()));
        try (var b = new PostgresLogBackend(dataSource, id)) {
            assertThatThrownBy(() -> b.getBetween(0, 4))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("spliced")
                    .hasMessageContaining("(position 3, LogInitialized) has clock 5")
                    .hasMessageContaining("position 2) has clock 9")
                    .hasMessageContaining("reading stopped at row_id")
                    .hasMessageContaining("repair it before use")
                    // An engine has appended its LogLeader by the time its scan gets here.
                    .hasMessageNotContaining("nothing was changed");
            // A chunked scan is checked across its chunks: [0,3) is fine, [3,4) continues it.
            assertThat(b.getBetween(0, 3)).hasSize(3);
            assertThatThrownBy(() -> b.getBetween(3, 4))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("has clock 5")
                    .hasMessageContaining("has clock 9");
        }
    }

    @Test
    void an_engine_refuses_to_start_on_a_spliced_log() throws Exception {
        String id = "splice_engine_" + uuid();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
        }
        plantRow("fom_log_" + id, new LogInitialized(9L, 9L, "X", Map.of()));
        plantRow("fom_log_" + id, new LogInitialized(5L, 5L, "Y", Map.of()));
        try (var b = new PostgresLogBackend(dataSource, id)) {
            Throwable failure = org.assertj.core.api.Assertions.catchThrowable(() -> {
                Engine engine = new Engine(EngineConfig.defaults(), b, new JavaSerializableSerDe());
                try {
                    engine.newGraph(new GraphBuilder()
                            .add("P",
                                    (Supplier<ProcessInitializer>) () -> ctx ->
                                            CompletableFuture.completedFuture(Map.of("v", new byte[]{7})),
                                    (Supplier<ProcessLoader>) () -> (ctx, props) ->
                                            CompletableFuture.completedFuture(
                                                    (Process) (qctx, query) -> CompletableFuture.completedFuture("pong")))
                            .handles(String.class)
                            .build());
                } finally {
                    try {
                        engine.close();
                    } catch (RuntimeException ignored) {
                        // shutdown bookkeeping on a refused log
                    }
                }
            });
            assertThat(failure).isNotNull();
            assertThat(org.assertj.core.util.Throwables.getStackTrace(failure)).contains("spliced");
        }
    }

    // ───────────────── regression: compaction retries transient catalog conflicts ─────────────────

    @Test
    void transient_conflict_states_are_recognised_and_others_are_not() {
        assertThat(PostgresLogBackend.isTransientCatalogConflict(new SQLException("tuple concurrently updated", "XX000"))).isTrue();
        assertThat(PostgresLogBackend.isTransientCatalogConflict(new SQLException("deadlock detected", "40P01"))).isTrue();
        assertThat(PostgresLogBackend.isTransientCatalogConflict(new SQLException("could not serialize", "40001"))).isTrue();
        assertThat(PostgresLogBackend.isTransientCatalogConflict(new SQLException("cache lookup failed", "XX000"))).isFalse();
        assertThat(PostgresLogBackend.isTransientCatalogConflict(new SQLException("permission denied", "42501"))).isFalse();
    }

    @Test
    void compaction_racing_a_grant_on_all_tables_is_retried_and_succeeds() throws Exception {
        String schema = "grant_" + uuid();
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE SCHEMA " + schema);
        }
        DataSource ds = inSchema(schema);
        String id = "grant_" + uuid();
        try (var b = new PostgresLogBackend(ds, id); var migration = ds.getConnection()) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
            // A migration's open transaction: GRANT takes no table lock but row-locks every
            // table's pg_class entry, which the compaction's RENAME has to update too.
            migration.setAutoCommit(false);
            try (var st = migration.createStatement()) {
                st.execute("GRANT SELECT ON ALL TABLES IN SCHEMA " + schema + " TO PUBLIC");
            }
            var compaction = CompletableFuture.supplyAsync(
                    () -> b.compact(List.of(new LogLeader(0, 5L, "A")), "A"));
            await().atMost(Duration.ofSeconds(10)).until(() -> waitsOnATransaction(schema));
            migration.commit(); // the waiting RENAME now fails with "tuple concurrently updated"
            SnapshotResult result = compaction.get(30, TimeUnit.SECONDS);
            assertThat(result.archivedLogId()).isNotNull();
            assertThat(b.length()).isEqualTo(1);
            assertThat(b.append(new LogInitialized(0, 6L, "Q", Map.of()), "A")).isPresent();
        }
    }

    @Test
    void transient_conflict_states_include_a_lock_timeout_only_through_its_own_check() {
        assertThat(PostgresLogBackend.isLockTimeout(new SQLException("canceling statement due to lock timeout",
                "55P03"))).isTrue();
        assertThat(PostgresLogBackend.isLockTimeout(new SQLException("deadlock detected", "40P01"))).isFalse();
        // The table lock's own 55P03 ("readers keep the table busy") is not a catalog conflict.
        assertThat(PostgresLogBackend.isTransientCatalogConflict(new SQLException("could not obtain lock",
                "55P03"))).isFalse();
    }

    @Test
    void compaction_behind_an_open_migration_does_not_block_readers_and_succeeds_after_it_commits()
            throws Exception {
        String schema = "grant_wait_" + uuid();
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE SCHEMA " + schema);
        }
        DataSource ds = inSchema(schema);
        String id = "grant_wait_" + uuid();
        String table = schema + ".fom_log_" + id;
        try (var b = new PostgresLogBackend(ds, id); var migration = ds.getConnection()) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
            migration.setAutoCommit(false);
            try (var st = migration.createStatement()) {
                st.execute("GRANT SELECT ON ALL TABLES IN SCHEMA " + schema + " TO PUBLIC");
            }
            try {
                var compaction = CompletableFuture.supplyAsync(
                        () -> b.compact(List.of(new LogLeader(0, 5L, "A")), "A"));
                // The RENAME now waits on the migration while holding ACCESS EXCLUSIVE on the table.
                await().atMost(Duration.ofSeconds(10)).until(() -> waitsOnATransaction(schema));
                long started = System.nanoTime();
                var read = CompletableFuture.supplyAsync(() -> {
                    try (var c = dataSource.getConnection(); var st = c.createStatement();
                         var rs = st.executeQuery("SELECT count(*) FROM " + table)) {
                        rs.next();
                        return rs.getLong(1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                // Without a lock_timeout the reader would wait for the migration (held open here).
                assertThat(read.get(PostgresLogBackend.COMPACT_DDL_LOCK_TIMEOUT_MILLIS + 2_000, TimeUnit.MILLISECONDS))
                        .isEqualTo(2);
                assertThat(Duration.ofNanos(System.nanoTime() - started))
                        .isLessThan(Duration.ofMillis(PostgresLogBackend.COMPACT_DDL_LOCK_TIMEOUT_MILLIS + 1_500));
                assertThat(compaction).as("still retrying while the migration is open").isNotDone();
                migration.commit();
                SnapshotResult result = compaction.get(30, TimeUnit.SECONDS);
                assertThat(result.archivedLogId()).isNotNull();
                assertThat(b.length()).isEqualTo(1);
                assertThat(b.append(new LogInitialized(0, 6L, "Q", Map.of()), "A")).isPresent();
            } finally {
                if (!migration.getAutoCommit()) migration.rollback();
            }
        }
    }

    /** Whether a session of this database waits on another transaction (a row lock). */
    private static boolean waitsOnATransaction(String unused) throws SQLException {
        try (var c = dataSource.getConnection(); var st = c.createStatement();
             var rs = st.executeQuery("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'transactionid'"
                     + " AND NOT granted)")) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    @Test
    void compaction_gives_up_after_a_bounded_number_of_transient_conflicts() throws Exception {
        String id = "deadlock_" + uuid();
        var failures = new java.util.concurrent.atomic.AtomicInteger(Integer.MAX_VALUE);
        try (var b = new PostgresLogBackend(deadlockingRenames(dataSource, failures), id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
            assertThatThrownBy(() -> b.compact(List.of(new LogLeader(0, 5L, "A")), "A"))
                    .hasMessageContaining("deadlock detected")
                    .hasMessageContaining("the write was not applied");
            assertThat(Integer.MAX_VALUE - failures.get()).as("attempts").isEqualTo(PostgresLogBackend.COMPACT_ATTEMPTS);
            assertThat(b.length()).isEqualTo(2);
            // Still the leader and still writable: a failed compaction does not fence.
            failures.set(1);
            assertThat(b.compact(List.of(new LogLeader(0, 5L, "A")), "A").archivedLogId()).isNotNull();
            assertThat(b.length()).isEqualTo(1);
        }
    }

    /** Every {@code ALTER TABLE ... RENAME} fails with a deadlock while {@code failuresLeft > 0}. */
    private static DataSource deadlockingRenames(DataSource delegate,
                                                 java.util.concurrent.atomic.AtomicInteger failuresLeft) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
                    if (!(result instanceof Connection conn)) return result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                Object r = invokeUnwrapped(cMethod, conn, cArgs);
                                if (cMethod.getName().equals("createStatement") && r instanceof java.sql.Statement st) {
                                    return Proxy.newProxyInstance(java.sql.Statement.class.getClassLoader(),
                                            new Class<?>[]{java.sql.Statement.class}, (sProxy, sMethod, sArgs) -> {
                                                if (sMethod.getName().equals("execute")
                                                        && sArgs[0] instanceof String sql
                                                        && sql.startsWith("ALTER TABLE ") && sql.contains(" RENAME TO ")
                                                        && failuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : n) > 0) {
                                                    throw new SQLException("deadlock detected", "40P01");
                                                }
                                                return invokeUnwrapped(sMethod, st, sArgs);
                                            });
                                }
                                return r;
                            });
                });
    }

    // ───────────────── regression: advisory lock released with pooled sessions ─────────────────

    @Test
    void close_releases_advisory_lock_even_when_pool_keeps_the_session() throws Exception {
        String id = "pool_" + uuid();
        try (Connection physical = dataSource.getConnection()) {
            var pool = new SingleSessionPool(physical);
            int pid = backendPid(physical);

            var a = new PostgresLogBackend(pool, id);
            a.append(new LogLeader(0, 1L, "A"), "A");
            assertThat(advisoryLocksHeldBy(pid)).isEqualTo(1);

            a.close();
            assertThat(advisoryLocksHeldBy(pid)).isZero();
            try (var b = new PostgresLogBackend(dataSource, id)) {
                assertThat(b.length()).isEqualTo(1);
            }
        }
    }

    @Test
    void close_during_a_cancel_flood_does_not_leave_the_lock_on_a_pooled_session() throws Exception {
        String appName = "fom-closeflood-" + uuid();
        var pool = new ReusingPool(appName);
        try {
            for (int round = 0; round < 6; round++) {
                String id = "closeflood_" + uuid();
                var b = new PostgresLogBackend(pool, id);
                b.append(new LogLeader(0, 1L, "A"), "A");
                var stop = new java.util.concurrent.atomic.AtomicBoolean();
                var cancels = new java.util.concurrent.atomic.AtomicInteger();
                // Cancel every statement of the pool's sessions in a tight loop while close() runs.
                Thread canceller = Thread.ofPlatform().start(() -> {
                    try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity"
                                    + " WHERE application_name = ? AND pid <> pg_backend_pid()")) {
                        st.setString(1, appName);
                        while (!stop.get()) {
                            st.executeQuery().close();
                            cancels.incrementAndGet();
                        }
                    } catch (Exception ignored) {
                        // test helper
                    }
                });
                try {
                    await().atMost(Duration.ofSeconds(5)).until(() -> cancels.get() > 50);
                    b.close();
                } finally {
                    stop.set(true);
                    canceller.join(10_000);
                }
                PostgresLogBackend standby = await("a standby takes the lock after close() in round " + round)
                        .atMost(Duration.ofSeconds(10))
                        .pollInterval(Duration.ofMillis(100))
                        .until(() -> tryOpen(id), Objects::nonNull);
                try (standby) {
                    assertThat(standby.length()).isEqualTo(1);
                }
            }
        } finally {
            pool.closeAll();
        }
    }

    @Test
    void introspect_reports_last_known_values_when_the_database_does_not_answer() throws Exception {
        String id = "stall_" + uuid();
        var mode = new java.util.concurrent.atomic.AtomicReference<>("ok");
        var release = new java.util.concurrent.CountDownLatch(1);
        var b = new PostgresLogBackend(stalling(dataSource, mode, release), id);
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
            assertThat(b.introspect().length()).isEqualTo(2);

            mode.set("throw"); // the pool cannot hand out a connection for the report query
            var report = java.util.concurrent.CompletableFuture.supplyAsync(b::introspect)
                    .get(15, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(report.length()).isEqualTo(2);
            assertThat(report.currentLeader()).isEqualTo("A");
            assertThat(report.eventCounts()).containsEntry("LogInitialized", 1);

            mode.set("block"); // unresponsive server: the leader-session check and the report query hang
            for (int i = 0; i < 2; i++) {
                long start = System.nanoTime();
                report = java.util.concurrent.CompletableFuture.supplyAsync(b::introspect)
                        .get(15, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(Duration.ofNanos(System.nanoTime() - start))
                        .isLessThan(Duration.ofMillis(PostgresLogBackend.INTROSPECT_TIMEOUT_MILLIS + 3_000));
                assertThat(report.length()).isEqualTo(2);
                assertThat(report.currentLeader()).as("not positively lost: still leader").isEqualTo("A");
            }

            mode.set("ok");
            release.countDown();
            assertThat(b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A")).isPresent();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(b.introspect().length()).isEqualTo(3));
            assertThat(b.introspect().currentLeader()).isEqualTo("A");
        } finally {
            mode.set("ok");
            release.countDown();
            b.close();
        }
    }

    @Test
    void failed_construction_releases_advisory_lock_on_pooled_session() throws Exception {
        String id = "badleader_" + uuid();
        new PostgresLogBackend(dataSource, id).close(); // creates the table
        try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                "INSERT INTO fom_log_" + id + " (type, payload, ts_millis) VALUES ('LogLeader', ?, 0)")) {
            st.setBytes(1, new byte[]{1, 2, 3}); // undecodable -> readCurrentLeader throws after the lock is taken
            st.executeUpdate();
        }
        try (Connection physical = dataSource.getConnection()) {
            var pool = new SingleSessionPool(physical);
            int pid = backendPid(physical);
            assertThatThrownBy(() -> new PostgresLogBackend(pool, id)).isInstanceOf(SQLException.class);
            assertThat(advisoryLocksHeldBy(pid)).isZero();
        }
    }

    // ───────────────── regression: fencing after leader session loss ─────────────────

    @Test
    void lost_leader_session_is_fenced_and_cannot_split_brain() throws Exception {
        String id = "split_" + uuid();
        String appName = "fom-split-" + uuid();
        var dsA = new PGSimpleDataSource();
        dsA.setUrl(postgres.getJdbcUrl());
        dsA.setUser(postgres.getUsername());
        dsA.setPassword(postgres.getPassword());
        dsA.setApplicationName(appName);

        var a = new PostgresLogBackend(dsA, id);
        try {
            assertThat(a.append(new LogLeader(0, 1L, "A"), "A")).isPresent();

            // Kill A's leader session (simulates idle timeout / failover); its advisory lock dies with it.
            try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = ?")) {
                st.setString(1, appName);
                st.executeQuery().close();
            }

            PostgresLogBackend b = await().atMost(Duration.ofSeconds(10))
                    .until(() -> tryOpen(id), Objects::nonNull);
            try (b) {
                assertThat(b.append(new LogLeader(0, 2L, "B"), "B")).isPresent();
                assertThat(a.introspect().currentLeader())
                        .as("a fenced instance stops claiming leadership before its next write")
                        .isNull();

                assertThatThrownBy(() -> a.append(new LogInitialized(0, 3L, "Foo", Map.of("k", new byte[]{1})), "A"))
                        .as("losing leadership is one type for callers, whatever refused it")
                        .isInstanceOf(LeadershipLostException.class)
                        .hasMessageContaining("advisory lock");
                assertThat(a.introspect().currentLeader()).isNull();
                // Stays fenced: even a fresh leader claim is refused.
                assertThatThrownBy(() -> a.append(new LogLeader(0, 4L, "A"), "A"))
                        .isInstanceOf(LeadershipLostException.class)
                        .hasMessageContaining("advisory lock");
                assertThatThrownBy(() -> a.compact(List.of(new LogLeader(0, 5L, "A")), "A"))
                        .isInstanceOf(LeadershipLostException.class)
                        .hasMessageContaining("advisory lock");
                assertThatThrownBy(() -> a.purgeArchives(0))
                        .isInstanceOf(LeadershipLostException.class)
                        .hasMessageContaining("advisory lock");

                // B's log is untouched by A and B keeps working.
                assertThat(b.length()).isEqualTo(2);
                assertThat(((LogLeader) b.get(1)).instanceId()).isEqualTo("B");
                assertThat(b.append(new LogInitialized(0, 6L, "Bar", Map.of("k", new byte[]{2})), "B")).isPresent();
                assertThat(b.length()).isEqualTo(3);
            }
        } finally {
            a.close();
        }
    }

    @Test
    void purge_keeps_only_the_newest_archive_tables() throws Exception {
        String id = "purge_" + uuid();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            List<String> archives = new java.util.ArrayList<>();
            for (int i = 0; i < 4; i++) {
                archives.add(b.compact(List.of(new LogLeader(0, 10L + i, "A")), "A").archivedLogId());
                Thread.sleep(2);
            }
            b.purgeArchives(2);
            assertThat(relationExists(archives.get(0))).isFalse();
            assertThat(relationExists(archives.get(1))).isFalse();
            assertThat(relationExists(archives.get(2))).isTrue();
            assertThat(relationExists(archives.get(3))).isTrue();
            assertThat(b.length()).isEqualTo(1);
        }
    }

    @Test
    void log_ids_differing_only_in_case_are_one_log_with_one_lock_and_purgeable_archives() throws Exception {
        String id = "Case_" + uuid();
        try (var mixed = new PostgresLogBackend(dataSource, id)) {
            assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id.toLowerCase(java.util.Locale.ROOT)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("advisory lock");

            mixed.append(new LogLeader(0, 1L, "A"), "A");
            List<String> archives = new java.util.ArrayList<>();
            for (int i = 0; i < 3; i++) {
                SnapshotResult r = mixed.compact(List.of(new LogLeader(0, 10L + i, "A")), "A");
                assertThat(relationExists(r.newLogId())).as("reported table exists: %s", r.newLogId()).isTrue();
                archives.add(r.archivedLogId());
                Thread.sleep(2);
            }
            for (String archive : archives) {
                assertThat(relationExists(archive)).as("reported archive exists: %s", archive).isTrue();
            }
            mixed.purgeArchives(1);
            assertThat(relationExists(archives.get(0))).isFalse();
            assertThat(relationExists(archives.get(1))).isFalse();
            assertThat(relationExists(archives.get(2))).isTrue();
        }
    }

    @Test
    void purge_never_touches_a_live_table_named_like_an_archive_of_another_log() throws Exception {
        String id = "inv_" + uuid().substring(0, 8); // short: the foreign table name below must fit 63 bytes
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id + "_archived_2024"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("_archived_");
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, "x", "fom_log_" + id + "_ARCHIVED_7"))
                .isInstanceOf(IllegalArgumentException.class);

        // A foreign table that merely looks like an archive with a huge suffix is ignored, not parsed.
        String foreign = "fom_log_" + id + "_archived_" + "9".repeat(20);
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE TABLE " + foreign + " (x int)");
        }
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.compact(List.of(new LogLeader(0, 2L, "A")), "A");
            b.purgeArchives(0);
        }
        assertThat(relationExists(foreign)).isTrue();
    }

    @Test
    void purge_finds_the_hashed_archives_of_a_long_table_name() throws Exception {
        String id = "l" + uuid() + "_" + "y".repeat(12); // table name = 54 chars: archive names are hashed
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            List<String> archives = new java.util.ArrayList<>();
            for (int i = 0; i < 3; i++) {
                archives.add(b.compact(List.of(new LogLeader(0, 10L + i, "A")), "A").archivedLogId());
            }
            b.purgeArchives(1);
            assertThat(relationExists(archives.get(0))).isFalse();
            assertThat(relationExists(archives.get(1))).isFalse();
            assertThat(relationExists(archives.get(2))).isTrue();
        }
    }

    @Test
    void nodes_starting_together_on_a_new_log_get_the_documented_lock_error() throws Exception {
        for (int round = 0; round < 8; round++) {
            String id = "race_" + uuid();
            var start = new java.util.concurrent.CountDownLatch(1);
            var opened = new java.util.concurrent.CopyOnWriteArrayList<PostgresLogBackend>();
            var errors = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
            List<Thread> threads = new java.util.ArrayList<>();
            for (int i = 0; i < 4; i++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        opened.add(new PostgresLogBackend(dataSource, id));
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }));
            }
            start.countDown();
            for (Thread t : threads) t.join(10_000);
            assertThat(opened).hasSize(1);
            assertThat(errors).hasSize(3).allSatisfy(e -> assertThat(e)
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("advisory lock"));
            opened.forEach(PostgresLogBackend::close);
        }
    }

    @Test
    void cancelled_statements_on_the_leader_session_do_not_fence_it() throws Exception {
        String id = "cancel_" + uuid();
        String appName = "fom-cancel-" + uuid();
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        ds.setApplicationName(appName);
        var failures = new java.util.concurrent.atomic.AtomicInteger();
        try (var b = new PostgresLogBackend(ds, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            for (int round = 0; round < 4; round++) {
                var stop = new java.util.concurrent.atomic.AtomicBoolean();
                // Flood: cancel every statement of the backend's sessions (the leader session and
                // the short-lived ones) in a tight loop while appends and health checks run.
                Thread canceller = Thread.ofPlatform().start(() -> {
                    try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity"
                                    + " WHERE application_name = ? AND pid <> pg_backend_pid()")) {
                        st.setString(1, appName);
                        while (!stop.get()) {
                            st.executeQuery().close();
                        }
                    } catch (Exception ignored) {
                        // test helper
                    }
                });
                Runnable appends = () -> {
                    while (!stop.get()) {
                        try {
                            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
                        } catch (RuntimeException cancelled) {
                            failures.incrementAndGet(); // a cancelled write may fail; it must not fence the node
                        }
                    }
                };
                Runnable healthChecks = () -> {
                    while (!stop.get()) {
                        try {
                            b.introspect();
                        } catch (RuntimeException cancelled) {
                            failures.incrementAndGet();
                        }
                    }
                };
                Thread appender = Thread.ofPlatform().start(appends);
                Thread checker = Thread.ofPlatform().start(healthChecks);
                try {
                    Thread.sleep(1_750);
                } finally {
                    stop.set(true);
                    canceller.join(10_000);
                    appender.join(10_000);
                    checker.join(10_000);
                }
                assertThat(b.introspect().currentLeader()).as("still leader after round %d", round).isEqualTo("A");
                assertThat(b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A"))
                        .as("append after round %d", round).isPresent();
                assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id))
                        .as("a standby is still refused after round %d", round)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Could not acquire advisory lock");
            }
        }
        assertThat(failures.get()).as("the flood did disrupt statements").isPositive();
    }

    @Test
    void a_failed_lock_check_inside_a_write_keeps_its_cause_and_does_not_fence() throws Exception {
        String id = "lockcheck_" + uuid();
        // The in-transaction check plus all three leader-session re-checks fail as if cancelled,
        // so the lock is confirmed from a separate session.
        var failuresLeft = new java.util.concurrent.atomic.AtomicInteger();
        try (var b = new PostgresLogBackend(failingLockChecks(dataSource, failuresLeft), id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            failuresLeft.set(4);
            assertThatThrownBy(() -> b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"))
                    .isInstanceOf(RuntimeException.class)
                    .isNotInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("could not verify the advisory lock")
                    .hasMessageContaining("canceling statement due to user request")
                    .hasMessageContaining("the write was not applied")
                    .hasMessageNotContaining("no longer holds")
                    .hasRootCauseInstanceOf(SQLException.class)
                    .rootCause().hasMessage("canceling statement due to user request")
                    .satisfies(root -> assertThat(((SQLException) root).getSQLState()).isEqualTo("57014"));
            assertThat(failuresLeft.get()).as("all injected failures were hit").isLessThanOrEqualTo(0);
            assertThat(b.introspect().currentLeader()).isEqualTo("A");
            assertThat(b.length()).isEqualTo(1);
            assertThat(b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A")).isPresent();
        }
    }

    @Test
    void a_cancelled_insert_says_the_write_was_not_applied_and_does_not_fence() throws Exception {
        String id = "insertcancel_" + uuid();
        // The INSERT itself is cancelled — the lock check before it succeeded, so the failure
        // arrives without the lock-check wording, but must still say what became of the write.
        var failuresLeft = new java.util.concurrent.atomic.AtomicInteger();
        try (var b = new PostgresLogBackend(failingInserts(dataSource, failuresLeft), id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            failuresLeft.set(1);
            assertThatThrownBy(() -> b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"))
                    .isInstanceOf(RuntimeException.class)
                    .isNotInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("append failed on ")
                    .hasMessageContaining("canceling statement due to user request")
                    .hasMessageContaining("the write was not applied")
                    .hasMessageNotContaining("could not verify the advisory lock")
                    .hasMessageNotContaining("no longer holds")
                    .hasRootCauseInstanceOf(SQLException.class)
                    .rootCause().hasMessage("canceling statement due to user request");
            assertThat(failuresLeft.get()).as("the injected failure was hit").isLessThanOrEqualTo(0);
            // And the claim is true: nothing was written, the node is still the leader.
            assertThat(b.length()).isEqualTo(1);
            assertThat(b.introspect().currentLeader()).isEqualTo("A");
            assertThat(b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A")).isPresent();
            assertThat(b.length()).isEqualTo(2);
        }
    }

    @Test
    void a_leader_session_killed_behind_the_backend_s_back_is_fenced_with_the_real_cause() throws Exception {
        String id = "killed_" + uuid();
        String appName = "fom-killed-" + uuid();
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        ds.setApplicationName(appName);
        try (var b = new PostgresLogBackend(ds, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                    "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = ?"
                            + " AND pid IN (SELECT pid FROM pg_locks WHERE locktype = 'advisory')")) {
                st.setString(1, appName);
                try (var rs = st.executeQuery()) {
                    rs.next();
                    assertThat(rs.getLong(1)).as("leader session terminated").isEqualTo(1);
                }
            }
            // The health check's probe (a separate session) is what finds the lock gone.
            await().atMost(Duration.ofSeconds(10)).until(() -> b.introspect().currentLeader() == null);
            Throwable refused = org.assertj.core.api.Assertions.catchThrowable(
                    () -> b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"));
            assertThat(refused)
                    .isInstanceOf(LeadershipLostException.class)
                    .hasMessageContaining("leader session was lost")
                    .hasMessageContaining("no longer holds the lock")
                    .hasMessageContaining("cause:")
                    .hasCauseInstanceOf(SQLException.class);
            // The leader session's own answer: an administrator's terminate, or the broken socket.
            assertThat(refused.getMessage()).containsPattern("SQLState (57P01|08\\w{3})");
        }
    }

    @Test
    void an_append_that_finds_the_leader_session_killed_is_fenced_with_the_real_cause() throws Exception {
        String id = "killedappend_" + uuid();
        String appName = "fom-killedappend-" + uuid();
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        ds.setApplicationName(appName);
        try (var b = new PostgresLogBackend(ds, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                    "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = ?")) {
                st.setString(1, appName);
                try (var rs = st.executeQuery()) {
                    rs.next();
                    assertThat(rs.getLong(1)).as("leader session terminated").isEqualTo(1);
                }
            }
            await().atMost(Duration.ofSeconds(10)).until(() -> sessionsNamed(appName) == 0);
            // No health check ran: the append itself is what finds the session gone.
            Throwable refused = org.assertj.core.api.Assertions.catchThrowable(
                    () -> b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"));
            assertThat(refused)
                    .isInstanceOf(LeadershipLostException.class)
                    .hasMessageContaining("the leader session is gone")
                    .hasMessageContaining("cause:")
                    .hasMessageContaining("SQLState 57P01")
                    .hasMessageNotContaining("leader session gone, lock not held, or the lock could not be verified");
            // Every later write names the same reason.
            assertThatThrownBy(() -> b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A"))
                    .isInstanceOf(LeadershipLostException.class)
                    .hasMessageContaining("SQLState 57P01");
        }
    }

    @Test
    void a_lock_really_released_on_a_live_leader_session_is_detected() throws Exception {
        String id = "released_" + uuid();
        try (Connection physical = dataSource.getConnection()) {
            var b = new PostgresLogBackend(new SingleSessionPool(physical), id);
            try {
                b.append(new LogLeader(0, 1L, "A"), "A");
                try (var st = physical.createStatement()) {
                    st.executeQuery("SELECT pg_advisory_unlock_all()").close();
                }
                assertThat(b.introspect().currentLeader()).as("session alive, lock gone: fenced").isNull();
                assertThatThrownBy(() -> b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"))
                        .isInstanceOf(LeadershipLostException.class)
                        .hasMessageContaining("no longer holds the advisory lock");
            } finally {
                b.close();
            }
        }
    }

    @Test
    void a_lost_lock_is_detected_from_a_separate_session_when_the_leader_session_checks_fail() throws Exception {
        String id = "confirm_" + uuid();
        var failuresLeft = new java.util.concurrent.atomic.AtomicInteger();
        try (Connection physical = dataSource.getConnection()) {
            var b = new PostgresLogBackend(failingLockChecks(new SingleSessionPool(physical), failuresLeft), id);
            try {
                b.append(new LogLeader(0, 1L, "A"), "A");
                try (var st = physical.createStatement()) {
                    st.executeQuery("SELECT pg_advisory_unlock_all()").close();
                }
                failuresLeft.set(Integer.MAX_VALUE); // the leader session can no longer answer at all
                assertThatThrownBy(() -> b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"))
                        .isInstanceOf(LeadershipLostException.class)
                        .hasMessageContaining("no longer holds the advisory lock");
                assertThat(b.introspect().currentLeader()).isNull();
            } finally {
                failuresLeft.set(0);
                b.close();
            }
        }
    }

    @Test
    void a_lock_confirmation_blocked_in_getConnection_does_not_hang_the_backend() throws Exception {
        String id = "stuckpool_" + uuid();
        String appName = "fom-stuck-" + uuid();
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        ds.setApplicationName(appName);
        var failuresLeft = new java.util.concurrent.atomic.AtomicInteger();
        var release = new java.util.concurrent.CountDownLatch(1);
        var blocked = new java.util.concurrent.atomic.AtomicInteger();
        var b = new PostgresLogBackend(failingLockChecks(blockingConfirmations(ds, release, blocked), failuresLeft), id);
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            failuresLeft.set(Integer.MAX_VALUE); // leader-session checks fail as if cancelled, forever

            long start = System.nanoTime();
            var failedAppend = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"));
            assertThatThrownBy(() -> failedAppend.get(15, java.util.concurrent.TimeUnit.SECONDS))
                    .as("append returns instead of hanging on the blocked confirmation")
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .cause()
                    .isInstanceOf(RuntimeException.class)
                    .isNotInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("could not verify the advisory lock");
            assertThat(blocked.get()).as("the confirmation really blocked in getConnection").isPositive();

            var report = java.util.concurrent.CompletableFuture.supplyAsync(b::introspect)
                    .get(15, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(report.currentLeader()).as("inconclusive checks do not fence immediately").isEqualTo("A");
            assertThat(report.length()).isEqualTo(1);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(15));

            // The pool recovers: the late connection is handed back, and the backend works again.
            failuresLeft.set(0);
            release.countDown();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(sessionsNamed(appName)).as("only the leader session remains").isEqualTo(1));
            assertThat(b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A")).isPresent();
            assertThat(b.introspect().currentLeader()).isEqualTo("A");
        } finally {
            failuresLeft.set(0);
            release.countDown();
            b.close();
        }
    }

    @Test
    void persistently_inconclusive_lock_checks_eventually_fence() throws Exception {
        String id = "inconclusive_" + uuid();
        var failuresLeft = new java.util.concurrent.atomic.AtomicInteger();
        var release = new java.util.concurrent.CountDownLatch(1);
        var blocked = new java.util.concurrent.atomic.AtomicInteger();
        DataSource ds = failingLockChecks(blockingConfirmations(dataSource, release, blocked), failuresLeft);
        var b = new PostgresLogBackend(ds, id, "fom_log_" + id, 300, 1_000);
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            failuresLeft.set(Integer.MAX_VALUE);

            assertThatThrownBy(() -> b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"))
                    .isNotInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("could not verify the advisory lock");
            assertThat(b.introspect().currentLeader()).as("within the bound: not fenced yet").isEqualTo("A");

            Thread.sleep(1_200); // past the inconclusive bound
            assertThat(b.introspect().currentLeader()).as("inconclusive for too long: fenced").isNull();
            assertThatThrownBy(() -> b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A"))
                    .isInstanceOf(LeadershipLostException.class)
                    .hasMessageContaining("no longer holds the advisory lock");

            // Fencing is permanent, even once checks can answer again.
            failuresLeft.set(0);
            release.countDown();
            assertThatThrownBy(() -> b.append(new LogLeader(0, 4L, "A"), "A"))
                    .isInstanceOf(LeadershipLostException.class)
                    .hasMessageContaining("no longer holds the advisory lock");
        } finally {
            failuresLeft.set(0);
            release.countDown();
            b.close();
        }
    }

    /**
     * A healthy node hammering {@code introspect()} must not flood the log: a lock check
     * that comes out inconclusive, and a confirmation skipped because the previous runner
     * is still alive, are both warned about the first time and then only at doubling
     * counts. Fencing (the error) is unaffected — the inconclusive bound is far away here.
     */
    @Test
    void inconclusive_lock_checks_and_skipped_confirmations_warn_at_doubling_counts() throws Exception {
        String id = "warnthrottle_" + uuid();
        var release = new java.util.concurrent.CountDownLatch(1);
        var blocked = new java.util.concurrent.atomic.AtomicInteger();
        // Short confirmation bound so every check is inconclusive fast; the inconclusive
        // bound stays far off, so the node keeps its leadership throughout.
        var b = new PostgresLogBackend(blockingConfirmations(dataSource, release, blocked),
                id, "fom_log_" + id, 100, 300_000);
        var captured = new ByteArrayOutputStream();
        java.io.PrintStream originalErr = System.err;
        int calls = 12;
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            System.setErr(new java.io.PrintStream(captured, true, java.nio.charset.StandardCharsets.UTF_8));
            for (int i = 0; i < calls; i++) {
                assertThat(b.introspect().currentLeader()).as("still leader after %d checks", i).isEqualTo("A");
            }
        } finally {
            System.setErr(originalErr);
            release.countDown();
            b.close();
        }
        String logged = captured.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(blocked.get()).as("exactly one confirmation runner was started and blocked").isEqualTo(1);
        long inconclusive = countWarnings(logged, "could not verify the advisory lock");
        long skipped = countWarnings(logged, "not starting another");
        assertThat(inconclusive).as("first inconclusive check warns:\n%s", logged).isGreaterThanOrEqualTo(1);
        assertThat(skipped).as("first skipped confirmation warns:\n%s", logged).isGreaterThanOrEqualTo(1);
        // 12 occurrences would be 12 warnings unthrottled; powers of two give 1, 2, 4, 8.
        assertThat(inconclusive).as("inconclusive warnings throttled:\n%s", logged).isLessThanOrEqualTo(5);
        assertThat(skipped).as("skipped-confirmation warnings throttled:\n%s", logged).isLessThanOrEqualTo(5);
        assertThat(logged).as("a check that answered nothing is not reported as '0 ms'")
                .contains("this check answered nothing")
                .doesNotContain("inconclusive for 0 ms");
    }

    /**
     * The thread-exit window: a confirmation runner whose task already answered is still
     * alive for a moment afterwards. It must not count as in flight — a check that treats
     * it as one has no answer, and an answer-less check is evidence towards
     * {@code inconclusiveLimitMillis} and fencing, which a healthy node must not collect.
     * A runner that was really given up on (task cancelled, thread still stuck) must
     * still count, so that stuck threads cannot pile up.
     */
    @Test
    void a_runner_that_answered_is_not_in_flight_but_an_abandoned_stuck_one_is() throws Exception {
        var park = new java.util.concurrent.CountDownLatch(1);
        var answered = new java.util.concurrent.FutureTask<Boolean>(() -> Boolean.TRUE);
        var didAnswer = new java.util.concurrent.CountDownLatch(1);
        Thread finishing = Thread.ofVirtual().start(() -> {
            answered.run();
            didAnswer.countDown();
            awaitIgnoringInterrupts(park); // hold the thread in the window after the answer
        });
        var entered = new java.util.concurrent.CountDownLatch(1);
        var abandoned = new java.util.concurrent.FutureTask<Boolean>(() -> {
            entered.countDown();
            awaitIgnoringInterrupts(park); // a checkout that ignores the interrupt, like an exhausted pool
            return Boolean.TRUE;
        });
        Thread stuck = Thread.ofVirtual().start(abandoned);
        try {
            assertThat(didAnswer.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            abandoned.cancel(true); // what lockHeldPerSeparateSession() does at its deadline

            assertThat(answered.isDone() && finishing.isAlive()).as("the window is set up").isTrue();
            assertThat(PostgresLogBackend.confirmationInFlight(finishing, answered))
                    .as("a runner that already answered is not in flight").isFalse();
            assertThat(stuck.isAlive()).isTrue();
            assertThat(PostgresLogBackend.confirmationInFlight(stuck, abandoned))
                    .as("a runner given up on and still stuck is in flight").isTrue();
        } finally {
            park.countDown();
        }
        finishing.join(TimeUnit.SECONDS.toMillis(10));
        stuck.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(PostgresLogBackend.confirmationInFlight(finishing, answered))
                .as("a terminated runner is not in flight").isFalse();
        assertThat(PostgresLogBackend.confirmationInFlight(stuck, abandoned))
                .as("a stuck runner that finally exited is not in flight").isFalse();
        assertThat(PostgresLogBackend.confirmationInFlight(null, null))
                .as("no runner yet").isFalse();
    }

    /**
     * End-to-end guard for the same thing: a busy health endpoint on a healthy leader
     * must never produce an inconclusive lock check (nor the warning that reports one),
     * and must never cost the node its leadership.
     */
    @Test
    void hammering_introspect_on_a_healthy_leader_records_no_inconclusive_check() throws Exception {
        String id = "hammer_" + uuid();
        var b = new PostgresLogBackend(dataSource, id, "fom_log_" + id, 1_000, 300_000);
        var captured = new ByteArrayOutputStream();
        java.io.PrintStream originalErr = System.err;
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            System.setErr(new java.io.PrintStream(captured, true, java.nio.charset.StandardCharsets.UTF_8));
            var threads = new java.util.ArrayList<Thread>();
            var failures = new java.util.concurrent.ConcurrentLinkedQueue<String>();
            for (int t = 0; t < 4; t++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    for (int i = 0; i < 100; i++) {
                        String leader = b.introspect().currentLeader();
                        if (!"A".equals(leader)) failures.add("leader became " + leader);
                    }
                }));
            }
            for (Thread t : threads) t.join(TimeUnit.SECONDS.toMillis(60));
            assertThat(failures).as("leadership never wavered").isEmpty();
        } finally {
            System.setErr(originalErr);
            b.close();
        }
        String logged = captured.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(countWarnings(logged, "could not verify the advisory lock"))
                .as("no inconclusive check on a healthy node:\n%s", logged).isZero();
        assertThat(countWarnings(logged, "not starting another"))
                .as("no confirmation mistaken for stuck:\n%s", logged).isZero();
    }

    /**
     * The adversarial scenario: statements on the pool's sessions are cancelled while an
     * endpoint hammers {@code introspect()}, so every confirmation fails. That is one
     * warning per check, so it is throttled like the others — and the node, whose own
     * leader session is fine, must keep its leadership and keep writing.
     */
    @Test
    void confirmations_that_keep_failing_warn_at_doubling_counts_and_do_not_fence() throws Exception {
        String id = "failconf_" + uuid();
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        var b = new PostgresLogBackend(failingConfirmations(dataSource, attempts),
                id, "fom_log_" + id, 1_000, 300_000);
        var captured = new ByteArrayOutputStream();
        java.io.PrintStream originalErr = System.err;
        int calls = 12;
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            System.setErr(new java.io.PrintStream(captured, true, java.nio.charset.StandardCharsets.UTF_8));
            for (int i = 0; i < calls; i++) {
                assertThat(b.introspect().currentLeader())
                        .as("a failing confirmation does not fence the leader (check %d)", i).isEqualTo("A");
            }
            assertThat(b.append(new LogInitialized(0, 2L, "P", Map.of()), "A"))
                    .as("the leader session is healthy: writes still go through").isPresent();
        } finally {
            System.setErr(originalErr);
            b.close();
        }
        String logged = captured.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(attempts.get()).as("every check really tried a confirmation").isGreaterThanOrEqualTo(calls);
        long failed = countWarnings(logged, "advisory lock confirmation from a separate session failed");
        assertThat(failed).as("the first failure warns:\n%s", logged).isGreaterThanOrEqualTo(1);
        assertThat(failed).as("repeats are throttled to doubling counts:\n%s", logged).isLessThanOrEqualTo(5);
        assertThat(logged).as("the cause stays in the message").contains("canceling statement");
    }

    /**
     * Same for a confirmation that hangs past its budget on every check: one timeout
     * warning per check, throttled. The runner here finishes on its own afterwards, so
     * the next check really starts a new confirmation instead of being skipped.
     */
    @Test
    void confirmations_that_keep_timing_out_warn_at_doubling_counts() throws Exception {
        String id = "slowconf_" + uuid();
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        // 50 ms budget, a confirmation that needs 80 ms: it misses the budget, then finishes
        // and its thread exits, so the next check starts a fresh one instead of skipping it.
        var b = new PostgresLogBackend(slowConfirmations(dataSource, 80, attempts),
                id, "fom_log_" + id, 50, 300_000);
        var captured = new ByteArrayOutputStream();
        java.io.PrintStream originalErr = System.err;
        int calls = 10;
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            System.setErr(new java.io.PrintStream(captured, true, java.nio.charset.StandardCharsets.UTF_8));
            for (int i = 0; i < calls; i++) {
                assertThat(b.introspect().currentLeader())
                        .as("a slow confirmation does not fence the leader (check %d)", i).isEqualTo("A");
                Thread.sleep(60); // let the abandoned runner finish, as a polling endpoint would
            }
        } finally {
            System.setErr(originalErr);
            b.close();
        }
        String logged = captured.toString(java.nio.charset.StandardCharsets.UTF_8);
        long timedOut = countWarnings(logged, "did not complete within");
        assertThat(attempts.get()).as("each check really started its own confirmation").isGreaterThanOrEqualTo(5);
        assertThat(timedOut).as("the first timeout warns:\n%s", logged).isGreaterThanOrEqualTo(1);
        assertThat(timedOut).as("repeats are throttled to doubling counts:\n%s", logged).isLessThanOrEqualTo(5);
    }

    /**
     * Wraps {@code delegate} so that a lock confirmation's {@code getConnection} fails the
     * way a session whose statements are being cancelled does ({@code pg_cancel_backend}).
     */
    private static DataSource failingConfirmations(DataSource delegate,
                                                   java.util.concurrent.atomic.AtomicInteger attempts) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    if (dsMethod.getName().equals("getConnection")
                            && Thread.currentThread().getName().startsWith("fom-pg-lock-confirm-")) {
                        attempts.incrementAndGet();
                        throw new SQLException("ERROR: canceling statement due to user request", "57014");
                    }
                    return invokeUnwrapped(dsMethod, delegate, dsArgs);
                });
    }

    /**
     * Wraps {@code delegate} so that a lock confirmation takes {@code millis} before it gets
     * its connection, ignoring interrupts — long enough to miss its budget, short enough that
     * the runner then finishes and its thread exits.
     */
    private static DataSource slowConfirmations(DataSource delegate, long millis,
                                                java.util.concurrent.atomic.AtomicInteger attempts) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    if (dsMethod.getName().equals("getConnection")
                            && Thread.currentThread().getName().startsWith("fom-pg-lock-confirm-")) {
                        attempts.incrementAndGet();
                        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
                        boolean interrupted = false;
                        for (long left; (left = until - System.nanoTime()) > 0; ) {
                            try {
                                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(left) + 1);
                            } catch (InterruptedException e) {
                                interrupted = true;
                            }
                        }
                        Object c = invokeUnwrapped(dsMethod, delegate, dsArgs);
                        if (interrupted) Thread.currentThread().interrupt();
                        return c;
                    }
                    return invokeUnwrapped(dsMethod, delegate, dsArgs);
                });
    }

    /**
     * Closing is the documented way to hand leadership over, so an operation that loses the
     * race with it must read as "this backend is closed" and nothing else: no executor
     * rejection attached, whose lambda identity and thread-pool state dump observers such as
     * {@code ProcessFSM} would render into the operator-visible line. {@code purgeArchives}
     * reaches the leader executor without a prior open check, so it takes exactly that path.
     */
    @Test
    void an_operation_rejected_by_the_closed_leader_executor_reports_only_that_it_is_closed() throws Exception {
        String id = "closedmsg_" + uuid();
        var b = new PostgresLogBackend(dataSource, id);
        b.append(new LogLeader(0, 1L, "A"), "A");
        b.close();

        assertThatThrownBy(() -> b.purgeArchives(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("PostgresLogBackend " + id + " is closed")
                .hasNoCause();
    }

    /**
     * A snapshot plan's clocks come from the log's last clock, so a takeover between planning
     * and {@code compact} makes them stale. The honest complaint is then the takeover, not the
     * plan: a deposed leader must get {@link LeadershipLostException}, not the
     * {@code IllegalArgumentException} about non-increasing clocks that the stale plan would
     * otherwise trigger. The in-transaction check stays authoritative, and the pre-check uses
     * its predicate, so a log nobody has claimed yet and the current leader still compact.
     */
    @Test
    void a_deposed_leader_with_a_stale_plan_is_told_it_lost_leadership_not_that_its_clocks_are_wrong()
            throws Exception {
        String id = "stale_" + uuid();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            // A log nobody has claimed yet compacts: the pre-check must not stand in the way.
            assertThat(b.compact(List.of(new LogLeader(0, 1L, "A")), "A").eventsCopied()).isEqualTo(1);
            b.append(new LogInitialized(1, 2L, "P", Map.of()), "A");
            // The current leader still compacts.
            assertThat(b.compact(List.of(new LogLeader(0, 3L, "A"),
                    new LogInitialized(1, 4L, "P", Map.of())), "A").eventsCopied()).isEqualTo(2);

            // B takes the log over, which is what a real cross-instance takeover leaves behind.
            b.append(new LogLeader(2, 5L, "B"), "B");

            // A's plan was built before the takeover: its clocks restart at 0 and do not increase.
            List<LogEvent> stalePlan = List.of(new LogLeader(0, 6L, "A"), new LogInitialized(0, 7L, "P", Map.of()));
            assertThatThrownBy(() -> io.fom.log.LogClocks.requireIncreasingClocks(stalePlan))
                    .as("the plan really is the kind that would be reported as bad clocks")
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> b.compact(stalePlan, "A"))
                    .isInstanceOf(LeadershipLostException.class)
                    .hasMessageContaining("is no longer the leader (B is)")
                    .hasMessageContaining("nothing was written");

            // Nothing was written: B's log is intact.
            assertThat(b.length()).isEqualTo(3);
            assertThat(b.introspect().currentLeader()).isEqualTo("B");
        }
    }

    /**
     * A busy health endpoint on a healthy database: many callers overlap on a backend that
     * has not read a report yet. Every one of them must get the real length — not the
     * {@code -1} of "never read" — with no warning telling the operator to fix a database
     * that is fine, and the database must see one report query at a time, however many
     * callers there are.
     */
    @Test
    void concurrent_introspect_calls_on_a_healthy_database_all_get_a_real_report() throws Exception {
        String id = "concintro_" + uuid();
        var tracker = new ReportQueryTracker();
        var b = new PostgresLogBackend(trackingReportQueries(dataSource, tracker), id);
        var captured = new ByteArrayOutputStream();
        java.io.PrintStream originalErr = System.err;
        int callers = 32;
        var lengths = new java.util.concurrent.ConcurrentLinkedQueue<Integer>();
        var leaders = new java.util.concurrent.ConcurrentLinkedQueue<String>();
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(1, 2L, "P", Map.of()), "A");
            System.setErr(new java.io.PrintStream(captured, true, java.nio.charset.StandardCharsets.UTF_8));
            var gate = new java.util.concurrent.CountDownLatch(1);
            var threads = new java.util.ArrayList<Thread>();
            for (int t = 0; t < callers; t++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    awaitIgnoringInterrupts(gate);
                    var report = b.introspect();
                    lengths.add(report.length());
                    leaders.add(String.valueOf(report.currentLeader()));
                }));
            }
            gate.countDown(); // all at once
            for (Thread t : threads) t.join(TimeUnit.SECONDS.toMillis(30));
        } finally {
            System.setErr(originalErr);
            b.close();
        }
        String logged = captured.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(lengths).as("every caller answered").hasSize(callers);
        assertThat(lengths).as("every caller got the real length, none the -1 of 'never read'").containsOnly(2);
        assertThat(leaders).containsOnly("A");
        assertThat(logged).as("no warning about a database that is fine:\n%s", logged)
                .doesNotContain("could not read the database")
                .doesNotContain("still stuck");
        assertThat(tracker.maxConcurrent.get())
                .as("one report query at a time, not one per caller").isEqualTo(1);
        assertThat(tracker.started.get())
                .as("callers shared queries instead of each starting one").isLessThan(callers);
    }

    /**
     * What the old bound was for still holds: callers overlapping on a database that does not
     * answer are bounded in time, keep the leadership and the last known values, and tie up a
     * single report thread between them — the stuck query is shared, not multiplied.
     */
    @Test
    void concurrent_introspect_calls_on_a_stuck_database_tie_up_one_query_and_stay_bounded() throws Exception {
        String id = "concstuck_" + uuid();
        var tracker = new ReportQueryTracker();
        var b = new PostgresLogBackend(trackingReportQueries(dataSource, tracker), id);
        int callers = 16;
        try {
            b.append(new LogLeader(0, 1L, "A"), "A");
            assertThat(b.introspect().length()).as("a first report is known").isEqualTo(1);

            tracker.block(); // report queries now hang in the checkout, ignoring interrupts
            var reports = new java.util.concurrent.ConcurrentLinkedQueue<io.fom.log.LogBackendReport>();
            var gate = new java.util.concurrent.CountDownLatch(1);
            var threads = new java.util.ArrayList<Thread>();
            long start = System.nanoTime();
            for (int t = 0; t < callers; t++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    awaitIgnoringInterrupts(gate);
                    reports.add(b.introspect());
                }));
            }
            gate.countDown();
            for (Thread t : threads) t.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(Duration.ofNanos(System.nanoTime() - start))
                    .as("bounded in time")
                    .isLessThan(Duration.ofMillis(PostgresLogBackend.INTROSPECT_TIMEOUT_MILLIS + 3_000));
            assertThat(reports).hasSize(callers);
            assertThat(reports).allSatisfy(r -> {
                assertThat(r.length()).as("last known length").isEqualTo(1);
                assertThat(r.currentLeader()).as("a stuck report query never fences").isEqualTo("A");
            });
            assertThat(tracker.blocked.get()).as("one stuck report thread, not one per caller").isEqualTo(1);

            // The database comes back: the stuck query finishes, and reports are fresh again.
            tracker.release();
            b.append(new LogInitialized(1, 2L, "P", Map.of()), "A");
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(b.introspect().length()).isEqualTo(2));
            assertThat(tracker.maxConcurrent.get()).as("never more than one report query at a time").isEqualTo(1);
        } finally {
            tracker.release();
            b.close();
        }
    }

    /** Counts report queries ({@code fom-pg-introspect-*} threads) and can make them hang. */
    private static final class ReportQueryTracker {
        final java.util.concurrent.atomic.AtomicInteger started = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger active = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger maxConcurrent = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger blocked = new java.util.concurrent.atomic.AtomicInteger();
        private volatile java.util.concurrent.CountDownLatch gate; // null: pass through

        void block() {
            gate = new java.util.concurrent.CountDownLatch(1);
        }

        void release() {
            var g = gate;
            gate = null;
            if (g != null) g.countDown();
        }
    }

    /**
     * Wraps {@code delegate} so that every report query's connection — from checkout until
     * {@code close()} — is counted in {@code tracker}, and hangs in the checkout (ignoring
     * interrupts, like an exhausted pool) while the tracker is blocked.
     */
    private static DataSource trackingReportQueries(DataSource delegate, ReportQueryTracker tracker) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    if (!dsMethod.getName().equals("getConnection")
                            || !Thread.currentThread().getName().startsWith("fom-pg-introspect-")) {
                        return invokeUnwrapped(dsMethod, delegate, dsArgs);
                    }
                    tracker.started.incrementAndGet();
                    tracker.maxConcurrent.accumulateAndGet(tracker.active.incrementAndGet(), Math::max);
                    boolean handedOut = false;
                    try {
                        var g = tracker.gate;
                        if (g != null) {
                            tracker.blocked.incrementAndGet();
                            awaitIgnoringInterrupts(g);
                        }
                        Connection conn = (Connection) invokeUnwrapped(dsMethod, delegate, dsArgs);
                        var closed = new java.util.concurrent.atomic.AtomicBoolean();
                        Object proxy = Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                    if (cMethod.getName().equals("close") && closed.compareAndSet(false, true)) {
                                        tracker.active.decrementAndGet();
                                    }
                                    return invokeUnwrapped(cMethod, conn, cArgs);
                                });
                        handedOut = true;
                        return proxy;
                    } finally {
                        if (!handedOut) tracker.active.decrementAndGet();
                    }
                });
    }

    /** WARN lines in captured slf4j-simple output that mention {@code needle}. */
    private static long countWarnings(String logged, String needle) {
        return logged.lines().filter(l -> l.contains("WARN") && l.contains(needle)).count();
    }

    @Test
    void a_table_name_taken_by_another_logs_sequence_is_refused() throws Exception {
        String id = "seq_" + uuid().substring(0, 8);
        try (var owner = new PostgresLogBackend(dataSource, id)) {
            assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id + "_rid_seq"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("non-table");
            assertThat(owner.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
        }
    }

    @Test
    void a_log_whose_sequence_name_is_another_logs_table_is_refused_cleanly() throws Exception {
        String id = "sq" + uuid().substring(0, 8);
        // Log '<id>_rid_seq' owns table fom_log_<id>_rid_seq: exactly the sequence name log '<id>' needs.
        try (var other = new PostgresLogBackend(dataSource, id + "_rid_seq")) {
            assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fom_log_" + id + "_rid_seq")
                    .hasMessageContaining("sequence");
            assertThat(relationExists("fom_log_" + id)).as("nothing half-created").isFalse();
            assertThat(other.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
        }
    }

    @Test
    void a_log_whose_primary_key_name_is_another_logs_table_is_refused_cleanly() throws Exception {
        String id = "pk" + uuid().substring(0, 8);
        try (var other = new PostgresLogBackend(dataSource, id + "_rid_pk")) {
            assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fom_log_" + id + "_rid_pk")
                    .hasMessageContaining("primary-key");
            assertThat(relationExists("fom_log_" + id)).as("nothing half-created").isFalse();
            assertThat(relationExists("fom_log_" + id + "_rid_seq")).as("sequence rolled back too").isFalse();
            assertThat(other.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
        }
    }

    @Test
    void an_existing_non_log_table_is_refused_with_the_missing_column() throws Exception {
        String table = "customers_" + uuid().substring(0, 8);
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT)");
        }
        for (int i = 0; i < 2; i++) { // twice: the refused open released its advisory lock
            assertThatThrownBy(() -> new PostgresLogBackend(dataSource, "cust", table))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("table " + table + " exists but is not a fom log table")
                    .hasMessageContaining("missing column row_id");
        }
    }

    @Test
    void an_existing_table_with_a_log_column_of_the_wrong_type_is_refused() throws Exception {
        String table = "almostlog_" + uuid().substring(0, 8);
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE TABLE " + table
                    + " (row_id BIGSERIAL PRIMARY KEY, type TEXT, payload TEXT, ts_millis BIGINT)");
        }
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, "almost", table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a fom log table")
                .hasMessageContaining("payload is text, expected bytea");
    }

    @Test
    void a_system_catalog_reached_through_the_search_path_is_refused() {
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, "cat", "pg_class"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pg_catalog.pg_class");
    }

    /**
     * A range read of several large events must not be one statement: under a server
     * {@code statement_timeout} that every single append met, one SELECT of them all can
     * still time out and the log could never be reopened. Payload bytes per statement are
     * bounded by {@link PostgresLogBackend#READ_BATCH_BYTES} (a larger single row alone).
     */
    @Test
    void range_read_fetches_a_bounded_number_of_payload_bytes_per_statement() throws Exception {
        String id = "bigrange_" + uuid();
        var perStatement = new java.util.concurrent.ConcurrentLinkedQueue<Long>();
        var events = new java.util.ArrayList<LogEvent>();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            events.add(b.append(new LogLeader(0, 1L, "A"), "A").orElseThrow());
            for (int i = 0; i < 5; i++) { // 5 x 3 MB = 15 MB, well over the 8 MiB bound
                byte[] blob = new byte[3_000_000];
                blob[i] = (byte) (i + 1);
                events.add(b.append(new LogInitialized(0, 2L + i, "P" + i, Map.of("k", blob)), "A").orElseThrow());
            }
        }
        try (var b = new PostgresLogBackend(countingPayloadBytes(dataSource, perStatement), id)) {
            perStatement.clear();
            LogEvent[] read = b.getBetween(0, events.size());
            assertThat(read).hasSize(events.size());
            for (int i = 1; i < read.length; i++) {
                var init = (LogInitialized) read[i];
                assertThat(init.processName()).isEqualTo("P" + (i - 1));
                assertThat(init.properties().get("k")[i - 1]).isEqualTo((byte) i);
            }
            assertThat(perStatement).as("payload bytes per SELECT").isNotEmpty()
                    .allSatisfy(bytes -> assertThat(bytes).isLessThanOrEqualTo(PostgresLogBackend.READ_BATCH_BYTES));
            assertThat(perStatement.size()).as("more than one statement").isGreaterThan(1);
        }
    }

    /**
     * A streamed scan ({@code forEachBetween}, used by the startup scan and compaction) decodes
     * at most {@link PostgresLogBackend#READ_BATCH_BYTES} of payload ahead of the event it hands
     * out, not a whole 1,000-event batch, so its heap follows the largest record, not the log.
     */
    @Test
    void streamed_scan_holds_a_bounded_number_of_payload_bytes() throws Exception {
        String id = "bigscan_" + uuid();
        var perStatement = new java.util.concurrent.ConcurrentLinkedQueue<Long>();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            for (int i = 0; i < 5; i++) { // 5 x 3 MB = 15 MB, well over the 8 MiB bound
                byte[] blob = new byte[3_000_000];
                blob[i] = (byte) (i + 1);
                b.append(new LogInitialized(0, 2L + i, "P" + i, Map.of("k", blob)), "A");
            }
        }
        try (var b = new PostgresLogBackend(countingPayloadBytes(dataSource, perStatement), id)) {
            perStatement.clear();
            var fetchedWhenHandedOut = new java.util.ArrayList<Long>();
            var seen = new java.util.ArrayList<String>();
            b.forEachBetween(0, b.length(), event -> {
                fetchedWhenHandedOut.add(perStatement.stream().mapToLong(Long::longValue).sum());
                seen.add(event instanceof LogInitialized init
                        ? init.processName() + ":" + init.properties().get("k")[Integer.parseInt(init.processName().substring(1))]
                        : event.getClass().getSimpleName());
            });
            assertThat(seen).containsExactly("LogLeader", "P0:1", "P1:2", "P2:3", "P3:4", "P4:5");
            assertThat(fetchedWhenHandedOut.get(1)).as("payload bytes fetched when the first big event is handed out")
                    .isLessThanOrEqualTo(PostgresLogBackend.READ_BATCH_BYTES);
            assertThat(perStatement).allSatisfy(bytes ->
                    assertThat(bytes).isLessThanOrEqualTo(PostgresLogBackend.READ_BATCH_BYTES));
            assertThatThrownBy(() -> b.forEachBetween(0, b.length() + 1, e -> { }))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    /**
     * A statement that is cancelled anyway (a {@code statement_timeout} tighter than the
     * byte bound allows for, 57014) is retried in halves down to single rows, which are
     * as readable as they were writable. Simulated: every statement handing out a second
     * payload is cancelled.
     */
    @Test
    void a_cancelled_range_read_is_retried_in_halves_down_to_single_rows() throws Exception {
        String id = "cancelrange_" + uuid();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            for (int i = 0; i < 6; i++) {
                b.append(new LogInitialized(0, 2L + i, "P" + i, Map.of()), "A");
            }
        }
        var cancelled = new java.util.concurrent.atomic.AtomicInteger();
        try (var b = new PostgresLogBackend(cancellingMultiRowReads(dataSource, cancelled), id)) {
            LogEvent[] read = b.getBetween(0, 7);
            assertThat(read).hasSize(7);
            assertThat(read[0]).isInstanceOf(LogLeader.class);
            for (int i = 1; i < 7; i++) {
                assertThat(((LogInitialized) read[i]).processName()).isEqualTo("P" + (i - 1));
            }
            assertThat(cancelled.get()).as("cancellations injected").isPositive();
        }
    }

    @Test
    void a_reserved_word_is_refused_as_a_table_name() {
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, "kw", "order"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
    }

    @Test
    void a_snapshot_waiting_for_a_busy_table_does_not_block_readers() throws Exception {
        String id = "busy_" + uuid();
        try (var b = new PostgresLogBackend(dataSource, id);
             var reader = dataSource.getConnection()) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            reader.setAutoCommit(false);
            try (var st = reader.createStatement()) {
                st.executeQuery("SELECT count(*) FROM fom_log_" + id).close(); // holds ACCESS SHARE until commit
            }
            var compaction = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> b.compact(List.of(new LogLeader(0, 2L, "A")), "A"));
            Thread.sleep(300); // compact is now retrying for the table lock
            long start = System.nanoTime();
            assertThat(b.introspect().length()).isEqualTo(1);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
            reader.commit();
            assertThat(compaction.get(10, java.util.concurrent.TimeUnit.SECONDS).eventsCopied()).isEqualTo(1);
        }
    }

    @Test
    void log_ids_that_would_share_a_table_are_rejected() {
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, "orders-eu"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orders-eu");
    }

    // ───────────────── regression: archive table name length ─────────────────

    @Test
    void repeated_compaction_with_long_table_name_uses_unique_archive_names_within_63_bytes() throws Exception {
        String id = "l" + uuid() + "_" + "y".repeat(12); // table name = 54 chars
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            Set<String> names = new HashSet<>();
            for (int i = 0; i < 3; i++) { // back-to-back: may land in the same millisecond
                SnapshotResult r = b.compact(List.of(new LogLeader(0, 10L + i, "A")), "A");
                assertThat(r.archivedLogId().length()).isLessThanOrEqualTo(63);
                assertThat(relationExists(r.archivedLogId())).isTrue();
                names.add(r.archivedLogId());
            }
            assertThat(names).hasSize(3);
            assertThat(b.length()).isEqualTo(1);
            assertThat(b.append(new LogInitialized(0, 20L, "Foo", Map.of("k", new byte[]{1})), "A")).isPresent();
        }
    }

    @Test
    void archive_table_name_is_bounded_and_distinguishes_long_names() {
        long stamp = 1_789_244_808_171L;
        assertThat(PostgresLogBackend.archiveTableName("fom_log_short", stamp))
                .isEqualTo("fom_log_short_archived_" + stamp);

        String base = "fom_log_" + "x".repeat(54); // 62 chars
        String n1 = PostgresLogBackend.archiveTableName(base + "a", stamp);
        String n2 = PostgresLogBackend.archiveTableName(base + "b", stamp);
        assertThat(n1).hasSizeLessThanOrEqualTo(63).endsWith("_archived_" + stamp);
        assertThat(n2).hasSizeLessThanOrEqualTo(63).endsWith("_archived_" + stamp);
        assertThat(n1).isNotEqualTo(n2);
        assertThat(PostgresLogBackend.archiveTableName(base + "a", stamp + 1)).isNotEqualTo(n1);
    }

    // ───────────────── regression: a health check must not fence the leader ─────────────────

    @Test
    void a_health_check_during_a_database_stall_does_not_fence_the_leader() throws Exception {
        String id = "stallfence_" + uuid();
        var leaderSessionDown = new java.util.concurrent.atomic.AtomicBoolean();
        var poolDown = new java.util.concurrent.atomic.AtomicBoolean();
        var injected = new java.util.concurrent.atomic.AtomicInteger();
        DataSource ds = unreachableServer(dataSource, leaderSessionDown, poolDown, injected);
        try (var b = new PostgresLogBackend(ds, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");

            // The server stops answering: anything the backend sends on its leader session
            // now fails at the driver's socket timeout, which is indistinguishable from a
            // session the server terminated. One health check must not cost the leadership.
            leaderSessionDown.set(true);
            for (int i = 0; i < 2; i++) {
                poolDown.set(i == 1); // first only the leader session, then the whole server
                long start = System.nanoTime();
                var report = java.util.concurrent.CompletableFuture.supplyAsync(b::introspect)
                        .get(20, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(report.currentLeader())
                        .as("a read-only health check must not fence the node (pool down: %s)", poolDown.get())
                        .isEqualTo("A");
                assertThat(report.length()).isEqualTo(1);
                assertThat(Duration.ofNanos(System.nanoTime() - start))
                        .isLessThan(Duration.ofMillis(PostgresLogBackend.INTROSPECT_TIMEOUT_MILLIS + 3_000));
            }
            assertThat(injected.get())
                    .as("introspect() checked the lock without sending anything on the leader session")
                    .isZero();

            // The stall ends; the leader session was never touched, so the node just carries on.
            leaderSessionDown.set(false);
            poolDown.set(false);
            assertThat(b.append(new LogInitialized(0, 2L, "P", Map.of()), "A")).isPresent();
            assertThat(b.introspect().currentLeader()).isEqualTo("A");
            assertThat(b.length()).isEqualTo(2);
            assertThatThrownBy(() -> new PostgresLogBackend(dataSource, id))
                    .as("the advisory lock is still held, so no standby can take over")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Could not acquire advisory lock");
        } finally {
            leaderSessionDown.set(false);
            poolDown.set(false);
        }
    }

    // ───────────────── regression: schemas, last known length, read retries ─────────────────

    @Test
    void the_same_log_id_in_two_schemas_is_two_logs_each_with_its_own_leader() throws Exception {
        String id = "schemas_" + uuid();
        String sa = "sa_" + uuid();
        String sb = "sb_" + uuid();
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE SCHEMA " + sa);
            st.execute("CREATE SCHEMA " + sb);
        }
        // Distinct tables (sa.fom_log_<id>, sb.fom_log_<id>): the lock key must tell them apart.
        try (var a = new PostgresLogBackend(inSchema(sa), id);
             var b = new PostgresLogBackend(inSchema(sb), id)) {
            a.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogLeader(0, 1L, "B"), "B");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "B");
            assertThat(a.length()).isEqualTo(1);
            assertThat(b.length()).isEqualTo(2);
            assertThat(a.introspect().currentLeader()).isEqualTo("A");
            assertThat(b.introspect().currentLeader()).isEqualTo("B");
            assertThatThrownBy(() -> new PostgresLogBackend(inSchema(sa), id))
                    .as("the same log in the same schema is still refused")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Could not acquire advisory lock");
        }
    }

    @Test
    void a_search_path_naming_no_existing_schema_is_refused_with_a_clear_error() {
        assertThatThrownBy(() -> new PostgresLogBackend(inSchema("no_such_schema_" + uuid()), "nsp_" + uuid()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("current_schema() is null")
                .hasMessageContaining("currentSchema");
    }

    @Test
    void introspect_on_an_unreachable_database_reports_the_last_known_length() throws Exception {
        String id = "lastknown_" + uuid();
        var poolDown = new java.util.concurrent.atomic.AtomicBoolean();
        DataSource ds = unreachableServer(dataSource, new java.util.concurrent.atomic.AtomicBoolean(), poolDown,
                new java.util.concurrent.atomic.AtomicInteger());
        try (var b = new PostgresLogBackend(ds, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            assertThat(b.introspect().length()).isEqualTo(1);
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
            b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A");

            poolDown.set(true);
            var report = CompletableFuture.supplyAsync(b::introspect).get(20, TimeUnit.SECONDS);
            assertThat(report.length())
                    .as("the length this instance knows (its own appends), not the last introspect()'s")
                    .isEqualTo(3);
            assertThat(report.currentLeader()).isEqualTo("A");
        } finally {
            poolDown.set(false);
        }
    }

    @Test
    void a_read_whose_pooled_session_was_terminated_is_retried_on_a_fresh_connection() throws Exception {
        String id = "readreset_" + uuid();
        String app = "rr_" + uuid();
        var pool = new ReusingPool(app);
        try (var b = new PostgresLogBackend(pool, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
            assertThat(b.getBetween(0, 2)).hasSize(2); // leaves an idle pooled read session

            terminateReadSessions(app);
            assertThat(b.getBetween(0, 2)).hasSize(2);
            terminateReadSessions(app);
            assertThat(b.length()).isEqualTo(2);
            terminateReadSessions(app);
            assertThat(b.get(1)).isInstanceOf(LogInitialized.class);

            // The leader session was left alone: the node still leads.
            assertThat(b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A")).isPresent();
        } finally {
            pool.closeAll();
        }
    }

    @Test
    void a_read_that_keeps_failing_reports_the_cause_and_its_sqlstate() throws Exception {
        String id = "readfail_" + uuid();
        var broken = new java.util.concurrent.atomic.AtomicBoolean();
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        DataSource ds = wrappingResultSets(dataSource, rs -> {
            if (!broken.get()) return rs;
            attempts.incrementAndGet();
            return (java.sql.ResultSet) Proxy.newProxyInstance(java.sql.ResultSet.class.getClassLoader(),
                    new Class<?>[]{java.sql.ResultSet.class}, (rProxy, rMethod, rArgs) -> {
                        if (rMethod.getName().equals("next")) {
                            throw new SQLException("FATAL: terminating connection due to administrator command",
                                    "57P01");
                        }
                        return invokeUnwrapped(rMethod, rs, rArgs);
                    });
        });
        try (var b = new PostgresLogBackend(ds, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            broken.set(true);
            assertThatThrownBy(() -> b.getBetween(0, 1))
                    .hasMessageContaining("getBetween(0,1) failed on fom_log_" + id)
                    .hasMessageContaining("terminating connection due to administrator command")
                    .hasMessageContaining("SQLState 57P01");
            assertThat(attempts.get()).as("retried exactly once").isEqualTo(2);
        } finally {
            broken.set(false);
        }
    }

    private static DataSource inSchema(String schema) {
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        ds.setCurrentSchema(schema);
        return ds;
    }

    /** Terminates every session of {@code app} but the leader's (the one holding an advisory lock). */
    private static void terminateReadSessions(String app) throws SQLException {
        try (var c = dataSource.getConnection();
             var st = c.prepareStatement("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity"
                     + " WHERE application_name = ? AND pid NOT IN"
                     + " (SELECT pid FROM pg_locks WHERE locktype = 'advisory')")) {
            st.setString(1, app);
            try (var rs = st.executeQuery()) {
                rs.next();
                assertThat(rs.getLong(1)).as("idle read sessions terminated").isPositive();
            }
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> sessionsNamed(app) == 1);
    }

    // ───────────────── regression: concurrent compaction of long, similar table names ─────────────────

    @Test
    void two_logs_with_a_long_shared_table_name_prefix_can_compact_at_the_same_time() throws Exception {
        // 57-char names agreeing on the first 56 characters: Postgres' implicit names for a
        // BIGSERIAL primary key truncate the table name and become the same for both.
        String base = ("fom_log_p" + uuid() + uuid()).substring(0, 56);
        assertThat(PostgresLogBackend.sequenceName(base + "a"))
                .as("the sequence names the backend picks stay distinct")
                .isNotEqualTo(PostgresLogBackend.sequenceName(base + "b"));

        var barrier = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.CyclicBarrier>();
        var target = new java.util.concurrent.atomic.AtomicReference<String>();
        DataSource ds = createsInLockstep(dataSource, barrier, target);
        try (var a = new PostgresLogBackend(ds, "sharedpfx_a_" + uuid(), base + "a");
             var b = new PostgresLogBackend(ds, "sharedpfx_b_" + uuid(), base + "b")) {
            a.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogLeader(0, 1L, "B"), "B");
            for (int round = 0; round < 3; round++) {
                // Both logs create their new relation at the same instant.
                barrier.set(new java.util.concurrent.CyclicBarrier(2,
                        () -> target.set(String.valueOf((System.currentTimeMillis() + 400) / 1000.0))));
                long clock = 10L + round;
                var ca = java.util.concurrent.CompletableFuture.supplyAsync(
                        () -> a.compact(List.of(new LogLeader(0, clock, "A")), "A"));
                var cb = java.util.concurrent.CompletableFuture.supplyAsync(
                        () -> b.compact(List.of(new LogLeader(0, clock, "B")), "B"));
                assertThat(ca.get(30, java.util.concurrent.TimeUnit.SECONDS).eventsCopied())
                        .as("round %d of log A", round).isEqualTo(1);
                assertThat(cb.get(30, java.util.concurrent.TimeUnit.SECONDS).eventsCopied())
                        .as("round %d of log B", round).isEqualTo(1);
            }
            barrier.set(null);
            // Both logs are intact, independent and still writable.
            assertThat(a.append(new LogInitialized(0, 20L, "Foo", Map.of("k", new byte[]{1})), "A")).isPresent();
            assertThat(b.append(new LogInitialized(0, 20L, "Bar", Map.of("k", new byte[]{2})), "B")).isPresent();
            assertThat(a.length()).isEqualTo(2);
            assertThat(b.length()).isEqualTo(2);
            assertThat(((LogLeader) a.get(0)).instanceId()).isEqualTo("A");
            assertThat(((LogLeader) b.get(0)).instanceId()).isEqualTo("B");
            a.purgeArchives(0);
            b.purgeArchives(0);
            assertThat(relationsLike(base.substring(0, 20) + "%"))
                    .as("each log keeps only its table, sequence, primary-key and LogLeader indexes; "
                            + "a dropped archive takes its own with it")
                    .containsExactlyInAnyOrder(base + "a", base + "b",
                            PostgresLogBackend.sequenceName(base + "a"),
                            PostgresLogBackend.sequenceName(base + "b"),
                            PostgresLogBackend.primaryKeyName(base + "a"),
                            PostgresLogBackend.primaryKeyName(base + "b"),
                            PostgresLogBackend.leaderIndexName(base + "a"),
                            PostgresLogBackend.leaderIndexName(base + "b"));
        } finally {
            barrier.set(null);
        }
    }

    @Test
    void a_table_created_by_an_earlier_version_keeps_working_and_compacts() throws Exception {
        String id = "legacy_" + uuid();
        String table = "fom_log_" + id;
        // The shape an earlier version created: BIGSERIAL, so the sequence and the primary-key
        // index carry Postgres' implicit names, not the ones the backend derives today.
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE TABLE " + table + " (row_id BIGSERIAL PRIMARY KEY, type TEXT NOT NULL,"
                    + " payload BYTEA NOT NULL, ts_millis BIGINT NOT NULL)");
        }
        try (var b = new PostgresLogBackend(dataSource, id)) {
            assertThat(b.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(b.append(new LogInitialized(0, 2L, "P", Map.of()), "A")).isPresent();
            assertThat(relationsLike(table + "_row_id_seq")).as("still the old sequence").hasSize(1);

            assertThat(b.compact(List.of(new LogLeader(0, 3L, "A")), "A").eventsCopied()).isEqualTo(1);
            assertThat(b.append(new LogInitialized(0, 4L, "Q", Map.of()), "A")).isPresent();
            assertThat(b.length()).isEqualTo(2);
            assertThat(((LogLeader) b.get(0)).instanceId()).isEqualTo("A");

            b.purgeArchives(0);
            assertThat(relationsLike(table + "%"))
                    .as("the archive took the old sequence and index with it")
                    .containsExactlyInAnyOrder(table,
                            PostgresLogBackend.sequenceName(table), PostgresLogBackend.primaryKeyName(table),
                            PostgresLogBackend.leaderIndexName(table));
        }
    }

    @Test
    void derived_object_names_are_bounded_and_distinguish_long_names() {
        assertThat(PostgresLogBackend.sequenceName("fom_log_short")).isEqualTo("fom_log_short_rid_seq");
        assertThat(PostgresLogBackend.primaryKeyName("fom_log_short")).isEqualTo("fom_log_short_rid_pk");

        String base = "fom_log_" + "x".repeat(54); // 62 chars
        for (var name : List.of(PostgresLogBackend.sequenceName(base + "a"),
                PostgresLogBackend.primaryKeyName(base + "a"))) {
            assertThat(name).hasSizeLessThanOrEqualTo(63);
        }
        assertThat(PostgresLogBackend.sequenceName(base + "a"))
                .isNotEqualTo(PostgresLogBackend.sequenceName(base + "b"));
        assertThat(PostgresLogBackend.primaryKeyName(base + "a"))
                .isNotEqualTo(PostgresLogBackend.primaryKeyName(base + "b"));
        assertThat(PostgresLogBackend.sequenceName(base + "a"))
                .isNotEqualTo(PostgresLogBackend.primaryKeyName(base + "a"));
    }

    // ───────────────── regression: a read during a compaction never sees an empty log ─────────────────

    @Test
    void reads_during_repeated_compaction_never_see_an_empty_or_partial_log() throws Exception {
        String id = "readcompact_" + uuid();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of("k", new byte[]{1})), "A");

            var stop = new java.util.concurrent.atomic.AtomicBoolean();
            var failures = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
            var reads = new java.util.concurrent.atomic.AtomicLong();
            var readers = new java.util.ArrayList<Thread>();
            for (int i = 0; i < 4; i++) {
                Thread t = new Thread(() -> {
                    while (!stop.get()) {
                        try {
                            // The log always holds exactly two events, before, during and after
                            // every compaction round: this read can never be out of range.
                            assertThat(b.getBetween(0, 2)).hasSize(2);
                            assertThat(b.get(0)).isInstanceOf(LogLeader.class);
                            reads.incrementAndGet();
                        } catch (Throwable failure) {
                            failures.add(failure);
                        }
                    }
                }, "reader-" + i);
                t.start();
                readers.add(t);
            }
            try {
                for (int round = 0; round < 40; round++) {
                    long clock = 10L + 2L * round;
                    b.compact(List.of(new LogLeader(clock, clock, "A"),
                            new LogInitialized(clock + 1, clock + 1, "P", Map.of("k", new byte[]{1}))), "A");
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) t.join(30_000);
            }
            b.purgeArchives(0);
            assertThat(reads.get()).as("readers did run").isGreaterThan(40);
            assertThat(failures).as("reads that a compaction broke").isEmpty();
        }
    }

    // ───────────────── regression: a deposed leader cannot re-claim the log by compacting ─────────────────

    @Test
    void compact_is_refused_when_the_logs_latest_leader_is_another_instance() throws Exception {
        String id = "deposed_" + uuid();
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of("k", new byte[]{1})), "A");
            assertThat(b.append(new LogLeader(0, 3L, "B"), "B")).as("B takes the log over").isPresent();

            assertThatThrownBy(() -> b.compact(List.of(new LogLeader(10L, 10L, "A")), "A"))
                    .as("a deposed leader must not snapshot its own LogLeader back to the front")
                    .isInstanceOf(LeadershipLostException.class)
                    .hasMessageContaining("no longer the leader");

            // Nothing changed: same events, no archive table left behind.
            assertThat(b.length()).isEqualTo(3);
            assertThat(((LogLeader) b.get(2)).instanceId()).isEqualTo("B");
            assertThat(relationsLike("fom_log_" + id + "_archived_%")).isEmpty();

            // The instance that does hold the log still compacts.
            assertThat(b.compact(List.of(new LogLeader(10L, 10L, "B")), "B").eventsCopied()).isEqualTo(1);
            assertThat(b.length()).isEqualTo(1);
            assertThat(((LogLeader) b.get(0)).instanceId()).isEqualTo("B");
        }
    }

    // ───────────────── regression: a live instance must notice a takeover before it appends ─────────────────

    @Test
    void append_is_refused_when_another_instance_took_the_log_over_behind_our_back() throws Exception {
        String id = "stolen_" + uuid();
        try (var a = new PostgresLogBackend(dataSource, id)) {
            assertThat(a.append(new LogLeader(0, 1L, "inst-A"), "inst-A")).isPresent();
            assertThat(a.append(new LogInitialized(0, 2L, "P", Map.of("k", new byte[]{1})), "inst-A")).isPresent();
            assertThat(a.introspect().currentLeader()).isEqualTo("inst-A");

            // Another instance claims the log without going through this backend (as a node that
            // took over after this one's session looked dead would): the latest LogLeader is its own.
            plantLeaderRow("fom_log_" + id, "inst-B", 7L);

            assertThat(a.append(new LogInitialized(0, 3L, "Q", Map.of("k", new byte[]{2})), "inst-A"))
                    .as("the log's latest LogLeader names inst-B, so inst-A may not append")
                    .isEmpty();
            assertThat(a.length()).as("nothing was written").isEqualTo(3);
            assertThat(a.introspect().currentLeader())
                    .as("a deposed instance stops claiming leadership")
                    .isEqualTo("inst-B");
            // Refused again, now off the refreshed cache, and compact refuses too.
            assertThat(a.append(new LogInitialized(0, 4L, "R", Map.of("k", new byte[]{3})), "inst-A")).isEmpty();
            assertThatThrownBy(() -> a.compact(List.of(new LogLeader(10L, 10L, "inst-A")), "inst-A"))
                    .isInstanceOf(LeadershipLostException.class)
                    .hasMessageContaining("no longer the leader");
            assertThat(a.length()).isEqualTo(3);

            // It may still take the log back the documented way — by appending its own LogLeader,
            // which continues after the clock the log really holds (7, the planted claim's).
            LogEvent reclaim = a.append(new LogLeader(0, 5L, "inst-A"), "inst-A").orElseThrow();
            assertThat(reclaim.clock()).isEqualTo(8L);
            assertThat(a.append(new LogInitialized(0, 6L, "S", Map.of("k", new byte[]{4})), "inst-A")).isPresent();
            assertThat(a.length()).isEqualTo(5);
            assertThat(a.introspect().currentLeader()).isEqualTo("inst-A");
        }
    }

    @Test
    void a_takeover_landing_after_a_compaction_is_still_caught() throws Exception {
        String id = "stolenc_" + uuid();
        try (var a = new PostgresLogBackend(dataSource, id)) {
            a.append(new LogLeader(0, 1L, "inst-A"), "inst-A");
            a.append(new LogInitialized(0, 2L, "P", Map.of("k", new byte[]{1})), "inst-A");
            a.compact(List.of(new LogLeader(5L, 5L, "inst-A")), "inst-A");
            assertThat(a.append(new LogInitialized(0, 6L, "Q", Map.of("k", new byte[]{2})), "inst-A")).isPresent();

            plantLeaderRow("fom_log_" + id, "inst-B", 9L);

            assertThat(a.append(new LogInitialized(0, 10L, "R", Map.of("k", new byte[]{3})), "inst-A")).isEmpty();
            assertThat(a.introspect().currentLeader()).isEqualTo("inst-B");
        }
    }

    /** Writes a {@code LogLeader} row straight into the table, bypassing the backend. */
    private static void plantLeaderRow(String table, String instanceId, long clock) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(bytes)) {
            oos.writeObject(new LogLeader(clock, clock, instanceId));
        }
        try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                "INSERT INTO " + table + " (type, payload, ts_millis) VALUES ('LogLeader', ?, ?)")) {
            st.setBytes(1, bytes.toByteArray());
            st.setLong(2, clock);
            st.executeUpdate();
        }
    }

    // ───────────────── regression: the write fence — a claim still in flight cannot be raced ─────────────────

    /**
     * A second backend on the same table, keyed differently: what a node of a version with
     * another advisory-lock key is. The session lock cannot keep the two apart, so only the
     * write fence stands between them.
     */
    private static PostgresLogBackend otherKeyedBackend(String id) throws SQLException {
        return new PostgresLogBackend(dataSource, id, "fom_log_" + id,
                PostgresLogBackend.CONFIRM_TIMEOUT_MILLIS, PostgresLogBackend.INCONCLUSIVE_LIMIT_MILLIS,
                UUID.randomUUID().getMostSignificantBits());
    }

    /** Parks a write on the leader thread between its INSERT and its COMMIT until released. */
    private static final class Gate implements Runnable {
        final java.util.concurrent.CountDownLatch reached = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);

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

    /** Whether some session is waiting for a lock on {@code table}. */
    private static boolean someoneWaitsToLock(String table) throws SQLException {
        try (var c = dataSource.getConnection(); var st = c.prepareStatement(
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
        List<Long> expected = java.util.stream.LongStream.range(0, clocks.size()).boxed().toList();
        assertThat(clocks).as("clocks: unique, gap-free and increasing in log order").isEqualTo(expected);
    }

    /**
     * The race: another node's {@code LogLeader} is inserted but not committed yet, so under
     * {@code READ COMMITTED} the append guard of the old leader cannot see it. Without a fence
     * the old leader's append takes a higher row id, commits, moves its watermark past the claim
     * and keeps writing for ever. With the fence it waits for the claim, sees it, and is refused.
     */
    @Test
    void an_append_racing_an_uncommitted_takeover_waits_for_it_and_is_refused() throws Exception {
        String id = "race_" + uuid();
        String table = "fom_log_" + id;
        try (var a = new PostgresLogBackend(dataSource, id); var b = otherKeyedBackend(id)) {
            assertThat(a.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(a.append(new LogInitialized(0, 2L, "P", Map.of()), "A")).isPresent();

            Gate gate = new Gate();
            b.beforeCommitHook = gate;
            var claim = CompletableFuture.supplyAsync(() -> b.append(new LogLeader(0, 3L, "B"), "B"));
            assertThat(gate.reached.await(10, TimeUnit.SECONDS)).as("B's claim is inserted, not committed").isTrue();
            b.beforeCommitHook = null;

            var append = CompletableFuture.supplyAsync(() -> a.append(new LogInitialized(0, 4L, "Q", Map.of()), "A"));
            await().atMost(Duration.ofSeconds(10)).until(() -> append.isDone() || someoneWaitsToLock(table));
            gate.release.countDown();

            assertThat(claim.get(10, TimeUnit.SECONDS)).get().extracting(LogEvent::clock).isEqualTo(2L);
            assertThat(append.get(10, TimeUnit.SECONDS))
                    .as("A's append must see B's claim, not slip in before its commit")
                    .isEmpty();
            assertThat(a.introspect().currentLeader()).isEqualTo("B");
            assertThat(a.append(new LogInitialized(0, 5L, "R", Map.of()), "A")).as("and stays refused").isEmpty();
            assertThat(b.append(new LogInitialized(0, 6L, "S", Map.of()), "B")).isPresent();

            assertThat(a.length()).isEqualTo(4);
            assertThat(((LogLeader) a.get(2)).instanceId()).isEqualTo("B");
            assertClocksAreZeroToLength(a);
        }
    }

    /**
     * The other order: the old leader's append is in flight when another node claims. The
     * claim must continue after that append's clock, not reuse it, and the old leader is
     * refused from then on.
     */
    @Test
    void a_takeover_racing_an_uncommitted_append_continues_after_it_without_reusing_its_clock() throws Exception {
        String id = "racec_" + uuid();
        String table = "fom_log_" + id;
        try (var a = new PostgresLogBackend(dataSource, id); var b = otherKeyedBackend(id)) {
            assertThat(a.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(a.append(new LogInitialized(0, 2L, "P", Map.of()), "A")).isPresent();

            Gate gate = new Gate();
            a.beforeCommitHook = gate;
            var append = CompletableFuture.supplyAsync(() -> a.append(new LogInitialized(0, 3L, "Q", Map.of()), "A"));
            assertThat(gate.reached.await(10, TimeUnit.SECONDS)).isTrue();
            a.beforeCommitHook = null;

            var claim = CompletableFuture.supplyAsync(() -> b.append(new LogLeader(0, 4L, "B"), "B"));
            await().atMost(Duration.ofSeconds(10)).until(() -> claim.isDone() || someoneWaitsToLock(table));
            gate.release.countDown();

            assertThat(append.get(10, TimeUnit.SECONDS)).get().extracting(LogEvent::clock).isEqualTo(2L);
            assertThat(claim.get(10, TimeUnit.SECONDS)).get().extracting(LogEvent::clock)
                    .as("the claim continues after the append it waited for").isEqualTo(3L);
            assertThat(a.append(new LogInitialized(0, 5L, "R", Map.of()), "A")).isEmpty();
            assertThat(a.introspect().currentLeader()).isEqualTo("B");

            assertThat(a.length()).isEqualTo(4);
            assertClocksAreZeroToLength(a);
        }
    }

    /** Rows another writer left past the watermark that are no takeover: appended after, never over. */
    @Test
    void rows_written_past_the_watermark_by_someone_else_are_appended_after_with_a_higher_clock() throws Exception {
        String id = "foreign_" + uuid();
        try (var a = new PostgresLogBackend(dataSource, id)) {
            a.append(new LogLeader(0, 1L, "A"), "A");
            a.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
            plantRow("fom_log_" + id, new LogInitialized(7L, 7L, "X", Map.of()));

            assertThatThrownBy(() -> a.compact(List.of(new LogLeader(2L, 8L, "A")), "A"))
                    .as("the snapshot plan does not know the planted row; archiving it away would lose it")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("had not seen")
                    .hasMessageContaining("nothing was written");
            assertThat(a.length()).isEqualTo(3);

            plantRow("fom_log_" + id, new LogInitialized(9L, 9L, "Y", Map.of()));
            assertThat(a.append(new LogInitialized(0, 10L, "Q", Map.of()), "A")).get()
                    .extracting(LogEvent::clock).as("past the planted clock, not the one A last used").isEqualTo(10L);
            assertThat(a.length()).isEqualTo(5);
            assertThat(a.introspect().currentLeader()).isEqualTo("A");
        }
    }

    /** Writes an event row straight into the table, bypassing the backend. */
    @Test
    void an_undecodable_row_is_named_by_row_id_and_position() throws Exception {
        String id = "undecodable_" + uuid();
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogInitialized(0, 2L, "P", Map.of()), "A");
            b.append(new LogInitialized(0, 3L, "Q", Map.of()), "A");
            long rowId;
            try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                    "UPDATE " + table + " SET payload = ? WHERE row_id = "
                            + "(SELECT row_id FROM " + table + " ORDER BY row_id OFFSET 1 LIMIT 1) RETURNING row_id")) {
                st.setBytes(1, new byte[]{1, 2, 3, 4});
                try (var rs = st.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    rowId = rs.getLong(1);
                }
            }
            assertThatThrownBy(() -> b.getBetween(0, 3))
                    .hasMessageContaining("Cannot decode the payload at row_id " + rowId + " (position 1)");
        }
    }

    private static void plantRow(String table, LogEvent event) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(bytes)) {
            oos.writeObject(event);
        }
        try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                "INSERT INTO " + table + " (type, payload, ts_millis) VALUES (?, ?, ?)")) {
            st.setString(1, event.getClass().getSimpleName());
            st.setBytes(2, bytes.toByteArray());
            st.setLong(3, event.timestamp());
            st.executeUpdate();
        }
    }

    // ───────────────── regression: a large archive backlog purges in batches ─────────────────

    /**
     * One transaction dropping every surplus archive holds all their locks at once; with a
     * backlog larger than the shared lock table ({@code max_locks_per_transaction}) it fails
     * with "out of shared memory", rolls back, and so does every later purge. A server with the
     * smallest lock table shows it with a few hundred archives.
     */
    @Test
    void a_large_archive_backlog_is_purged_in_bounded_transactions() throws Exception {
        try (var small = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("fom").withUsername("test").withPassword("test")
                .withCommand("postgres", "-c", "max_locks_per_transaction=10", "-c", "fsync=off")) {
            small.start();
            var ds = new PGSimpleDataSource();
            ds.setUrl(small.getJdbcUrl());
            ds.setUser(small.getUsername());
            ds.setPassword(small.getPassword());
            String id = "backlog_" + uuid();
            String table = "fom_log_" + id;
            int archives = 600;
            try (var b = new PostgresLogBackend(ds, id)) {
                b.append(new LogLeader(0, 1L, "A"), "A");
                List<String> names = new java.util.ArrayList<>();
                try (var c = ds.getConnection(); var st = c.createStatement()) {
                    for (int i = 0; i < archives; i++) {
                        // What a compaction leaves behind: a table with a sequence, a primary key and TOAST.
                        String name = PostgresLogBackend.archiveTableName(table, 1_000L + i);
                        st.execute("CREATE TABLE " + name + " (row_id BIGSERIAL PRIMARY KEY, payload BYTEA)");
                        st.execute("COMMENT ON TABLE " + name + " IS '" + PostgresLogBackend.archiveComment(table) + "'");
                        names.add(name);
                    }
                    // The premise: dropping them all in one transaction exhausts the lock table.
                    c.setAutoCommit(false);
                    try {
                        assertThatThrownBy(() -> {
                            for (String name : names) st.execute("DROP TABLE " + name);
                        }).isInstanceOf(SQLException.class).hasMessageContaining("out of shared memory");
                    } finally {
                        c.rollback();
                        c.setAutoCommit(true);
                    }
                }

                b.purgeArchives(2);

                assertThat(b.purgeTransactions.get())
                        .isEqualTo((archives - 2 + PostgresLogBackend.PURGE_BATCH - 1) / PostgresLogBackend.PURGE_BATCH);
                try (var c = ds.getConnection(); var st = c.prepareStatement(
                        "SELECT tablename FROM pg_tables WHERE tablename LIKE ? ORDER BY tablename")) {
                    st.setString(1, table + "_archived_%");
                    List<String> left = new java.util.ArrayList<>();
                    try (var rs = st.executeQuery()) {
                        while (rs.next()) left.add(rs.getString(1));
                    }
                    assertThat(left).as("the two newest archives are kept")
                            .containsExactlyInAnyOrder(names.get(archives - 2), names.get(archives - 1));
                }
                assertThat(b.length()).isEqualTo(1);
            }
        }
    }

    // ───────────────── pool sizing ─────────────────

    /**
     * A backend keeps one pool connection for its leader session and needs another while
     * it opens: with a pool of one the constructor must say so, not surface the pool's raw
     * timeout, and must release the lock and the connection it took.
     */
    @Test
    void a_pool_of_one_fails_the_constructor_with_a_sizing_hint_and_releases_the_lock() throws Exception {
        String id = "pool1_" + uuid();
        var pool = new ReusingPool("fom-pool1", 1);
        try {
            assertThatThrownBy(() -> new PostgresLogBackend(pool, id))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("second connection")
                    .hasMessageContaining("number of backends + 1")
                    .hasCauseInstanceOf(java.sql.SQLTransientConnectionException.class);
            assertThat(pool.outstanding()).as("the leader connection went back").isZero();
            // The advisory lock was released too: the log opens normally afterwards.
            try (var ok = new PostgresLogBackend(dataSource, id)) {
                assertThat(ok.length()).isZero();
            }
        } finally {
            pool.closeAll();
        }
        // A pool of two is enough for one backend.
        var pool2 = new ReusingPool("fom-pool2", 2);
        try (var b = new PostgresLogBackend(pool2, "pool2_" + uuid())) {
            b.append(new LogLeader(0, 1000L, "A"), "A");
            assertThat(b.length()).isEqualTo(1);
        } finally {
            pool2.closeAll();
        }
    }

    // ───────────────── closed backend: introspect() still reports ─────────────────

    /**
     * Handing leadership over means closing the backend, so a health check built on
     * {@code introspect()} must keep answering afterwards: the last known length and
     * counts, and no leader. Everything else still fails.
     */
    @Test
    void introspect_after_close_reports_the_closed_state_while_writes_and_reads_still_fail() throws Exception {
        String id = "closedintro_" + uuid();
        var b = new PostgresLogBackend(dataSource, id);
        try {
            b.append(new LogLeader(0, 1000L, "A"), "A");
            b.append(new LogInitialized(0, 1001L, "Foo", Map.of("k", new byte[]{1})), "A");
            var open = b.introspect();
            assertThat(open.length()).isEqualTo(2);
            assertThat(open.currentLeader()).isEqualTo("A");
        } finally {
            b.close();
        }

        var report = b.introspect(); // no throw, no stage: a plain report
        assertThat(report.logId()).isEqualTo(id);
        assertThat(report.currentLeader()).as("closed: leads nothing").isNull();
        assertThat(report.length()).as("last known length").isEqualTo(2);
        assertThat(report.eventCounts())
                .containsEntry("LogLeader", 1)
                .containsEntry("LogInitialized", 1);
        assertThat(report.maxTimestampMillis()).isEqualTo(1001L);
        // Repeatable and stable: no database work behind it.
        assertThat(b.introspect()).isEqualTo(report);

        // Every other operation still fails exactly as before.
        assertThatThrownBy(() -> b.append(new LogLeader(2, 1002L, "A"), "A"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is closed");
        assertThatThrownBy(() -> b.compact(List.of(new LogLeader(0, 1003L, "A")), "A"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is closed");
        assertThatThrownBy(() -> b.get(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is closed");
        assertThatThrownBy(b::length)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is closed");
        assertThatThrownBy(() -> b.getBetween(0, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is closed");

        // The lock really was released: a standby can take the log over.
        try (var standby = new PostgresLogBackend(dataSource, id)) {
            assertThat(standby.length()).isEqualTo(2);
        }
    }

    /**
     * The closed report's length is the last one this instance knew, not only what the
     * last {@code introspect()} read: appends, {@code length()} and {@code getBetween}
     * keep it up to date. The event counts stay those of the last {@code introspect()}.
     */
    @Test
    void introspect_after_close_reports_the_length_known_from_appends_and_reads() throws Exception {
        String id = "closedlen_" + uuid();
        var b = new PostgresLogBackend(dataSource, id);
        try {
            b.append(new LogLeader(0, 1000L, "A"), "A");
            var open = b.introspect(); // counts: 1 LogLeader
            assertThat(open.length()).isEqualTo(1);
            b.append(new LogInitialized(0, 1001L, "Foo", Map.of("k", new byte[]{1})), "A");
            b.append(new LogInitialized(0, 1002L, "Bar", Map.of("k", new byte[]{2})), "A");
        } finally {
            b.close();
        }
        var report = b.introspect();
        assertThat(report.length()).as("kept up to date by the appends after introspect()").isEqualTo(3);
        assertThat(report.eventCounts()).as("counts of the last introspect()")
                .containsExactlyEntriesOf(Map.of("LogLeader", 1));
        assertThat(report.maxTimestampMillis()).isEqualTo(1000L);

        // Never introspected: a read gives the length, appends move it on, a compaction resets it.
        String id2 = "closedlen2_" + uuid();
        var c = new PostgresLogBackend(dataSource, id2);
        try {
            c.append(new LogLeader(0, 1000L, "A"), "A");
            assertThat(c.length()).isEqualTo(1);
            for (int i = 0; i < 4; i++) {
                c.append(new LogInitialized(0, 1001L + i, "P" + i, Map.of("k", new byte[]{1})), "A");
            }
            assertThat(c.getBetween(0, 2)).hasSize(2);
            c.append(new LogInitialized(0, 1010L, "Q", Map.of("k", new byte[]{1})), "A");
        } finally {
            c.close();
        }
        assertThat(c.introspect().length()).isEqualTo(6);
        assertThat(c.introspect().eventCounts()).isEmpty();

        String id3 = "closedlen3_" + uuid();
        var d = new PostgresLogBackend(dataSource, id3);
        try {
            d.append(new LogLeader(0, 1000L, "A"), "A");
            d.append(new LogInitialized(0, 1001L, "Foo", Map.of("k", new byte[]{1})), "A");
            d.append(new LogInitialized(0, 1002L, "Bar", Map.of("k", new byte[]{1})), "A");
            assertThat(d.length()).isEqualTo(3);
            long lastClock = d.get(2).clock();
            d.compact(List.of(new LogLeader(lastClock + 1, 1003L, "A")), "A");
            d.append(new LogInitialized(0, 1004L, "Baz", Map.of("k", new byte[]{1})), "A");
        } finally {
            d.close();
        }
        assertThat(d.introspect().length()).as("compaction left 1 row, then one append").isEqualTo(2);
    }

    /** A backend closed before {@code introspect()} ever ran has no last known values. */
    @Test
    void introspect_after_close_without_a_previous_read_reports_unknown_length() throws Exception {
        String id = "closedintro2_" + uuid();
        var b = new PostgresLogBackend(dataSource, id);
        b.append(new LogLeader(0, 1000L, "A"), "A");
        b.close();

        var report = b.introspect();
        assertThat(report.logId()).isEqualTo(id);
        assertThat(report.currentLeader()).isNull();
        assertThat(report.length()).as("never read one").isEqualTo(-1);
        assertThat(report.eventCounts()).isEmpty();
        assertThat(report.maxTimestampMillis()).isZero();
    }

    /**
     * The engine-level path: after the operator closes the backend to hand leadership
     * over, {@code engine.introspect()} still answers "not leader" instead of throwing,
     * and queries keep being served from memory.
     */
    @Test
    void engine_introspect_still_answers_not_leader_after_the_backend_was_closed() throws Exception {
        String id = "closedintroeng_" + uuid();
        var b = new PostgresLogBackend(dataSource, id);
        Engine engine = new Engine(EngineConfig.defaults(), b, new JavaSerializableSerDe());
        try {
            engine.newGraph(new GraphBuilder()
                    .add("Echo",
                            (Supplier<ProcessInitializer>) () -> ctx ->
                                    CompletableFuture.completedFuture(Map.of("v", new byte[]{7})),
                            (Supplier<ProcessLoader>) () -> (ctx, props) ->
                                    CompletableFuture.completedFuture(
                                            (Process) (qctx, query) -> CompletableFuture.completedFuture("pong")))
                    .handles(String.class)
                    .build());
            assertThat(engine.query("ping").toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo("pong");
            assertThat(engine.introspect().toCompletableFuture().get(10, TimeUnit.SECONDS).isLeader()).isTrue();

            b.close(); // hand the log to a standby, as the docs describe

            var report = engine.introspect().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertThat(report.isLeader()).as("not leader any more").isFalse();
            assertThat(report.log().currentLeader()).isNull();
            assertThat(report.graph().nodes()).hasSize(1);
            // Still serving from memory.
            assertThat(engine.query("ping").toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo("pong");
        } finally {
            try {
                engine.close();
            } catch (RuntimeException expected) {
                // The engine's own shutdown bookkeeping needs the log, which is gone.
            }
        }
    }

    // ───────────────── regression: archive stamps under clock skew ─────────────────

    private static long archiveStamp(String archive) {
        return Long.parseLong(archive.substring(archive.lastIndexOf("_archived_") + "_archived_".length()));
    }

    private static void renameTable(String from, String to) throws SQLException {
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("ALTER TABLE " + from + " RENAME TO " + to);
        }
    }

    @Test
    void an_archive_from_a_node_whose_clock_runs_ahead_does_not_make_purge_drop_the_newest() throws Exception {
        String id = "skew_" + uuid();
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            String first = b.compact(List.of(new LogLeader(0, 10L, "A")), "A").archivedLogId();
            // As if the previous leader's clock ran a day ahead when it compacted.
            long ahead = System.currentTimeMillis() + Duration.ofDays(1).toMillis();
            String skewed = PostgresLogBackend.archiveTableName(table, ahead);
            renameTable(first, skewed);

            String newest = b.compact(List.of(new LogLeader(0, 11L, "A")), "A").archivedLogId();
            assertThat(archiveStamp(newest)).as("floored above the skewed archive").isGreaterThan(ahead);
            b.purgeArchives(1);
            assertThat(relationExists(newest)).as("the newest archive is kept").isTrue();
            assertThat(relationExists(skewed)).as("the older (skewed) archive is purged").isFalse();
        }
    }

    @Test
    void an_absurdly_far_future_archive_stamp_is_ignored_and_purged_as_the_oldest() throws Exception {
        String id = "farfut_" + uuid().substring(0, 12);
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            String first = b.compact(List.of(new LogLeader(0, 10L, "A")), "A").archivedLogId();
            long planted = System.currentTimeMillis() + Duration.ofDays(200L * 366).toMillis();
            String far = PostgresLogBackend.archiveTableName(table, planted);
            renameTable(first, far);

            String second = b.compact(List.of(new LogLeader(0, 11L, "A")), "A").archivedLogId();
            assertThat(archiveStamp(second)).as("not pushed past the planted stamp")
                    .isLessThan(System.currentTimeMillis() + Duration.ofDays(1).toMillis());
            String third = b.compact(List.of(new LogLeader(0, 12L, "A")), "A").archivedLogId();
            b.purgeArchives(2);
            assertThat(relationExists(far)).as("counted as the oldest").isFalse();
            assertThat(relationExists(second)).isTrue();
            assertThat(relationExists(third)).isTrue();
        }
    }

    // ───────────────── regression: concurrent creation of colliding names ─────────────────

    @Test
    void a_sequence_name_committed_by_another_session_in_flight_is_refused_as_illegal_argument() throws Exception {
        String id = "race" + uuid().substring(0, 8);
        String sequence = "fom_log_" + id + "_rid_seq";
        try (Connection other = dataSource.getConnection()) {
            other.setAutoCommit(false);
            try (var st = other.createStatement()) {
                // Log '<id>_rid_seq' creating its table: uncommitted, so invisible to the checks.
                st.execute("CREATE TABLE " + sequence + " (x int)");
            }
            var opening = CompletableFuture.supplyAsync(() -> {
                try {
                    return new PostgresLogBackend(dataSource, id);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            // The CREATE SEQUENCE waits on the other session's catalog row...
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                        "SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE ?")) {
                    st.setString(1, "CREATE SEQUENCE " + sequence + "%");
                    try (var rs = st.executeQuery()) {
                        rs.next();
                        return rs.getLong(1) > 0;
                    }
                }
            });
            other.commit(); // ...and fails with 23505 once it commits.
            assertThatThrownBy(() -> opening.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .cause()
                    .hasMessageContaining(sequence)
                    .hasMessageContaining("sequence");
            assertThat(relationExists("fom_log_" + id)).as("nothing half-created").isFalse();
        } finally {
            try (var c = dataSource.getConnection(); var st = c.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + sequence);
            }
        }
        // The lock was released: once the name is free the log opens.
        try (var b = new PostgresLogBackend(dataSource, id)) {
            assertThat(b.length()).isZero();
        }
    }

    @Test
    void a_sequence_name_committed_by_another_session_just_before_the_create_is_refused_as_illegal_argument()
            throws Exception {
        String id = "race" + uuid().substring(0, 8);
        String sequence = "fom_log_" + id + "_rid_seq";
        // Just before the CREATE SEQUENCE runs, another log commits a table of that name (42P07).
        DataSource ds = beforeFirstCreateSequence(dataSource, () -> {
            try (var c = dataSource.getConnection(); var st = c.createStatement()) {
                return st.execute("CREATE TABLE " + sequence + " (x int)");
            }
        });
        try {
            assertThatThrownBy(() -> new PostgresLogBackend(ds, id))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(sequence)
                    .hasMessageContaining("sequence");
            assertThat(relationExists("fom_log_" + id)).as("nothing half-created").isFalse();
        } finally {
            try (var c = dataSource.getConnection(); var st = c.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + sequence);
            }
        }
    }

    // ───────────────── regression: read-only server / session ─────────────────

    @Test
    void a_read_only_session_is_refused_at_construction_for_new_and_existing_logs() throws Exception {
        var readOnly = new PGSimpleDataSource();
        readOnly.setUrl(postgres.getJdbcUrl());
        readOnly.setUser(postgres.getUsername());
        readOnly.setPassword(postgres.getPassword());
        readOnly.setOptions("-c default_transaction_read_only=on");

        String existing = "ro_" + uuid();
        new PostgresLogBackend(dataSource, existing).close();
        for (String id : List.of(existing, "ro_new_" + uuid())) {
            assertThatThrownBy(() -> new PostgresLogBackend(readOnly, id))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("read-only server (hot standby?)");
        }
        // No lock was left behind.
        try (var b = new PostgresLogBackend(dataSource, existing)) {
            assertThat(b.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
        }
    }

    // ───────────────── regression: transaction-pooling proxies ─────────────────

    @Test
    void two_backends_handed_the_same_server_session_do_not_both_lead() throws Exception {
        String id = "shared_" + uuid();
        try (Connection physical = dataSource.getConnection()) {
            // As a transaction-pooling proxy does: both nodes' connections run on one server session,
            // where pg_try_advisory_lock is re-entrant.
            try (var first = new PostgresLogBackend(new SingleSessionPool(physical), id)) {
                assertThatThrownBy(() -> new PostgresLogBackend(new SingleSessionPool(physical), id))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("pool_mode=session");
                assertThat(first.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
                assertThat(advisoryLocksHeldBy(backendPid(physical))).isEqualTo(1);
            }
        }
    }

    @Test
    void a_datasource_that_moves_transactions_between_server_sessions_is_refused() throws Exception {
        String id = "txpool_" + uuid();
        try (var pool = new TransactionPoolingProxy()) {
            assertThatThrownBy(() -> new PostgresLogBackend(pool, id))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("pool_mode=session");
            for (int pid : pool.pids()) {
                assertThat(advisoryLocksHeldBy(pid)).as("no lock left on server session %d", pid).isZero();
            }
        }
    }

    /** Runs {@code hook} once, just before the first {@code CREATE SEQUENCE} executed through {@code delegate}. */
    private static DataSource beforeFirstCreateSequence(DataSource delegate, java.util.concurrent.Callable<?> hook) {
        var fired = new java.util.concurrent.atomic.AtomicBoolean();
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
                    if (!(result instanceof Connection conn)) return result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                Object r = invokeUnwrapped(cMethod, conn, cArgs);
                                if (!cMethod.getName().equals("createStatement")
                                        || !(r instanceof java.sql.Statement st)) {
                                    return r;
                                }
                                return Proxy.newProxyInstance(java.sql.Statement.class.getClassLoader(),
                                        new Class<?>[]{java.sql.Statement.class}, (sProxy, sMethod, sArgs) -> {
                                            if (sMethod.getName().equals("execute")
                                                    && sArgs != null && sArgs.length == 1
                                                    && sArgs[0] instanceof String sql
                                                    && sql.startsWith("CREATE SEQUENCE ")
                                                    && fired.compareAndSet(false, true)) {
                                                hook.call();
                                            }
                                            return invokeUnwrapped(sMethod, st, sArgs);
                                        });
                            });
                });
    }

    /**
     * Mimics PgBouncer in {@code pool_mode=transaction}: every transaction (each autocommit
     * statement, each explicit transaction) runs on the next of two server sessions.
     */
    private static final class TransactionPoolingProxy implements DataSource, AutoCloseable {
        private final Connection[] servers = new Connection[2];
        private int current;
        private boolean autoCommit = true;
        private boolean inTransaction;

        TransactionPoolingProxy() throws SQLException {
            for (int i = 0; i < servers.length; i++) servers[i] = dataSource.getConnection();
        }

        List<Integer> pids() throws SQLException {
            List<Integer> pids = new java.util.ArrayList<>();
            for (Connection c : servers) pids.add(backendPid(c));
            return pids;
        }

        @Override
        public void close() throws SQLException {
            for (Connection c : servers) c.close();
        }

        @Override
        public synchronized Connection getConnection() {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        synchronized (this) {
                            switch (method.getName()) {
                                case "setAutoCommit" -> {
                                    boolean on = (Boolean) args[0];
                                    if (on && inTransaction) servers[current].setAutoCommit(true); // commits
                                    inTransaction = false;
                                    autoCommit = on;
                                    return null;
                                }
                                case "getAutoCommit" -> {
                                    return autoCommit;
                                }
                                case "commit", "rollback" -> {
                                    if (inTransaction) {
                                        invokeUnwrapped(method, servers[current], args);
                                        servers[current].setAutoCommit(true);
                                        inTransaction = false;
                                    }
                                    return null;
                                }
                                case "close", "abort" -> {
                                    if (inTransaction) {
                                        servers[current].rollback();
                                        servers[current].setAutoCommit(true);
                                        inTransaction = false;
                                    }
                                    return null;
                                }
                                case "isClosed" -> {
                                    return false;
                                }
                                case "createStatement", "prepareStatement" -> {
                                    if (!inTransaction) {
                                        current = (current + 1) % servers.length; // a new transaction
                                        if (!autoCommit) {
                                            servers[current].setAutoCommit(false);
                                            inTransaction = true;
                                        }
                                    }
                                    return invokeUnwrapped(method, servers[current], args);
                                }
                                default -> {
                                    return invokeUnwrapped(method, servers[current], args);
                                }
                            }
                        }
                    });
        }

        @Override public Connection getConnection(String user, String password) { return getConnection(); }
        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("not a wrapper"); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }

    // ───────────────── helpers ─────────────────

    private static String uuid() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static PostgresLogBackend tryOpen(String id) throws SQLException {
        try {
            return new PostgresLogBackend(dataSource, id);
        } catch (IllegalStateException stillLocked) {
            return null;
        }
    }

    private static int backendPid(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("SELECT pg_backend_pid()")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static long advisoryLocksHeldBy(int pid) throws SQLException {
        try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND pid = ?")) {
            st.setInt(1, pid);
            try (var rs = st.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static boolean relationExists(String name) throws SQLException {
        try (var c = dataSource.getConnection();
             var st = c.prepareStatement("SELECT COUNT(*) FROM pg_class WHERE relname = ?")) {
            st.setString(1, name);
            try (var rs = st.executeQuery()) {
                rs.next();
                return rs.getLong(1) == 1;
            }
        }
    }

    /**
     * Wraps {@code delegate} so that, while {@code failuresLeft} is positive, the leader
     * session's own advisory-lock check ({@code pid = pg_backend_pid()}) fails the way a
     * cancelled statement does. Other statements, including the confirmation run on a
     * separate session ({@code pid = ?}), are untouched.
     */
    private static DataSource failingLockChecks(DataSource delegate,
                                                java.util.concurrent.atomic.AtomicInteger failuresLeft) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
                    if (!(result instanceof Connection conn)) return result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                Object r = invokeUnwrapped(cMethod, conn, cArgs);
                                if (cMethod.getName().equals("prepareStatement")
                                        && cArgs[0] instanceof String sql && sql.contains("pid = pg_backend_pid()")
                                        && r instanceof java.sql.PreparedStatement ps) {
                                    return Proxy.newProxyInstance(java.sql.PreparedStatement.class.getClassLoader(),
                                            new Class<?>[]{java.sql.PreparedStatement.class}, (sProxy, sMethod, sArgs) -> {
                                                if (sMethod.getName().equals("executeQuery")
                                                        && failuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : n) > 0) {
                                                    throw new SQLException("canceling statement due to user request", "57014");
                                                }
                                                return invokeUnwrapped(sMethod, ps, sArgs);
                                            });
                                }
                                return r;
                            });
                });
    }

    /**
     * Wraps {@code delegate} so that, while {@code failuresLeft} is positive, an event INSERT
     * fails the way a cancelled statement does — the write itself, not the lock check before it.
     * Other statements are untouched.
     */
    private static DataSource failingInserts(DataSource delegate,
                                             java.util.concurrent.atomic.AtomicInteger failuresLeft) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
                    if (!(result instanceof Connection conn)) return result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                Object r = invokeUnwrapped(cMethod, conn, cArgs);
                                if (cMethod.getName().equals("prepareStatement")
                                        && cArgs[0] instanceof String sql && sql.startsWith("INSERT INTO ")
                                        && r instanceof java.sql.PreparedStatement ps) {
                                    return Proxy.newProxyInstance(java.sql.PreparedStatement.class.getClassLoader(),
                                            new Class<?>[]{java.sql.PreparedStatement.class}, (sProxy, sMethod, sArgs) -> {
                                                if (sMethod.getName().equals("executeUpdate")
                                                        && failuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : n) > 0) {
                                                    throw new SQLException("canceling statement due to user request", "57014");
                                                }
                                                return invokeUnwrapped(sMethod, ps, sArgs);
                                            });
                                }
                                return r;
                            });
                });
    }

    /**
     * Wraps {@code delegate} so that the payload bytes each executed query hands out
     * (through {@code ResultSet.getBytes}) are summed and recorded per statement execution.
     */
    private static DataSource countingPayloadBytes(DataSource delegate,
                                                   java.util.Queue<Long> perStatement) {
        return wrappingResultSets(delegate, rs -> {
            var sum = new java.util.concurrent.atomic.AtomicLong();
            return (java.sql.ResultSet) Proxy.newProxyInstance(java.sql.ResultSet.class.getClassLoader(),
                    new Class<?>[]{java.sql.ResultSet.class}, (rProxy, rMethod, rArgs) -> {
                        Object r = invokeUnwrapped(rMethod, rs, rArgs);
                        if (rMethod.getName().equals("getBytes") && r instanceof byte[] bytes) {
                            sum.addAndGet(bytes.length);
                        } else if (rMethod.getName().equals("close") && sum.get() > 0) {
                            perStatement.add(sum.get());
                            sum.set(0);
                        }
                        return r;
                    });
        });
    }

    /**
     * Wraps {@code delegate} so that a query result handing out a second payload
     * ({@code ResultSet.getBytes}) fails the way a statement cancelled by
     * {@code statement_timeout} does (SQLState 57014), counted in {@code cancelled}.
     */
    private static DataSource cancellingMultiRowReads(DataSource delegate,
                                                      java.util.concurrent.atomic.AtomicInteger cancelled) {
        return wrappingResultSets(delegate, rs -> {
            var payloads = new java.util.concurrent.atomic.AtomicInteger();
            return (java.sql.ResultSet) Proxy.newProxyInstance(java.sql.ResultSet.class.getClassLoader(),
                    new Class<?>[]{java.sql.ResultSet.class}, (rProxy, rMethod, rArgs) -> {
                        if (rMethod.getName().equals("getBytes") && payloads.incrementAndGet() > 1) {
                            cancelled.incrementAndGet();
                            throw new SQLException("canceling statement due to statement timeout", "57014");
                        }
                        return invokeUnwrapped(rMethod, rs, rArgs);
                    });
        });
    }

    /** Wraps every {@code ResultSet} a prepared statement of {@code delegate} returns. */
    private static DataSource wrappingResultSets(DataSource delegate,
                                                 java.util.function.UnaryOperator<java.sql.ResultSet> wrap) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
                    if (!(result instanceof Connection conn)) return result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                Object r = invokeUnwrapped(cMethod, conn, cArgs);
                                if (cMethod.getName().equals("prepareStatement")
                                        && r instanceof java.sql.PreparedStatement ps) {
                                    return Proxy.newProxyInstance(java.sql.PreparedStatement.class.getClassLoader(),
                                            new Class<?>[]{java.sql.PreparedStatement.class}, (sProxy, sMethod, sArgs) -> {
                                                Object sr = invokeUnwrapped(sMethod, ps, sArgs);
                                                return sMethod.getName().equals("executeQuery")
                                                        && sr instanceof java.sql.ResultSet rs ? wrap.apply(rs) : sr;
                                            });
                                }
                                return r;
                            });
                });
    }

    private static List<String> relationsLike(String pattern) throws SQLException {
        try (var c = dataSource.getConnection();
             var st = c.prepareStatement("SELECT relname FROM pg_class WHERE relname LIKE ? ORDER BY relname")) {
            st.setString(1, pattern);
            var names = new java.util.ArrayList<String>();
            try (var rs = st.executeQuery()) {
                while (rs.next()) names.add(rs.getString(1));
            }
            return names;
        }
    }

    /**
     * Wraps {@code delegate} to imitate an unreachable server: while {@code leaderSessionDown}
     * is set, the lock check the backend would run on its leader session fails the way
     * pgjdbc's {@code socketTimeout} does (SQLState 08006 — indistinguishable from a session
     * the server terminated), counted in {@code injected}; while {@code poolDown} is set, the
     * backend's own threads cannot obtain a short-lived connection either. Everything else,
     * including the leader session's other statements, is untouched.
     */
    private static DataSource unreachableServer(DataSource delegate,
                                                java.util.concurrent.atomic.AtomicBoolean leaderSessionDown,
                                                java.util.concurrent.atomic.AtomicBoolean poolDown,
                                                java.util.concurrent.atomic.AtomicInteger injected) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    if (dsMethod.getName().equals("getConnection") && poolDown.get()
                            && Thread.currentThread().getName().startsWith("fom-pg-")) {
                        throw new SQLException("An I/O error occurred while sending to the backend.", "08006");
                    }
                    Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
                    if (!(result instanceof Connection conn)) return result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                Object r = invokeUnwrapped(cMethod, conn, cArgs);
                                if (cMethod.getName().equals("prepareStatement")
                                        && cArgs[0] instanceof String sql && sql.contains("pid = pg_backend_pid()")
                                        && r instanceof java.sql.PreparedStatement ps) {
                                    return Proxy.newProxyInstance(java.sql.PreparedStatement.class.getClassLoader(),
                                            new Class<?>[]{java.sql.PreparedStatement.class}, (sProxy, sMethod, sArgs) -> {
                                                if (sMethod.getName().equals("executeQuery") && leaderSessionDown.get()) {
                                                    injected.incrementAndGet();
                                                    throw new SQLException(
                                                            "An I/O error occurred while sending to the backend.", "08006");
                                                }
                                                return invokeUnwrapped(sMethod, ps, sArgs);
                                            });
                                }
                                return r;
                            });
                });
    }

    /**
     * Wraps {@code delegate} so that the sessions of two logs compacting in parallel create
     * their new relation at the same instant instead of by luck: the first such statement of
     * each session waits at the current barrier, is then prefixed with a server-side sleep up
     * to a common wall-clock target so both backends run the CREATE in the same millisecond,
     * and afterwards keeps its transaction open for a moment — the window in which both
     * sessions pick the same implicit sequence name and insert it into the catalog.
     */
    private static DataSource createsInLockstep(
            DataSource delegate,
            java.util.concurrent.atomic.AtomicReference<java.util.concurrent.CyclicBarrier> barrier,
            java.util.concurrent.atomic.AtomicReference<String> targetEpochSeconds) {
        var awaited = new java.util.concurrent.ConcurrentHashMap<Thread, java.util.concurrent.CyclicBarrier>();
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
                    if (!(result instanceof Connection conn)) return result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                Object r = invokeUnwrapped(cMethod, conn, cArgs);
                                if (!cMethod.getName().equals("createStatement")
                                        || !(r instanceof java.sql.Statement st)) {
                                    return r;
                                }
                                return Proxy.newProxyInstance(java.sql.Statement.class.getClassLoader(),
                                        new Class<?>[]{java.sql.Statement.class}, (sProxy, sMethod, sArgs) -> {
                                            boolean aligned = false;
                                            if (sMethod.getName().equals("execute")
                                                    && sArgs != null && sArgs.length == 1
                                                    && sArgs[0] instanceof String sql
                                                    && isCompactionCreate(sql)) {
                                                var bar = barrier.get();
                                                if (bar != null
                                                        && awaited.put(Thread.currentThread(), bar) != bar) {
                                                    bar.await(30, java.util.concurrent.TimeUnit.SECONDS);
                                                    sArgs = new Object[]{"SELECT pg_sleep(GREATEST(0, "
                                                            + targetEpochSeconds.get()
                                                            + " - EXTRACT(EPOCH FROM clock_timestamp())));" + sql};
                                                    aligned = true;
                                                }
                                            }
                                            Object out = invokeUnwrapped(sMethod, st, sArgs);
                                            // Hold the new relation uncommitted for a while, so the
                                            // other log's CREATE (a few ms behind at worst) really
                                            // runs against an in-progress catalog insert.
                                            if (aligned) Thread.sleep(300);
                                            return out;
                                        });
                            });
                });
    }

    /** The statement that creates the object two logs could be handed the same name for. */
    private static boolean isCompactionCreate(String sql) {
        return sql.startsWith("CREATE SEQUENCE ")
                || (sql.startsWith("CREATE TABLE ") && !sql.contains("IF NOT EXISTS"));
    }

    private static long sessionsNamed(String appName) throws SQLException {
        try (var c = dataSource.getConnection();
             var st = c.prepareStatement("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = ?")) {
            st.setString(1, appName);
            try (var rs = st.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /**
     * Wraps {@code delegate} so that {@code getConnection} called by the backend's lock
     * confirmation thread ({@code fom-pg-lock-confirm-*}) blocks until {@code release}
     * opens, ignoring interrupts, like an exhausted pool without a checkout timeout. It
     * then still hands out a real connection. Other callers are untouched.
     */
    private static DataSource blockingConfirmations(DataSource delegate,
                                                    java.util.concurrent.CountDownLatch release,
                                                    java.util.concurrent.atomic.AtomicInteger blocked) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    if (dsMethod.getName().equals("getConnection")
                            && Thread.currentThread().getName().startsWith("fom-pg-lock-confirm-")) {
                        blocked.incrementAndGet();
                        boolean interrupted = false;
                        while (true) {
                            try {
                                release.await();
                                break;
                            } catch (InterruptedException e) {
                                interrupted = true;
                            }
                        }
                        try {
                            return invokeUnwrapped(dsMethod, delegate, dsArgs);
                        } finally {
                            if (interrupted) Thread.currentThread().interrupt();
                        }
                    }
                    return invokeUnwrapped(dsMethod, delegate, dsArgs);
                });
    }

    /**
     * Wraps {@code delegate} to imitate an unavailable database for {@code introspect()}:
     * in mode {@code "throw"} a report query's {@code getConnection} (thread
     * {@code fom-pg-introspect-*}) fails like an exhausted pool; in mode {@code "block"}
     * it and the leader session's own lock check hang, ignoring interrupts, until
     * {@code release} opens. Mode {@code "ok"} passes everything through.
     */
    private static DataSource stalling(DataSource delegate,
                                       java.util.concurrent.atomic.AtomicReference<String> mode,
                                       java.util.concurrent.CountDownLatch release) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (dsProxy, dsMethod, dsArgs) -> {
                    if (dsMethod.getName().equals("getConnection")
                            && Thread.currentThread().getName().startsWith("fom-pg-introspect-")) {
                        if (mode.get().equals("throw")) throw new SQLException("pool exhausted", "08001");
                        if (mode.get().equals("block")) awaitIgnoringInterrupts(release);
                    }
                    Object result = invokeUnwrapped(dsMethod, delegate, dsArgs);
                    if (!(result instanceof Connection conn)) return result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                            new Class<?>[]{Connection.class}, (cProxy, cMethod, cArgs) -> {
                                Object r = invokeUnwrapped(cMethod, conn, cArgs);
                                if (cMethod.getName().equals("prepareStatement")
                                        && cArgs[0] instanceof String sql && sql.contains("pid = pg_backend_pid()")
                                        && r instanceof java.sql.PreparedStatement ps) {
                                    return Proxy.newProxyInstance(java.sql.PreparedStatement.class.getClassLoader(),
                                            new Class<?>[]{java.sql.PreparedStatement.class}, (sProxy, sMethod, sArgs) -> {
                                                if (sMethod.getName().equals("executeQuery")
                                                        && mode.get().equals("block")) {
                                                    awaitIgnoringInterrupts(release);
                                                }
                                                return invokeUnwrapped(sMethod, ps, sArgs);
                                            });
                                }
                                return r;
                            });
                });
    }

    private static void awaitIgnoringInterrupts(java.util.concurrent.CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private static Object invokeUnwrapped(java.lang.reflect.Method method, Object target, Object[] args)
            throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * Tiny connection pool: {@code close()} hands the physical session back for reuse
     * (its session-level advisory locks survive), {@code abort()} discards it.
     */
    private static final class ReusingPool implements DataSource {
        private final PGSimpleDataSource physicalSource = new PGSimpleDataSource();
        private final java.util.concurrent.ConcurrentLinkedDeque<Connection> idle =
                new java.util.concurrent.ConcurrentLinkedDeque<>();
        private final java.util.concurrent.CopyOnWriteArrayList<Connection> all =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        /** Connections handed out and not yet returned, and how many may be ({@code 0}: unbounded). */
        private final java.util.concurrent.atomic.AtomicInteger outstanding =
                new java.util.concurrent.atomic.AtomicInteger();
        private final int maxSize;

        ReusingPool(String appName) {
            this(appName, 0);
        }

        ReusingPool(String appName, int maxSize) {
            this.maxSize = maxSize;
            physicalSource.setUrl(postgres.getJdbcUrl());
            physicalSource.setUser(postgres.getUsername());
            physicalSource.setPassword(postgres.getPassword());
            physicalSource.setApplicationName(appName);
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (maxSize > 0 && outstanding.incrementAndGet() > maxSize) {
                outstanding.decrementAndGet();
                // What HikariCP throws when the pool is exhausted.
                throw new java.sql.SQLTransientConnectionException(
                        "pool - Connection is not available, request timed out after 30000ms");
            } else if (maxSize <= 0) {
                outstanding.incrementAndGet();
            }
            Connection pooled = idle.poll();
            while (pooled != null && pooled.isClosed()) pooled = idle.poll();
            if (pooled == null) {
                pooled = physicalSource.getConnection();
                all.add(pooled);
            }
            Connection physical = pooled;
            var handedBack = new java.util.concurrent.atomic.AtomicBoolean();
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "close" -> {
                                if (!handedBack.compareAndSet(false, true)) return null;
                                outstanding.decrementAndGet();
                                if (!physical.isClosed()) {
                                    try {
                                        if (!physical.getAutoCommit()) physical.rollback();
                                        physical.setAutoCommit(true);
                                    } catch (SQLException ignored) {
                                        // a cancelled reset; the session is reused anyway
                                    }
                                    idle.push(physical);
                                }
                                return null;
                            }
                            case "abort" -> {
                                if (handedBack.compareAndSet(false, true)) outstanding.decrementAndGet(); // never reused
                                physical.abort((java.util.concurrent.Executor) args[0]);
                                return null;
                            }
                            case "isClosed" -> {
                                return handedBack.get() || physical.isClosed();
                            }
                            default -> {
                                return invokeUnwrapped(method, physical, args);
                            }
                        }
                    });
        }

        int outstanding() {
            return outstanding.get();
        }

        void closeAll() {
            for (Connection c : all) {
                try { c.close(); } catch (SQLException ignored) { }
            }
        }

        @Override public Connection getConnection(String user, String password) throws SQLException {
            return getConnection();
        }
        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("not a wrapper"); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }

    /**
     * Minimal pool stand-in: always hands out the same physical session, and
     * {@code close()} only returns it (resets autocommit), exactly like a real pool.
     */
    private static final class SingleSessionPool implements DataSource {
        private final Connection physical;

        SingleSessionPool(Connection physical) {
            this.physical = physical;
        }

        @Override
        public Connection getConnection() {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "close" -> {
                                if (!physical.getAutoCommit()) {
                                    physical.rollback();
                                    physical.setAutoCommit(true);
                                }
                                return null;
                            }
                            case "isClosed" -> {
                                return false;
                            }
                            default -> {
                                try {
                                    return method.invoke(physical, args);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                            }
                        }
                    });
        }

        @Override public Connection getConnection(String user, String password) { return getConnection(); }
        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("not a wrapper"); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
