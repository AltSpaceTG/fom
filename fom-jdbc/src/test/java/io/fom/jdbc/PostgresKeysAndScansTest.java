package io.fom.jdbc;

import io.fom.log.LogEvent;
import io.fom.log.LogLeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Lock keys and derived names that cannot collide, an append guard that stays an index
 * lookup under a generic plan, and range reads that scan the log in linear time.
 */
@Testcontainers
class PostgresKeysAndScansTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("fom")
                    .withUsername("test")
                    .withPassword("test");

    private static DataSource dataSource;

    @BeforeAll
    static void setupDataSource() {
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        dataSource = ds;
    }

    /** The hash lock keys and shortened names used before: {@code h = 31 * h + c}. */
    private static long polynomialHash(String s) {
        long h = 1125899906842597L;
        for (int i = 0; i < s.length(); i++) h = 31 * h + s.charAt(i);
        return h;
    }

    /**
     * Two lower-case suffixes the polynomial hash cannot tell apart after any common prefix:
     * {@code 'c'*31 + '0' == 'a'*31 + 'n'} (3117). {@code fom_log_ar0} / {@code fom_log_c2n}
     * was the reported pair.
     */
    private static final String SUFFIX_A = "c0";
    private static final String SUFFIX_B = "an";

    // ───────────────── lock keys ─────────────────

    @Test
    void logs_whose_names_collided_under_the_old_hash_both_lead() throws Exception {
        String prefix = "k" + uuid().substring(0, 10) + "_";
        String a = "fom_log_" + prefix + SUFFIX_A;
        String b = "fom_log_" + prefix + SUFFIX_B;
        assertThat(polynomialHash("public." + a)).as("premise: the old lock keys collide")
                .isEqualTo(polynomialHash("public." + b));
        assertThat(polynomialHash("public.fom_log_ar0")).as("premise: the reported pair")
                .isEqualTo(polynomialHash("public.fom_log_c2n"));
        assertThat(PostgresLogBackend.advisoryKey("public." + a))
                .isNotEqualTo(PostgresLogBackend.advisoryKey("public." + b));

        try (var first = new PostgresLogBackend(dataSource, prefix + SUFFIX_A);
             var second = new PostgresLogBackend(dataSource, prefix + SUFFIX_B)) {
            assertThat(first.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(second.append(new LogLeader(0, 1L, "B"), "B")).isPresent();
            assertThat(first.introspect().currentLeader()).isEqualTo("A");
            assertThat(second.introspect().currentLeader()).isEqualTo("B");
        }
        // The reported names themselves (fixed names: nothing else in this class uses them).
        try (var ar0 = new PostgresLogBackend(dataSource, "ar0");
             var c2n = new PostgresLogBackend(dataSource, "c2n")) {
            assertThat(ar0.append(new LogLeader(0, 1L, "A"), "A")).isPresent();
            assertThat(c2n.append(new LogLeader(0, 1L, "B"), "B")).isPresent();
        }
    }

    // ───────────────── derived names and archive ownership ─────────────────

    @Test
    void purging_one_long_log_never_drops_the_archives_of_another_whose_names_collided_before() throws Exception {
        // Table names of 60 bytes: archive, sequence and primary-key names are all shortened
        // to a prefix plus a hash, and the two tables differ only in their last two bytes.
        String head = "l" + uuid();
        String pad = "_".repeat(60 - "fom_log_".length() - head.length() - 2);
        String idA = head + pad + SUFFIX_A;
        String idB = head + pad + SUFFIX_B;
        String tableA = "fom_log_" + idA;
        String tableB = "fom_log_" + idB;
        assertThat(tableA).hasSize(60);
        assertThat(polynomialHash(tableA)).as("premise: the old shortened names were identical")
                .isEqualTo(polynomialHash(tableB));
        long stamp = System.currentTimeMillis();
        assertThat(PostgresLogBackend.archiveTableName(tableA, stamp))
                .isNotEqualTo(PostgresLogBackend.archiveTableName(tableB, stamp));
        assertThat(PostgresLogBackend.sequenceName(tableA)).isNotEqualTo(PostgresLogBackend.sequenceName(tableB));
        assertThat(PostgresLogBackend.primaryKeyName(tableA)).isNotEqualTo(PostgresLogBackend.primaryKeyName(tableB));

        try (var a = new PostgresLogBackend(dataSource, idA);
             var b = new PostgresLogBackend(dataSource, idB)) {
            a.append(new LogLeader(0, 1L, "A"), "A");
            b.append(new LogLeader(0, 1L, "B"), "B");
            List<String> archivesA = new ArrayList<>();
            List<String> archivesB = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                archivesB.add(b.compact(List.of(new LogLeader(0, 10L + i, "B")), "B").archivedLogId());
                archivesA.add(a.compact(List.of(new LogLeader(0, 10L + i, "A")), "A").archivedLogId());
            }
            // A table somebody else created under exactly the name A derives for an archive,
            // without A's owner mark: a name match alone is not ownership.
            String squatter = PostgresLogBackend.archiveTableName(tableA, 1_000L);
            try (var c = dataSource.getConnection(); var st = c.createStatement()) {
                st.execute("CREATE TABLE " + squatter + " (x int)");
            }

            a.purgeArchives(0);

            for (String archive : archivesA) assertThat(relationExists(archive)).as(archive).isFalse();
            for (String archive : archivesB) {
                assertThat(relationExists(archive)).as("B's archive %s survives A's purge", archive).isTrue();
            }
            assertThat(relationExists(squatter)).as("an unmarked table is never purged").isTrue();

            b.purgeArchives(1);
            assertThat(relationExists(archivesB.get(0))).isFalse();
            assertThat(relationExists(archivesB.get(1))).isFalse();
            assertThat(relationExists(archivesB.get(2))).isTrue();
            assertThat(a.length()).isEqualTo(1);
            assertThat(b.length()).isEqualTo(1);
        }
    }

    // ───────────────── the append guard under a generic plan ─────────────────

    /**
     * pgjdbc server-prepares a statement after a few executions and Postgres then may run it
     * with a generic plan, which cannot see the watermark parameter. The old guard
     * ({@code NOT EXISTS (… WHERE row_id > ?)}) became a sequential scan of the whole log on
     * every append there; the {@code max(row_id)} guard stays a one-row index lookup.
     */
    @Test
    void the_append_guard_does_not_scan_the_table_under_a_generic_plan() throws Exception {
        String id = "guard_" + uuid();
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
        }
        int rows = 5_000;
        fill(table, rows);

        String oldGuard = "INSERT INTO " + table + " (type, payload, ts_millis)"
                + " SELECT ?::text, ?::bytea, ?::bigint"
                + " WHERE NOT EXISTS (SELECT 1 FROM " + table + " WHERE row_id > ?) RETURNING row_id";
        assertThat(seqScansOfGuard(table, oldGuard, 1)).as("premise: the old guard scans the table").isPositive();
        assertThat(seqScansOfGuard(table, PostgresLogBackend.guardedInsertSqlFor(table), 2))
                .as("sequential scans by the new guard").isZero();

        // And end to end: appends after the backend's statement is server-prepared stay cheap.
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 2L, "A"), "A");
            for (int i = 0; i < 20; i++) {
                assertThat(b.append(new LogLeader(0, 3L + i, "A"), "A")).isPresent();
            }
            assertThat(b.length()).isEqualTo(rows + 1 + 21);
        }
    }

    /**
     * Runs {@code guardSql} 20 times (server-prepared, generic plan forced) with a watermark
     * below the last row — the guard refuses, nothing is inserted — and returns the sequential
     * scans of {@code table} this transaction made.
     */
    private static long seqScansOfGuard(String table, String guardSql, int watermarkParams) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try (var st = c.createStatement()) {
                st.execute("SET LOCAL plan_cache_mode = force_generic_plan");
            }
            try (var ps = c.prepareStatement(guardSql)) {
                for (int i = 0; i < 20; i++) {
                    ps.setString(1, "LogLeader");
                    ps.setBytes(2, new byte[0]);
                    ps.setLong(3, 0L);
                    for (int p = 0; p < watermarkParams; p++) ps.setLong(4 + p, 1L);
                    try (var rs = ps.executeQuery()) {
                        assertThat(rs.next()).as("the guard refuses: rows exist past the watermark").isFalse();
                    }
                }
            }
            try (var ps = c.prepareStatement(
                    "SELECT coalesce(seq_scan, 0) FROM pg_stat_xact_user_tables WHERE relname = ?")) {
                ps.setString(1, table);
                try (var rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong(1) : 0;
                }
            } finally {
                c.rollback();
            }
        }
    }

    // ───────────────── linear range scans ─────────────────

    /**
     * A chunked scan of the whole log (what an engine start, a resume and a snapshot do) used
     * to count the whole table and skip to the chunk with OFFSET on every chunk: quadratic.
     * Keyset reads visit each row a small constant number of times.
     */
    @Test
    void a_chunked_full_scan_visits_each_row_a_constant_number_of_times() throws Exception {
        String id = "scan_" + uuid();
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
        }
        int rows = 50_000;
        fill(table, rows - 1);
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("VACUUM ANALYZE " + table);
        }
        int chunk = 500;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            int length = b.length();
            assertThat(length).isEqualTo(rows);
            long before = settledRowVisits(table);
            int read = 0;
            for (int from = 0; from < length; from += chunk) {
                LogEvent[] events = b.getBetween(from, Math.min(length, from + chunk));
                assertThat(events).hasSize(Math.min(chunk, length - from));
                read += events.length;
            }
            assertThat(read).isEqualTo(rows);
            assertThat(b.keysetRangeReads.get()).as("every chunk after the first is read by key")
                    .isEqualTo(rows / chunk - 1);
            long visits = settledRowVisits(table) - before;
            // Keyset: ~2 visits per row (listing + payload). Count + OFFSET per chunk: ~150 per row here.
            assertThat(visits).as("row visits for a scan of %d rows", rows).isLessThan(5L * rows);

            // After a compaction the cursor points into the archived table: reads fall back and are right.
            b.compact(List.of(new LogLeader(0, 1_000_000L, "A")), "A");
            assertThat(b.getBetween(0, 1)).hasSize(1);
            assertThat(((LogLeader) b.getBetween(0, 1)[0]).timestamp()).isEqualTo(1_000_000L);
        }
    }

    // ───────────────── the latest LogLeader is an index lookup ─────────────────

    @Test
    void the_latest_log_leader_is_found_through_a_partial_index_not_a_table_scan() throws Exception {
        String id = "leaderix_" + uuid();
        String table = "fom_log_" + id;
        String index = PostgresLogBackend.leaderIndexName(table);
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
        }
        assertThat(indexesOf(table)).contains(index);
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            // Plenty of non-leader rows (never decoded: the table is not opened again).
            st.execute("INSERT INTO " + table + " (type, payload, ts_millis)"
                    + " SELECT 'LogInitialized', '\\x00'::bytea, g FROM generate_series(1, 50000) g");
            st.execute("ANALYZE " + table);
            var plan = new StringBuilder();
            try (var rs = st.executeQuery("EXPLAIN SELECT payload FROM " + table
                    + " WHERE type = 'LogLeader' ORDER BY row_id DESC LIMIT 1")) {
                while (rs.next()) plan.append(rs.getString(1)).append('\n');
            }
            assertThat(plan.toString()).contains(index).doesNotContain("Seq Scan");
        }
    }

    @Test
    void a_table_without_the_log_leader_index_gets_it_when_opened() throws Exception {
        String id = "oldtable_" + uuid();
        String table = "fom_log_" + id;
        String index = PostgresLogBackend.leaderIndexName(table);
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
        }
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("DROP INDEX " + index); // as a table created by an older version
        }
        assertThat(indexesOf(table)).doesNotContain(index);
        try (var b = new PostgresLogBackend(dataSource, id)) {
            assertThat(indexesOf(table)).contains(index);
            assertThat(b.introspect().currentLeader()).isEqualTo("A");
            assertThat(b.append(new LogLeader(0, 2L, "A"), "A")).isPresent();
        }
        try (var b = new PostgresLogBackend(dataSource, id)) { // and again: a no-op
            assertThat(indexesOf(table)).containsOnlyOnce(index);
            assertThat(b.length()).isEqualTo(2);
        }
    }

    @Test
    void a_role_that_does_not_own_an_old_table_still_opens_it_without_the_index() throws Exception {
        String id = "notowner_" + uuid();
        String table = "fom_log_" + id;
        String role = "ix_" + uuid().substring(0, 12);
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
        }
        try (var c = dataSource.getConnection(); var st = c.createStatement()) {
            st.execute("DROP INDEX " + PostgresLogBackend.leaderIndexName(table));
            st.execute("CREATE ROLE " + role + " LOGIN PASSWORD 'pw'");
            st.execute("GRANT SELECT, INSERT, UPDATE ON " + table + " TO " + role);
            st.execute("GRANT USAGE ON SEQUENCE " + PostgresLogBackend.sequenceName(table) + " TO " + role);
        }
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(role);
        ds.setPassword("pw");
        try (var b = new PostgresLogBackend(ds, id)) {
            assertThat(b.introspect().currentLeader()).isEqualTo("A");
            assertThat(b.append(new LogLeader(0, 2L, "A"), "A")).isPresent();
        }
        assertThat(indexesOf(table)).doesNotContain(PostgresLogBackend.leaderIndexName(table));
    }

    @Test
    void compaction_moves_the_log_leader_index_to_the_archive_and_purge_drops_it() throws Exception {
        String id = "ixcompact_" + uuid();
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            String first = b.compact(List.of(new LogLeader(0, 5L, "A")), "A").archivedLogId();
            Thread.sleep(2);
            String second = b.compact(List.of(new LogLeader(0, 6L, "A")), "A").archivedLogId();
            assertThat(indexesOf(table)).contains(PostgresLogBackend.leaderIndexName(table));
            assertThat(indexesOf(first)).contains(PostgresLogBackend.leaderIndexName(first));
            assertThat(indexesOf(second)).contains(PostgresLogBackend.leaderIndexName(second));
            assertThat(b.append(new LogLeader(0, 7L, "A"), "A")).isPresent();
            b.purgeArchives(0);
            assertThat(relationExists(PostgresLogBackend.leaderIndexName(first))).isFalse();
            assertThat(relationExists(PostgresLogBackend.leaderIndexName(second))).isFalse();
        }
        try (var b = new PostgresLogBackend(dataSource, id)) {
            assertThat(b.introspect().currentLeader()).isEqualTo("A");
            assertThat(b.length()).isEqualTo(2);
        }
    }

    @Test
    void a_log_with_gaps_in_its_row_ids_still_opens() throws Exception {
        String id = "gaps_" + uuid();
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
            try (var c = dataSource.getConnection(); var st = c.createStatement()) {
                // Row ids taken by appends that rolled back.
                st.execute("SELECT nextval('" + PostgresLogBackend.sequenceName(table) + "')"
                        + " FROM generate_series(1, 5)");
            }
            b.append(new LogLeader(0, 2L, "A"), "A");
        }
        try (var b = new PostgresLogBackend(dataSource, id)) {
            assertThat(b.length()).isEqualTo(2);
            assertThat(b.append(new LogLeader(0, 3L, "A"), "A")).isPresent();
        }
    }

    @Test
    void an_open_cancelled_by_statement_timeout_says_so() throws Exception {
        String id = "timeout_" + uuid();
        String table = "fom_log_" + id;
        try (var b = new PostgresLogBackend(dataSource, id)) {
            b.append(new LogLeader(0, 1L, "A"), "A");
        }
        var tight = new PGSimpleDataSource();
        tight.setUrl(postgres.getJdbcUrl());
        tight.setUser(postgres.getUsername());
        tight.setPassword(postgres.getPassword());
        tight.setOptions("-c statement_timeout=300");
        try (var blocker = dataSource.getConnection()) {
            // Holds the open's reads up past the statement_timeout, as a scan of a huge table would.
            blocker.setAutoCommit(false);
            try (var st = blocker.createStatement()) {
                st.execute("LOCK TABLE " + table + " IN ACCESS EXCLUSIVE MODE");
            }
            Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(
                    () -> new PostgresLogBackend(tight, id).close());
            assertThat(thrown).isInstanceOf(SQLException.class)
                    .hasMessageContaining("was cancelled by the server")
                    .hasMessageContaining("statement_timeout")
                    .hasMessageContaining("CREATE INDEX IF NOT EXISTS " + PostgresLogBackend.leaderIndexName(table));
            assertThat(((SQLException) thrown).getSQLState()).isEqualTo("57014");
            blocker.rollback();
        }
        // Nothing leaked: the lock was released and the log opens normally.
        try (var b = new PostgresLogBackend(dataSource, id)) {
            assertThat(b.length()).isEqualTo(1);
        }
    }

    private static List<String> indexesOf(String table) throws SQLException {
        var names = new ArrayList<String>();
        try (var c = dataSource.getConnection(); var st = c.prepareStatement(
                "SELECT indexname FROM pg_indexes WHERE tablename = ? ORDER BY indexname")) {
            st.setString(1, table);
            try (var rs = st.executeQuery()) {
                while (rs.next()) names.add(rs.getString(1));
            }
        }
        return names;
    }

    // ───────────────── helpers ─────────────────

    private static String uuid() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /** Adds {@code rows} copies of the log's first row (payload and all). */
    /**
     * Appends {@code rows} {@code LogLeader} rows straight into the table, with clocks continuing
     * after the first row's (0): the backend refuses a log whose clocks do not strictly increase.
     */
    private static void fill(String table, int rows) throws SQLException {
        try (var c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try (var ins = c.prepareStatement(
                    "INSERT INTO " + table + " (type, payload, ts_millis) VALUES ('LogLeader', ?, ?)")) {
                for (int i = 1; i <= rows; i++) {
                    var bytes = new java.io.ByteArrayOutputStream();
                    try (var oos = new java.io.ObjectOutputStream(bytes)) {
                        oos.writeObject(new LogLeader(i, i, "A"));
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                    ins.setBytes(1, bytes.toByteArray());
                    ins.setLong(2, i);
                    ins.addBatch();
                    if (i % 1_000 == 0) ins.executeBatch();
                }
                ins.executeBatch();
            }
            c.commit();
            c.setAutoCommit(true);
            try (var st = c.createStatement()) {
                st.execute("ANALYZE " + table);
            }
        }
    }


    /**
     * Tuples of {@code table} read by sequential and index scans so far, once the counter has
     * stopped moving (sessions report their counts when they end, a moment after closing).
     */
    private static long settledRowVisits(String table) {
        var last = new AtomicLong(-1);
        var stableSince = new AtomicLong();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).until(() -> {
            long now = rowVisits(table);
            if (now != last.get()) {
                last.set(now);
                stableSince.set(System.nanoTime());
                return false;
            }
            return System.nanoTime() - stableSince.get() > Duration.ofMillis(1_500).toNanos();
        });
        return last.get();
    }

    private static long rowVisits(String table) throws SQLException {
        try (var c = dataSource.getConnection(); var ps = c.prepareStatement(
                "SELECT coalesce(t.seq_tup_read, 0) + coalesce((SELECT sum(i.idx_tup_read)"
                        + " FROM pg_stat_user_indexes i WHERE i.relid = t.relid), 0)"
                        + " FROM pg_stat_user_tables t WHERE t.relname = ?")) {
            ps.setString(1, table);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
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
}
