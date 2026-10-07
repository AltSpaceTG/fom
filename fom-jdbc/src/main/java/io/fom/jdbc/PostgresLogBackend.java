package io.fom.jdbc;

import io.fom.SnapshotResult;
import io.fom.api.LeadershipLostException;
import io.fom.log.LogClocks;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogEvent;
import io.fom.log.LogLeader;
import io.fom.log.LogSnapshot;
import io.fom.serde.ObjectInputFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Postgres-backed append-only log, one table per log:
 * <pre>
 * CREATE SEQUENCE &lt;table&gt;_rid_seq;
 * CREATE TABLE &lt;table&gt; (
 *   row_id    BIGINT    NOT NULL DEFAULT nextval('&lt;table&gt;_rid_seq'),
 *   type      TEXT      NOT NULL,
 *   payload   BYTEA     NOT NULL,
 *   ts_millis BIGINT    NOT NULL,
 *   CONSTRAINT &lt;table&gt;_rid_pk PRIMARY KEY (row_id)
 * );
 * </pre>
 * The sequence and key are named explicitly (hashed when the name would pass 63 bytes):
 * Postgres' implicit names cut the table name, so two logs with a long common prefix would
 * race for one sequence. {@code row_id} only orders rows; {@link #get(int)} and
 * {@link #getBetween} address events by 0-based position, and each event's clock lives in
 * its payload.
 *
 * <p><b>Leadership and fencing.</b> The constructor takes
 * {@code pg_try_advisory_lock(hash64(current_schema() + "." + table))} on a dedicated leader
 * connection and holds it until {@link #close()}, which unlocks explicitly (a pooled session
 * outlives {@code Connection.close()}) and aborts the connection if the release cannot be
 * confirmed. A second instance on the same table fails with {@link IllegalStateException}.
 * Every write runs on the leader connection and first locks the table in
 * {@code SHARE ROW EXCLUSIVE} mode (compaction: {@code ACCESS EXCLUSIVE}) at
 * {@code READ COMMITTED}, so every other writer has committed or waits; it then checks the
 * advisory lock and the latest {@link LogLeader}. A lost lock fails the write with
 * {@link LeadershipLostException} and fences this instance for good. A lock check that
 * fails is confirmed from a separate session; checks that stay inconclusive for more than
 * {@value #INCONCLUSIVE_LIMIT_MILLIS} ms fence too. An append by an instance whose log was
 * taken over returns {@link Optional#empty()}.</p>
 *
 * <p><b>Reads.</b> {@link #length()}, {@link #get(int)} and {@link #getBetween} use
 * short-lived {@link DataSource} sessions and take no lock. A range read lists its window
 * and the bound check under one snapshot, then fetches payloads in runs of at most
 * {@value #READ_BATCH_BYTES} bytes, each checked against the listed table's oid; a read
 * that raced a compaction's table swap is retried, so it sees the whole log before or after
 * it. A run cancelled by {@code statement_timeout} ({@code 57014}) is retried in halves; a
 * read whose session is gone (SQLState class {@code 08}, {@code 57P01}–{@code 57P03}) is
 * retried once on a fresh connection.</p>
 *
 * <p><b>Checks on open.</b> The table is looked up in {@code current_schema()} only; a name
 * resolving elsewhere, an existing table without the log's columns, or a sequence/index
 * name already taken is refused with {@link IllegalArgumentException}. A hot standby or
 * read-only session, and a {@link DataSource} that does not keep one server session per
 * connection (PgBouncer {@code pool_mode=transaction}), are refused with
 * {@link IllegalStateException} when detected.</p>
 *
 * <p>Give the {@link DataSource} connection and socket timeouts: reads take connections
 * from it directly. {@link #introspect()} bounds its own work to
 * {@value #INTROSPECT_TIMEOUT_MILLIS} ms, never uses the leader connection, and falls back
 * to the last known values; after {@link #close()} it still answers, with no leader. Every
 * other operation on a closed backend throws {@link IllegalStateException}.</p>
 *
 * <p>Payloads are Java-serialized and read under the {@link ObjectInputFilters#logPayload()}
 * allowlist, since anyone with write access to the table could plant a forged payload.</p>
 */
public final class PostgresLogBackend implements LogBackend {

    private static final Logger log = LoggerFactory.getLogger(PostgresLogBackend.class);

    private static final ObjectInputFilter LOG_FILTER = ObjectInputFilters.logPayload();

    /** Postgres NAMEDATALEN - 1: longer identifiers are silently truncated. */
    static final int MAX_IDENTIFIER_LENGTH = 63;

    /** Separates a log's table name from the stamp in its archive table names. */
    private static final String ARCHIVE_MARKER = "_archived_";

    /** How often the idle leader session is touched (and its lock checked). */
    private static final long KEEPALIVE_MILLIS = 15_000;

    /** How long compact() retries to lock the table without queueing before it gives up. */
    private static final long COMPACT_LOCK_WAIT_MILLIS = 5_000;

    /**
     * {@code lock_timeout} for the rest of a compaction once it holds the table. Its RENAMEs
     * touch {@code pg_class} rows a concurrent migration may hold (an open
     * {@code GRANT ... ON ALL TABLES}); unbounded, that wait would block every reader behind
     * the table lock. With it the attempt fails with {@code 55P03} and is retried.
     */
    static final long COMPACT_DDL_LOCK_TIMEOUT_MILLIS = 1_000;

    /** Start of a loss reason established from a separate session. */
    private static final String SEEN_FROM_SEPARATE_SESSION = "a separate session sees that the leader session";

    /** Bound on the one diagnostic statement {@link #markLockLost()} may send on the leader session. */
    static final int LEADER_DIAGNOSIS_TIMEOUT_MILLIS = 2_000;

    private final DataSource dataSource;
    private final String logId;
    private final String tableName;
    private final Connection leaderConnection;
    /** {@code pg_backend_pid()} of the leader session, to confirm the lock from another session. */
    private final int leaderPid;
    private final long lockKey;
    /** Isolation level the leader connection came with; restored before it is handed back. */
    private int pooledIsolation = Connection.TRANSACTION_READ_COMMITTED;
    private final ReentrantLock appendLock = new ReentrantLock();

    /**
     * The one platform thread that uses {@link #leaderConnection}. Interrupting a virtual
     * thread closes its socket, which on the leader session would drop the advisory lock and
     * fence this instance, so callers hand the work over and wait uninterruptibly.
     */
    private final ScheduledThreadPoolExecutor leaderExecutor;
    private final ScheduledFuture<?> keepalive;

    private volatile String currentLeader;
    /** Clock of the last event (-1 while empty); written only on the leader thread. */
    private volatile long lastClock = -1;
    /**
     * {@code row_id} of the newest row this instance has seen (-1 while empty). {@link #append}
     * inserts only while nothing was written past it, which is what makes
     * {@link #currentLeader} safe to trust between appends. Leader thread only.
     */
    private volatile long lastRowId = -1;
    private volatile boolean closed = false;
    /** Set once the leader session is found not to hold the advisory lock; permanent. */
    private volatile boolean lockLost = false;
    /** Why the lock counted as lost; reported by every {@link LeadershipLostException} after it. */
    private volatile String lockLossReason;
    /** The failure behind {@link #lockLossReason}, or {@code null}. */
    private volatile Throwable lockLossCause;

    /**
     * The log's length as last known, and the write generation it holds for (see
     * {@link #writesStarted}). Kept by reads, reports and this instance's own writes, so a
     * closed backend can still report the length the engine last saw.
     */
    private record KnownLength(int length, long generation) { }

    private final AtomicReference<KnownLength> knownLength = new AtomicReference<>();
    /**
     * Write generations: {@code writesStarted} is bumped just before a commit and
     * {@code writesFinished} catches up once its outcome is settled. Only this instance writes,
     * so a read that saw {@code writesFinished == g} before its query and
     * {@code writesStarted == g} after it saw exactly generation {@code g}. Under {@link #appendLock}.
     */
    private volatile long writesStarted = 0;
    private volatile long writesFinished = 0;

    /** Bound on one lock confirmation from a separate session (checkout + query). */
    private final long confirmTimeoutMillis;
    /** How long lock checks may stay inconclusive before the lock is treated as lost. */
    private final long inconclusiveLimitMillis;

    // Lock-check state, leader thread only.
    private boolean inconclusiveRun = false;
    private long inconclusiveSinceNanos;
    /** The last confirmation runner; no new one starts while an abandoned one is alive. */
    private Thread confirmationThread;
    private FutureTask<Boolean> confirmationTask;
    private long confirmationStartedNanos;
    /** Occurrence counters for the repeating lock-check warnings, see {@link #logThrottled}. */
    private long skippedConfirmations;
    private long timedOutConfirmations;
    private long failedConfirmations;
    private long inconclusiveChecks;

    /**
     * Log in table {@code fom_log_<logId>}. Like any unquoted Postgres identifier the name is
     * case-insensitive: {@code Stations} and {@code stations} are the same log.
     *
     * @throws IllegalArgumentException if {@code logId} has characters other than ASCII
     *         letters, digits and {@code _}; otherwise {@code stations-eu} and
     *         {@code stations_eu} could map to one table
     */
    public PostgresLogBackend(DataSource dataSource, String logId) throws SQLException {
        this(dataSource, logId, "fom_log_" + requireSafeLogId(logId));
    }

    /**
     * Log in an explicit table. The name is folded to lower case, as Postgres does with
     * unquoted identifiers; reported names ({@link SnapshotResult}) are the lower-cased ones.
     *
     * @throws IllegalArgumentException if {@code tableName} is not a plain SQL identifier,
     *         exceeds 63 bytes, or contains {@code _archived_} (reserved for archive tables)
     */
    public PostgresLogBackend(DataSource dataSource, String logId, String tableName) throws SQLException {
        this(dataSource, logId, tableName, CONFIRM_TIMEOUT_MILLIS, INCONCLUSIVE_LIMIT_MILLIS);
    }

    /** Tests only: explicit bounds for the lock confirmation and for inconclusive lock checks. */
    PostgresLogBackend(DataSource dataSource, String logId, String tableName,
                       long confirmTimeoutMillis, long inconclusiveLimitMillis) throws SQLException {
        this(dataSource, logId, tableName, confirmTimeoutMillis, inconclusiveLimitMillis, null);
    }

    /**
     * Tests only: {@code lockKeyOverride} replaces the advisory-lock key, so two backends can
     * write one table at once, as a node with a different lock key would; the write fence
     * must survive that.
     */
    PostgresLogBackend(DataSource dataSource, String logId, String tableName,
                       long confirmTimeoutMillis, long inconclusiveLimitMillis,
                       Long lockKeyOverride) throws SQLException {
        if (confirmTimeoutMillis <= 0 || inconclusiveLimitMillis < 0) {
            throw new IllegalArgumentException("confirmTimeoutMillis must be > 0 and inconclusiveLimitMillis >= 0");
        }
        this.confirmTimeoutMillis = confirmTimeoutMillis;
        this.inconclusiveLimitMillis = inconclusiveLimitMillis;
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.logId = Objects.requireNonNull(logId, "logId");
        tableName = normalizeTableName(tableName);
        this.tableName = tableName;

        Connection conn = dataSource.getConnection();
        boolean locked = false;
        boolean ok = false;
        try {
            conn.setAutoCommit(true);
            forceReadCommitted(conn);
            requireWritableServer(conn, tableName);
            rejectReservedWord(conn, tableName);
            // The same name in two schemas is two logs, so the schema is part of the key.
            String schema = targetSchema(conn, tableName);
            this.lockKey = lockKeyOverride != null ? lockKeyOverride : advisoryKey(schema + "." + tableName);
            int probedPid = sessionPid(conn);
            // One transaction, so a transaction-pooling proxy cannot run the check and the lock
            // on different server sessions.
            conn.setAutoCommit(false);
            try {
                int lockPid = requireLockNotYetHeld(conn);
                if (lockPid != probedPid) {
                    throw sessionSharingProxy(tableName, "consecutive transactions ran on different server "
                            + "sessions (backend pid " + probedPid + ", then " + lockPid + ")");
                }
                try (var st = conn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                    st.setLong(1, lockKey);
                    try (ResultSet rs = st.executeQuery()) {
                        rs.next();
                        if (!rs.getBoolean(1)) {
                            throw new IllegalStateException("Could not acquire advisory lock for " + tableName
                                    + " — another instance holds it");
                        }
                        locked = true;
                    }
                }
                conn.commit();
            } finally {
                if (!conn.getAutoCommit()) {
                    try { conn.rollback(); } catch (SQLException ignored) { /* after commit: a no-op */ }
                    conn.setAutoCommit(true);
                }
            }
            if (!runsOnLockedSession(conn, probedPid)) {
                throw sessionSharingProxy(tableName, "the advisory lock taken on backend pid " + probedPid
                        + " is not held by the session the next transaction ran on");
            }
            // Only the lock holder creates the table, so nodes starting together on a new log
            // get the lock error rather than racing on CREATE TABLE.
            prepareTable(conn);
            this.leaderPid = sessionPid(conn);
            if (leaderPid != probedPid) {
                throw sessionSharingProxy(tableName, "the session that took the advisory lock (backend pid "
                        + probedPid + ") is not the one later transactions run on (" + leaderPid + ")");
            }
            this.leaderConnection = conn;
            readInitialState();
            ok = true;
        } catch (SQLException e) {
            throw cancelledWhileOpening(e);
        } finally {
            if (!ok) {
                // Explicit unlock: closing a pooled connection keeps its session and the lock.
                if (locked) {
                    releaseLockAndClose(conn);
                } else {
                    restoreIsolationAndClose(conn);
                }
            }
        }
        this.leaderExecutor = newLeaderExecutor("fom-pg-leader-" + tableName);
        this.keepalive = leaderExecutor.scheduleWithFixedDelay(
                this::keepLeaderSessionAlive, KEEPALIVE_MILLIS, KEEPALIVE_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** Validates {@code tableName} and folds it to lower case, the name Postgres stores. */
    private static String normalizeTableName(String tableName) {
        Objects.requireNonNull(tableName, "tableName");
        validateIdentifier(tableName);
        if (tableName.toLowerCase(Locale.ROOT).contains(ARCHIVE_MARKER)) {
            // purgeArchives recognises archives by name and could drop such a live table.
            throw new IllegalArgumentException("Table name '" + tableName + "' must not contain '"
                    + ARCHIVE_MARKER + "' (reserved for archive tables)");
        }
        tableName = tableName.toLowerCase(Locale.ROOT);
        if (tableName.length() > MAX_IDENTIFIER_LENGTH) {
            // Postgres would truncate it, and two logIds could end up on one table.
            throw new IllegalArgumentException("Table name exceeds Postgres' 63-byte limit: " + tableName);
        }
        return tableName;
    }

    /** A single platform thread: its socket I/O ignores interrupts. */
    private static ScheduledThreadPoolExecutor newLeaderExecutor(String threadName) {
        var executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /**
     * Fails if the session already holds this log's lock: {@code pg_try_advisory_lock} is
     * re-entrant and would succeed on a session another client locked through a pooler.
     * Returns the session's backend pid.
     */
    private int requireLockNotYetHeld(Connection conn) throws SQLException {
        try (var st = conn.prepareStatement("SELECT pg_backend_pid(), EXISTS (SELECT 1 FROM pg_locks"
                + " WHERE pid = pg_backend_pid() AND " + LOCK_ROW_MATCH + ")")) {
            st.setLong(1, lockKey >>> 32);
            st.setLong(2, lockKey & 0xFFFFFFFFL);
            try (ResultSet rs = st.executeQuery()) {
                rs.next();
                if (rs.getBoolean(2)) {
                    throw sessionSharingProxy(tableName, "the server session handed out already holds "
                            + "this log's advisory lock (another client locked it on the same session)");
                }
                return rs.getInt(1);
            }
        }
    }

    /** Creates the log table if missing, then checks that it is one. Runs under the advisory lock. */
    private void prepareTable(Connection conn) throws SQLException {
        requireInTargetSchema(conn, tableName);
        String relkind = relkindOf(conn, tableName);
        if (relkind == null) {
            // The sequence and indexes share the table namespace; a taken name would fail the
            // CREATE with a raw "relation already exists".
            requireFreeDerivedNames(conn, tableName);
            SQLException nameClash = null;
            conn.setAutoCommit(false); // all or nothing: a leftover sequence would fail the next attempt
            try (var st = conn.createStatement()) {
                createLiveTable(st, tableName);
                conn.commit();
            } catch (SQLException e) {
                try { conn.rollback(); } catch (SQLException ignored) { /* reported below */ }
                if (!isRelationNameClash(e)) throw e;
                nameClash = e;
            } finally {
                conn.setAutoCommit(true);
            }
            relkind = relkindOf(conn, tableName);
            if (nameClash != null && relkind == null) {
                // Another session created one of our derived names concurrently; it has committed
                // now, so the checks can name it. A clash on the table name itself falls through.
                requireFreeDerivedNames(conn, tableName);
                throw new IllegalArgumentException("Cannot create log table '" + tableName + "': another "
                        + "session created a relation with the name of the table, its sequence or one of "
                        + "its indexes at the same time (" + nameClash.getMessage()
                        + "); choose another logId or table name", nameClash);
            }
        }
        if (relkind != null && !"r".equals(relkind)) {
            throw new IllegalArgumentException("'" + tableName + "' already names a non-table object"
                    + " (relkind " + relkind + ", e.g. another log's sequence or index);"
                    + " choose another logId or table name");
        }
        requireLogTableColumns(conn, tableName);
        ensureLeaderIndex(conn);
    }

    /** Reads the leadership state on a second connection, and refuses a spliced log. */
    private void readInitialState() throws SQLException {
        Connection second;
        try {
            second = dataSource.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException("PostgresLogBackend " + logId + ": could not get a second "
                    + "connection from the DataSource (" + e.getMessage() + "); the leader session holds one "
                    + "for the backend's lifetime, so size the pool to at least the number of backends + 1", e);
        }
        try (second) {
            readLeaderState(second);
            requireLastClockCoversLength(second);
        }
    }

    /** {@code pg_backend_pid()} of the server session the next statement runs on. */
    private static int sessionPid(Connection conn) throws SQLException {
        try (var st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /** Whether the next statement runs on server session {@code expectedPid} and it holds this log's lock. */
    private boolean runsOnLockedSession(Connection conn, int expectedPid) throws SQLException {
        try (var st = conn.prepareStatement("SELECT pg_backend_pid() = ? AND EXISTS (SELECT 1 FROM pg_locks"
                + " WHERE pid = pg_backend_pid() AND " + LOCK_ROW_MATCH + ")")) {
            st.setInt(1, expectedPid);
            st.setLong(2, lockKey >>> 32);
            st.setLong(3, lockKey & 0xFFFFFFFFL);
            try (ResultSet rs = st.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private static IllegalStateException sessionSharingProxy(String tableName, String evidence) {
        return new IllegalStateException("PostgresLogBackend[" + tableName + "]: the DataSource does not keep one "
                + "server session per connection (" + evidence + "); leadership needs a direct connection "
                + "or pool_mode=session");
    }

    /** Fails on a hot standby or read-only session, which would otherwise fail only at the first write. */
    private static void requireWritableServer(Connection conn, String tableName) throws SQLException {
        try (var st = conn.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT pg_is_in_recovery(), current_setting('transaction_read_only')")) {
            rs.next();
            boolean recovery = rs.getBoolean(1);
            boolean readOnly = "on".equals(rs.getString(2));
            if (recovery || readOnly) {
                throw new IllegalStateException("PostgresLogBackend[" + tableName + "]: the database is a read-only "
                        + "server (hot standby?)" + (recovery ? " — pg_is_in_recovery() is true"
                        : " — the session is read-only (default_transaction_read_only = on?)"));
            }
        }
    }

    /** {@code current_schema()}, where the table is created and looked up; fails if there is none. */
    private static String targetSchema(Connection conn, String tableName) throws SQLException {
        try (var st = conn.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT current_schema(), current_setting('search_path')")) {
            rs.next();
            String schema = rs.getString(1);
            if (schema == null) {
                throw new IllegalStateException("PostgresLogBackend[" + tableName + "]: current_schema() is null — "
                        + "no schema on the search_path (" + rs.getString(2) + ") exists (pgjdbc: "
                        + "currentSchema=<schema>)");
            }
            return schema;
        }
    }

    /** Unquoted table names can't be reserved SQL keywords; fails clearly rather than with a syntax error. */
    private static void rejectReservedWord(Connection conn, String name) throws SQLException {
        try (var st = conn.prepareStatement("SELECT catcode FROM pg_get_keywords() WHERE word = ?")) {
            st.setString(1, name);
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next() && ("R".equals(rs.getString(1)) || "T".equals(rs.getString(1)))) {
                    throw new IllegalArgumentException("Table name '" + name + "' is a reserved SQL keyword");
                }
            }
        }
    }

    /**
     * Runs {@code op} on the leader thread and waits for it, ignoring interrupts (the flag
     * is restored), so an interrupted caller can neither break the leader session nor
     * abandon a write halfway.
     */
    private <T> T onLeaderThread(Callable<T> op) {
        Future<T> future;
        try {
            future = leaderExecutor.submit(op);
        } catch (RejectedExecutionException e) {
            if (closed) {
                // The normal outcome after close(); the rejection itself says nothing useful,
                // so it is not chained into the exception.
                log.debug("PostgresLogBackend[{}] a leader-session operation was rejected after close()",
                        tableName, e);
                throw new IllegalStateException("PostgresLogBackend " + logId + " is closed");
            }
            throw new IllegalStateException("PostgresLogBackend " + logId
                    + ": the leader executor rejected an operation while the backend is open", e);
        }
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    // The failure's own trace shows the leader thread; keep the caller's too.
                    cause.addSuppressed(new Exception("Caller of the failed leader-session operation"));
                    if (cause instanceof RuntimeException re) throw re;
                    if (cause instanceof Error err) throw err;
                    throw new RuntimeException(cause);
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /** Keeps idle timeouts off the leader session and notices a lost lock before the next write. */
    private void keepLeaderSessionAlive() {
        if (closed || lockLost || currentLeader == null || !appendLock.tryLock()) return;
        try {
            if (!holdsAdvisoryLock(leaderConnection)) markLockLost();
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Takes the ACCESS EXCLUSIVE lock the archive RENAME needs, with NOWAIT retried briefly:
     * a queued request would block every reader of the table behind any long-running reader.
     */
    private void lockTableWithoutQueueing(Connection conn) throws SQLException {
        try (var st = conn.createStatement()) {
            st.execute(SET_READ_COMMITTED);
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(COMPACT_LOCK_WAIT_MILLIS);
        while (true) {
            Savepoint savepoint = conn.setSavepoint();
            try (var st = conn.createStatement()) {
                st.execute("LOCK TABLE " + tableName + " IN ACCESS EXCLUSIVE MODE NOWAIT");
                conn.releaseSavepoint(savepoint);
                // Bounds every later lock wait while this transaction holds the table.
                st.execute("SET LOCAL lock_timeout = '" + COMPACT_DDL_LOCK_TIMEOUT_MILLIS + "ms'");
                return;
            } catch (SQLException e) {
                if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) throw e;
                conn.rollback(savepoint);
                if (System.nanoTime() >= deadline) {
                    throw new SQLException("Could not lock " + tableName + " for compaction within "
                            + COMPACT_LOCK_WAIT_MILLIS + " ms: other sessions keep using it", LOCK_NOT_AVAILABLE, e);
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while waiting to lock " + tableName, ie);
                }
            }
        }
    }

    /** Attempts {@link #compact} makes on a transient conflict; the waits double from {@value #COMPACT_RETRY_BASE_MILLIS} ms. */
    static final int COMPACT_ATTEMPTS = 5;
    private static final long COMPACT_RETRY_BASE_MILLIS = 50;

    /**
     * Compaction failures caused by a concurrent catalog update (a migration's
     * {@code GRANT ... ON ALL TABLES IN SCHEMA}, say): {@code XX000} "tuple concurrently
     * updated/deleted", deadlock {@code 40P01} or serialization failure {@code 40001}. The
     * transaction rolls back whole, so a retry is safe. {@code 55P03} after the table is locked
     * is the same conflict and is retried by {@link #compact} separately: from the table lock
     * itself it means readers keep the table busy.
     */
    static boolean isTransientCatalogConflict(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && s.getSQLState() != null) {
                String state = s.getSQLState();
                if ("40P01".equals(state) || "40001".equals(state)) return true;
                if ("XX000".equals(state) && s.getMessage() != null
                        && s.getMessage().contains("tuple concurrently")) {
                    return true;
                }
            }
        }
        return false;
    }

    /** A lock wait cut short by {@code lock_timeout}. */
    static boolean isLockTimeout(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && LOCK_NOT_AVAILABLE.equals(s.getSQLState())) return true;
        }
        return false;
    }

    private static void pauseBeforeCompactRetry(int attempt) {
        try {
            Thread.sleep(COMPACT_RETRY_BASE_MILLIS << (attempt - 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("compact was interrupted while waiting to retry; the write was not applied", e);
        }
    }

    /** The log's last row: its {@code row_id} and its event's clock; {@code (-1, -1)} when empty. */
    private record LastRow(long rowId, long clock) { }

    private LastRow readLastRow(Connection conn) throws SQLException {
        try (var st = conn.prepareStatement(
                "SELECT row_id, payload FROM " + tableName + " ORDER BY row_id DESC LIMIT 1")) {
            try (ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return new LastRow(-1, -1);
                long rowId = rs.getLong(1);
                if (!(deserialize(rs.getBytes(2)) instanceof LogEvent last)) {
                    throw new SQLException("Last row of " + tableName + " is not a LogEvent");
                }
                return new LastRow(rowId, last.clock());
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new SQLException("Failed to decode the last event of " + tableName, e);
        }
    }

    /**
     * Re-reads the watermark, clock and leader the log really holds. The last row is read
     * first, so a {@code LogLeader} landing in between lies past the watermark and the next
     * append's guard still catches it.
     */
    private void readLeaderState(Connection conn) throws SQLException {
        LastRow last = readLastRow(conn);
        this.lastRowId = last.rowId();
        // Never rewind: a clock this instance handed out stays spent.
        this.lastClock = Math.max(this.lastClock, last.clock());
        this.currentLeader = readCurrentLeader(conn);
    }

    /**
     * The open-time splice check: clocks start at 0 and strictly increase, so the last of
     * {@code n} events has a clock of at least {@code n - 1}; old rows re-inserted at the end
     * break that. {@link #requireIncreasingClocks} does the full check on every range read.
     * The row count is first bounded by the {@code row_id} range (two key lookups), which
     * settles it for dense row ids; only gaps make it fall back to {@code count(*)}.
     */
    private void requireLastClockCoversLength(Connection conn) throws SQLException {
        if (lastRowId < 0) return;
        long rows;
        try (var st = conn.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT (SELECT max(row_id) FROM " + tableName + ") - (SELECT min(row_id) FROM " + tableName + ") + 1")) {
            rs.next();
            rows = rs.getLong(1); // an upper bound on the row count
        }
        if (lastClock >= rows - 1) return;
        try (var st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT count(*) FROM " + tableName)) {
            rs.next();
            rows = rs.getLong(1);
        }
        if (lastClock < rows - 1) {
            throw new IllegalStateException("Log table " + tableName + " is spliced or reordered: its last row "
                    + "(row_id " + lastRowId + ") has clock " + lastClock + ", but the table holds " + rows
                    + " rows; it is refused (nothing was changed)");
        }
    }

    /**
     * Names the cause when {@code statement_timeout} or {@code lock_timeout} cancels an
     * opening statement, instead of a bare "canceling statement". Other failures pass through.
     */
    private SQLException cancelledWhileOpening(SQLException e) {
        if (!QUERY_CANCELED.equals(e.getSQLState()) && !LOCK_NOT_AVAILABLE.equals(e.getSQLState())) return e;
        return new SQLException("PostgresLogBackend " + logId + ": opening log table " + tableName
                + " was cancelled by the server (SQLState " + e.getSQLState() + ": " + e.getMessage() + "); "
                + "raise statement_timeout, or build the LogLeader index beforehand: "
                + leaderIndexDdl(tableName, true), e.getSQLState(), e);
    }

    /** Marks the start of a commit that changes the log; returns its write generation. Under {@link #appendLock}. */
    private long beginCommit() {
        long generation = writesStarted + 1;
        writesStarted = generation;
        return generation;
    }

    /**
     * Settles the write started by {@link #beginCommit()}, if any. A commit that did not
     * succeed may still have been applied, so the known length is dropped.
     */
    private void endCommit(boolean committed) {
        if (writesFinished != writesStarted) {
            if (!committed) knownLength.set(null);
            writesFinished = writesStarted;
        }
    }

    /** The generation a read about to start observes; pass it to {@link #observeLength}. */
    private long readGeneration() {
        return writesFinished;
    }

    /** Records a length a read saw, unless a commit overlapped the read or something newer is known. */
    private void observeLength(int length, long generation) {
        if (writesStarted != generation) return;
        knownLength.accumulateAndGet(new KnownLength(length, generation),
                (cur, seen) -> cur == null || seen.generation() >= cur.generation() ? seen : cur);
    }

    /** Instance id of the log's latest {@code LogLeader} (null if the log has none). */
    private String readCurrentLeader(Connection conn) throws SQLException {
        try (var st = conn.prepareStatement(
                "SELECT payload FROM " + tableName + " WHERE type = 'LogLeader' ORDER BY row_id DESC LIMIT 1")) {
            try (ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return null;
                Object obj = deserialize(rs.getBytes(1));
                if (!(obj instanceof LogLeader leader)) {
                    throw new SQLException("Latest LogLeader payload decoded to " + typeName(obj));
                }
                return leader.instanceId();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new SQLException("Failed to decode latest LogLeader payload", e);
        }
    }

    @Override
    public String logId() {
        return logId;
    }

    @Override
    public int length() {
        ensureOpen();
        long generation = readGeneration();
        return read("length()", conn -> {
            try (var st = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
                 var rs = st.executeQuery()) {
                rs.next();
                int length = rs.getInt(1);
                observeLength(length, generation);
                return length;
            }
        });
    }

    /** A read over one short-lived {@link DataSource} session. */
    @FunctionalInterface
    private interface Read<T> {
        T run(Connection conn) throws SQLException, IOException, ClassNotFoundException;
    }

    /**
     * Runs an idempotent read on a connection from the {@link DataSource}. A pooled connection
     * whose session died while idle ({@link #isConnectionFailure}) is retried once on a fresh
     * one; any other failure, including failing to get a connection, is not.
     */
    private <T> T read(String op, Read<T> read) {
        for (int attempt = 1; ; attempt++) {
            Connection conn;
            try {
                conn = dataSource.getConnection();
            } catch (SQLException e) {
                throw readFailure(op, e);
            }
            try (conn) {
                return read.run(conn);
            } catch (SQLException e) {
                if (attempt == 1 && isConnectionFailure(e) && !Thread.currentThread().isInterrupted()) {
                    log.warn("PostgresLogBackend[{}] {} lost its connection ({}, SQLState {}); retrying once on a "
                            + "fresh connection", tableName, op, e.getMessage(), e.getSQLState());
                    continue;
                }
                throw readFailure(op, e);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(op + " failed on " + tableName + ": " + e, e);
            }
        }
    }

    private RuntimeException readFailure(String op, SQLException e) {
        return new RuntimeException(op + " failed on " + tableName + ": " + e.getMessage()
                + " (SQLState " + e.getSQLState() + ")", e);
    }

    /**
     * Whether the session itself is gone: class {@code 08} (connection exception) or
     * {@code 57P01}–{@code 57P03} (terminated, crash, cannot connect now). A cancelled
     * statement ({@code 57014}) is not.
     */
    static boolean isConnectionFailure(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && s.getSQLState() != null) {
                String state = s.getSQLState();
                if (state.startsWith("08") || "57P01".equals(state) || "57P02".equals(state)
                        || "57P03".equals(state)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public LogEvent get(int position) {
        ensureOpen();
        if (position < 0) {
            throw new IndexOutOfBoundsException("position " + position + " < 0");
        }
        for (int attempt = 1; ; attempt++) {
            // Retried like a short range read: see RANGE_READ_ATTEMPTS.
            Optional<LogEvent> event = readAt(position);
            if (event.isPresent()) return event.get();
            if (attempt == RANGE_READ_ATTEMPTS || !pauseBeforeRangeRetry(attempt)) {
                throw new IndexOutOfBoundsException("position " + position + " not present");
            }
        }
    }

    private Optional<LogEvent> readAt(int position) {
        return read("get(" + position + ")", conn -> {
            try (var st = conn.prepareStatement(
                    "SELECT payload FROM " + tableName + " ORDER BY row_id ASC OFFSET ? LIMIT 1")) {
                st.setInt(1, position);
                try (var rs = st.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(decodeEvent(rs.getBytes(1), " at position " + position));
                }
            }
        });
    }

    /**
     * Attempts a range read makes before it believes a log that looks too short. A reader
     * whose snapshot predates a compaction's commit but who resolved the table name after it
     * sees the new, empty table; a fresh snapshot sees the whole log before or after.
     */
    private static final int RANGE_READ_ATTEMPTS = 4;

    /** Unguarded insert: a {@link LogLeader} claim (that is how leadership is taken over) and compaction. */
    private String plainInsertSql() {
        return "INSERT INTO " + tableName + " (type, payload, ts_millis) VALUES (?, ?, ?) RETURNING row_id";
    }

    /**
     * Append that inserts only while no row was written past {@link #lastRowId}; otherwise it
     * inserts nothing and the caller re-reads the log. Sound only behind the write fence
     * ({@link #lockOutOtherWriters}), which makes every other writer's rows visible.
     *
     * <p>The guard compares with {@code max(row_id)}, one backward step on the key. A
     * {@code NOT EXISTS (... WHERE row_id > ?)} would turn into a sequential scan once pgjdbc
     * server-prepares it and Postgres picks the generic plan.</p>
     */
    private String guardedInsertSql() {
        return guardedInsertSqlFor(tableName);
    }

    /** The guarded append statement for {@code tableName} (package-private for tests). */
    static String guardedInsertSqlFor(String tableName) {
        return "INSERT INTO " + tableName + " (type, payload, ts_millis)"
                + " SELECT ?::text, ?::bytea, ?::bigint"
                + " WHERE COALESCE((SELECT max(row_id) FROM " + tableName + "), ?) <= ?"
                + " RETURNING row_id";
    }

    /**
     * Inserts {@code event}'s row with an empty payload, filled in by {@link #writePayload} in
     * the same transaction once the clock is known. Returns its {@code row_id}, or {@code -1}
     * if the guard matched and nothing was inserted.
     */
    private long insertPlaceholder(Connection conn, LogEvent event, boolean unguarded) throws SQLException {
        try (var insert = conn.prepareStatement(unguarded ? plainInsertSql() : guardedInsertSql(),
                Statement.RETURN_GENERATED_KEYS)) {
            insert.setString(1, event.getClass().getSimpleName());
            insert.setBytes(2, new byte[0]);
            insert.setLong(3, event.timestamp());
            if (!unguarded) {
                insert.setLong(4, lastRowId); // an empty table passes: no max, coalesced to the watermark
                insert.setLong(5, lastRowId);
            }
            insert.executeUpdate();
            try (var rs = insert.getGeneratedKeys()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        }
    }

    private void writePayload(Connection conn, long rowId, byte[] payload) throws SQLException {
        try (var update = conn.prepareStatement("UPDATE " + tableName + " SET payload = ? WHERE row_id = ?")) {
            update.setBytes(1, payload);
            update.setLong(2, rowId);
            update.executeUpdate();
        }
    }

    /** How long a write waits for the write fence ({@link #lockOutOtherWriters}) before it fails. */
    static final long WRITE_FENCE_TIMEOUT_MILLIS = 10_000;

    /**
     * The write fence: {@code SHARE ROW EXCLUSIVE} on the live table until the transaction
     * ends. It conflicts with every {@code INSERT}/{@code UPDATE} but not with reads, so once
     * granted every other write (another node's claim, a manual {@code INSERT}) has committed
     * and is visible, or waits. Without it an uncommitted {@code LogLeader} would be invisible
     * to the append guard, and the append would move {@link #lastRowId} past it for good.
     *
     * <p>Must be the transaction's first statement, at {@code READ COMMITTED}: under
     * {@code REPEATABLE READ} an earlier query would fix a snapshot that hides writes committed
     * while the fence was waited for. The wait is bounded by
     * {@value #WRITE_FENCE_TIMEOUT_MILLIS} ms (a manual {@code VACUUM} also conflicts).</p>
     */
    private void lockOutOtherWriters(Connection conn) throws SQLException {
        try (var st = conn.createStatement()) {
            st.execute(SET_READ_COMMITTED + "; SET LOCAL lock_timeout = '" + WRITE_FENCE_TIMEOUT_MILLIS + "ms'; "
                    + "LOCK TABLE " + tableName + " IN SHARE ROW EXCLUSIVE MODE");
        } catch (SQLException e) {
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) throw e;
            throw new SQLException("another session kept " + tableName + " locked for writing (an open write "
                    + "transaction, a manual VACUUM?) for more than " + WRITE_FENCE_TIMEOUT_MILLIS + " ms",
                    e.getSQLState(), e);
        }
    }

    /** Tests only: runs on the leader thread just before an append commits. */
    volatile Runnable beforeCommitHook;

    /**
     * The window's row ids, payload sizes and table oid plus the total row count, in one
     * statement so the bound check and the rows share a snapshot. The count is joined so it
     * arrives for an empty window too; {@code octet_length} reads only the TOAST header.
     */
    private String rangeQuery() {
        return "SELECT t.total, r.row_id, r.len, r.tableoid"
                + " FROM (SELECT count(*) AS total FROM " + tableName + ") t"
                + " LEFT JOIN (SELECT row_id, octet_length(payload) AS len, tableoid FROM " + tableName
                + " ORDER BY row_id ASC OFFSET ? LIMIT ?) r ON true"
                + " ORDER BY r.row_id ASC";
    }

    /**
     * {@link #rangeQuery()} by key from a known row: no {@code count(*)}, and the
     * {@code OFFSET} is only the gap from that row (zero for a sequential scan).
     */
    private String keysetRangeQuery() {
        return "SELECT row_id, octet_length(payload) AS len, tableoid FROM " + tableName
                + " WHERE row_id > ? ORDER BY row_id ASC OFFSET ? LIMIT ?";
    }

    /**
     * The row at {@code position} of table {@code tableOid} has {@code rowId}. Holds while that
     * table is live: rows are never deleted, and the write fence makes writers commit in
     * {@code row_id} order. Keyset reads check the oid.
     */
    private record ReadCursor(long tableOid, int position, long rowId, long clock) { }

    /**
     * Where the last range read ended, so a chunked scan continues by key rather than
     * counting and skipping with {@code OFFSET} on every chunk (quadratic). Best effort: the
     * read falls back to {@link #rangeQuery()}.
     */
    private final AtomicReference<ReadCursor> readCursor = new AtomicReference<>();

    /** Tests only: range reads that listed their window by key rather than by count + OFFSET. */
    final AtomicLong keysetRangeReads = new AtomicLong();

    /** A listed window: the log length it proves, the table it came from, row ids and payload sizes. */
    private record Listing(int total, long tableOid, List<Long> rowIds, List<Long> sizes) { }

    /** Payloads of a run of consecutive rows, with the oid of the table they came from. */
    private String payloadQuery() {
        return "SELECT row_id, tableoid, payload FROM " + tableName
                + " WHERE row_id >= ? AND row_id <= ? ORDER BY row_id ASC";
    }

    /**
     * Payload bytes one range-read statement fetches at most (a larger single row alone), so
     * a log of large events stays readable under a server {@code statement_timeout}.
     */
    static final long READ_BATCH_BYTES = 8L * 1024 * 1024;

    /** SQLState of a statement cancelled by {@code statement_timeout} or a cancel request. */
    private static final String QUERY_CANCELED = "57014";
    /** SQLState lock_not_available: a NOWAIT lock refused, or a {@code lock_timeout} hit. */
    private static final String LOCK_NOT_AVAILABLE = "55P03";

    /**
     * One range read: the log length it saw (for a keyset listing, the lower bound its window
     * proves), the events read and how many rows were listed.
     */
    private record Range(int total, List<LogEvent> events, int listed) { }

    @Override
    public LogEvent[] getBetween(int fromPosition, int toPosition) {
        ensureOpen();
        checkRange(fromPosition, toPosition);
        int wanted = toPosition - fromPosition;
        for (int attempt = 1; ; attempt++) {
            Range range = readRange(fromPosition, wanted, Long.MAX_VALUE);
            if (range.total() >= toPosition && range.events().size() == wanted) {
                return range.events().toArray(LogEvent[]::new);
            }
            if (attempt == RANGE_READ_ATTEMPTS || !pauseBeforeRangeRetry(attempt)) {
                if (range.total() < toPosition) {
                    throw new IndexOutOfBoundsException("toPosition=" + toPosition
                            + " > length=" + range.total());
                }
                throw new RuntimeException("getBetween(" + fromPosition + "," + toPosition + ") on "
                        + tableName + " read " + range.events().size() + " of " + wanted
                        + " events while the log held " + range.total());
            }
        }
    }

    /**
     * Streams the range in windows of at most 1,000 events and {@link #READ_BATCH_BYTES} of
     * payload, so a log of large records is never decoded 1,000 at a time. {@code action}
     * runs outside any database session.
     */
    @Override
    public void forEachBetween(int fromPosition, int toPosition, Consumer<? super LogEvent> action) {
        Objects.requireNonNull(action, "action");
        ensureOpen();
        checkRange(fromPosition, toPosition);
        int position = fromPosition;
        while (position < toPosition) {
            int wanted = Math.min(1_000, toPosition - position);
            List<LogEvent> events = readBoundedWindow(position, wanted);
            position += events.size();
            for (int i = 0; i < events.size(); i++) {
                action.accept(events.set(i, null)); // a handed-out record can be collected early
            }
        }
    }

    private static void checkRange(int fromPosition, int toPosition) {
        if (fromPosition < 0 || toPosition < fromPosition) {
            throw new IndexOutOfBoundsException("invalid range [" + fromPosition + "," + toPosition + ")");
        }
    }

    /** The first events of {@code [fromPosition, fromPosition + wanted)} that fit the byte bound; never empty. */
    private List<LogEvent> readBoundedWindow(int fromPosition, int wanted) {
        int toPosition = fromPosition + wanted;
        for (int attempt = 1; ; attempt++) {
            ensureOpen();
            Range range = readRange(fromPosition, wanted, READ_BATCH_BYTES);
            if (range.total() >= toPosition && range.listed() > 0 && range.events().size() == range.listed()) {
                return range.events();
            }
            if (attempt == RANGE_READ_ATTEMPTS || !pauseBeforeRangeRetry(attempt)) {
                if (range.total() < toPosition) {
                    throw new IndexOutOfBoundsException("toPosition=" + toPosition
                            + " > length=" + range.total());
                }
                throw new RuntimeException("forEachBetween(" + fromPosition + "," + toPosition + ") on "
                        + tableName + " read " + range.events().size() + " of " + range.listed()
                        + " events while the log held " + range.total());
            }
        }
    }

    /** Returns false if interrupted: the caller should stop retrying. */
    private static boolean pauseBeforeRangeRetry(int attempt) {
        try {
            Thread.sleep(Math.min(8L, 1L << attempt)); // a rename holds the table for microseconds
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * One range read: lists the window (by key from {@link #readCursor}, else by count and
     * offset), keeps the prefix that fits {@code maxBytes}, then fetches the payloads in runs
     * of at most {@link #READ_BATCH_BYTES}. Every run must come from the listed table (by
     * oid); after a compaction's swap the read comes back short and the caller retries. Rows
     * never change once written, so runs in separate snapshots still form one window.
     */
    private Range readRange(int fromPosition, int wanted, long maxBytes) {
        long generation = readGeneration();
        return read("getBetween(" + fromPosition + "," + (fromPosition + wanted) + ")", conn -> {
            ReadCursor before = readCursor.get();
            Listing listing = listByKey(conn, fromPosition, wanted);
            if (listing == null) {
                listing = listByOffset(conn, fromPosition, wanted);
                observeLength(listing.total(), generation);
            }
            List<Long> rowIds = listing.rowIds();
            List<Long> sizes = listing.sizes();
            if (maxBytes != Long.MAX_VALUE && !rowIds.isEmpty()) {
                int k = 1; // at least one row
                long bytes = sizes.get(0);
                while (k < rowIds.size() && bytes + sizes.get(k) <= maxBytes) bytes += sizes.get(k++);
                rowIds = rowIds.subList(0, k);
                sizes = sizes.subList(0, k);
            }
            int n = rowIds.size();
            List<LogEvent> out = readPayloads(conn, listing.tableOid(), rowIds, sizes, fromPosition);
            if (n > 0 && out.size() == n) {
                // Continuing the previous read on the same table: check clocks across the chunks too.
                boolean continues = before != null && before.tableOid() == listing.tableOid()
                        && before.position() == fromPosition - 1;
                requireIncreasingClocks(fromPosition, rowIds, out,
                        continues ? before.rowId() : -1, continues ? before.clock() : -1);
                readCursor.set(new ReadCursor(listing.tableOid(), fromPosition + n - 1, rowIds.get(n - 1),
                        out.get(n - 1).clock()));
            }
            return new Range(listing.total(), out, n);
        });
    }

    /** Fetches the listed rows in byte-bounded runs; stops short if the table was swapped. */
    private List<LogEvent> readPayloads(Connection conn, long tableOid, List<Long> rowIds, List<Long> sizes,
                                        int firstPosition) throws SQLException, IOException, ClassNotFoundException {
        var out = new ArrayList<LogEvent>(rowIds.size());
        int n = rowIds.size();
        for (int from = 0; from < n; ) {
            int to = from;
            long bytes = 0;
            do {
                bytes += sizes.get(to++);
            } while (to < n && bytes + sizes.get(to) <= READ_BATCH_BYTES);
            if (!readRun(conn, tableOid, rowIds, from, to, out, firstPosition)) break;
            from = to;
        }
        return out;
    }

    /**
     * Fails closed on a window whose clocks do not strictly increase in {@code row_id} order,
     * starting above {@code previousClock} when {@code previousRowId >= 0}. A clock that does
     * not grow means two timelines were spliced; appending after them would re-issue clocks.
     */
    private void requireIncreasingClocks(int fromPosition, List<Long> rowIds, List<LogEvent> events,
                                         long previousRowId, long previousClock) {
        long prevRow = previousRowId;
        long prevClock = previousClock;
        int prevPosition = fromPosition - 1;
        for (int i = 0; i < events.size(); i++) {
            long clock = events.get(i).clock();
            if (prevRow >= 0 || i > 0) {
                if (clock <= prevClock) throw splicedLog(prevPosition, prevRow, prevClock, fromPosition + i,
                        rowIds.get(i), clock, events.get(i));
            } else if (clock < 0) {
                throw splicedLog(-1, -1, -1, fromPosition + i, rowIds.get(i), clock, events.get(i));
            }
            prevRow = rowIds.get(i);
            prevClock = clock;
            prevPosition = fromPosition + i;
        }
    }

    private IllegalStateException splicedLog(int prevPosition, long prevRowId, long prevClock,
                                             int position, long rowId, long clock, LogEvent event) {
        String previous = prevRowId < 0 ? "clocks start at 0"
                : "the previous row (row_id " + prevRowId + ", position " + prevPosition + ") has clock " + prevClock;
        return new IllegalStateException("Log table " + tableName + " is spliced or reordered: row_id " + rowId
                + " (position " + position + ", " + event.getClass().getSimpleName() + ") has clock " + clock
                + " but " + previous + ", so the log is spliced or corrupt: reading stopped at row_id " + rowId
                + " and the log is refused. Stop every instance on this log and repair it before use");
    }

    /**
     * The window listed by key from {@link #readCursor}, or {@code null} when there is no
     * usable cursor, the rows are from another table, or the window is short (only a count
     * can settle its bound).
     */
    private Listing listByKey(Connection conn, int fromPosition, int wanted) throws SQLException {
        ReadCursor cursor = readCursor.get();
        if (wanted == 0 || cursor == null || cursor.position() >= fromPosition) return null;
        var rowIds = new ArrayList<Long>(Math.min(wanted, 1024));
        var sizes = new ArrayList<Long>(Math.min(wanted, 1024));
        try (var st = conn.prepareStatement(keysetRangeQuery())) {
            st.setLong(1, cursor.rowId());
            st.setInt(2, fromPosition - cursor.position() - 1);
            st.setInt(3, wanted);
            try (var rs = st.executeQuery()) {
                while (rs.next()) {
                    if (rs.getLong(3) != cursor.tableOid()) return null;
                    rowIds.add(rs.getLong(1));
                    sizes.add(rs.getLong(2));
                }
            }
        }
        if (rowIds.size() != wanted) return null;
        keysetRangeReads.incrementAndGet();
        return new Listing(fromPosition + wanted, cursor.tableOid(), rowIds, sizes);
    }

    /** The window listed with {@link #rangeQuery()}: counted and skipped to, under one snapshot. */
    private Listing listByOffset(Connection conn, int fromPosition, int wanted) throws SQLException {
        int total = 0;
        long tableOid = 0;
        var rowIds = new ArrayList<Long>(Math.min(wanted, 1024));
        var sizes = new ArrayList<Long>(Math.min(wanted, 1024));
        try (var st = conn.prepareStatement(rangeQuery())) {
            st.setInt(1, fromPosition);
            st.setInt(2, wanted);
            try (var rs = st.executeQuery()) {
                while (rs.next()) {
                    total = rs.getInt(1);
                    long rowId = rs.getLong(2);
                    if (rs.wasNull()) continue; // empty window: the count arrives on its own row
                    rowIds.add(rowId);
                    sizes.add(rs.getLong(3));
                    tableOid = rs.getLong(4);
                }
            }
        }
        return new Listing(total, tableOid, rowIds, sizes);
    }

    /**
     * Appends the events of rows {@code rowIds[from..to)} to {@code out}; false, with nothing
     * appended, if they are not all in table {@code tableOid}. A cancelled run is split in halves.
     */
    private boolean readRun(Connection conn, long tableOid, List<Long> rowIds, int from, int to,
                            List<LogEvent> out, int firstPosition) throws SQLException, IOException, ClassNotFoundException {
        int mark = out.size();
        try (var st = conn.prepareStatement(payloadQuery())) {
            st.setLong(1, rowIds.get(from));
            st.setLong(2, rowIds.get(to - 1));
            try (var rs = st.executeQuery()) {
                int next = from;
                while (rs.next()) {
                    if (next == to || rs.getLong(1) != rowIds.get(next) || rs.getLong(2) != tableOid) {
                        out.subList(mark, out.size()).clear();
                        return false;
                    }
                    out.add(decodeEvent(rs.getBytes(3),
                            " at row_id " + rowIds.get(next) + " (position " + (firstPosition + next) + ")"));
                    next++;
                }
                if (next != to) {
                    out.subList(mark, out.size()).clear();
                    return false;
                }
                return true;
            }
        } catch (SQLException e) {
            if (to - from == 1 || !QUERY_CANCELED.equals(e.getSQLState())) {
                throw e;
            }
            out.subList(mark, out.size()).clear();
            if (!conn.getAutoCommit()) {
                conn.rollback(); // a pool handing out non-autocommit sessions: leave the aborted transaction
            }
            int mid = (from + to) >>> 1;
            log.debug("Range read of {} rows of {} was cancelled; retrying it in halves", to - from, tableName);
            return readRun(conn, tableOid, rowIds, from, mid, out, firstPosition)
                    && readRun(conn, tableOid, rowIds, mid, to, out, firstPosition);
        }
    }

    @Override
    public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
        return onLeaderThread(() -> appendOnLeader(event, leaderInstanceId));
    }

    private Optional<LogEvent> appendOnLeader(LogEvent event, String leaderInstanceId) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        ensureOpen();
        appendLock.lock();
        try {
            ensureOpen();
            ensureLockNotLost();
            boolean isLeaderClaim = event instanceof LogLeader newLeader
                    && newLeader.instanceId().equals(leaderInstanceId);
            if (!isLeaderClaim && !leaderInstanceId.equals(currentLeader)) {
                // Known not to be ours: refused without a round trip. The INSERT guard is the real check.
                return Optional.empty();
            }
            Connection conn = leaderConnection;
            boolean committed = false;
            boolean commitAttempted = false;
            try {
                // On the leader connection: if that session died, the lock died with it.
                conn.setAutoCommit(false);
                lockOutOtherWriters(conn); // must be the first statement
                verifyAdvisoryLockHeld(conn);
                if (isLeaderClaim) {
                    // A new leadership continues after what the log holds now.
                    readLeaderState(conn);
                }
                long rowId = insertPlaceholder(conn, event, isLeaderClaim);
                if (rowId < 0) {
                    rowId = insertAfterUnseenRows(conn, event, leaderInstanceId);
                    if (rowId < 0) return Optional.empty();
                }
                // The clock continues after the last event's, never reused; row_id only orders rows.
                LogEvent withClock = LogClocks.withClock(event, Math.addExact(lastClock, 1));
                byte[] payload = serialize(withClock);
                ensureReadableAfterRestart(withClock, payload); // before commit: refused, nothing written
                writePayload(conn, rowId, payload);
                Runnable hook = beforeCommitHook;
                if (hook != null) hook.run();
                long generation = beginCommit();
                commitAttempted = true;
                conn.commit();
                committed = true;
                knownLength.updateAndGet(cur -> cur != null && cur.generation() == generation - 1
                        ? new KnownLength(cur.length() + 1, generation) : cur);
                lastClock = withClock.clock();
                lastRowId = rowId;
                if (withClock instanceof LogLeader claimed) {
                    currentLeader = claimed.instanceId();
                }
                return Optional.of(withClock);
            } catch (SQLException | IOException e) {
                throw writeFailure("append", e, commitAttempted);
            } finally {
                endCommit(committed);
                endLeaderTransaction(conn, committed);
            }
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * The guard found rows past everything this instance has seen. Re-reads the log (exact:
     * the fence holds it still) and returns {@code -1} if its latest {@code LogLeader} names
     * another instance; otherwise, say after an in-doubt commit of ours that did land,
     * inserts after those rows and returns the new {@code row_id}.
     */
    private long insertAfterUnseenRows(Connection conn, LogEvent event, String leaderInstanceId)
            throws SQLException {
        long seen = lastRowId;
        readLeaderState(conn);
        if (!leaderInstanceId.equals(currentLeader)) {
            log.warn("PostgresLogBackend[{}] refused an append by {}: the log's latest "
                    + "LogLeader now names {}", tableName, leaderInstanceId, currentLeader);
            return -1;
        }
        log.warn("PostgresLogBackend[{}] found rows it had not seen past row {}; appending after "
                + "them (clock {})", tableName, seen, lastClock + 1);
        long rowId = insertPlaceholder(conn, event, false);
        if (rowId < 0) {
            throw new SQLException("the log changed under the write fence of " + tableName);
        }
        return rowId;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Refuses with {@link LeadershipLostException}, changing nothing, when the log's
     * latest {@link LogLeader} names another instance: a deposed leader must not be able
     * to re-claim the log by snapshotting its own {@code LogLeader} back to the front.</p>
     */
    @Override
    public SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId) {
        return onLeaderThread(() -> compactOnLeader(snapshotEvents, leaderInstanceId));
    }

    private SnapshotResult compactOnLeader(List<LogEvent> snapshotEvents, String leaderInstanceId) {
        Objects.requireNonNull(snapshotEvents, "snapshotEvents");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        if (snapshotEvents.isEmpty()
                || !(snapshotEvents.get(0) instanceof LogLeader first)
                || !first.instanceId().equals(leaderInstanceId)) {
            throw new IllegalArgumentException(
                    "snapshotEvents must start with LogLeader(" + leaderInstanceId + ")");
        }
        // A plan made before a takeover has stale clocks; report the takeover rather than the
        // clocks. Cheap pre-check only: the check inside the transaction decides.
        ensureLockNotLost();
        requireStillLeader(currentLeader, leaderInstanceId);
        LogClocks.requireIncreasingClocks(snapshotEvents);
        ensureOpen();
        appendLock.lock();
        try {
            ensureOpen();
            ensureLockNotLost();
            for (int attempt = 1; ; attempt++) {
                Connection conn = leaderConnection;
                boolean committed = false;
                boolean commitAttempted = false;
                boolean tableLocked = false;
                try {
                    conn.setAutoCommit(false);
                    // ACCESS EXCLUSIVE is also the write fence, so it comes before any query.
                    lockTableWithoutQueueing(conn);
                    tableLocked = true;
                    verifyAdvisoryLockHeld(conn);
                    // A deposed leader must not snapshot its own LogLeader back to the front.
                    requireStillLeader(readCurrentLeader(conn), leaderInstanceId);
                    LastRow last = readLastRow(conn);
                    if (last.rowId() != lastRowId) {
                        // The plan missed these rows; archiving them away would lose them.
                        long seen = lastRowId;
                        readLeaderState(conn);
                        throw new IllegalStateException("compact on " + tableName + " refused: the log has rows "
                                + "past row " + seen + " that this instance had not seen (up to clock " + last.clock()
                                + "); nothing was written — plan the snapshot again");
                    }
                    String archived = archiveLiveTable(conn);
                    Copied copied = copySnapshot(conn, snapshotEvents);
                    long generation = beginCommit();
                    commitAttempted = true;
                    conn.commit();
                    committed = true;
                    knownLength.set(new KnownLength(copied.count(), generation));
                    readCursor.set(null); // it points into the archive now
                    currentLeader = leaderInstanceId;
                    lastClock = snapshotEvents.get(snapshotEvents.size() - 1).clock();
                    lastRowId = copied.lastRowId(); // the new table numbers its rows afresh
                    return new SnapshotResult(tableName, archived,
                            copied.checkpoint() < 0 ? Math.max(copied.count() - 1, 0) : copied.checkpoint(),
                            copied.count());
                } catch (SQLException | IOException e) {
                    if (!commitAttempted && attempt < COMPACT_ATTEMPTS && e instanceof SQLException s
                            && (isTransientCatalogConflict(s) || (tableLocked && isLockTimeout(s)))) {
                        // Rolled back below with nothing applied: the same plan is tried again.
                        log.warn("PostgresLogBackend[{}] compact hit a transient conflict ({}, SQLState {}); "
                                + "retrying (attempt {} of {})", tableName, s.getMessage(), s.getSQLState(),
                                attempt + 1, COMPACT_ATTEMPTS);
                    } else {
                        throw writeFailure("compact", e, commitAttempted);
                    }
                } finally {
                    endCommit(committed);
                    endLeaderTransaction(conn, committed);
                }
                pauseBeforeCompactRetry(attempt);
                ensureOpen();
                ensureLockNotLost();
            }
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Renames the live table to a fresh archive name, marks it as this log's and creates a new
     * live table. DDL is transactional, so a later failure rolls all of it back.
     */
    private String archiveLiveTable(Connection conn) throws SQLException {
        String archived = freeArchiveTableName(conn);
        // Looked up: tables made by older versions carry Postgres' implicit BIGSERIAL names.
        String liveSequence = catalogName(conn, OWNED_SEQUENCE_QUERY, tableName);
        String livePrimaryKey = catalogName(conn, PRIMARY_KEY_INDEX_QUERY, tableName);
        String liveLeaderIndex = catalogName(conn, LEADER_INDEX_QUERY, tableName);
        try (var stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE " + tableName + " RENAME TO " + archived);
            // The owner mark purgeArchives trusts; a name alone could be anybody's table.
            stmt.execute("COMMENT ON TABLE " + archived + " IS '" + archiveComment(tableName) + "'");
            // The new live table needs these names; move them to names derived from the archive.
            if (liveSequence != null) {
                stmt.execute("ALTER SEQUENCE " + liveSequence + " RENAME TO " + sequenceName(archived));
            }
            if (livePrimaryKey != null) {
                stmt.execute("ALTER INDEX " + livePrimaryKey + " RENAME TO " + primaryKeyName(archived));
            }
            if (liveLeaderIndex != null) {
                stmt.execute("ALTER INDEX " + liveLeaderIndex + " RENAME TO " + leaderIndexName(archived));
            }
            createLiveTable(stmt, tableName);
        }
        return archived;
    }

    /** What {@link #copySnapshot} wrote: row count, the {@link LogSnapshot} checkpoint (-1 if none), last row id. */
    private record Copied(int count, long checkpoint, long lastRowId) { }

    /** Inserts the snapshot events, with the clocks they carry, into the new live table. */
    private Copied copySnapshot(Connection conn, List<LogEvent> snapshotEvents) throws SQLException, IOException {
        int count = 0;
        long checkpoint = -1;
        long lastInsertedRowId = -1;
        try (var insert = conn.prepareStatement(plainInsertSql(), Statement.RETURN_GENERATED_KEYS)) {
            for (LogEvent e : snapshotEvents) {
                insert.setString(1, e.getClass().getSimpleName());
                insert.setBytes(2, new byte[0]);
                insert.setLong(3, e.timestamp());
                insert.executeUpdate();
                try (var rs = insert.getGeneratedKeys()) {
                    rs.next();
                    long rowId = rs.getLong(1);
                    lastInsertedRowId = rowId;
                    byte[] payload = serialize(e);
                    ensureReadableAfterRestart(e, payload); // before commit: the compaction rolls back
                    writePayload(conn, rowId, payload);
                    count++;
                    if (e instanceof LogSnapshot s) {
                        checkpoint = s.checkpointClock();
                    }
                }
            }
        }
        return new Copied(count, checkpoint, lastInsertedRowId);
    }

    /**
     * Drops all but the newest {@code keepHistory} archive tables of this log, oldest first,
     * {@value #PURGE_BATCH} per transaction: each dropped table holds its locks until commit,
     * and one transaction over a large backlog would exhaust {@code max_locks_per_transaction}.
     * Batches that committed stay dropped if a later one fails.
     */
    @Override
    public void purgeArchives(int keepHistory) {
        if (keepHistory < 0) throw new IllegalArgumentException("keepHistory < 0");
        onLeaderThread(() -> {
            purgeArchivesOnLeader(keepHistory);
            return null;
        });
    }

    /** Archive tables dropped per {@link #purgeArchives} transaction. */
    static final int PURGE_BATCH = 50;

    /** Tests only: counts the transactions {@link #purgeArchives} committed. */
    final AtomicLong purgeTransactions = new AtomicLong();

    private void purgeArchivesOnLeader(int keepHistory) {
        ensureOpen();
        appendLock.lock();
        try {
            ensureOpen();
            ensureLockNotLost();
            Connection conn = leaderConnection;
            List<String> surplus;
            boolean listed = false;
            try {
                conn.setAutoCommit(false);
                verifyAdvisoryLockHeld(conn);
                List<Archive> found = listArchives(conn, System.currentTimeMillis());
                found.sort(Comparator.comparingLong(Archive::stamp));
                surplus = found.stream().limit(Math.max(0, found.size() - keepHistory))
                        .map(Archive::name).toList();
                conn.commit();
                listed = true;
            } catch (SQLException e) {
                throw writeFailure("purgeArchives", e, false);
            } finally {
                endLeaderTransaction(conn, listed);
            }
            for (int from = 0; from < surplus.size(); from += PURGE_BATCH) {
                List<String> batch = surplus.subList(from, Math.min(from + PURGE_BATCH, surplus.size()));
                boolean committed = false;
                boolean commitAttempted = false;
                try {
                    conn.setAutoCommit(false);
                    verifyAdvisoryLockHeld(conn);
                    try (var stmt = conn.createStatement()) {
                        for (String archive : batch) {
                            validateIdentifier(archive);
                            stmt.execute("DROP TABLE IF EXISTS " + archive);
                        }
                    }
                    commitAttempted = true;
                    conn.commit();
                    committed = true;
                    purgeTransactions.incrementAndGet();
                } catch (SQLException e) {
                    throw writeFailure("purgeArchives", e, commitAttempted);
                } finally {
                    endLeaderTransaction(conn, committed);
                }
            }
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Reports this instance as leader only while its session still holds the advisory lock.
     * A closed backend answers too, with no leader, so a health check keeps saying "not
     * leader" after a handover instead of failing.
     */
    @Override
    public LogBackendReport introspect() {
        if (closed) return lastKnownReport(null); // closing released the lock: no leader
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(INTROSPECT_TIMEOUT_MILLIS);
        // A write in flight verifies the lock itself; the health check does not queue behind it.
        String problem = null;
        if (!closed && !lockLost && currentLeader != null && leaderExecutor.getActiveCount() == 0) {
            problem = probeAdvisoryLock(deadline);
        }
        if (problem == null) {
            problem = readReportBounded(deadline);
            if (problem == null) {
                LogBackendReport fresh = lastReport;
                // currentLeader is read after the query: a lock lost meanwhile is not reported as held.
                return new LogBackendReport(logId, fresh.length(), currentLeader,
                        fresh.eventCounts(), fresh.maxTimestampMillis());
            }
        } else {
            readReportBounded(deadline); // still refresh the counts if the database answers
        }
        return fallbackReport(problem);
    }

    /** Bound on {@link #introspect()}'s database work (leader-session probe plus report query). */
    static final long INTROSPECT_TIMEOUT_MILLIS = 5_000;

    /** How long one report query may run, checkout included, before it counts as stuck and is cancelled. */
    static final long REPORT_QUERY_BUDGET_MILLIS = INTROSPECT_TIMEOUT_MILLIS;

    /** How often {@link #introspect()} may warn that it fell back to last known values. */
    private static final long INTROSPECT_WARN_INTERVAL_MILLIS = 30_000;

    /** The last report read from the database; {@code null} until one was. */
    private volatile LogBackendReport lastReport;
    /** The lock probe started by {@link #introspect()} and possibly still pending. */
    private final AtomicReference<Future<?>> pendingProbe = new AtomicReference<>();
    /** The latest report query (single-flight, see {@link #readReportBounded}). */
    private final AtomicReference<ReportQuery> reportQuery = new AtomicReference<>();
    private final AtomicLong lastIntrospectWarnNanos = new AtomicLong(System.nanoTime()
            - TimeUnit.MILLISECONDS.toNanos(INTROSPECT_WARN_INTERVAL_MILLIS));

    /**
     * Checks the lock from a short-lived {@link DataSource} session, waiting until
     * {@code deadline} at most; returns why it could not finish, or {@code null}.
     *
     * <p>Never on the leader session: against an unreachable server a statement there fails
     * like a lost session and would fence the node, and a health check must not cost
     * leadership. It runs on the leader thread to keep the lock-check state there. A probe
     * that times out keeps running and is not repeated while pending.</p>
     */
    private String probeAdvisoryLock(long deadline) {
        Future<?> previous = pendingProbe.get();
        if (previous != null && !previous.isDone()) {
            return null; // an earlier probe is still running; its result will update currentLeader
        }
        FutureTask<Void> probe = new FutureTask<>(() -> {
            if (!closed && !lockLost && !holdsAdvisoryLockPerSeparateSession()) markLockLost();
            return null;
        });
        if (!pendingProbe.compareAndSet(previous, probe)) {
            return null; // a concurrent introspect() started one
        }
        try {
            leaderExecutor.execute(probe);
        } catch (RejectedExecutionException closing) {
            return null; // closed concurrently
        }
        try {
            probe.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            return null;
        } catch (TimeoutException e) {
            return "the advisory-lock check did not finish within " + INTROSPECT_TIMEOUT_MILLIS + " ms";
        } catch (ExecutionException e) {
            return "the advisory-lock check failed: " + e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "interrupted while waiting for the advisory-lock check";
        }
    }

    /**
     * One report query. It occupies the single report slot while running and, once cancelled
     * as stuck, while its thread lives: that thread may still be blocked in a checkout that
     * ignores the interrupt. A query that finished on its own holds no connection, so it frees
     * the slot at once.
     */
    private record ReportQuery(FutureTask<LogBackendReport> task, Thread runner, long startedNanos) {
        boolean occupiesSlot() {
            return !task.isDone() || (task.isCancelled() && runner.isAlive());
        }

        long ageMillis() {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
        }
    }

    /**
     * Brings {@link #lastReport} up to date, waiting until {@code deadline} but at least
     * 500 ms; returns why it could not, or {@code null}.
     *
     * <p>Single-flight: one report query at a time, on its own virtual thread; concurrent
     * callers share it. The query stores its own result, so a late answer still refreshes the
     * last known values. A query past {@link #REPORT_QUERY_BUDGET_MILLIS} is cancelled, and
     * while its thread lingers callers get the last known values at once, so a stuck database
     * ties up one thread at most. A query within its budget is never cancelled by a waiter:
     * others may be sharing it.</p>
     */
    private String readReportBounded(long deadline) {
        ReportQuery query;
        while (true) {
            ReportQuery current = reportQuery.get();
            if (current != null && current.occupiesSlot()) {
                long age = current.ageMillis();
                if (age > REPORT_QUERY_BUDGET_MILLIS) {
                    current.task().cancel(true);
                    return "the report query has been running for " + age
                            + " ms (connection checkout or query); not starting another";
                }
                query = current; // share the running query
                break;
            }
            FutureTask<LogBackendReport> task = new FutureTask<>(() -> {
                LogBackendReport report = readReport();
                lastReport = report; // one query at a time, so reports are stored in order
                return report;
            });
            var fresh = new ReportQuery(task,
                    Thread.ofVirtual().name("fom-pg-introspect-" + tableName).unstarted(task), System.nanoTime());
            if (reportQuery.compareAndSet(current, fresh)) {
                try {
                    fresh.runner().start();
                } catch (RuntimeException | Error e) {
                    task.cancel(false); // never started: frees the slot
                    throw e;
                }
                query = fresh;
                break;
            }
            // Another caller published a query first: share it.
        }
        long waitStart = System.nanoTime();
        long waitNanos = Math.max(deadline - waitStart, TimeUnit.MILLISECONDS.toNanos(500));
        try {
            query.task().get(waitNanos, TimeUnit.NANOSECONDS);
            return null;
        } catch (TimeoutException e) {
            long age = query.ageMillis();
            if (age > REPORT_QUERY_BUDGET_MILLIS) {
                query.task().cancel(true); // stuck: the interrupt closes its connection
                return "the report query did not finish within " + REPORT_QUERY_BUDGET_MILLIS + " ms";
            }
            // Within budget and possibly shared: only this caller's time is up.
            return "the report query did not answer within the "
                    + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitStart) + " ms this call could wait";
        } catch (CancellationException e) {
            return "the report query was cancelled after running for more than " + REPORT_QUERY_BUDGET_MILLIS + " ms";
        } catch (ExecutionException e) {
            return "the report query failed: " + e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // the query may be shared: leave it running
            return "interrupted while waiting for the report query";
        }
    }

    private LogBackendReport readReport() throws SQLException {
        long generation = readGeneration();
        try (var conn = dataSource.getConnection();
             var st = conn.prepareStatement(
                     "SELECT type, COUNT(*), COALESCE(MAX(ts_millis), 0) FROM " + tableName + " GROUP BY type")) {
            st.setQueryTimeout((int) Math.max(1, TimeUnit.MILLISECONDS.toSeconds(INTROSPECT_TIMEOUT_MILLIS + 999)));
            long lastTs = 0L;
            int total = 0;
            Map<String, Integer> counts = new LinkedHashMap<>();
            try (var rs = st.executeQuery()) {
                while (rs.next()) {
                    int count = rs.getInt(2);
                    counts.put(rs.getString(1), count);
                    total += count;
                    lastTs = Math.max(lastTs, rs.getLong(3));
                }
            }
            observeLength(total, generation);
            return new LogBackendReport(logId, total, currentLeader, new HashMap<>(counts), lastTs);
        }
    }

    /** The report {@link #introspect()} falls back to when the database did not answer in time. */
    private LogBackendReport fallbackReport(String problem) {
        long now = System.nanoTime();
        long lastWarn = lastIntrospectWarnNanos.get();
        if (now - lastWarn >= TimeUnit.MILLISECONDS.toNanos(INTROSPECT_WARN_INTERVAL_MILLIS)
                && lastIntrospectWarnNanos.compareAndSet(lastWarn, now)) {
            log.warn("PostgresLogBackend[{}] introspect() could not read the database ({}); reporting the last known "
                    + "values", tableName, problem);
        } else {
            log.debug("PostgresLogBackend[{}] introspect() reporting last known values: {}", tableName, problem);
        }
        return lastKnownReport(currentLeader);
    }

    /**
     * A report from values already read, touching no database: the last length this instance
     * knew (else the last report's, else -1) and the last report's counts.
     */
    private LogBackendReport lastKnownReport(String leader) {
        LogBackendReport last = lastReport;
        KnownLength known = knownLength.get();
        int length = known != null ? known.length() : last != null ? last.length() : -1;
        if (last == null) {
            return new LogBackendReport(logId, length, leader, Map.of(), 0L);
        }
        return new LogBackendReport(logId, length, leader, last.eventCounts(), last.maxTimestampMillis());
    }

    @Override
    public void close() {
        if (closed) return;
        appendLock.lock();
        try {
            if (closed) return;
            closed = true;
            currentLeader = null; // the lock goes with the session below
        } finally {
            appendLock.unlock();
        }
        keepalive.cancel(false);
        try {
            onLeaderThread(() -> {
                releaseLockAndClose(leaderConnection);
                return null;
            });
        } finally {
            leaderExecutor.shutdown();
        }
    }


    /** This log's lock in {@code pg_locks}: a bigint key shows as classid (high 32 bits), objid (low), objsubid 1. */
    private static final String LOCK_ROW_MATCH = "locktype = 'advisory' AND granted"
            + " AND classid::bigint = ? AND objid::bigint = ? AND objsubid = 1";

    /** Deadline for one lock confirmation from a separate session, checkout included; writes wait for it. */
    static final long CONFIRM_TIMEOUT_MILLIS = 3_000;

    /** How long lock checks may stay inconclusive before the lock counts as lost: two keepalive periods. */
    static final long INCONCLUSIVE_LIMIT_MILLIS = 30_000;

    private static final String RELEASED_ON_LIVE_SESSION = "the leader session is alive but no longer holds the lock "
            + "(released by pg_advisory_unlock_all(), DISCARD ALL or a pooler/proxy resetting the session?)";

    private void ensureLockNotLost() {
        if (lockLost) {
            throw lockLostException(null);
        }
    }

    /**
     * Runs inside the write transaction on the leader connection. Once verified, the lock can
     * only go with the session, which also aborts the transaction.
     */
    private void verifyAdvisoryLockHeld(Connection conn) throws SQLException {
        boolean held;
        try {
            held = queryAdvisoryLock(conn);
        } catch (SQLException e) {
            // Not proof the lock is gone; writeFailure decides.
            throw new SQLException("could not verify the advisory lock (statement failed: "
                    + e.getMessage() + ")", e.getSQLState(), e);
        }
        if (!held) {
            noteLockLoss(RELEASED_ON_LIVE_SESSION, null);
            markLockLost();
            throw lockLostException(null);
        }
        inconclusiveRun = false; // conclusively held
    }

    /**
     * Whether the leader session still holds the advisory lock. Lost only when established: the
     * connection is closed or broken, or a check answers "not held". A failed check is retried,
     * then confirmed from a separate session; with no answer the lock counts as held (writes
     * verify it themselves) until checks stay inconclusive past {@link #inconclusiveLimitMillis}.
     * Leader thread.
     */
    private boolean holdsAdvisoryLock(Connection conn) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                boolean held = queryAdvisoryLock(conn);
                if (held) inconclusiveRun = false;
                else noteLockLoss(RELEASED_ON_LIVE_SESSION, null);
                return held;
            } catch (SQLException e) {
                if (sessionGone(conn, e)) {
                    log.warn("PostgresLogBackend[{}] leader session is gone: {}", tableName, e.toString());
                    noteLockLoss("the leader session is gone", e);
                    return false;
                }
                log.debug("PostgresLogBackend[{}] advisory lock check failed (attempt {}): {}",
                        tableName, attempt, e.toString());
                clearAbortedTransaction(conn);
            }
            try {
                Thread.sleep(50L * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return holdsAdvisoryLockPerSeparateSession();
    }

    /**
     * {@link #holdsAdvisoryLock} as seen from a separate session: an answer decides, no answer
     * counts as held until the inconclusive limit. Leader thread; never touches the leader session.
     */
    private boolean holdsAdvisoryLockPerSeparateSession() {
        Boolean confirmed = lockHeldPerSeparateSession();
        if (confirmed != null) {
            if (confirmed) inconclusiveRun = false;
            else noteLockLoss(SEEN_FROM_SEPARATE_SESSION + " (backend pid " + leaderPid
                    + ") no longer holds the lock", null);
            return confirmed;
        }
        long now = System.nanoTime();
        if (!inconclusiveRun) {
            inconclusiveRun = true;
            inconclusiveSinceNanos = now;
        }
        long inconclusiveMillis = TimeUnit.NANOSECONDS.toMillis(now - inconclusiveSinceNanos);
        if (inconclusiveMillis > inconclusiveLimitMillis) {
            log.error("PostgresLogBackend[{}] could not verify the advisory lock for {} ms (limit {} ms): "
                            + "no session could answer; treating the lock as lost",
                    tableName, inconclusiveMillis, inconclusiveLimitMillis);
            noteLockLoss("the lock could not be verified for " + inconclusiveMillis + " ms (limit "
                    + inconclusiveLimitMillis + " ms): no session could answer", null);
            return false;
        }
        String elapsed = inconclusiveMillis == 0
                ? "this check answered nothing"
                : "inconclusive for " + inconclusiveMillis + " ms";
        logThrottled(++inconclusiveChecks, "PostgresLogBackend[{}] could not verify the advisory lock ({}; "
                + "inconclusive check #{}); assuming it is still held for up to {} ms",
                tableName, elapsed, inconclusiveChecks, inconclusiveLimitMillis);
        return true;
    }

    /**
     * Whether an abandoned confirmation runner is still alive (it may be stuck in a checkout),
     * so no new one may start. A task that completed on its own has answered and returned its
     * connection; its exiting thread does not count, or a healthy node would collect
     * inconclusive checks towards fencing. Visible for tests.
     */
    static boolean confirmationInFlight(Thread runner, FutureTask<Boolean> task) {
        if (runner == null || !runner.isAlive()) return false;
        return task == null || !task.isDone() || task.isCancelled();
    }

    /** Whether the session behind {@code conn} still holds the lock; throws if the query fails. */
    private boolean queryAdvisoryLock(Connection conn) throws SQLException {
        try (var st = conn.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = pg_backend_pid() AND " + LOCK_ROW_MATCH + ")")) {
            st.setLong(1, lockKey >>> 32);
            st.setLong(2, lockKey & 0xFFFFFFFFL);
            try (ResultSet rs = st.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /**
     * Asks a short-lived session whether the leader backend ({@link #leaderPid}) holds the
     * lock; {@code null} if it could not tell. Runs on its own virtual thread, checkout
     * included, and is awaited for at most {@link #confirmTimeoutMillis}, so the leader thread
     * never blocks longer. No new runner starts while an abandoned one lives.
     */
    private Boolean lockHeldPerSeparateSession() {
        if (confirmationInFlight(confirmationThread, confirmationTask)) {
            long aliveMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - confirmationStartedNanos);
            logThrottled(++skippedConfirmations, "PostgresLogBackend[{}] an earlier advisory lock confirmation "
                    + "is still stuck after {} ms; not starting another (skipped confirmation #{})",
                    tableName, aliveMillis, skippedConfirmations);
            return null;
        }
        FutureTask<Boolean> task = new FutureTask<>(this::queryLockFromSeparateSession);
        confirmationStartedNanos = System.nanoTime();
        confirmationTask = task;
        confirmationThread = Thread.ofVirtual().name("fom-pg-lock-confirm-" + tableName).start(task);
        try {
            return task.get(confirmTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            task.cancel(true);
            logThrottled(++timedOutConfirmations, "PostgresLogBackend[{}] advisory lock confirmation did not "
                    + "complete within {} ms; result inconclusive (timed-out confirmation #{})",
                    tableName, confirmTimeoutMillis, timedOutConfirmations);
            return null;
        } catch (InterruptedException e) {
            task.cancel(true);
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            logThrottled(++failedConfirmations, "PostgresLogBackend[{}] advisory lock confirmation from a separate "
                    + "session failed (failed confirmation #{}): {}", tableName, failedConfirmations,
                    String.valueOf(e.getCause()));
            return null;
        }
    }

    /**
     * WARN on the 1st, 2nd, 4th, 8th... occurrence and DEBUG otherwise: these repeat once per
     * lock check, and a busy {@code introspect()} endpoint makes many. The count in the
     * message shows what was suppressed.
     */
    private static void logThrottled(long occurrence, String format, Object... args) {
        if (Long.bitCount(occurrence) == 1) {
            log.warn(format, args);
        } else {
            log.debug(format, args);
        }
    }

    /** Body of {@link #lockHeldPerSeparateSession()}, on its own thread; {@code null} if abandoned. */
    private Boolean queryLockFromSeparateSession() throws SQLException {
        Connection c = dataSource.getConnection();
        try (c) {
            if (Thread.currentThread().isInterrupted()) return null; // the caller gave up waiting
            try (var st = c.prepareStatement(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = ? AND " + LOCK_ROW_MATCH + ")")) {
                st.setQueryTimeout((int) Math.max(1, TimeUnit.MILLISECONDS.toSeconds(confirmTimeoutMillis + 999)));
                st.setInt(1, leaderPid);
                st.setLong(2, lockKey >>> 32);
                st.setLong(3, lockKey & 0xFFFFFFFFL);
                try (ResultSet rs = st.executeQuery()) {
                    return rs.next() && rs.getBoolean(1);
                }
            }
        }
    }

    /**
     * Whether {@code failure} proves the session behind {@code conn} is gone: the connection
     * is closed, or the SQLState is class 08 or 57P.. (terminated, idle timeout). A cancelled
     * statement (57014) is not.
     */
    private static boolean sessionGone(Connection conn, SQLException failure) {
        try {
            if (conn.isClosed()) return true;
        } catch (SQLException ignored) {
            // judge by the failure alone
        }
        return carriesSessionGoneState(failure);
    }

    /** Whether {@code failure}'s cause chain has a connection-exception (08...) or terminated-session (57P..) SQLState. */
    private static boolean carriesSessionGoneState(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && s.getSQLState() != null
                    && (s.getSQLState().startsWith("08") || s.getSQLState().startsWith("57P"))) {
                return true;
            }
        }
        return false;
    }

    /** Best-effort rollback of an aborted transaction so the next check can run. */
    private static void clearAbortedTransaction(Connection conn) {
        try {
            if (!conn.getAutoCommit()) conn.rollback();
        } catch (SQLException ignored) {
            // retried by the next attempt
        }
    }

    /** Records why the check that is about to report "not held" decided so. Leader thread. */
    private void noteLockLoss(String reason, Throwable cause) {
        if (lockLost) return; // the first reason is the one that fenced
        lockLossReason = reason;
        lockLossCause = cause;
    }

    /**
     * Fences this instance, permanently. Leader thread. When a separate session decided it
     * without seeing a failure, the leader session is asked once (bounded by
     * {@link #LEADER_DIAGNOSIS_TIMEOUT_MILLIS}) to capture the real reason, e.g. 57P05 idle
     * timeout or 57P01 terminate.
     */
    private void markLockLost() {
        if (!lockLost) {
            String reason = lockLossReason;
            Throwable cause = lockLossCause;
            if (reason == null) reason = "leader session gone, lock not held, or the lock could not be verified "
                    + "for too long";
            if (cause == null && reason.startsWith(SEEN_FROM_SEPARATE_SESSION)) {
                LeaderDiagnosis diagnosis = diagnoseLeaderSession();
                if (diagnosis.failure() != null) {
                    cause = diagnosis.failure();
                    lockLossCause = cause;
                } else if (diagnosis.answered()) {
                    reason += "; the leader session itself still answers, so the lock was released on a live "
                            + "session (pg_advisory_unlock_all(), DISCARD ALL, a pooler/proxy resetting it?)";
                }
            }
            lockLossReason = reason;
            lockLost = true;
            if (cause == null) {
                log.error("PostgresLogBackend[{}] lost its advisory lock: {}; this instance is fenced and will "
                        + "reject all further writes", tableName, reason);
            } else {
                log.error("PostgresLogBackend[{}] lost its advisory lock: {} (cause: {}, SQLState {}); this "
                                + "instance is fenced and will reject all further writes",
                        tableName, reason, cause.getMessage(), sqlStateOf(cause), cause);
            }
        }
        currentLeader = null;
    }

    /** What one statement on the leader session showed: how it failed, or whether it answered. */
    private record LeaderDiagnosis(SQLException failure, boolean answered) { }

    /**
     * Sends one {@code SELECT 1} on the leader session under a network timeout and reports how
     * it failed or that it answered; neither if the driver cannot bound the wait. Diagnosis only.
     */
    private LeaderDiagnosis diagnoseLeaderSession() {
        Connection conn = leaderConnection;
        if (conn == null) return new LeaderDiagnosis(null, false);
        int previousTimeout;
        try {
            previousTimeout = conn.getNetworkTimeout();
            conn.setNetworkTimeout(Runnable::run, LEADER_DIAGNOSIS_TIMEOUT_MILLIS);
        } catch (SQLFeatureNotSupportedException unbounded) {
            return new LeaderDiagnosis(null, false);
        } catch (SQLException e) {
            return new LeaderDiagnosis(e, false); // closed, typically: that is the answer
        }
        try (var st = conn.createStatement()) {
            st.execute("SELECT 1");
            return new LeaderDiagnosis(null, true);
        } catch (SQLException e) {
            return new LeaderDiagnosis(e, false);
        } finally {
            try {
                if (!conn.isClosed()) conn.setNetworkTimeout(Runnable::run, previousTimeout);
            } catch (SQLException ignored) {
                // a broken session: nothing to restore
            }
        }
    }

    private static String sqlStateOf(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && s.getSQLState() != null) return s.getSQLState();
        }
        return "none";
    }

    /**
     * Refuses a compaction by an instance other than the one {@code claimed} names; an
     * unclaimed log ({@code null}) compacts. Shared by the pre-check and the in-transaction
     * check so they cannot drift apart.
     */
    private void requireStillLeader(String claimed, String leaderInstanceId) {
        if (claimed != null && !claimed.equals(leaderInstanceId)) {
            throw new LeadershipLostException("Cannot compact " + logId + ": instance "
                    + leaderInstanceId + " is no longer the leader (" + claimed
                    + " is); nothing was written");
        }
    }

    private LeadershipLostException lockLostException(Throwable cause) {
        String reason = lockLossReason;
        Throwable lossCause = lockLossCause;
        String detail = "leader session was lost";
        if (reason != null) {
            detail += " (" + reason + (lossCause == null ? ""
                    : "; cause: " + lossCause.getMessage() + ", SQLState " + sqlStateOf(lossCause)) + ")";
        }
        var lost = new LeadershipLostException("PostgresLogBackend " + logId
                + " no longer holds the advisory lock for " + tableName
                + " — " + detail + "; another instance may own the log");
        if (cause != null) {
            lost.initCause(cause);
            if (lossCause != null && lossCause != cause) lost.addSuppressed(lossCause);
        } else if (lossCause != null) {
            lost.initCause(lossCause);
        }
        return lost;
    }

    /**
     * Turns a failed write into a fencing error if the lock is lost, else into a failure that
     * says whether the write was applied: not before the commit, unknown if the commit failed.
     *
     * @param commitInDoubt the failure came from the {@code COMMIT} itself
     */
    private RuntimeException writeFailure(String op, Exception e, boolean commitInDoubt) {
        if (!lockLost) {
            boolean gone = false;
            try {
                leaderConnection.rollback(); // clears the aborted transaction so the check can run
            } catch (SQLException rollbackFailure) {
                gone = sessionGone(leaderConnection, rollbackFailure);
                if (gone) {
                    // The write's own failure usually carries the server's reason; the ROLLBACK
                    // only sees a closed connection.
                    noteLockLoss("the leader session is gone",
                            carriesSessionGoneState(e) ? e : rollbackFailure);
                }
            }
            if (gone || !holdsAdvisoryLock(leaderConnection)) {
                markLockLost();
            }
        }
        if (lockLost) {
            return lockLostException(e);
        }
        String outcome = commitInDoubt
                ? "; the commit was interrupted, so the write may or may not have been applied"
                        + " — re-read the log before retrying"
                : "; the write was not applied";
        return new RuntimeException(op + " failed on " + tableName + ": " + e.getMessage() + outcome, e);
    }

    private void endLeaderTransaction(Connection conn, boolean committed) {
        if (!committed) {
            try {
                conn.rollback();
            } catch (SQLException rollbackFailure) {
                log.debug("PostgresLogBackend[{}] rollback failed: {}", tableName, rollbackFailure.toString());
            }
        }
        try {
            conn.setAutoCommit(true);
        } catch (SQLException ignored) {
            // a dead session: the next write's lock check reports it
        }
    }

    /** Unlock attempts in {@link #releaseLockAndClose} before the connection is aborted. */
    private static final int UNLOCK_ATTEMPTS = 5;

    /**
     * Releases this log's advisory lock on {@code conn}'s session, confirms it from the same
     * session, then closes {@code conn}; failures are retried with backoff. If the release
     * cannot be confirmed the connection is aborted instead, or a pool would keep the session
     * and its lock alive and no standby could take over.
     */
    private void releaseLockAndClose(Connection conn) {
        SQLException lastFailure = null;
        for (int attempt = 1; attempt <= UNLOCK_ATTEMPTS; attempt++) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
                // Later attempts check first: the previous unlock may have run with its answer lost.
                if (attempt == 1 || queryAdvisoryLock(conn)) {
                    try (var st = conn.prepareStatement("SELECT pg_advisory_unlock(?)")) {
                        st.setLong(1, lockKey);
                        st.executeQuery().close();
                    }
                }
                if (!queryAdvisoryLock(conn)) {
                    restoreIsolationAndClose(conn);
                    return;
                }
                log.debug("PostgresLogBackend[{}] advisory lock still held after unlock attempt {}", tableName, attempt);
            } catch (SQLException e) {
                if (sessionGone(conn, e)) {
                    log.debug("PostgresLogBackend[{}] leader session already gone at close, and its lock with it: {}",
                            tableName, e.toString());
                    closeQuietly(conn);
                    return;
                }
                lastFailure = e;
                log.debug("PostgresLogBackend[{}] advisory unlock attempt {} failed: {}", tableName, attempt, e.toString());
            }
            if (attempt < UNLOCK_ATTEMPTS) {
                try {
                    Thread.sleep(25L << (attempt - 1)); // 25, 50, 100, 200 ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.warn("PostgresLogBackend[{}] could not confirm that the advisory lock was released ({}); aborting the "
                        + "connection so its session and the lock end instead of going back to the pool", tableName,
                lastFailure != null ? "last failure: " + lastFailure : "still held after " + UNLOCK_ATTEMPTS + " attempts");
        try {
            conn.abort(Runnable::run);
        } catch (SQLException | RuntimeException e) {
            log.warn("PostgresLogBackend[{}] aborting the leader connection failed ({}); closing it instead — "
                    + "a pooled session may keep the advisory lock", tableName, e.toString());
        }
        closeQuietly(conn); // lets a pool take back its dead handle
    }

    /** First statement of every write transaction on the leader connection. */
    private static final String SET_READ_COMMITTED = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";

    /**
     * Makes {@code READ COMMITTED} the leader session's default whatever the pool, role or
     * database default is: the write fence and append guard need each statement to see all
     * that committed before it. Write transactions also set it themselves; this covers lock
     * checks, catalog reads and purges.
     */
    private void forceReadCommitted(Connection conn) throws SQLException {
        pooledIsolation = conn.getTransactionIsolation();
        if (pooledIsolation != Connection.TRANSACTION_READ_COMMITTED) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    /** Hands the leader connection back at the isolation level it came with, then closes it. */
    private void restoreIsolationAndClose(Connection conn) {
        if (pooledIsolation != Connection.TRANSACTION_READ_COMMITTED) {
            try {
                if (!conn.isClosed()) {
                    if (!conn.getAutoCommit()) {
                        conn.rollback();
                        conn.setAutoCommit(true);
                    }
                    conn.setTransactionIsolation(pooledIsolation);
                }
            } catch (SQLException | RuntimeException e) {
                log.debug("PostgresLogBackend[{}] could not restore the isolation level of the leader connection: {}",
                        tableName, e.toString());
            }
        }
        closeQuietly(conn);
    }

    private static void closeQuietly(Connection conn) {
        try {
            conn.close();
        } catch (SQLException | RuntimeException ignored) {
            // best-effort
        }
    }


    /** Sequences a table's {@code row_id} column owns ({@code BIGSERIAL} or {@code OWNED BY}). */
    private static final String OWNED_SEQUENCE_QUERY =
            "SELECT s.relname FROM pg_class s JOIN pg_depend d ON d.objid = s.oid"
                    + " WHERE s.relkind = 'S' AND d.classid = 'pg_class'::regclass"
                    + " AND d.refclassid = 'pg_class'::regclass AND d.deptype = 'a'"
                    + " AND d.refobjid = to_regclass(?) ORDER BY s.relname";

    /** The index backing a table's primary key. */
    private static final String PRIMARY_KEY_INDEX_QUERY =
            "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
                    + " WHERE i.indrelid = to_regclass(?) AND i.indisprimary ORDER BY c.relname";

    /** The partial index on a table's {@code LogLeader} rows ({@link #leaderIndexDdl}), whatever its name. */
    private static final String LEADER_INDEX_QUERY =
            "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
                    + " WHERE i.indrelid = to_regclass(?) AND i.indpred IS NOT NULL"
                    + " AND pg_get_expr(i.indpred, i.indrelid) LIKE '%''LogLeader''%' ORDER BY c.relname";

    /** Relation {@code relname = ?} in the schema unqualified CREATEs go to ({@code current_schema()}). */
    private static final String TARGET_SCHEMA_RELATION =
            " FROM pg_class c WHERE c.relname = ?"
                    + " AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())";

    /**
     * {@code relkind} of relation {@code name} in {@code current_schema()}, or {@code null}.
     * Not {@code to_regclass}: that walks the {@code search_path} and would return
     * {@code pg_catalog.pg_class} for a log table named {@code pg_class}.
     */
    private static String relkindOf(Connection conn, String name) throws SQLException {
        try (var st = conn.prepareStatement("SELECT c.relkind" + TARGET_SCHEMA_RELATION)) {
            st.setString(1, name);
            try (ResultSet rs = st.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /**
     * Statements name the table unqualified, so it must resolve to {@code current_schema()};
     * otherwise the backend would use a table it did not create or check (a system catalog,
     * a same-named table in another schema).
     */
    private static void requireInTargetSchema(Connection conn, String tableName) throws SQLException {
        try (var st = conn.prepareStatement("SELECT n.nspname, current_schema() FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass(?)")) {
            st.setString(1, tableName);
            try (ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return; // nothing yet: it will be created in the target schema
                String schema = rs.getString(1);
                String target = rs.getString(2);
                if (!schema.equals(target)) {
                    throw new IllegalArgumentException("'" + tableName + "' resolves to " + schema + "."
                            + tableName + ", outside the schema the log lives in ("
                            + (target == null ? "none: search_path names no existing schema" : target)
                            + "); choose another logId or table name");
                }
            }
        }
    }

    /**
     * A CREATE that lost a race for a relation name: {@code 42P07} (the other committed
     * first) or {@code 23505} (on the catalog's name index, both in flight).
     */
    private static boolean isRelationNameClash(SQLException e) {
        return "42P07".equals(e.getSQLState()) || "23505".equals(e.getSQLState());
    }

    private static void requireFreeDerivedNames(Connection conn, String tableName) throws SQLException {
        requireFreeDerivedName(conn, tableName, sequenceName(tableName), "sequence");
        requireFreeDerivedName(conn, tableName, primaryKeyName(tableName), "primary-key index");
        requireFreeDerivedName(conn, tableName, leaderIndexName(tableName), "LogLeader index");
    }

    /** Fails if {@code name}, derived for {@code tableName}'s {@code what}, is taken by another relation. */
    private static void requireFreeDerivedName(Connection conn, String tableName, String name, String what)
            throws SQLException {
        String relkind = relkindOf(conn, name);
        if (relkind != null) {
            throw new IllegalArgumentException("Cannot create log table '" + tableName + "': the name of its "
                    + what + " '" + name + "' already names another object (relkind " + relkind
                    + ", e.g. another log's table); choose another logId or table name");
        }
    }

    /** Columns (and {@code format_type} types) a log table has; {@link #createLiveTable} makes them. */
    private static final Map<String, String> LOG_COLUMNS = Map.of(
            "row_id", "bigint", "type", "text", "payload", "bytea", "ts_millis", "bigint");

    /** Rejects an existing table that is not a log table, rather than failing later on a missing column. */
    private static void requireLogTableColumns(Connection conn, String tableName) throws SQLException {
        var found = new HashMap<String, String>();
        try (var st = conn.prepareStatement("SELECT a.attname, format_type(a.atttypid, a.atttypmod)"
                + " FROM pg_attribute a WHERE a.attnum > 0 AND NOT a.attisdropped"
                + " AND a.attrelid = (SELECT c.oid" + TARGET_SCHEMA_RELATION + ")")) {
            st.setString(1, tableName);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) found.put(rs.getString(1), rs.getString(2));
            }
        }
        for (String column : List.of("row_id", "type", "payload", "ts_millis")) {
            String type = found.get(column);
            if (type == null) {
                throw new IllegalArgumentException("table " + tableName + " exists but is not a fom log table"
                        + " (missing column " + column + ")");
            }
            if (!type.equals(LOG_COLUMNS.get(column))) {
                throw new IllegalArgumentException("table " + tableName + " exists but is not a fom log table"
                        + " (column " + column + " is " + type + ", expected " + LOG_COLUMNS.get(column) + ")");
            }
        }
    }

    /** First name {@code query} reports for {@code table}, or {@code null}; validated, since it goes into DDL. */
    private static String catalogName(Connection conn, String query, String table) throws SQLException {
        try (var st = conn.prepareStatement(query)) {
            st.setString(1, table);
            try (ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return null;
                String name = rs.getString(1);
                if (name == null || !name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                    throw new SQLException("Cannot rename '" + name + "': not a plain SQL identifier");
                }
                return name;
            }
        }
    }

    /**
     * Creates the live table with an explicitly named sequence and primary key. Postgres'
     * implicit {@code BIGSERIAL} names cut long table names, so two logs with a common prefix
     * would collide when created concurrently; the derived names carry a hash instead.
     */
    private static void createLiveTable(Statement st, String table) throws SQLException {
        String sequence = sequenceName(table);
        st.execute("CREATE SEQUENCE " + sequence);
        st.execute("CREATE TABLE " + table + " ("
                + "row_id BIGINT NOT NULL DEFAULT nextval('" + sequence + "'), "
                + "type TEXT NOT NULL, "
                + "payload BYTEA NOT NULL, "
                + "ts_millis BIGINT NOT NULL, "
                + "CONSTRAINT " + primaryKeyName(table) + " PRIMARY KEY (row_id))");
        // Owned, so DROP TABLE (purgeArchives) takes the sequence with it.
        st.execute("ALTER SEQUENCE " + sequence + " OWNED BY " + table + ".row_id");
        st.execute(leaderIndexDdl(table, false));
    }

    /**
     * The partial index on a table's {@code LogLeader} rows, so finding the latest one does
     * not scan the table. Its predicate must match {@link #readCurrentLeader}'s literally.
     */
    private static String leaderIndexDdl(String table, boolean ifNotExists) {
        return "CREATE INDEX " + (ifNotExists ? "IF NOT EXISTS " : "") + leaderIndexName(table)
                + " ON " + table + " (row_id) WHERE type = 'LogLeader'";
    }

    /** Adds the {@code LogLeader} index to a table created by an older version. Blocks writers only. */
    private void ensureLeaderIndex(Connection conn) throws SQLException {
        if (catalogName(conn, LEADER_INDEX_QUERY, tableName) != null) return;
        String index = leaderIndexName(tableName);
        requireFreeDerivedName(conn, tableName, index, "LogLeader index");
        log.info("PostgresLogBackend[{}] creating the LogLeader index {} (a table created by an older version)",
                tableName, index);
        try (var st = conn.createStatement()) {
            st.execute(leaderIndexDdl(tableName, false));
        } catch (SQLException e) {
            // 42501: the role does not own the table. The log works without the index.
            if (!"42501".equals(e.getSQLState())) throw e;
            log.warn("PostgresLogBackend[{}] could not create the LogLeader index ({}): finding the latest "
                    + "LogLeader on open scans the whole table until the table's owner runs: {}",
                    tableName, e.getMessage(), leaderIndexDdl(tableName, true));
        }
    }

    /** Name of the partial {@code LogLeader} index of {@code tableName} (see {@link #leaderIndexDdl}). */
    static String leaderIndexName(String tableName) {
        return derivedName(tableName, "_leader_ix");
    }

    /** Name of the {@code row_id} sequence of {@code tableName}. */
    static String sequenceName(String tableName) {
        return derivedName(tableName, "_rid_seq");
    }

    /** Name of the primary-key constraint and index of {@code tableName}. */
    static String primaryKeyName(String tableName) {
        return derivedName(tableName, "_rid_pk");
    }

    /**
     * {@code <tableName><suffix>}, or past 63 bytes a prefix of the table name plus a hash of
     * the full name, so names with a long common prefix never derive the same name.
     */
    private static String derivedName(String tableName, String suffix) {
        String name = tableName + suffix;
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            String hash = "_" + String.format("%016x", advisoryKey(tableName));
            int keep = MAX_IDENTIFIER_LENGTH - suffix.length() - hash.length();
            name = tableName.substring(0, Math.min(keep, tableName.length())) + hash + suffix;
        }
        validateIdentifier(name);
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            throw new IllegalStateException("Derived name exceeds 63 bytes: " + name);
        }
        return name;
    }


    /** Archive stamps further ahead of the wall clock than this are not believed (as in FileLogBackend). */
    private static final long MAX_ARCHIVE_STAMP_AHEAD_MILLIS = 100L * 366 * 24 * 3600 * 1000;

    /** An archive table of this log and its stamp as used for ordering. */
    private record Archive(String name, long stamp) { }

    /**
     * This log's archive tables with their stamps, unsorted. A table counts only if its name
     * is exactly what {@link #archiveTableName} derives for its stamp and it carries this
     * log's owner mark ({@link #archiveComment}): {@link #purgeArchives} must never drop a
     * table that merely has a matching name. A stamp more than 100 years ahead of {@code now}
     * counts as the oldest ({@code -1}), as in the file backend.
     */
    private List<Archive> listArchives(Connection conn, long now) throws SQLException {
        List<Archive> found = new ArrayList<>();
        try (var st = conn.prepareStatement(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p')"
                        + " AND strpos(c.relname, ?) > 0 AND obj_description(c.oid, 'pg_class') = ?")) {
            st.setString(1, ARCHIVE_MARKER);
            st.setString(2, archiveComment(tableName));
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    String digits = name.substring(name.lastIndexOf(ARCHIVE_MARKER) + ARCHIVE_MARKER.length());
                    if (!digits.matches("[0-9]{1,18}")) continue;
                    long stamp = Long.parseLong(digits);
                    if (!name.equals(archiveTableName(tableName, stamp))) continue;
                    if (stamp > now + MAX_ARCHIVE_STAMP_AHEAD_MILLIS) {
                        log.warn("PostgresLogBackend[{}] archive {} carries a stamp more than 100 years ahead of "
                                + "the clock; treating it as the oldest archive", tableName, name);
                        stamp = -1;
                    }
                    found.add(new Archive(name, stamp));
                }
            }
        }
        return found;
    }

    /** The table comment that marks an archive as {@code tableName}'s; a validated identifier, safe in SQL. */
    static String archiveComment(String tableName) {
        return "fom archive of " + tableName;
    }

    /**
     * A free archive name stamped with the wall clock, but above every existing stamp: an
     * archive from a node whose clock runs ahead must not look newer, or
     * {@link #purgeArchives} would drop the newest one.
     */
    private String freeArchiveTableName(Connection conn) throws SQLException {
        long now = System.currentTimeMillis();
        long highest = -1;
        for (Archive a : listArchives(conn, now)) highest = Math.max(highest, a.stamp());
        long stamp = Math.max(now, highest + 1);
        for (int attempt = 0; attempt < 1_000; attempt++) {
            String candidate = archiveTableName(tableName, stamp + attempt);
            try (var st = conn.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
                st.setString(1, candidate);
                try (ResultSet rs = st.executeQuery()) {
                    rs.next();
                    if (!rs.getBoolean(1)) {
                        return candidate;
                    }
                }
            }
        }
        throw new SQLException("No free archive table name for " + tableName);
    }

    /**
     * {@code <table>_archived_<stamp>}, or past 63 bytes a prefix of the table name plus a
     * hash of the full name, so the stamp is always kept.
     */
    static String archiveTableName(String tableName, long stamp) {
        String suffix = "_archived_" + stamp;
        String name = tableName + suffix;
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            String hash = "_" + String.format("%016x", advisoryKey(tableName));
            int keep = MAX_IDENTIFIER_LENGTH - suffix.length() - hash.length();
            if (keep < 1) {
                throw new IllegalArgumentException("Archive stamp too long: " + stamp);
            }
            name = tableName.substring(0, Math.min(keep, tableName.length())) + hash + suffix;
        }
        validateIdentifier(name);
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            throw new IllegalStateException("Archive table name exceeds 63 bytes: " + name);
        }
        return name;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PostgresLogBackend " + logId + " is closed");
        }
    }

    private static byte[] serialize(LogEvent event) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(event);
        }
        return baos.toByteArray();
    }

    /** Payloads up to this size cannot trip any {@link ObjectInputFilters#logPayload()} cap; larger ones are test-read. */
    private static final int READ_BACK_CHECK_THRESHOLD_BYTES = 1_000_000;

    /** Refuses an event the read-side filter would reject: written, it could never be read back. */
    private static void ensureReadableAfterRestart(LogEvent event, byte[] payload) {
        if (payload.length <= READ_BACK_CHECK_THRESHOLD_BYTES) return;
        var rejected = new AtomicReference<ObjectInputFilter.FilterInfo>();
        try (var ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
            ois.setObjectInputFilter(info -> {
                ObjectInputFilter.Status status = LOG_FILTER.checkInput(info);
                if (status == ObjectInputFilter.Status.REJECTED) rejected.compareAndSet(null, info);
                return status;
            });
            ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            ObjectInputFilter.FilterInfo info = rejected.get();
            String which = info == null ? "it could not be decoded (" + e + ")" : violatedLimit(info);
            throw new IllegalArgumentException(event.getClass().getSimpleName() + " exceeds the log payload "
                    + "limits: " + which + " (the event serialises to " + payload.length + " bytes; limits: "
                    + ObjectInputFilters.logPayloadLimits() + "), so it could not be read back"
                    + "; nothing was written", e);
        }
    }

    /**
     * Which of the filter's limits {@code info} broke, found by putting each measure to the
     * filter alone; if none explains it, the class allowlist did.
     */
    private static String violatedLimit(ObjectInputFilter.FilterInfo info) {
        if (rejectedAlone(-1, 0, 0, info.streamBytes())) {
            return "it reads past " + info.streamBytes() + " bytes of stream, over the per-event size limit";
        }
        if (rejectedAlone(info.arrayLength(), 0, 0, 0)) {
            return "it holds an array of " + String.format(Locale.ROOT, "%,d", info.arrayLength())
                    + " elements (" + (info.serialClass() == null ? "?" : info.serialClass().getTypeName())
                    + "), over the array-length limit";
        }
        if (rejectedAlone(-1, info.depth(), 0, 0)) {
            return "it nests objects " + info.depth() + " levels deep, over the nesting-depth limit";
        }
        if (rejectedAlone(-1, 0, info.references(), 0)) {
            return "it holds more than " + String.format(Locale.ROOT, "%,d", info.references() - 1)
                    + " object references, over the reference limit";
        }
        return "it contains " + (info.serialClass() == null ? "a class" : info.serialClass().getName())
                + ", which the log payload allowlist rejects";
    }

    private static boolean rejectedAlone(long arrayLength, long depth, long references, long streamBytes) {
        ObjectInputFilter.FilterInfo probe = new ObjectInputFilter.FilterInfo() {
            @Override public Class<?> serialClass() { return null; }
            @Override public long arrayLength() { return arrayLength; }
            @Override public long depth() { return depth; }
            @Override public long references() { return references; }
            @Override public long streamBytes() { return streamBytes; }
        };
        return LOG_FILTER.checkInput(probe) == ObjectInputFilter.Status.REJECTED;
    }

    private static LogEvent decodeEvent(byte[] payload, String where) throws IOException, ClassNotFoundException {
        Object obj;
        try {
            obj = deserialize(payload);
        } catch (InvalidClassException e) {
            throw e; // a filter rejection: its own message names the class
        } catch (IOException e) {
            throw new IOException("Cannot decode the payload" + where + ": " + e, e);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException("Cannot decode the payload" + where + ": " + e.getMessage(), e);
        }
        if (!(obj instanceof LogEvent event)) {
            throw new IOException("Decoded payload" + where + " is not a LogEvent: " + typeName(obj));
        }
        return event;
    }

    private static String typeName(Object obj) {
        return obj == null ? "null" : obj.getClass().getName();
    }

    private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        var bais = new ByteArrayInputStream(bytes);
        try (var ois = new ObjectInputStream(bais)) {
            ois.setObjectInputFilter(LOG_FILTER);
            return ois.readObject();
        }
    }

    private static String requireSafeLogId(String logId) {
        Objects.requireNonNull(logId, "logId");
        if (!logId.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("logId '" + logId + "' may contain only ASCII letters, digits "
                    + "and '_' (it becomes part of the table name); or pass an explicit table name");
        }
        return logId;
    }

    private static void validateIdentifier(String name) {
        if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Illegal SQL identifier: " + name);
        }
    }

    /**
     * The advisory-lock key of {@code schema.table}, also the hash in shortened derived names:
     * the first 8 bytes of SHA-256. {@code String.hashCode()}-style hashes collide on short
     * names, and two logs sharing a key could never both lead.
     */
    static long advisoryKey(String s) {
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
        return ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
    }
}
