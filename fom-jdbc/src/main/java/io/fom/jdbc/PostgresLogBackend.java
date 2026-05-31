package io.fom.jdbc;

import io.fom.SnapshotResult;
import io.fom.log.ClockRewriter;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogEvent;
import io.fom.log.LogLeader;
import io.fom.serde.ObjectInputFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Postgres-backed append-only log. Stores events in a single table:
 * <pre>
 * CREATE TABLE IF NOT EXISTS &lt;table&gt; (
 *   clock     SERIAL PRIMARY KEY,
 *   type      TEXT      NOT NULL,
 *   payload   BYTEA     NOT NULL,
 *   ts_millis BIGINT    NOT NULL
 * );
 * </pre>
 *
 * <p>The {@code clock} column is an internal row id used for ordering only; the
 * engine-visible clock exposed via {@link #get(int)}/{@link #getBetween} is the
 * dense 0-based row position the {@link LogBackend} SPI mandates, independent of
 * any gaps the {@code SERIAL} sequence may develop on rolled-back appends.</p>
 *
 * <p>Leader coordination is handled by a Postgres advisory lock — the
 * constructor acquires {@code pg_try_advisory_lock(hash64(tableName))} and holds
 * it on a dedicated leader connection until {@link #close()}. A second instance
 * trying to open the same table fails fast with {@link IllegalStateException}.</p>
 *
 * <p>Payloads use Java serialization — the same format the in-memory and file
 * backends use. Because any actor with write access to the table could plant a
 * forged payload, deserialisation runs under an allowlist
 * {@link ObjectInputFilter} ({@link ObjectInputFilters#logPayload()}) that
 * rejects gadget-chain classes.</p>
 */
public final class PostgresLogBackend implements LogBackend {

    private static final Logger log = LoggerFactory.getLogger(PostgresLogBackend.class);

    /** Allowlist filter: log payloads are only {@code io.fom.*} + JDK types; rejects gadget classes. */
    private static final ObjectInputFilter LOG_FILTER = ObjectInputFilters.logPayload();

    private final DataSource dataSource;
    private final String logId;
    private final String tableName;
    private final Connection leaderConnection;
    private final ReentrantLock appendLock = new ReentrantLock();

    private volatile String currentLeader;
    private volatile boolean closed = false;

    public PostgresLogBackend(DataSource dataSource, String logId) throws SQLException {
        this(dataSource, logId, "fom_log_" + sanitise(logId));
    }

    public PostgresLogBackend(DataSource dataSource, String logId, String tableName) throws SQLException {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.logId = Objects.requireNonNull(logId, "logId");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        validateIdentifier(tableName);
        if (tableName.length() > 63) {
            // Postgres NAMEDATALEN truncates identifiers at 63 bytes, which could
            // silently collapse two distinct logIds onto one table. Fail loudly.
            throw new IllegalArgumentException("Table name exceeds Postgres' 63-byte limit: " + tableName);
        }

        try (var conn = dataSource.getConnection(); var st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS " + tableName + " ("
                    + "clock SERIAL PRIMARY KEY, "
                    + "type TEXT NOT NULL, "
                    + "payload BYTEA NOT NULL, "
                    + "ts_millis BIGINT NOT NULL)");
        }
        Connection conn = dataSource.getConnection();
        boolean ok = false;
        try {
            conn.setAutoCommit(true);
            long lockKey = advisoryKey(tableName);
            try (var st = conn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                st.setLong(1, lockKey);
                try (ResultSet rs = st.executeQuery()) {
                    rs.next();
                    if (!rs.getBoolean(1)) {
                        throw new IllegalStateException(
                                "Could not acquire advisory lock for " + tableName + " — another instance holds it");
                    }
                }
            }
            this.leaderConnection = conn;
            this.currentLeader = readCurrentLeader();
            ok = true;
        } finally {
            // Never leak the connection (and the advisory lock it holds) if any
            // step after getConnection — including readCurrentLeader — throws.
            if (!ok) {
                try { conn.close(); } catch (SQLException ignored) { }
            }
        }
    }

    private String readCurrentLeader() throws SQLException {
        try (var conn = dataSource.getConnection();
             var st = conn.prepareStatement(
                     "SELECT payload FROM " + tableName + " WHERE type = 'LogLeader' ORDER BY clock DESC LIMIT 1")) {
            try (ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return null;
                Object obj = deserialize(rs.getBytes(1));
                if (!(obj instanceof LogLeader leader)) {
                    throw new SQLException("Latest LogLeader payload decoded to "
                            + (obj == null ? "null" : obj.getClass().getName()));
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
        try (var conn = dataSource.getConnection();
             var st = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName)) {
            try (var rs = st.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("length() failed on " + tableName, e);
        }
    }

    @Override
    public LogEvent get(int clock) {
        ensureOpen();
        if (clock < 0) {
            throw new IndexOutOfBoundsException("clock " + clock + " < 0");
        }
        try (var conn = dataSource.getConnection();
             var st = conn.prepareStatement(
                     "SELECT payload FROM " + tableName + " ORDER BY clock ASC OFFSET ? LIMIT 1")) {
            st.setInt(1, clock);
            try (var rs = st.executeQuery()) {
                if (!rs.next()) {
                    throw new IndexOutOfBoundsException("clock " + clock + " not present");
                }
                Object obj = deserialize(rs.getBytes(1));
                if (!(obj instanceof LogEvent event)) {
                    throw new IOException("Decoded payload at clock " + clock + " is not a LogEvent: "
                            + (obj == null ? "null" : obj.getClass().getName()));
                }
                return event;
            }
        } catch (SQLException | IOException | ClassNotFoundException e) {
            throw new RuntimeException("get(" + clock + ") failed", e);
        }
    }

    @Override
    public LogEvent[] getBetween(int fromClock, int toClock) {
        ensureOpen();
        if (fromClock < 0 || toClock < fromClock) {
            throw new IndexOutOfBoundsException("invalid range [" + fromClock + "," + toClock + ")");
        }
        // Bound-check (COUNT) and the range read run on ONE connection in a single
        // REPEATABLE READ transaction, so a concurrent append/compact cannot change
        // the row set between the two and silently shorten the result.
        try (var conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            try {
                int total;
                try (var count = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
                     var rs = count.executeQuery()) {
                    rs.next();
                    total = rs.getInt(1);
                }
                if (toClock > total) {
                    throw new IndexOutOfBoundsException("toClock=" + toClock + " > length=" + total);
                }
                try (var st = conn.prepareStatement(
                        "SELECT payload FROM " + tableName + " ORDER BY clock ASC OFFSET ? LIMIT ?")) {
                    st.setInt(1, fromClock);
                    st.setInt(2, toClock - fromClock);
                    try (var rs = st.executeQuery()) {
                        var out = new ArrayList<LogEvent>(toClock - fromClock);
                        while (rs.next()) {
                            Object obj = deserialize(rs.getBytes(1));
                            if (!(obj instanceof LogEvent event)) {
                                throw new IOException("Decoded payload is not a LogEvent: "
                                        + (obj == null ? "null" : obj.getClass().getName()));
                            }
                            out.add(event);
                        }
                        return out.toArray(LogEvent[]::new);
                    }
                }
            } finally {
                try { conn.rollback(); } catch (SQLException ignored) { } // read-only snapshot
            }
        } catch (SQLException | IOException | ClassNotFoundException e) {
            throw new RuntimeException("getBetween failed", e);
        }
    }

    @Override
    public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        ensureOpen();
        appendLock.lock();
        try {
            ensureOpen();
            boolean isLeaderClaim = event instanceof LogLeader newLeader
                    && newLeader.instanceId().equals(leaderInstanceId);
            if (!isLeaderClaim) {
                String leader = currentLeader;
                if (leader == null || !leader.equals(leaderInstanceId)) {
                    return Optional.empty();
                }
            }
            try (var conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try (var insert = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (type, payload, ts_millis) VALUES (?, ?, ?) RETURNING clock",
                        Statement.RETURN_GENERATED_KEYS)) {
                    // Insert with placeholder payload; we rebuild the event after Postgres
                    // assigns a row id. Both statements run in one transaction, so the
                    // placeholder row is never visible to another connection.
                    insert.setString(1, event.getClass().getSimpleName());
                    insert.setBytes(2, new byte[0]); // placeholder, replaced below
                    insert.setLong(3, event.timestamp());
                    insert.executeUpdate();
                    try (var rs = insert.getGeneratedKeys()) {
                        rs.next();
                        int rowId = rs.getInt(1);
                        // Engine-visible clock is the dense 0-based row position; SERIAL is 1-based.
                        LogEvent withClock = ClockRewriter.withClock(event, rowId - 1);
                        byte[] payload = serialize(withClock);
                        try (var update = conn.prepareStatement(
                                "UPDATE " + tableName + " SET payload = ? WHERE clock = ?")) {
                            update.setBytes(1, payload);
                            update.setInt(2, rowId);
                            update.executeUpdate();
                        }
                        conn.commit();
                        if (withClock instanceof LogLeader claimed) {
                            currentLeader = claimed.instanceId();
                        }
                        return Optional.of(withClock);
                    }
                } catch (SQLException | IOException e) {
                    conn.rollback();
                    throw new RuntimeException("append failed", e);
                }
            } catch (SQLException e) {
                throw new RuntimeException("append connection failed", e);
            }
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId) {
        Objects.requireNonNull(snapshotEvents, "snapshotEvents");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        if (snapshotEvents.isEmpty()
                || !(snapshotEvents.get(0) instanceof LogLeader first)
                || !first.instanceId().equals(leaderInstanceId)) {
            throw new IllegalArgumentException(
                    "snapshotEvents must start with LogLeader(" + leaderInstanceId + ")");
        }
        ensureOpen();
        appendLock.lock();
        try (var conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Archive table name (per-snapshot timestamp). DDL is transactional in
                // Postgres, so a failure mid-loop rolls the RENAME/CREATE back too.
                String archived = tableName + "_archived_" + System.currentTimeMillis();
                validateIdentifier(archived);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("ALTER TABLE " + tableName + " RENAME TO " + archived);
                    stmt.execute("CREATE TABLE " + tableName + " ("
                            + "clock SERIAL PRIMARY KEY, "
                            + "type TEXT NOT NULL, "
                            + "payload BYTEA NOT NULL, "
                            + "ts_millis BIGINT NOT NULL)");
                }
                int copied = 0;
                int checkpoint = -1;
                try (var insert = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (type, payload, ts_millis) VALUES (?, ?, ?) RETURNING clock",
                        Statement.RETURN_GENERATED_KEYS)) {
                    for (LogEvent e : snapshotEvents) {
                        insert.setString(1, e.getClass().getSimpleName());
                        insert.setBytes(2, new byte[0]);
                        insert.setLong(3, e.timestamp());
                        insert.executeUpdate();
                        try (var rs = insert.getGeneratedKeys()) {
                            rs.next();
                            int rowId = rs.getInt(1);
                            LogEvent persisted = ClockRewriter.withClock(e, rowId - 1);
                            byte[] payload = serialize(persisted);
                            try (var update = conn.prepareStatement(
                                    "UPDATE " + tableName + " SET payload = ? WHERE clock = ?")) {
                                update.setBytes(1, payload);
                                update.setInt(2, rowId);
                                update.executeUpdate();
                            }
                            copied++;
                            if (persisted instanceof io.fom.log.LogSnapshot s) {
                                checkpoint = s.checkpointClock();
                            }
                        }
                    }
                }
                conn.commit();
                currentLeader = leaderInstanceId;
                return new SnapshotResult(tableName, archived,
                        checkpoint < 0 ? Math.max(copied - 1, 0) : checkpoint, copied);
            } catch (SQLException | IOException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackFailure) {
                    log.warn("PostgresLogBackend[{}] rollback after failed compact also failed: {}",
                            tableName, rollbackFailure.toString());
                }
                throw new RuntimeException("compact failed", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("compact connection failed", e);
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public LogBackendReport introspect() {
        ensureOpen();
        try (var conn = dataSource.getConnection()) {
            int len;
            long lastTs = 0L;
            Map<String, Integer> counts = new LinkedHashMap<>();
            try (var st = conn.prepareStatement(
                    "SELECT type, COUNT(*), COALESCE(MAX(ts_millis), 0) FROM " + tableName + " GROUP BY type")) {
                try (var rs = st.executeQuery()) {
                    int total = 0;
                    while (rs.next()) {
                        String type = rs.getString(1);
                        int count = rs.getInt(2);
                        long maxTs = rs.getLong(3);
                        counts.put(type, count);
                        total += count;
                        if (maxTs > lastTs) lastTs = maxTs;
                    }
                    len = total;
                }
            }
            return new LogBackendReport(logId, len, currentLeader, new HashMap<>(counts), lastTs);
        } catch (SQLException e) {
            throw new RuntimeException("introspect failed", e);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        appendLock.lock();
        try {
            if (closed) return;
            closed = true;
        } finally {
            appendLock.unlock();
        }
        try {
            leaderConnection.close();
        } catch (Exception ignored) {
            // best-effort — closing the connection releases the advisory lock implicitly
        }
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

    private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        var bais = new ByteArrayInputStream(bytes);
        try (var ois = new ObjectInputStream(bais)) {
            ois.setObjectInputFilter(LOG_FILTER);
            return ois.readObject();
        }
    }

    private static String sanitise(String logId) {
        var sb = new StringBuilder(logId.length());
        for (int i = 0; i < logId.length(); i++) {
            char c = logId.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') sb.append(c);
            else sb.append('_');
        }
        return sb.toString();
    }

    private static void validateIdentifier(String name) {
        if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Illegal SQL identifier: " + name);
        }
    }

    /** 64-bit hash so the advisory-lock key space is not collapsed to 32 bits. */
    private static long advisoryKey(String s) {
        long h = 1125899906842597L; // prime seed
        for (int i = 0; i < s.length(); i++) {
            h = 31 * h + s.charAt(i);
        }
        return h;
    }
}
