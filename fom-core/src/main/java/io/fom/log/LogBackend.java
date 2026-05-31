package io.fom.log;

import io.fom.SnapshotResult;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * SPI of an append-only event log.
 *
 * <p>Implementations must guarantee:</p>
 * <ul>
 *   <li>{@link #append(LogEvent, String)} is atomic — partial appends are
 *       never visible to {@link #get(int)} or {@link #getBetween(int, int)};</li>
 *   <li>at most one leader at a time — the latest {@link LogLeader} in the log
 *       determines who may append. A caller using a stale {@code leaderInstanceId}
 *       receives {@link Optional#empty()};</li>
 *   <li>{@link #get(int)} and {@link #getBetween(int, int)} are safe for
 *       concurrent reads while {@code append} is in flight on another thread.</li>
 * </ul>
 *
 * <p>{@code clock} values are assigned by the backend and are dense:
 * the first event in an empty log gets clock {@code 0}, the next {@code 1},
 * and so on.</p>
 */
public interface LogBackend extends Closeable {

    /** Stable identifier of this log (e.g. file path, table name, S3 prefix). */
    String logId();

    /** Number of events currently in the log. Events have clocks {@code [0, length())}. */
    int length();

    /**
     * @throws IndexOutOfBoundsException if {@code clock < 0} or {@code clock >= length()}.
     */
    LogEvent get(int clock);

    /**
     * Returns events with clocks in {@code [fromClock, toClock)}.
     *
     * @throws IndexOutOfBoundsException if the range is invalid.
     */
    LogEvent[] getBetween(int fromClock, int toClock);

    /**
     * Append {@code event} atomically iff the latest {@link LogLeader} in the log
     * has {@code instanceId == leaderInstanceId}, or if the log is empty and
     * {@code event} itself is a {@link LogLeader}.
     *
     * <p>The {@code clock}, {@code timestamp} and {@code formatVersion} fields of
     * the incoming {@code event} are advisory — the backend MUST overwrite
     * {@code clock} with {@code length()} before persisting. {@code timestamp}
     * may be either trusted or replaced with the backend's own clock source;
     * the contract is decided by each implementation.</p>
     *
     * @return the persisted event with its assigned {@code clock} on success;
     *         {@link Optional#empty()} if someone else is the leader.
     */
    Optional<LogEvent> append(LogEvent event, String leaderInstanceId);

    /** Lightweight introspection for {@code Engine.introspect()}. */
    LogBackendReport introspect();

    /**
     * Atomically replace the log contents with {@code snapshotEvents}, archiving
     * the prior contents per implementation (file-backed implementations move
     * the previous file to a sibling {@code *.archived.<timestamp>}; in-memory
     * implementations simply discard).
     *
     * <p>The backend re-assigns clocks to the supplied events starting from 0
     * and overwrites embedded clocks (so the {@code LogSnapshot} marker in
     * {@code snapshotEvents} retains its {@code checkpointClock} payload but
     * gets a fresh framing clock of 0).</p>
     *
     * @return descriptor of the new log + the archived old log
     * @throws IllegalArgumentException if the first event is not a {@code LogLeader}
     *         whose {@code instanceId} matches {@code leaderInstanceId}
     */
    SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId);

    @Override
    void close();
}
