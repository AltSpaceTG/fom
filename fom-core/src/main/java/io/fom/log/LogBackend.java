package io.fom.log;

import io.fom.SnapshotResult;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * An append-only event log.
 *
 * <p>Implementations must guarantee:</p>
 * <ul>
 *   <li>{@link #append(LogEvent, String)} is atomic: a partial append is never
 *       visible to {@link #get(int)} or {@link #getBetween(int, int)};</li>
 *   <li>one leader at a time: the latest {@link LogLeader} decides who may append,
 *       and a stale {@code leaderInstanceId} gets {@link Optional#empty()};</li>
 *   <li>reads are safe while an {@code append} runs on another thread.</li>
 * </ul>
 *
 * <p><strong>Positions and clocks.</strong> Reads address events by
 * <em>position</em> {@code [0, length())}. Each event also has a {@code clock},
 * assigned on {@link #append} as the last event's clock plus one ({@code 0} in an
 * empty log). {@link #compact} keeps the clocks it is given, so afterwards clocks
 * have gaps and differ from positions, while a {@code Sid} (process name + clock
 * of its {@code LogInitialized}) stays valid.</p>
 */
public interface LogBackend extends Closeable {

    /** Stable identifier of this log (e.g. file path, table name, S3 prefix). */
    String logId();

    /** Number of events currently in the log. Events have positions {@code [0, length())}. */
    int length();

    /**
     * The event at {@code position} (not its clock: after a compaction the two differ).
     *
     * @throws IndexOutOfBoundsException if {@code position < 0} or {@code position >= length()}.
     */
    LogEvent get(int position);

    /**
     * Returns the events at positions {@code [fromPosition, toPosition)}.
     *
     * @throws IndexOutOfBoundsException if the range is invalid.
     */
    LogEvent[] getBetween(int fromPosition, int toPosition);

    /**
     * Hands the events at positions {@code [fromPosition, toPosition)} to {@code action}, in order.
     * The engine reads whole logs this way, so an implementation should keep only a bounded number
     * of decoded events alive at a time. The default reads {@link #getBetween} in batches of 1,000.
     *
     * <p>Not atomic against {@link #compact}; the caller must keep the two apart.</p>
     *
     * @throws IndexOutOfBoundsException if the range is invalid.
     */
    default void forEachBetween(int fromPosition, int toPosition, Consumer<? super LogEvent> action) {
        Objects.requireNonNull(action, "action");
        if (fromPosition < 0 || toPosition < fromPosition) {
            throw new IndexOutOfBoundsException("Invalid range [" + fromPosition + ", " + toPosition + ")");
        }
        for (int from = fromPosition; from < toPosition; from += 1_000) {
            LogEvent[] batch = getBetween(from, Math.min(toPosition, from + 1_000));
            for (int k = 0; k < batch.length; k++) {
                LogEvent event = batch[k];
                batch[k] = null; // let a handled event be collected before the batch ends
                action.accept(event);
            }
        }
    }

    /**
     * Appends {@code event} atomically if the latest {@link LogLeader} names
     * {@code leaderInstanceId}, or if {@code event} is itself a {@link LogLeader}
     * for {@code leaderInstanceId} (that is how an instance takes over).
     *
     * <p>The backend MUST replace the event's {@code clock} with
     * {@link LogClocks#nextClock}; it may keep or replace {@code timestamp}.</p>
     *
     * <p>How a failure is reported decides what the engine does with the process:</p>
     * <ul>
     *   <li>{@link Optional#empty()}: someone else is the leader. Permanent; the process ends
     *       {@code Dead} with a {@code LeadershipLostException}.</li>
     *   <li>throwing {@link io.fom.api.LeadershipLostException}: the same, for a backend that
     *       learns it by failing (a fenced lock, a lost lease). Throw it only when this instance
     *       can never write again.</li>
     *   <li>throwing {@link IllegalArgumentException}: this event can never be stored (over a
     *       payload limit, say). Permanent.</li>
     *   <li>any other exception: transient; the engine retries with backoff.</li>
     * </ul>
     *
     * @return the persisted event with its assigned {@code clock} on success;
     *         {@link Optional#empty()} if someone else is the leader.
     */
    Optional<LogEvent> append(LogEvent event, String leaderInstanceId);

    /** Lightweight introspection for {@code Engine.introspect()}. */
    LogBackendReport introspect();

    /**
     * Atomically replaces the log with {@code snapshotEvents}, archiving the old
     * contents as the implementation sees fit (a file backend keeps a sibling
     * {@code *.archived.<timestamp>}; the in-memory one discards them).
     *
     * <p>The events keep their clocks, which must strictly increase, and end up at
     * positions {@code 0..n-1}; later appends continue after the last clock.</p>
     *
     * <p><strong>Leadership.</strong> If the latest {@link LogLeader} names an
     * instance other than {@code leaderInstanceId}, {@code compact} MUST refuse and
     * change nothing (no archive, no temporary file); otherwise a deposed leader
     * would put its own {@code LogLeader} back at the front. Check this before the
     * clocks: a plan made just before a takeover has stale clocks, and the caller
     * should hear about the lost leadership.</p>
     *
     * @return descriptor of the new log + the archived old log
     * @throws IllegalArgumentException if the first event is not a {@code LogLeader}
     *         whose {@code instanceId} matches {@code leaderInstanceId}, or the
     *         clocks do not strictly increase
     * @throws io.fom.api.LeadershipLostException if the log's latest
     *         {@link LogLeader} names a different instance; the log is left untouched
     */
    SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId);

    /**
     * Removes archives left by {@link #compact}, keeping the newest
     * {@code keepHistory}. The default does nothing, for backends without archives.
     *
     * <p>The engine never runs this during a {@link #compact} of the same log;
     * a direct caller must ensure the same.</p>
     *
     * @throws IllegalArgumentException if {@code keepHistory < 0}
     */
    default void purgeArchives(int keepHistory) {
        if (keepHistory < 0) throw new IllegalArgumentException("keepHistory < 0");
    }

    @Override
    void close();
}
