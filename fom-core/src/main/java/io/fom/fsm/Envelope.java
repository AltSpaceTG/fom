package io.fom.fsm;

import io.fom.Sid;
import io.fom.api.Process;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** A message in a {@link ProcessFSM}'s mailbox. */
sealed interface Envelope {

    /** Run {@code init} from scratch. */
    record SpawnInit() implements Envelope {

        static final SpawnInit INSTANCE = new SpawnInit();
    }

    /** Run {@code load} on the properties of the {@code LogInitialized} at {@code clock}. */
    record SpawnLoad(long clock, Map<String, byte[]> properties) implements Envelope {

        public SpawnLoad {
            Objects.requireNonNull(properties, "properties");
        }
    }

    /**
     * How an append made off the dispatcher ended: {@code written} (with the new Sid for an init),
     * refused by the log ({@code written} false, no failure), or failed with {@code failure}.
     */
    record Write(boolean written, Sid sid, RuntimeException failure) {

        static final Write REFUSED = new Write(false, null, null);

        static Write written(Sid sid) {
            return new Write(true, sid, null);
        }

        static Write failed(RuntimeException failure) {
            return new Write(false, null, Objects.requireNonNull(failure, "failure"));
        }
    }

    /**
     * What an init attempt of {@code cycle} produced: properties or a failure. {@code write} is how the
     * replacement's {@code LogInitialized} append ended, or {@code null} when nothing was written yet.
     */
    record InitResult(long cycle, int attempt, Map<String, byte[]> properties, Throwable failure, Write write)
            implements Envelope {

        InitResult(long cycle, int attempt, Map<String, byte[]> properties, Throwable failure) {
            this(cycle, attempt, properties, failure, null);
        }

        public InitResult {
            if (attempt < 1) {
                throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
            }
            if ((properties == null) == (failure == null)) {
                throw new IllegalArgumentException(
                        "exactly one of properties/failure must be non-null");
            }
        }

        boolean ok() {
            return properties != null;
        }
    }

    /**
     * What a load attempt of {@code cycle} on {@code sid} produced: a process or a failure. {@code write}
     * is how the replacement's {@code LogLoaded} append ended, or {@code null} when nothing was written yet.
     */
    record LoadResult(long cycle, Sid sid, int attempt, Process process, Throwable failure, Write write)
            implements Envelope {

        LoadResult(long cycle, Sid sid, int attempt, Process process, Throwable failure) {
            this(cycle, sid, attempt, process, failure, null);
        }

        public LoadResult {
            if (attempt < 1) {
                throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
            }
            if ((process == null) == (failure == null)) {
                throw new IllegalArgumentException(
                        "exactly one of process/failure must be non-null");
            }
        }

        boolean ok() {
            return process != null;
        }
    }

    /** A query to answer through {@code reply}. */
    record Query(UUID queryId,
                 Object message,
                 CompletableFuture<Object> reply,
                 long deadlineEpochMillis) implements Envelope {

        public Query {
            Objects.requireNonNull(queryId, "queryId");
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(reply, "reply");
        }
    }

    /** Fires when the backoff before retry {@code forAttempt} of {@code cycle} has passed. */
    record RetryTick(Phase phase, long cycle, int forAttempt) implements Envelope {

        public RetryTick {
            Objects.requireNonNull(phase, "phase");
            if (forAttempt < 1) {
                throw new IllegalArgumentException("forAttempt must be >= 1, was " + forAttempt);
            }
        }
    }

    /**
     * Stop the FSM; {@code reply} completes once it is {@link State.Dead}. With
     * {@code retireSid} it first writes {@code LogDead} for the current Sid, as a
     * graph swap does for a changed node.
     */
    record Shutdown(CompletableFuture<Void> reply, boolean retireSid) implements Envelope {

        public Shutdown {
            Objects.requireNonNull(reply, "reply");
        }

        public Shutdown(CompletableFuture<Void> reply) {
            this(reply, false);
        }
    }

    /**
     * {@code Process.cleanUp()} of {@code generation} returned or threw.
     *
     * @param generation the Sid being cleaned up (clock 0 when there was no state)
     * @param startedAt when that {@code cleanUp} began, or {@code null} if there was
     *                  no process to clean up
     */
    record CleanupDone(Sid generation, Throwable failure, Instant startedAt) implements Envelope {

        public CleanupDone {
            Objects.requireNonNull(generation, "generation");
        }
    }

    /** Re-init, after a trigger, a watcher tick or a dependency change. */
    record ReinitRequest(ReinitCause cause) implements Envelope {

        public ReinitRequest {
            Objects.requireNonNull(cause, "cause");
        }
    }

    /**
     * Replace the serving version with the candidate persisted at {@code clock} (written before a
     * restart or a pause): load it without a new init.
     */
    record ReplaceFrom(long clock, Map<String, byte[]> properties) implements Envelope {

        public ReplaceFrom {
            Objects.requireNonNull(properties, "properties");
        }
    }

    /** The engine is closing: start no re-init any more and drop the one under way (the old version serves on). */
    record StopReinits() implements Envelope {

        static final StopReinits INSTANCE = new StopReinits();
    }

    /** The backoff after a failed re-init has passed; ignored unless {@code token} is still current. */
    record ReinitRetry(long token, ReinitCause cause) implements Envelope {

        public ReinitRetry {
            Objects.requireNonNull(cause, "cause");
        }
    }

    /**
     * Cancel the running init/load of {@code targetSid}, or whatever runs when it is {@code null}
     * (a replacement's included). Ignored when nothing matching runs.
     */
    record CancelRequest(Sid targetSid, CompletableFuture<Void> reply) implements Envelope {

        public CancelRequest {
            Objects.requireNonNull(reply, "reply");
        }
    }

    /** Which retry phase a {@link RetryTick} belongs to. */
    enum Phase {
        INIT, LOAD
    }
}
