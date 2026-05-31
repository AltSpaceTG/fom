package io.fom.fsm;

import io.fom.Sid;
import io.fom.api.Process;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Mailbox message accepted by {@link ProcessFSM}. Sealed: exhaustive
 * pattern-match dispatch in the FSM main loop.
 */
sealed interface Envelope {

    /** Cold-start request — run {@code initializer.init()} from scratch. */
    record SpawnInit() implements Envelope {

        static final SpawnInit INSTANCE = new SpawnInit();
    }

    /** Warm-restart request — call {@code loader.load()} against persisted properties. */
    record SpawnLoad(Map<String, byte[]> properties) implements Envelope {

        public SpawnLoad {
            Objects.requireNonNull(properties, "properties");
        }
    }

    /** Result of an init worker — either a properties map or a failure. */
    record InitResult(int attempt, Map<String, byte[]> properties, Throwable failure) implements Envelope {

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

    /** Result of a load worker — either a Process or a failure. */
    record LoadResult(int attempt, Process process, Throwable failure) implements Envelope {

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

    /**
     * Inbound query. Reply is delivered through {@code reply} —
     * the FSM forks a V-thread that calls {@code process.compute(...)}.
     */
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

    /** Scheduled retry tick — fires after backoff delay. */
    record RetryTick(Phase phase, int forAttempt) implements Envelope {

        public RetryTick {
            Objects.requireNonNull(phase, "phase");
            if (forAttempt < 1) {
                throw new IllegalArgumentException("forAttempt must be >= 1, was " + forAttempt);
            }
        }
    }

    /**
     * External request to terminate this FSM. Reply completes once the FSM
     * reaches {@link State.Dead}. When {@code retireSid=true}, the FSM also
     * writes {@code LogDead} for the current Sid before cleanup — used by
     * {@code GraphMachine} when swapping a changed node out of the graph.
     */
    record Shutdown(CompletableFuture<Void> reply, boolean retireSid) implements Envelope {

        public Shutdown {
            Objects.requireNonNull(reply, "reply");
        }

        public Shutdown(CompletableFuture<Void> reply) {
            this(reply, false);
        }
    }

    /** Process.cleanUp() returned (or threw). */
    record CleanupDone(Throwable failure) implements Envelope {
    }

    /**
     * External re-init request — written by the engine in response to
     * {@code engine.trigger(...)}, a watcher tick, or a dependency cascade.
     * Multiple requests within the dedup window collapse into one.
     */
    record ReinitRequest(io.fom.fsm.ReinitCause cause) implements Envelope {

        public ReinitRequest {
            Objects.requireNonNull(cause, "cause");
        }
    }

    /**
     * Admin request to cancel the in-flight init/load for the given Sid.
     * Honoured only while in {@code Initializing} or {@code Loading} for that Sid.
     */
    record CancelRequest(Sid targetSid, CompletableFuture<Void> reply) implements Envelope {

        public CancelRequest {
            Objects.requireNonNull(targetSid, "targetSid");
            Objects.requireNonNull(reply, "reply");
        }
    }

    /** Which retry phase a {@link RetryTick} belongs to. */
    enum Phase {
        INIT, LOAD
    }
}
