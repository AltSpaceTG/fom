package io.fom.fsm;

import io.fom.Sid;

import java.util.Map;
import java.util.Objects;

/**
 * Lifecycle state of a {@link ProcessFSM}. Sealed: every variant is enumerated
 * here so pattern-match dispatch is exhaustive.
 *
 * <p>Transitions are documented per variant; the FSM main loop enforces them.</p>
 */
public sealed interface State {

    String name();

    /** Node declared in the graph but FSM not yet spawned. */
    record NotPresent() implements State {

        public static final NotPresent INSTANCE = new NotPresent();

        @Override
        public String name() {
            return "NotPresent";
        }
    }

    /** Currently running {@code init()}. {@code attempt} starts at 1; {@code lastError} is null on first try. */
    record Initializing(int attempt, Throwable lastError) implements State {

        public Initializing {
            if (attempt < 1) {
                throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
            }
        }

        @Override
        public String name() {
            return "Initializing";
        }
    }

    /**
     * Currently running {@code load()} against {@code properties}.
     * After more than {@code load.max-retries} failures the FSM falls back
     * to {@link Initializing}.
     */
    record Loading(int attempt, Throwable lastError, Map<String, byte[]> properties) implements State {

        public Loading {
            if (attempt < 1) {
                throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
            }
            Objects.requireNonNull(properties, "properties");
        }

        @Override
        public String name() {
            return "Loading";
        }
    }

    /** Ready to handle queries. The live {@code Process} lives in a separate field of the FSM. */
    record Serving() implements State {

        public static final Serving INSTANCE = new Serving();

        @Override
        public String name() {
            return "Serving";
        }
    }

    /**
     * Running {@code process.cleanUp()}; waiting for in-flight queries to drain.
     * The {@code mode} determines what happens after cleanup completes
     * — see {@link CleanupMode}.
     */
    record CleaningUp(Sid sidBeingRetired, CleanupMode mode) implements State {

        public CleaningUp {
            Objects.requireNonNull(sidBeingRetired, "sidBeingRetired");
            Objects.requireNonNull(mode, "mode");
        }

        @Override
        public String name() {
            return "CleaningUp";
        }
    }

    /**
     * What the FSM does after the cleanup callback finishes.
     */
    enum CleanupMode {
        /** Graceful shutdown — terminal {@code Dead}, no {@code LogDead} written
         *  (so a JVM restart can warm-load the prior Sid → idempotent restart). */
        SHUTDOWN,
        /** Triggered re-init — {@code LogDead} already written before cleanup;
         *  recycle to {@code Initializing(1, null)} with a fresh attempt counter. */
        REINIT,
        /** Graph-swap replacement — {@code LogDead} already written before cleanup;
         *  terminal {@code Dead}. A new FSM instance for the same name is spawned
         *  by {@code GraphMachine.applyGraphChange}. */
        REPLACE
    }

    /** Terminal — FSM no longer accepts envelopes. */
    record Dead() implements State {

        public static final Dead INSTANCE = new Dead();

        @Override
        public String name() {
            return "Dead";
        }
    }
}
