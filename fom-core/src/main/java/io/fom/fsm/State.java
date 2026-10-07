package io.fom.fsm;

import io.fom.Sid;

import java.util.Map;
import java.util.Objects;

/** Lifecycle state of a {@link ProcessFSM}. */
public sealed interface State {

    String name();

    /** In the graph, not started yet. */
    record NotPresent() implements State {

        public static final NotPresent INSTANCE = new NotPresent();

        @Override
        public String name() {
            return "NotPresent";
        }
    }

    /**
     * Running {@code init}. {@code cycle} identifies this (re)initialisation, so results of an
     * abandoned one are told apart; {@code attempt} starts at 1; {@code lastError} is null on the first.
     */
    record Initializing(long cycle, int attempt, Throwable lastError) implements State {

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
     * Running {@code load} on {@code properties}, in the same {@code cycle} as the init that wrote
     * them. Past {@code load.max-retries} failures the FSM falls back to {@link Initializing}.
     */
    record Loading(long cycle, int attempt, Throwable lastError, Map<String, byte[]> properties) implements State {

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

    /**
     * Answering queries with version {@code sid}. While {@code replacement} is not {@code null} a new
     * version is being initialised or loaded beside it; {@code sid} keeps answering until it serves.
     */
    record Serving(Sid sid, Replacement replacement) implements State {

        public Serving {
            Objects.requireNonNull(sid, "sid");
        }

        @Override
        public String name() {
            return "Serving";
        }
    }

    /** The new version a {@link Serving} process is making, with its own retry counters. */
    sealed interface Replacement {

        long cycle();

        int attempt();

        Throwable lastError();

        String name();

        /** Running {@code init} for the new version. */
        record Initializing(long cycle, int attempt, Throwable lastError) implements Replacement {

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

        /** Running {@code load} on {@code properties}, the state persisted as {@code candidate}. */
        record Loading(long cycle, int attempt, Throwable lastError, Map<String, byte[]> properties, Sid candidate)
                implements Replacement {

            public Loading {
                if (attempt < 1) {
                    throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
                }
                Objects.requireNonNull(properties, "properties");
                Objects.requireNonNull(candidate, "candidate");
            }

            @Override
            public String name() {
                return "Loading";
            }
        }
    }

    /** Draining queries and running {@code cleanUp}; {@code mode} says what comes next. */
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

    /** What the FSM does after {@code cleanUp}. */
    enum CleanupMode {
        /** Go {@code Dead} without writing {@code LogDead}, so a restart can load the Sid again. */
        SHUTDOWN,
        /**
         * {@code LogDead} is already written; start over at a new {@code Initializing} cycle. Only
         * {@link io.fom.ReinitStrategy#RELEASE_FIRST} re-inits go through it.
         */
        REINIT,
        /** {@code LogDead} is already written; go {@code Dead}, a new FSM takes the name. */
        REPLACE
    }

    /** Terminal: accepts no more envelopes. */
    record Dead() implements State {

        public static final Dead INSTANCE = new Dead();

        @Override
        public String name() {
            return "Dead";
        }
    }
}
