package io.fom;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Declarative watcher that polls external state at a fixed interval and
 * emits a {@code LogTrigger} whenever {@link #check} returns a new value.
 *
 * <p>{@link #check} runs on {@link #executor} — it MUST NOT block the engine
 * dispatchers. The Engine wires watchers through a dedicated
 * {@code watcher-dispatcher} when {@link #executor} is {@code null}.</p>
 */
public final class ScheduledWatcher<V extends Serializable> {

    private final String processName;
    private final Class<V> stateClass;
    private final V initialValue;
    private final Duration initialDelay;
    private final Duration interval;
    private final Function<V, Optional<V>> check;
    private final Executor executor;

    public ScheduledWatcher(String processName,
                            Class<V> stateClass,
                            V initialValue,
                            Duration initialDelay,
                            Duration interval,
                            Function<V, Optional<V>> check,
                            Executor executor) {
        this.processName = Objects.requireNonNull(processName, "processName");
        if (processName.isEmpty()) {
            throw new IllegalArgumentException("processName must not be empty");
        }
        this.stateClass = Objects.requireNonNull(stateClass, "stateClass");
        this.initialValue = Objects.requireNonNull(initialValue, "initialValue");
        this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
        if (initialDelay.isNegative()) {
            throw new IllegalArgumentException("initialDelay must be >= 0, was " + initialDelay);
        }
        this.interval = Objects.requireNonNull(interval, "interval");
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("interval must be > 0, was " + interval);
        }
        this.check = Objects.requireNonNull(check, "check");
        this.executor = executor;
    }

    public String processName() { return processName; }
    public Class<V> stateClass() { return stateClass; }
    public V initialValue() { return initialValue; }
    public Duration initialDelay() { return initialDelay; }
    public Duration interval() { return interval; }
    public Function<V, Optional<V>> check() { return check; }
    public Optional<Executor> executor() { return Optional.ofNullable(executor); }
}
