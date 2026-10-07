package io.fom.api;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * The wall-clock time a query must answer by; nested dependency queries inherit it.
 * A long {@code compute} loop can check it to give up early
 * ({@link QueryableContext#currentQueryDeadline()}).
 */
public record Deadline(Instant at) {

    public Deadline {
        Objects.requireNonNull(at, "at");
    }

    public static Deadline in(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        return new Deadline(Instant.now().plus(duration));
    }

    public static Deadline in(Duration duration, Clock clock) {
        Objects.requireNonNull(duration, "duration");
        Objects.requireNonNull(clock, "clock");
        return new Deadline(clock.instant().plus(duration));
    }

    public boolean isExpired() {
        return Instant.now().isAfter(at);
    }

    public boolean isExpired(Clock clock) {
        Objects.requireNonNull(clock, "clock");
        return clock.instant().isAfter(at);
    }

    public Duration remaining() {
        return remaining(Clock.systemUTC());
    }

    public Duration remaining(Clock clock) {
        Objects.requireNonNull(clock, "clock");
        Duration left = Duration.between(clock.instant(), at);
        return left.isNegative() ? Duration.ZERO : left;
    }
}
