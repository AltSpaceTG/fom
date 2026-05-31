package io.fom.fsm;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with multiplicative jitter:
 * {@code delay = min(max, min * 2^(attempt-1)) * uniform(0.5, 1.5)}.
 *
 * <p>{@code attempt} is 1-based: {@code delayFor(1)} returns the first retry delay.</p>
 */
public final class BackoffPolicy {

    private final Duration min;
    private final Duration max;
    private final RandomGenerator rng;

    public BackoffPolicy(Duration min, Duration max) {
        this(min, max, RandomGenerator.getDefault());
    }

    public BackoffPolicy(Duration min, Duration max, RandomGenerator rng) {
        this.min = Objects.requireNonNull(min, "min");
        this.max = Objects.requireNonNull(max, "max");
        this.rng = Objects.requireNonNull(rng, "rng");
        if (min.isNegative() || min.isZero()) {
            throw new IllegalArgumentException("min must be > 0, was " + min);
        }
        if (max.compareTo(min) < 0) {
            throw new IllegalArgumentException("max must be >= min");
        }
    }

    public Duration delayFor(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
        }
        long minNanos = min.toNanos();
        long maxNanos = max.toNanos();

        long base;
        if (attempt - 1 >= 62) {
            base = maxNanos;
        } else {
            long shifted;
            try {
                shifted = Math.multiplyExact(minNanos, 1L << (attempt - 1));
            } catch (ArithmeticException overflow) {
                shifted = maxNanos;
            }
            base = Math.min(maxNanos, shifted);
        }

        double jitter = 0.5 + rng.nextDouble();
        long jittered = (long) (base * jitter);
        return Duration.ofNanos(Math.max(1L, jittered));
    }

    public Duration min() {
        return min;
    }

    public Duration max() {
        return max;
    }
}
