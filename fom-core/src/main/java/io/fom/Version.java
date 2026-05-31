package io.fom;

import java.io.Serializable;

/**
 * Logical version number of the process graph. Grows monotonically
 * with each {@code newGraph} call that produces a non-trivial change.
 */
public record Version(int value) implements Serializable, Comparable<Version> {

    public static final Version INITIAL = new Version(0);

    public Version {
        if (value < 0) {
            throw new IllegalArgumentException("version must be >= 0, was " + value);
        }
    }

    public Version next() {
        return new Version(value + 1);
    }

    @Override
    public int compareTo(Version other) {
        return Integer.compare(this.value, other.value);
    }
}
