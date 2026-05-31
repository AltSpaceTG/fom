package io.fom;

import java.util.Objects;

/**
 * Type-safe key for {@link Properties}.
 *
 * <p>Use a single {@code TypedKey} instance per logical cell so that all
 * writers and readers agree on the same {@link Codec}. Two {@code TypedKey}
 * instances with the same {@code name} but different {@code codec} types
 * are not interchangeable and will lead to silent corruption if mixed.</p>
 *
 * <p>{@code equals}/{@code hashCode} are derived from {@code name} and
 * {@code codec.getClass()} — two keys with the same name and the same
 * codec class are considered equal.</p>
 */
public record TypedKey<T>(String name, Codec<T> codec) {

    public TypedKey {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("TypedKey name must not be empty");
        }
        Objects.requireNonNull(codec, "codec");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypedKey<?> that)) return false;
        return name.equals(that.name) && codec.getClass().equals(that.codec.getClass());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, codec.getClass());
    }
}
