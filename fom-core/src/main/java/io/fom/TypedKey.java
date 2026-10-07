package io.fom;

import java.util.Objects;

/**
 * A {@link Properties} cell name with the {@link Codec} for its value. Share one
 * constant per cell: keys with the same name but different codecs read each other's
 * bytes as garbage.
 *
 * <p>Equal when the name and the codec's class match.</p>
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
