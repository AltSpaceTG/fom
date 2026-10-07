package io.fom;

/**
 * Turns one {@link Properties} value into bytes and back, for a {@link TypedKey}.
 *
 * <p>Must be deterministic: the same value always encodes to the same bytes, and
 * {@code decode(encode(v)).equals(v)}. {@code decode} throws {@link CodecException}
 * on malformed bytes.</p>
 */
public interface Codec<T> {

    byte[] encode(T value);

    T decode(byte[] bytes);
}
