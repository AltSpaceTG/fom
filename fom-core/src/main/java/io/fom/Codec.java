package io.fom;

/**
 * Per-value encoder/decoder used by {@link TypedKey} for typed access
 * to {@link Properties}. Independent of the engine-wide {@code SerDe}
 * SPI (see {@link io.fom.serde.SerDe}) which handles log events and
 * process graph serialization.
 *
 * <p>Implementations must be deterministic and pure:
 * the same input value must produce the same byte sequence,
 * and {@code decode(encode(v)).equals(v)} must hold for all valid {@code v}.</p>
 *
 * <p>Implementations should not throw on encode for valid input. On decode,
 * a {@link CodecException} is appropriate for malformed bytes.</p>
 */
public interface Codec<T> {

    byte[] encode(T value);

    T decode(byte[] bytes);
}
