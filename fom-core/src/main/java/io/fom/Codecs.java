package io.fom;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Built-in {@link Codec} factories for primitive and common scalar types.
 *
 * <p>All codecs in this class are stateless singletons and safe for
 * concurrent use across threads.</p>
 */
public final class Codecs {

    private Codecs() {
    }

    private static final Codec<Integer> INT = new Codec<>() {
        @Override
        public byte[] encode(Integer value) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        }

        @Override
        public Integer decode(byte[] bytes) {
            if (bytes.length != Integer.BYTES) {
                throw new CodecException("Expected " + Integer.BYTES + " bytes, got " + bytes.length);
            }
            return ByteBuffer.wrap(bytes).getInt();
        }
    };

    private static final Codec<Long> LONG = new Codec<>() {
        @Override
        public byte[] encode(Long value) {
            return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        }

        @Override
        public Long decode(byte[] bytes) {
            if (bytes.length != Long.BYTES) {
                throw new CodecException("Expected " + Long.BYTES + " bytes, got " + bytes.length);
            }
            return ByteBuffer.wrap(bytes).getLong();
        }
    };

    private static final Codec<String> STRING = new Codec<>() {
        @Override
        public byte[] encode(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    };

    private static final Codec<URI> URI_CODEC = new Codec<>() {
        @Override
        public byte[] encode(URI value) {
            return STRING.encode(value.toString());
        }

        @Override
        public URI decode(byte[] bytes) {
            try {
                return new URI(STRING.decode(bytes));
            } catch (URISyntaxException e) {
                throw new CodecException("Invalid URI bytes", e);
            }
        }
    };

    public static Codec<Integer> intCodec() {
        return INT;
    }

    public static Codec<Long> longCodec() {
        return LONG;
    }

    public static Codec<String> stringCodec() {
        return STRING;
    }

    public static Codec<URI> uri() {
        return URI_CODEC;
    }
}
