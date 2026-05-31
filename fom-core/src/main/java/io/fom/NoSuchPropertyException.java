package io.fom;

/**
 * Thrown by {@link Properties#get(TypedKey)} and {@link Properties#getRaw(String)}
 * when no value is bound to the requested key.
 */
public class NoSuchPropertyException extends RuntimeException {

    private final String key;

    public NoSuchPropertyException(String key) {
        super("No such property: " + key);
        this.key = key;
    }

    public String key() {
        return key;
    }
}
