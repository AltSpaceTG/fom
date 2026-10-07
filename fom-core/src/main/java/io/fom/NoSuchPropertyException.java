package io.fom;

/** Thrown by {@link Properties#get(TypedKey)} and {@link Properties#getRaw(String)} for a missing key. */
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
