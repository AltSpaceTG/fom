package io.fom;

/**
 * Completion marker returned by lifecycle calls such as {@code Engine.shutdown()}
 * — equivalent to {@code Akka}'s {@code akka.Done}.
 *
 * <p>Singleton; identity-equality is the intended comparison.</p>
 */
public enum Done {

    INSTANCE;

    public static Done done() {
        return INSTANCE;
    }
}
