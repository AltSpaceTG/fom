package io.fom;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A {@link Function} that is also {@link Serializable}. Used by
 * {@link QueryRoute.Dynamic} so the graph can be serialised into the log
 * without losing routing logic across JVM restarts.
 *
 * <p>Lambdas declared against this type are automatically serialised by the
 * JVM (intersection-cast under the hood).</p>
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
