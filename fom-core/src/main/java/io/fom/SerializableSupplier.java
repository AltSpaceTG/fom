package io.fom;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * A {@link Supplier} that is also {@link Serializable}. Required by
 * {@code GraphBuilder.add(...)} so that the graph can be serialised into
 * {@code LogChangeGraph} events without losing the ability to re-create
 * {@link io.fom.api.ProcessInitializer}/{@link io.fom.api.ProcessLoader}
 * instances after a JVM restart.
 *
 * <p>Use intersection-cast in Java to convert a method reference:
 * {@code (SerializableSupplier<MyInit> & Serializable) MyInit::new}.
 * In Kotlin (via {@code fom-kotlin}), the DSL hides this behind
 * {@code @JvmSerializableLambda}.</p>
 */
@FunctionalInterface
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
