package io.fom.guice;

import com.google.inject.Injector;
import io.fom.SerializableSupplier;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bridges a Guice {@link Injector} into fom's {@link SerializableSupplier}
 * contract. The returned supplier resolves a fresh binding on every {@code get()},
 * so {@code @Singleton}-scoped bindings produce the same instance and prototype
 * bindings produce fresh ones.
 *
 * <p>The injector itself is held in a static {@link AtomicReference}; the
 * suppliers stay {@link SerializableSupplier serialisable} because they only
 * close over the target {@link Class}. Set the injector once at startup with
 * {@link #setInjector(Injector)} before constructing the graph.</p>
 */
public final class GuiceFactories {

    private static final AtomicReference<Injector> INJECTOR = new AtomicReference<>();

    private GuiceFactories() {
    }

    public static void setInjector(Injector injector) {
        Objects.requireNonNull(injector, "injector");
        INJECTOR.set(injector);
    }

    public static Injector requireInjector() {
        Injector inj = INJECTOR.get();
        if (inj == null) {
            throw new IllegalStateException(
                    "GuiceFactories.setInjector(...) was not called before resolving bindings");
        }
        return inj;
    }

    /**
     * Returns a {@code SerializableSupplier<T>} that resolves {@code T} from
     * the registered {@link Injector}. The class reference travels with the
     * graph; the injector is looked up by static reference at {@code get()} time.
     */
    public static <T> SerializableSupplier<T> bound(Class<T> type) {
        Objects.requireNonNull(type, "type");
        return new BoundSupplier<>(type);
    }

    private record BoundSupplier<T>(Class<T> type) implements SerializableSupplier<T> {

        @Override
        public T get() {
            return requireInjector().getInstance(type);
        }
    }
}
