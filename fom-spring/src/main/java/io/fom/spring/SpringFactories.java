package io.fom.spring;

import io.fom.SerializableSupplier;
import org.springframework.context.ApplicationContext;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bridges a Spring {@link ApplicationContext} into fom's {@link SerializableSupplier}
 * contract. The returned supplier resolves a bean from the registered context
 * on every {@code get()}.
 *
 * <p>Set the context once at startup via {@link #setContext(ApplicationContext)}
 * before constructing the graph. Suppliers stay {@link SerializableSupplier serialisable}
 * because they only close over the target {@link Class}.</p>
 */
public final class SpringFactories {

    private static final AtomicReference<ApplicationContext> CONTEXT = new AtomicReference<>();

    private SpringFactories() {
    }

    public static void setContext(ApplicationContext context) {
        Objects.requireNonNull(context, "context");
        CONTEXT.set(context);
    }

    public static ApplicationContext requireContext() {
        ApplicationContext ctx = CONTEXT.get();
        if (ctx == null) {
            throw new IllegalStateException(
                    "SpringFactories.setContext(...) was not called before resolving beans");
        }
        return ctx;
    }

    /**
     * Returns a {@code SerializableSupplier<T>} that resolves a bean of type
     * {@code T} from the registered {@link ApplicationContext}.
     */
    public static <T> SerializableSupplier<T> bean(Class<T> type) {
        Objects.requireNonNull(type, "type");
        return new BeanSupplier<>(type);
    }

    private record BeanSupplier<T>(Class<T> type) implements SerializableSupplier<T> {

        @Override
        public T get() {
            return requireContext().getBean(type);
        }
    }
}
