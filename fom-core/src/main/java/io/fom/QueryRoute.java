package io.fom;

import java.io.Serializable;
import java.util.Objects;

/**
 * Routing rule that maps a query class to a target process. Held inside a
 * {@link Graph} under the {@code typeRouting} field.
 *
 * <p>{@link Static} pins every instance of the class to one process.
 * {@link Dynamic} computes the process name from the query value — useful for
 * multi-tenant fan-out where the tenant ID is part of the message.</p>
 *
 * <p>Routes are matched by exact class (no subclass fallback): each subclass
 * registers separately. Two routes for the same class are a build-time error.</p>
 */
public sealed interface QueryRoute extends Serializable {

    /** All instances of the registered class go to {@code processName}. */
    record Static(String processName) implements QueryRoute {

        public Static {
            Objects.requireNonNull(processName, "processName");
            if (processName.isEmpty()) {
                throw new IllegalArgumentException("processName must not be empty");
            }
        }
    }

    /**
     * Process name is computed from the query at dispatch time. The {@code resolver}
     * must be pure and quick — the engine may invoke it more than once.
     */
    record Dynamic(SerializableFunction<Object, String> resolver) implements QueryRoute {

        public Dynamic {
            Objects.requireNonNull(resolver, "resolver");
        }
    }
}
