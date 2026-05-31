package io.fom.api;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Parameterised variant of {@link ProcessInitializer}: the parameter
 * {@code param} is part of the node identity and is supplied
 * on every {@code init} invocation. {@code param} must be {@link Serializable}
 * and immutable.
 */
@FunctionalInterface
public interface ParamProcessInitializer<P extends Serializable> {

    CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, P param) throws InitializationException;
}
