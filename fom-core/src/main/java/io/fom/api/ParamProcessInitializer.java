package io.fom.api;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A {@link ProcessInitializer} that also receives the node's {@code param}. The param is
 * part of the node's identity, so it must be immutable and implement {@code equals}.
 */
@FunctionalInterface
public interface ParamProcessInitializer<P extends Serializable> {

    CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, P param) throws InitializationException;
}
