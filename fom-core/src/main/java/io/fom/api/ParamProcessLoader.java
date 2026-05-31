package io.fom.api;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Parameterised variant of {@link ProcessLoader}.
 */
@FunctionalInterface
public interface ParamProcessLoader<P extends Serializable> {

    CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties, P param) throws LoadException;
}
