package io.fom.api;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/** A {@link ProcessLoader} that also receives the node's {@code param}. */
@FunctionalInterface
public interface ParamProcessLoader<P extends Serializable> {

    CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties, P param) throws LoadException;
}
