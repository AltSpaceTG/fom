package io.fom.api;

/**
 * Optional contract on a query message: the message itself knows which process
 * should serve it. When the engine dispatches a query, {@code Routable} wins
 * over a graph-level {@code QueryRoute} registration.
 *
 * <p>Useful when a message lives in a reusable library together with the
 * process that consumes it — the routing rule travels with the message and
 * does not have to be re-declared in every {@code Graph} that uses the
 * library.</p>
 *
 * <p><strong>Contract:</strong> {@link #targetProcess()} must be pure and
 * fast — the engine may call it more than once. Heavy lookups (DB, network)
 * are anti-patterns here; resolve the target ahead of time and embed it in
 * the message.</p>
 */
public interface Routable {

    String targetProcess();
}
