package io.fom;

import io.fom.log.LogBackendReport;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** What {@code Engine.introspect()} returns: leadership, the log, and the state of every process. */
public record EngineReport(String instanceId,
                           boolean isLeader,
                           LogBackendReport log,
                           GraphReport graph) {

    public EngineReport {
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(graph, "graph");
    }

    /**
     * Per-process snapshot.
     *
     * @param sid           the current Sid, or {@code null} if the process has none yet
     * @param state         FSM state name (e.g. {@code "Serving"}), or {@code "Paused"}
     * @param initRetries   failed {@code init} attempts in the process's latest (re)initialisation
     * @param loadRetries   failed {@code load} attempts in the process's latest (re)initialisation
     * @param lastException the most recent init/load failure as {@code "Class: message"}, or
     *                      {@code null}. It outlives a recovery, so it may be old news, and is lost
     *                      when the node is replaced (a {@code Dead} process restarted, a
     *                      {@code newGraph} respawn). Read it with the engine's log, not as a verdict
     * @param replacement   {@code "Initializing"} or {@code "Loading"} while a new version is being made
     *                      beside the serving one ({@code sid}), otherwise {@code null}
     * @param stale         the serving version was asked to be replaced and is not yet: a re-init is in
     *                      flight, queued, or failed (it is retried, see {@code EngineConfig})
     */
    public record NodeReport(String name,
                             Sid sid,
                             String state,
                             int initRetries,
                             int loadRetries,
                             String lastException,
                             String replacement,
                             boolean stale) {

        public NodeReport {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(state, "state");
        }

        public NodeReport(String name, Sid sid, String state, int initRetries, int loadRetries,
                          String lastException) {
            this(name, sid, state, initRetries, loadRetries, lastException, null, false);
        }
    }

    public record GraphReport(List<NodeReport> nodes, Map<String, Integer> mailboxSizes) {

        public GraphReport {
            Objects.requireNonNull(nodes, "nodes");
            Objects.requireNonNull(mailboxSizes, "mailboxSizes");
            nodes = List.copyOf(nodes);
            mailboxSizes = Map.copyOf(mailboxSizes);
        }
    }
}
