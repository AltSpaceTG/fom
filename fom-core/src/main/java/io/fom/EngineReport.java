package io.fom;

import io.fom.log.LogBackendReport;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Snapshot returned by {@code Engine.introspect()} — feeds Prometheus/Grafana
 * style exporters without attaching a debugger.
 */
public record EngineReport(String instanceId,
                           boolean leaderAtStart,
                           boolean isLeader,
                           Version version,
                           LogBackendReport log,
                           GraphMachineReport graph) {

    public EngineReport {
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(graph, "graph");
    }

    /**
     * Per-process snapshot. {@code state} is the FSM state name
     * (e.g. {@code "Serving"}). {@code lastException} is the most recent
     * uncaught exception class name + message, or {@code null}.
     */
    public record NodeReport(String name,
                             Sid sid,
                             String state,
                             int initRetries,
                             int loadRetries,
                             String lastException) {

        public NodeReport {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(state, "state");
            // sid and lastException may be null
        }
    }

    public record GraphMachineReport(List<NodeReport> nodes, Map<String, Integer> mailboxSizes) {

        public GraphMachineReport {
            Objects.requireNonNull(nodes, "nodes");
            Objects.requireNonNull(mailboxSizes, "mailboxSizes");
            nodes = List.copyOf(nodes);
            mailboxSizes = Map.copyOf(mailboxSizes);
        }
    }
}
