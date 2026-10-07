package io.fom.api;

/** Why a {@code ScheduledWatcher} stopped itself. Kept small so it is safe as a metric tag. */
public enum WatcherStopReason {

    /** Its process is no longer in the graph. Re-adding the process does not revive the watcher. */
    PROCESS_REMOVED,

    /** The {@code Executor} the caller gave it was shut down, so its check can never run again. */
    EXECUTOR_SHUT_DOWN,

    /** Its trigger could not be recorded: this instance no longer leads the log. */
    LEADERSHIP_LOST
}
