package io.fom.api;

/**
 * Another instance now owns the log, so this one's write was refused and not recorded.
 *
 * <p>Queries keep being served from memory, but no re-init, pause or graph change can
 * be persisted any more; close the engine and open a new one to compete again.</p>
 *
 * <p>Two operations can be left half applied, and their message says how far they got:
 * a graph change that could not retire the removed processes (the new graph is live, but
 * their state would be loaded again on restart), and a pause that stopped between nodes.</p>
 */
public class LeadershipLostException extends IllegalStateException {

    public LeadershipLostException(String message) {
        super(message);
    }
}
