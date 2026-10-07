package io.fom.fsm;

import io.fom.Sid;

/**
 * Called when a {@link ProcessFSM} starts serving with a new {@link Sid};
 * {@link GraphMachine} uses it to start the reactive cascade.
 */
@FunctionalInterface
public interface SidPromotionListener {

    /**
     * @param processName process whose Sid was promoted
     * @param previousSid the retired Sid, or {@code null} on the first init
     * @param newSid      the Sid now serving
     */
    void onSidPromotion(String processName, Sid previousSid, Sid newSid);
}
