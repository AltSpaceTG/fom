package io.fom.fsm;

import io.fom.Sid;

/**
 * Callback invoked when a {@link ProcessFSM} reaches a fresh {@code Serving}
 * state with a new {@link Sid}. {@link GraphMachine} subscribes to fan out
 * the reactive-dependency cascade.
 */
@FunctionalInterface
public interface SidPromotionListener {

    /**
     * @param processName process whose Sid was promoted
     * @param previousSid the Sid that was retired (may be {@code null} on first cold init)
     * @param newSid      the freshly-installed Sid
     */
    void onSidPromotion(String processName, Sid previousSid, Sid newSid);
}
