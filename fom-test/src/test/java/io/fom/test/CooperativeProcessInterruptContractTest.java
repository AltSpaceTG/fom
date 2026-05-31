package io.fom.test;

import io.fom.api.Process;

/** Self-test: the bundled {@link InterruptContractTest.CooperativeBusyLoop} satisfies the contract. */
class CooperativeProcessInterruptContractTest extends InterruptContractTest {

    @Override
    protected Process newProcess() {
        return new CooperativeBusyLoop();
    }

    @Override
    protected Object cancellableQuery() {
        return "long-query";
    }
}
