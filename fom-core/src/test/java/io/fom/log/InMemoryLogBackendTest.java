package io.fom.log;

/** Runs the {@link LogBackendContractTest} against the in-memory implementation. */
class InMemoryLogBackendTest extends LogBackendContractTest {

    @Override
    protected LogBackend create() {
        return new InMemoryLogBackend();
    }
}
