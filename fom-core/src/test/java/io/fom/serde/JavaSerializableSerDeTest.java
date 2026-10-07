package io.fom.serde;

import io.fom.test.SerDeContractTest;

/** Runs the {@link SerDeContractTest} against the Java-serialization fallback. */
class JavaSerializableSerDeTest extends SerDeContractTest {

    @Override
    protected SerDe createSerDe() {
        return new JavaSerializableSerDe();
    }
}
