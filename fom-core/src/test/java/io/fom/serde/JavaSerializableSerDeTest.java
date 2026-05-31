package io.fom.serde;

/** Runs the {@link SerDeContractTest} against the Java-serialization fallback. */
class JavaSerializableSerDeTest extends SerDeContractTest {

    @Override
    protected SerDe createSerDe() {
        return new JavaSerializableSerDe();
    }
}
