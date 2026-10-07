package io.fom.fury;

import io.fom.serde.SerDe;
import io.fom.test.SerDeContractTest;

/** Runs the shared {@link SerDeContractTest} against {@link FurySerDe}. */
class FurySerDeTest extends SerDeContractTest {

    @Override
    protected SerDe createSerDe() {
        return new FurySerDe();
    }
}
