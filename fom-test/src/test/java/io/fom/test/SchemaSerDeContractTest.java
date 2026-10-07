package io.fom.test;

import io.fom.serde.SerDe;
import io.fom.serde.SerDeException;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A SerDe that only knows its own type — like a schema/registration-based one — passes the
 * contract by supplying its own sample params instead of {@code io.fom.test}'s records.
 */
class SchemaSerDeContractTest extends SerDeContractTest {

    record Order(String region, int qty) implements Serializable { }

    /** Encodes {@link Order} only, as {@code region|qty}; anything else is refused. */
    static final class OrderOnlySerDe implements SerDe {
        @Override
        public byte[] serializeParam(String processName, Serializable param) {
            if (!(param instanceof Order o)) {
                throw new SerDeException("unregistered type " + param.getClass().getName());
            }
            return (o.region() + "|" + o.qty()).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Object loadParam(String processName, byte[] bytes) {
            String[] parts = new String(bytes, StandardCharsets.UTF_8).split("\\|", 2);
            return new Order(parts[0], Integer.parseInt(parts[1]));
        }
    }

    @Override
    protected SerDe createSerDe() {
        return new OrderOnlySerDe();
    }

    @Override
    protected List<Serializable> sampleParams() {
        return List.of(new Order("EU", 3), new Order("US", 0));
    }

    @Override
    protected List<Serializable> jdkParams() {
        return List.of();
    }
}
