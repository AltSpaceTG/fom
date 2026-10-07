package io.fom.test;

import io.fom.serde.SerDe;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract for a {@link SerDe}: a param round-trips to a value {@code equals} to the
 * original, which is how the engine detects definition changes across restarts. Only
 * params are checked, not trigger values or process properties.
 *
 * <p>The default params are this class's records and a few JDK types, which a reflective
 * SerDe (Java serialization, Fury) handles as is. A schema- or registration-based SerDe
 * overrides {@link #sampleParams()} (and, if needed, {@link #jdkParams()}) with its own types:</p>
 *
 * <pre>{@code
 * class MySerDeTest extends SerDeContractTest {
 *     protected SerDe createSerDe() { return new MySerDe(); }
 *     protected List<Serializable> sampleParams() {
 *         return List.of(new SubscriptionParams("EU", 3), new TenantParams("t1", Map.of("k", "v")));
 *     }
 * }
 * }</pre>
 */
public abstract class SerDeContractTest {

    protected abstract SerDe createSerDe();

    /** Application-shaped params (records, nested values) to round-trip. Must not be empty. */
    protected List<Serializable> sampleParams() {
        return List.of(
                new ContractParam("ST-42", 7, List.of("a", "b")),
                new NestedParam("tenant", new ContractParam("x", 1, List.of()), Map.of("k", "v")));
    }

    /**
     * Plain JDK values used as params: {@code Integer}, a {@code Long} beyond the {@code int}
     * range, {@code String} and an {@code ArrayList}. Return an empty list if your SerDe only
     * accepts its own schema types.
     */
    protected List<Serializable> jdkParams() {
        return List.of(Integer.valueOf(42), Long.valueOf(1L << 40), "hello",
                new ArrayList<>(List.of(1, 2, 3)));
    }

    @Test
    void sample_params_round_trip_to_equal_values() {
        SerDe s = createSerDe();
        List<Serializable> params = sampleParams();
        assertThat(params).as("sampleParams() must supply at least one param").isNotEmpty();
        for (Serializable param : params) {
            assertThat(s.loadParam("A", s.serializeParam("A", param))).as("%s", param).isEqualTo(param);
        }
    }

    @Test
    void jdk_type_params_round_trip() {
        SerDe s = createSerDe();
        for (Serializable value : jdkParams()) {
            assertThat(s.loadParam("A", s.serializeParam("A", value))).as("%s", value).isEqualTo(value);
        }
    }

    public record ContractParam(String stationId, int version, List<String> tags) implements Serializable { }

    public record NestedParam(String name, ContractParam inner, Map<String, String> attributes)
            implements Serializable { }
}
