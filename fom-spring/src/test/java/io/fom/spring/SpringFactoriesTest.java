package io.fom.spring;

import io.fom.SerializableSupplier;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class SpringFactoriesTest {

    @Test
    void bean_supplier_resolves_singleton() {
        try (var ctx = new AnnotationConfigApplicationContext(GreeterConfig.class)) {
            SpringFactories.setContext(ctx);
            SerializableSupplier<Greeter> supplier = SpringFactories.bean(Greeter.class);
            Greeter g1 = supplier.get();
            Greeter g2 = supplier.get();
            assertThat(g1).isSameAs(g2);
            assertThat(g1.greet()).isEqualTo("hello");
        }
    }

    interface Greeter {
        String greet();
    }

    static final class GreeterImpl implements Greeter {

        @Override
        public String greet() {
            return "hello";
        }
    }

    @Configuration
    static class GreeterConfig {

        @Bean
        Greeter greeter() {
            return new GreeterImpl();
        }
    }
}
