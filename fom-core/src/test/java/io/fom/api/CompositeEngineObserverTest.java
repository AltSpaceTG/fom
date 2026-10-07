package io.fom.api;

import io.fom.Sid;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeEngineObserverTest {

    /** Records the name of every callback it receives. */
    static final class Recorder implements EngineObserver {
        final List<String> calls = new ArrayList<>();

        @Override public void onStateTransition(String p, String from, String to) { calls.add("onStateTransition"); }
        @Override public void onInitStarted(String p, int attempt) { calls.add("onInitStarted"); }
        @Override public void onInitCompleted(String p, Sid sid, Duration d) { calls.add("onInitCompleted"); }
        @Override public void onInitFailed(String p, int attempt, Throwable c) { calls.add("onInitFailed"); }
        @Override public void onLoadStarted(String p, Sid sid, int attempt) { calls.add("onLoadStarted"); }
        @Override public void onLoadCompleted(String p, Sid sid, Duration d) { calls.add("onLoadCompleted"); }
        @Override public void onLoadFailed(String p, Sid sid, int attempt, Throwable c) { calls.add("onLoadFailed"); }
        @Override public void onQuerySent(String p, UUID id, Class<?> t, UUID parent) { calls.add("onQuerySent"); }
        @Override public void onProcessRemoved(String p) { calls.add("onProcessRemoved"); }
        @Override public void onWatcherStopped(String p, WatcherStopReason r) { calls.add("onWatcherStopped"); }
        @Override public void onQueryCompleted(String p, UUID id, Duration d) { calls.add("onQueryCompleted"); }
        @Override public void onQueryFailed(String p, UUID id, String r, Throwable c) { calls.add("onQueryFailed"); }
        @Override public void onComputeDuration(String p, Duration d) { calls.add("onComputeDuration"); }
        @Override public void onDedupCollapsed(String p, int collapsed) { calls.add("onDedupCollapsed"); }
        @Override public void onCleanupCompleted(String p, Sid s, boolean ok, Duration d) { calls.add("onCleanupCompleted"); }
        @Override public void onSidPromotion(String p, Sid prev, Sid next) { calls.add("onSidPromotion"); }
        @Override public void onReinitStarted(String p, Sid serving) { calls.add("onReinitStarted"); }
        @Override public void onReinitFailed(String p, Sid kept, Throwable c) { calls.add("onReinitFailed"); }
    }

    /** Throws on every callback. */
    static final class Exploding implements EngineObserver {
        int calls;

        private void boom() {
            calls++;
            throw new IllegalStateException("observer blew up");
        }

        @Override public void onStateTransition(String p, String from, String to) { boom(); }
        @Override public void onInitStarted(String p, int attempt) { boom(); }
        @Override public void onInitCompleted(String p, Sid sid, Duration d) { boom(); }
        @Override public void onInitFailed(String p, int attempt, Throwable c) { boom(); }
        @Override public void onLoadStarted(String p, Sid sid, int attempt) { boom(); }
        @Override public void onLoadCompleted(String p, Sid sid, Duration d) { boom(); }
        @Override public void onLoadFailed(String p, Sid sid, int attempt, Throwable c) { boom(); }
        @Override public void onQuerySent(String p, UUID id, Class<?> t, UUID parent) { boom(); }
        @Override public void onProcessRemoved(String p) { boom(); }
        @Override public void onWatcherStopped(String p, WatcherStopReason r) { boom(); }
        @Override public void onQueryCompleted(String p, UUID id, Duration d) { boom(); }
        @Override public void onQueryFailed(String p, UUID id, String r, Throwable c) { boom(); }
        @Override public void onComputeDuration(String p, Duration d) { boom(); }
        @Override public void onDedupCollapsed(String p, int collapsed) { boom(); }
        @Override public void onCleanupCompleted(String p, Sid s, boolean ok, Duration d) { boom(); }
        @Override public void onSidPromotion(String p, Sid prev, Sid next) { boom(); }
        @Override public void onReinitStarted(String p, Sid serving) { boom(); }
        @Override public void onReinitFailed(String p, Sid kept, Throwable c) { boom(); }
    }

    /** Every instance callback declared by the SPI. */
    private static List<Method> spiCallbacks() {
        List<Method> methods = new ArrayList<>();
        for (Method m : EngineObserver.class.getDeclaredMethods()) {
            if (!Modifier.isStatic(m.getModifiers()) && !m.isSynthetic()) methods.add(m);
        }
        assertThat(methods).isNotEmpty();
        return methods;
    }

    /** A harmless argument for each parameter type used by the SPI. */
    private static Object sampleArg(Class<?> type) {
        if (type == String.class) return "P";
        if (type == int.class) return 1;
        if (type == boolean.class) return true;
        if (type == long.class) return 1L;
        if (type == Duration.class) return Duration.ofMillis(1);
        if (type == UUID.class) return UUID.randomUUID();
        if (type == Sid.class) return new Sid("P", 1L);
        if (type == Throwable.class) return new RuntimeException("x");
        if (type == Class.class) return String.class;
        if (type == WatcherStopReason.class) return WatcherStopReason.PROCESS_REMOVED;
        throw new AssertionError("CompositeEngineObserverTest needs a sample value for " + type
                + " — a new EngineObserver parameter type was added");
    }

    private static Object[] sampleArgs(Method m) {
        Class<?>[] types = m.getParameterTypes();
        Object[] args = new Object[types.length];
        for (int i = 0; i < types.length; i++) args[i] = sampleArg(types[i]);
        return args;
    }

    @Test
    void forwards_every_callback_to_every_delegate_in_order() throws Exception {
        var first = new Recorder();
        var second = new Recorder();
        EngineObserver composite = EngineObserver.composite(first, second);

        List<String> expected = new ArrayList<>();
        for (Method m : spiCallbacks()) {
            m.invoke(composite, sampleArgs(m));
            expected.add(m.getName());
        }

        assertThat(first.calls).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(second.calls).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(((CompositeEngineObserver) composite).delegates())
                .containsExactly(first, second);
    }

    /**
     * Fails if a new {@link EngineObserver} method is not overridden by the
     * composite: an unforwarded default method would silently no-op.
     */
    @Test
    void every_spi_method_is_overridden_by_the_composite() {
        for (Method m : spiCallbacks()) {
            Method override;
            try {
                override = CompositeEngineObserver.class.getDeclaredMethod(m.getName(), m.getParameterTypes());
            } catch (NoSuchMethodException e) {
                throw new AssertionError("CompositeEngineObserver does not forward EngineObserver." + m.getName()
                        + " — add an override (a missing one silently drops the callback)", e);
            }
            assertThat(override.getDeclaringClass()).isEqualTo(CompositeEngineObserver.class);
        }
    }

    @Test
    void a_throwing_delegate_does_not_stop_the_others() throws Exception {
        var before = new Recorder();
        var exploding = new Exploding();
        var after = new Recorder();
        EngineObserver composite = EngineObserver.composite(before, exploding, after);

        List<String> expected = new ArrayList<>();
        for (Method m : spiCallbacks()) {
            m.invoke(composite, sampleArgs(m)); // must not propagate
            expected.add(m.getName());
        }

        assertThat(exploding.calls).isEqualTo(expected.size());
        assertThat(before.calls).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(after.calls).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void composite_of_none_is_noop_and_of_one_is_that_observer() {
        var only = new Recorder();
        assertThat(EngineObserver.composite()).isSameAs(EngineObserver.NOOP);
        assertThat(EngineObserver.composite(only)).isSameAs(only);
        assertThat(EngineObserver.composite(List.of(only, only))).isInstanceOf(CompositeEngineObserver.class);
    }

    @Test
    void a_null_delegate_is_rejected() {
        assertThatThrownBy(() -> EngineObserver.composite(new Recorder(), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> EngineObserver.composite((EngineObserver[]) null))
                .isInstanceOf(NullPointerException.class);
    }
}
