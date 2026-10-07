package io.fom;

import java.time.Duration;

import static org.awaitility.Awaitility.await;

/** Waits for re-inits running in the background (KEEP_OLD) to finish. */
final class Settle {

    private Settle() {
    }

    /** Every node serves, none is replacing or due to replace its version — and still so a moment later. */
    static void awaitNoReinit(Engine e) {
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(50)).until(() -> quiet(e)
                && pause(200) && quiet(e));
    }

    private static boolean quiet(Engine e) throws Exception {
        return e.introspect().toCompletableFuture().get().graph().nodes().stream()
                .allMatch(n -> n.state().equals("Serving") && n.replacement() == null && !n.stale());
    }

    private static boolean pause(long millis) throws InterruptedException {
        Thread.sleep(millis);
        return true;
    }
}
