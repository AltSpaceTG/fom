package io.fom.fsm;

import io.fom.Sid;
import io.fom.api.ProcessContext;

import java.util.Objects;
import java.util.concurrent.Executor;

/** The {@link ProcessContext} handed to {@code Process.cleanUp()}. */
final class ProcessContextImpl implements ProcessContext {

    private final Sid sid;
    private final Executor executor;

    ProcessContextImpl(Sid sid, Executor executor) {
        this.sid = Objects.requireNonNull(sid, "sid");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public Sid sid() {
        return sid;
    }

    @Override
    public Executor executor() {
        return executor;
    }
}
