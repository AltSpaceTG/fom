package io.fom.micrometer;

import io.fom.Sid;
import io.fom.api.AttemptCancelledException;
import io.fom.api.EngineObserver;
import io.fom.api.LeadershipLostException;
import io.fom.api.WatcherStopReason;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.MeterFilter;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Records fom runtime metrics into a {@link MeterRegistry}.
 *
 * <p>Queries: {@code engine_query_duration_seconds{name}} (send to reply),
 * {@code engine_query_failures_total{name, reason}} with {@code reason} one of {@code timeout},
 * {@code rejected}, {@code init-in-progress}, {@code cancelled} or {@code exception} (see
 * {@link EngineObserver#onQueryFailed}), and {@code engine_query_cancellations_total{name}} — the
 * failures whose reason is {@code cancelled} or whose cause is a {@link CancellationException}.</p>
 *
 * <p>Lifecycle, kept apart from query failures so query alerts don't fire on init/load trouble:
 * {@code engine_process_init_failures_total}, {@code engine_process_load_failures_total},
 * {@code engine_process_cleanup_failures_total}, {@code engine_dedup_collapsed_total},
 * {@code engine_process_leadership_lost_total} and
 * {@code engine_process_init_cancellations_total} / {@code engine_process_load_cancellations_total},
 * all tagged {@code name}. An attempt that failed with a {@link LeadershipLostException} in its
 * cause chain (a fenced or deposed node) counts as a leadership loss, not a failure. An attempt
 * the engine cut short itself (pause, removal, graph swap, close, {@code cancelInit}) arrives with
 * an {@link AttemptCancelledException} and counts as a cancellation, not a failure. A plain
 * {@link CancellationException} from the user's own init or load is an ordinary failure.</p>
 *
 * <p>Re-init with the old version serving ({@code ReinitStrategy.KEEP_OLD}):
 * {@code engine_process_reinit_failures_total{name}} counts re-inits that gave up, gauge
 * {@code engine_process_stale{name}} is {@code 1} from such a failure until the next Sid promotion
 * (the old version is still answering), and timer {@code engine_process_reinit_duration_seconds{name}}
 * runs from the re-init's start to its promotion; a re-init that fails records no time.</p>
 *
 * <p>These counters and the dead and stale gauges are registered at {@code 0} when a process is first seen,
 * so PromQL's {@code increase()} catches a one-off failure. Query meters and timers are
 * registered on their first recording.</p>
 *
 * <p>Gauge {@code engine_process_dead{name}} is {@code 1} while the process is {@code Dead}
 * because it failed (init out of budget, load gave up, leadership lost, undeclared dependency)
 * and {@code 0} otherwise, including when it was stopped on purpose. It resets to {@code 0} on
 * the engine's {@code "Paused"} transition and when the name starts again, and is dropped on
 * removal. After {@code Engine.close()} it keeps its last value until a new observer on the same
 * registry and common tags reports the process. Failure counters stop moving once a process is
 * dead, so alert on this gauge rather than on their rate alone.</p>
 *
 * <p>Timers, tagged {@code name}: {@code engine_process_init_duration_seconds},
 * {@code engine_process_load_duration_seconds}, {@code engine_process_cleanup_duration_seconds},
 * {@code engine_process_compute_duration_seconds}, {@code engine_process_reinit_duration_seconds},
 * {@code engine_query_duration_seconds}.
 * {@code engine_watcher_stops_total{reason}} counts watchers that gave up.</p>
 *
 * <p><b>One observer per engine.</b> The observer remembers removed process names and ignores
 * their late callbacks, so sharing one between engines would silence one engine's process when
 * the other removes a process of the same name. Engines sharing a registry need distinguishing
 * common tags, e.g. {@code new MicrometerEngineObserver(registry, Tags.of("engine", "stations"))};
 * otherwise their meters for the same process name merge. {@link #onProcessRemoved} only removes
 * meters this instance registered.</p>
 *
 * <p><b>Cardinality.</b> Each process name costs 10 meters once seen, 16 with all timers, and up to
 * 22 with the query failure and cancellation counters. Fold tenant-shaped names with a
 * {@link MeterFilter} configured before the engine starts, e.g.
 * {@code MeterFilter.replaceTagValues("name", n -> n.replaceFirst("^tenant-[^/]+/", "tenant-ANY/"))}.
 * A folded meter is removed only when the last name using it is removed, and a folded dead or stale
 * gauge reads {@code 1} if any of its processes is dead or stale.</p>
 *
 * <p>Timers are recorded off the caller's thread, so no tracing context is current and registries
 * that attach exemplars attach none. There is no mailbox-size gauge; use
 * {@code Engine.introspect()} for full per-process state.</p>
 */
public final class MicrometerEngineObserver implements EngineObserver {

    /** The 0/1 gauges kept per process. */
    private enum Flag {
        DEAD("engine_process_dead"),
        STALE("engine_process_stale");

        final String gauge;

        Flag(String gauge) {
            this.gauge = gauge;
        }

        boolean of(Health h) {
            return this == DEAD ? h.dead : h.stale;
        }
    }

    /** Registered at 0 when a process is first seen. */
    private static final List<String> PRIMED_COUNTERS = List.of(
            "engine_process_init_failures_total",
            "engine_process_init_cancellations_total",
            "engine_process_load_failures_total",
            "engine_process_load_cancellations_total",
            "engine_process_cleanup_failures_total",
            "engine_process_leadership_lost_total",
            "engine_process_reinit_failures_total",
            "engine_dedup_collapsed_total");

    /** How long a removed name is remembered: far longer than any callback can trail its removal. */
    static final Duration REMOVED_RETENTION = Duration.ofMinutes(15);

    /** Hard cap on remembered removed names; the oldest go first. */
    static final int REMOVED_CAP = 10_000;

    private final MeterRegistry registry;
    private final Tags commonTags;

    /** Meters this instance registered, by process name, so removal never drops another observer's. */
    private final ConcurrentHashMap<String, Set<Meter.Id>> owned = new ConcurrentHashMap<>();

    /**
     * Process names using each meter id. A {@link MeterFilter} may fold several names into one id;
     * the meter goes only when the last of them is removed.
     */
    private final ConcurrentHashMap<Meter.Id, Set<String>> users = new ConcurrentHashMap<>();

    /**
     * Recently removed names → {@code nanoTime} of removal, so callbacks already under way don't
     * re-create their meters. Bounded by {@link #retentionNanos} and {@link #cap}.
     */
    private final ConcurrentHashMap<String, Long> removed = new ConcurrentHashMap<>();

    /** Removal order for eviction; guarded by its own monitor. */
    private final ArrayDeque<Removal> removalOrder = new ArrayDeque<>();

    private record Removal(String name, long at) { }

    private final ConcurrentHashMap<String, Health> health = new ConcurrentHashMap<>();

    /** The post-filter id of the gauge each name contributes to. */
    private final ConcurrentHashMap<FlagKey, Meter.Id> gaugeIds = new ConcurrentHashMap<>();

    /** {@code nanoClock} reading at each running re-init's start. */
    private final ConcurrentHashMap<String, Long> reinitStarts = new ConcurrentHashMap<>();

    private final long retentionNanos;
    private final int cap;
    private final LongSupplier nanoClock;

    /** Written on the process's dispatcher thread, read by the registry's scrape. */
    private static final class Health {
        volatile boolean dead;
        /** The last init/load attempt was cancelled by the engine rather than failing. */
        volatile boolean lastAttemptCancelled;
        /** A re-init gave up and the old version still serves. */
        volatile boolean stale;
    }

    /**
     * What a dead or stale gauge reads, shared per registry and post-filter id. Micrometer binds a
     * gauge to the state object of its first registration, so a gauge bound to one observer's state
     * would freeze once a new engine reuses the registry. Each observer instead puts its
     * {@link Health} here under its {@link FlagKey}: a later observer with the same tags replaces the
     * earlier one, while names folded by a filter each contribute.
     */
    private static final class FlagCell {
        final Flag flag;
        final ConcurrentHashMap<FlagKey, Health> contributors = new ConcurrentHashMap<>();
        /** Bumped on every read, so {@link #boundTo} can tell which cell a gauge reads. */
        volatile long evaluations;

        FlagCell(Flag flag) {
            this.flag = flag;
        }

        double value() {
            evaluations++;
            for (Health h : contributors.values()) {
                if (flag.of(h)) return 1.0;
            }
            return 0.0;
        }
    }

    /** Pre-filter identity of one process's dead or stale gauge. */
    private record FlagKey(Flag flag, Tags commonTags, String processName) { }

    private static final class RegistryCells {
        /** Post-filter gauge id → cell. */
        final Map<Meter.Id, FlagCell> cells = new HashMap<>();
        /** Pre-filter identity → the post-filter id its gauge got. */
        final Map<FlagKey, Meter.Id> ids = new HashMap<>();
    }

    private record BoundGauge(Gauge gauge, FlagCell cell) { }

    /** Weak in the registry, so a discarded registry takes its cells along. Guarded by its own monitor. */
    private static final Map<MeterRegistry, RegistryCells> FLAG_CELLS = new WeakHashMap<>();

    /** {@code MeterRegistry.getMappedId(Meter.Id)}, or {@code null} if this Micrometer does not allow it. */
    private static final MethodHandle MAPPED_ID = mappedIdHandle();

    public MicrometerEngineObserver(MeterRegistry registry) {
        this(registry, Tags.empty());
    }

    /**
     * An observer that adds {@code commonTags} to every meter it registers, typically one tag
     * naming the engine, so several engines can share {@code registry}.
     *
     * @throws IllegalArgumentException if a common tag uses the key {@code name} or {@code reason}
     */
    public MicrometerEngineObserver(MeterRegistry registry, Iterable<Tag> commonTags) {
        this(registry, commonTags, REMOVED_RETENTION, REMOVED_CAP, System::nanoTime);
    }

    /** For tests: a custom retention, cap and clock for the removed-name memory. */
    MicrometerEngineObserver(MeterRegistry registry, Duration retention, int cap, LongSupplier nanoClock) {
        this(registry, Tags.empty(), retention, cap, nanoClock);
    }

    MicrometerEngineObserver(MeterRegistry registry, Iterable<Tag> commonTags,
                             Duration retention, int cap, LongSupplier nanoClock) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.commonTags = Tags.of(Objects.requireNonNull(commonTags, "commonTags"));
        for (Tag tag : this.commonTags) {
            if ("name".equals(tag.getKey()) || "reason".equals(tag.getKey())) {
                throw new IllegalArgumentException(
                        "common tag key '" + tag.getKey() + "' is reserved by MicrometerEngineObserver");
            }
        }
        this.retentionNanos = Objects.requireNonNull(retention, "retention").toNanos();
        if (cap < 1) throw new IllegalArgumentException("cap must be positive: " + cap);
        this.cap = cap;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        // Not per process, so primed once, up front.
        for (WatcherStopReason reason : WatcherStopReason.values()) {
            watcherStops(reason);
        }
    }

    /** Number of removed names currently remembered (for tests). */
    int rememberedRemovals() {
        return removed.size();
    }

    @Override
    public void onInitStarted(String processName, int attempt) {
        attemptStarted(processName);
    }

    @Override
    public void onInitCompleted(String processName, Sid newSid, Duration duration) {
        if (isRemoved(processName)) return;
        timer("engine_process_init_duration_seconds", processName).record(duration);
    }

    @Override
    public void onInitFailed(String processName, int attempt, Throwable cause) {
        attemptFailed(processName, cause,
                "engine_process_init_cancellations_total", "engine_process_init_failures_total");
    }

    @Override
    public void onLoadStarted(String processName, Sid sid, int attempt) {
        attemptStarted(processName);
    }

    @Override
    public void onLoadCompleted(String processName, Sid sid, Duration duration) {
        if (isRemoved(processName)) return;
        timer("engine_process_load_duration_seconds", processName).record(duration);
    }

    @Override
    public void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) {
        attemptFailed(processName, cause,
                "engine_process_load_cancellations_total", "engine_process_load_failures_total");
    }

    /** A process with that name is starting (possibly added back): record its meters again. */
    private void attemptStarted(String processName) {
        removed.remove(processName);
        primeCounters(processName);
        health(processName).lastAttemptCancelled = false;
    }

    private void attemptFailed(String processName, Throwable cause, String cancelledCounter, String failedCounter) {
        if (isRemoved(processName)) return;
        boolean cancelled = causedBy(cause, AttemptCancelledException.class);
        noteAttemptEnd(processName, cancelled);
        String counter = cancelled ? cancelledCounter
                : causedBy(cause, LeadershipLostException.class) ? "engine_process_leadership_lost_total"
                : failedCounter;
        counter(counter, processName).increment();
    }

    /**
     * Drives the dead gauge. The engine reports an attempt it cut short before the state moves on,
     * so when {@code Dead} follows, the last attempt tells a stop from a failure. {@code Dead} via
     * {@code CleaningUp} is an orderly shutdown and never counts.
     */
    @Override
    public void onStateTransition(String processName, String fromState, String toState) {
        if ("NotPresent".equals(fromState)) {
            removed.remove(processName); // a fresh FSM: the name is live again
            primeCounters(processName);
        } else if (isRemoved(processName)) {
            return;
        }
        Health h = health(processName);
        // A re-init that never reached its promotion; a later one starts its own clock.
        if ("Dead".equals(toState) || "CleaningUp".equals(toState)) reinitStarts.remove(processName);
        if ("Dead".equals(toState)) {
            h.dead = !"CleaningUp".equals(fromState) && !h.lastAttemptCancelled;
        } else {
            h.dead = false;
            if ("Serving".equals(toState)) h.lastAttemptCancelled = false;
        }
    }

    @Override
    public void onReinitStarted(String processName, Sid servingSid) {
        if (isRemoved(processName)) return;
        reinitStarts.put(processName, nanoClock.getAsLong());
    }

    @Override
    public void onReinitFailed(String processName, Sid keptSid, Throwable cause) {
        if (isRemoved(processName)) return;
        reinitStarts.remove(processName);
        health(processName).stale = true;
        counter("engine_process_reinit_failures_total", processName).increment();
    }

    @Override
    public void onSidPromotion(String processName, Sid previousSid, Sid newSid) {
        if (isRemoved(processName)) return;
        Health h = health.get(processName);
        if (h != null) h.stale = false;
        Long started = reinitStarts.remove(processName);
        if (started != null) {
            timer("engine_process_reinit_duration_seconds", processName)
                    .record(Duration.ofNanos(nanoClock.getAsLong() - started));
        }
    }

    @Override
    public void onQueryCompleted(String processName, UUID queryId, Duration duration) {
        if (isRemoved(processName)) return;
        timer("engine_query_duration_seconds", processName).record(duration);
    }

    @Override
    public void onQueryFailed(String processName, UUID queryId, String reason, Throwable cause) {
        if (isRemoved(processName)) return;
        String r = reason == null ? "unknown" : reason;
        counter("engine_query_failures_total", processName, "reason", r).increment();
        if ("cancelled".equals(r) || cause instanceof CancellationException) {
            counter("engine_query_cancellations_total", processName).increment();
        }
    }

    @Override
    public void onComputeDuration(String processName, Duration duration) {
        if (isRemoved(processName)) return;
        timer("engine_process_compute_duration_seconds", processName).record(duration);
    }

    @Override
    public void onDedupCollapsed(String processName, int collapsedCount) {
        if (isRemoved(processName)) return;
        counter("engine_dedup_collapsed_total", processName).increment(collapsedCount);
    }

    @Override
    public void onCleanupCompleted(String processName, Sid sid, boolean ok, Duration duration) {
        if (isRemoved(processName)) return;
        timer("engine_process_cleanup_duration_seconds", processName).record(duration);
        if (!ok) {
            counter("engine_process_cleanup_failures_total", processName).increment();
        }
    }

    /**
     * Counted by reason only. The commonest reason is that the process was removed, and a
     * per-name tag would bring back the very meter {@link #onProcessRemoved} just dropped.
     */
    @Override
    public void onWatcherStopped(String processName, WatcherStopReason reason) {
        watcherStops(reason).increment();
    }

    /**
     * Drops the meters this observer registered for the process and remembers the name for a
     * while, so late callbacks don't bring them back.
     */
    @Override
    public void onProcessRemoved(String processName) {
        rememberRemoval(processName);
        reinitStarts.remove(processName);
        Health h = health.remove(processName);
        if (h != null) releaseGauges(processName, h);
        Set<Meter.Id> ids = owned.remove(processName);
        if (ids != null) {
            for (Meter.Id id : List.copyOf(ids)) {
                release(processName, id);
            }
        }
    }

    /** A late callback for a process that has left the graph. */
    private boolean isRemoved(String processName) {
        return removed.containsKey(processName);
    }

    /** Removes the meter once no other name of this observer uses {@code id}. */
    private void release(String processName, Meter.Id id) {
        users.computeIfPresent(id, (k, names) -> {
            names.remove(processName);
            if (!names.isEmpty()) return names;
            registry.remove(id);
            return null;
        });
    }

    private void rememberRemoval(String processName) {
        long now = nanoClock.getAsLong();
        synchronized (removalOrder) {
            removed.put(processName, now);
            removalOrder.addLast(new Removal(processName, now));
            Removal oldest;
            while ((oldest = removalOrder.peekFirst()) != null
                    && (removalOrder.size() > cap || now - oldest.at() > retentionNanos)) {
                removalOrder.pollFirst();
                // Only if it is still this removal: the name may have been removed again since.
                removed.remove(oldest.name(), oldest.at());
            }
        }
    }

    /** Whether {@code type} is anywhere in the cause chain (bounded, in case of a cycle). */
    static boolean causedBy(Throwable cause, Class<? extends Throwable> type) {
        int depth = 0;
        for (Throwable c = cause; c != null && depth < 32; c = c.getCause(), depth++) {
            if (type.isInstance(c)) return true;
            if (c.getCause() == c) return false;
        }
        return false;
    }

    private void noteAttemptEnd(String processName, boolean cancelled) {
        Health h = health.get(processName);
        if (h != null) h.lastAttemptCancelled = cancelled;
        else if (cancelled) health(processName).lastAttemptCancelled = true;
    }

    /** This name's health, registering its dead and stale gauges on first use. */
    private Health health(String processName) {
        Health existing = health.get(processName);
        if (existing != null) return existing;
        Health created = new Health();
        Health raced = health.putIfAbsent(processName, created);
        if (raced != null) return raced;
        for (Flag flag : Flag.values()) {
            registerGauge(flag, processName, created);
        }
        if (isRemoved(processName) && health.remove(processName, created)) {
            releaseGauges(processName, created); // removed meanwhile, as in dropIfRemovedMeanwhile
        }
        return created;
    }

    /**
     * Joins this name's {@code flag} gauge, or registers it, and makes {@code h} its contribution.
     * Looking up first avoids Micrometer's "Gauge has been already registered" WARN on every
     * re-created engine.
     */
    private void registerGauge(Flag flag, String processName, Health h) {
        FlagKey key = new FlagKey(flag, commonTags, processName);
        synchronized (FLAG_CELLS) {
            RegistryCells rc = FLAG_CELLS.computeIfAbsent(registry, r -> new RegistryCells());
            BoundGauge bound = existingGauge(rc, key);
            if (bound == null) bound = newGauge(rc, flag, processName);
            Meter.Id id = bound.gauge().getId();
            bound.cell().contributors.put(key, h);
            rc.ids.put(key, id);
            gaugeIds.put(key, id);
        }
    }

    private BoundGauge existingGauge(RegistryCells rc, FlagKey key) {
        Flag flag = key.flag();
        String processName = key.processName();
        // The id an earlier observer with these tags got (registry-level filters may change it).
        Meter.Id known = rc.ids.get(key);
        if (known != null) {
            FlagCell c = rc.cells.get(known);
            Gauge g = findGauge(flag.gauge, known.getTags());
            if (c != null && g != null && g.getId().equals(known) && boundTo(g, c)) return new BoundGauge(g, c);
        }
        // An unfiltered gauge with exactly these tags.
        Gauge g = findGauge(flag.gauge, Tags.concat(commonTags, "name", processName));
        FlagCell c = g == null ? null : rc.cells.get(g.getId());
        if (c != null && boundTo(g, c)) return new BoundGauge(g, c);
        // The gauge a filter folds this name into, registered for another name.
        Meter.Id mapped = mappedId(flag, processName);
        c = mapped == null ? null : rc.cells.get(mapped);
        g = c == null ? null : findGauge(mapped.getName(), mapped.getTags());
        if (g != null && boundTo(g, c)) return new BoundGauge(g, c);
        return null;
    }

    private BoundGauge newGauge(RegistryCells rc, Flag flag, String processName) {
        FlagCell fresh = new FlagCell(flag);
        // The gauge holds the cell weakly; FLAG_CELLS keeps it alive.
        Gauge gauge = Gauge.builder(flag.gauge, fresh, FlagCell::value)
                .tags(commonTags).tag("name", processName).register(registry);
        FlagCell cell = rc.cells.get(gauge.getId());
        if (cell == null || boundTo(gauge, fresh)) {
            // A new gauge; a cell still in the map belonged to one removed behind our back.
            cell = fresh;
            rc.cells.put(gauge.getId(), fresh);
        }
        return new BoundGauge(gauge, cell);
    }

    /** The registered gauge named {@code name} whose tags are exactly {@code tags}, if any. */
    private Gauge findGauge(String name, Iterable<Tag> tags) {
        Tags wanted = Tags.of(tags);
        for (Gauge g : registry.find(name).tags(wanted).gauges()) {
            if (Tags.of(g.getId().getTags()).equals(wanted)) return g;
        }
        return null;
    }

    /**
     * The id this name's {@code flag} gauge gets after the registry's filters, or {@code null} if that
     * cannot be computed. Micrometer has no public API for it; without it a folded gauge is still
     * joined, at the cost of one "already registered" WARN.
     */
    private Meter.Id mappedId(Flag flag, String processName) {
        if (MAPPED_ID == null) return null;
        Meter.Id raw = new Meter.Id(flag.gauge, Tags.concat(commonTags, "name", processName),
                null, null, Meter.Type.GAUGE);
        try {
            return (Meter.Id) MAPPED_ID.invoke(registry, raw);
        } catch (Throwable e) {
            return null;
        }
    }

    private static MethodHandle mappedIdHandle() {
        try {
            Method m = MeterRegistry.class.getDeclaredMethod("getMappedId", Meter.Id.class);
            m.setAccessible(true); // micrometer.core is an automatic module, so open
            return MethodHandles.lookup().unreflect(m);
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    /** Whether {@code gauge} reads {@code cell}: reading it bumps the cell's counter only then. */
    private static boolean boundTo(Gauge gauge, FlagCell cell) {
        long before = cell.evaluations;
        gauge.value();
        return cell.evaluations != before;
    }

    /** Withdraws {@code h} from this name's gauges; each goes once nothing contributes to it. */
    private void releaseGauges(String processName, Health h) {
        for (Flag flag : Flag.values()) {
            FlagKey key = new FlagKey(flag, commonTags, processName);
            Meter.Id id = gaugeIds.remove(key);
            if (id == null) continue;
            synchronized (FLAG_CELLS) {
                RegistryCells rc = FLAG_CELLS.get(registry);
                FlagCell cell = rc == null ? null : rc.cells.get(id);
                if (cell == null) continue;
                if (cell.contributors.remove(key, h)) rc.ids.remove(key, id);
                if (cell.contributors.isEmpty()) {
                    rc.cells.remove(id);
                    registry.remove(id);
                }
            }
        }
    }

    /** Registers the lifecycle counters at 0 if new; idempotent. */
    private void primeCounters(String processName) {
        for (String name : PRIMED_COUNTERS) {
            counter(name, processName);
        }
    }

    private Counter watcherStops(WatcherStopReason reason) {
        return Counter.builder("engine_watcher_stops_total")
                .tags(commonTags)
                .tag("reason", reason.name().toLowerCase(Locale.ROOT))
                .register(registry);
    }

    private Timer timer(String name, String processName) {
        return track(processName, Timer.builder(name).tags(commonTags).tag("name", processName).register(registry));
    }

    private Counter counter(String name, String processName) {
        return track(processName, Counter.builder(name).tags(commonTags).tag("name", processName).register(registry));
    }

    private Counter counter(String name, String processName, String tagKey, String tagValue) {
        return track(processName,
                Counter.builder(name).tags(commonTags).tag("name", processName).tag(tagKey, tagValue).register(registry));
    }

    private <M extends Meter> M track(String processName, M meter) {
        Meter.Id id = meter.getId();
        if (owned.computeIfAbsent(processName, k -> ConcurrentHashMap.newKeySet()).add(id)) {
            users.compute(id, (k, names) -> {
                Set<String> s = names != null ? names : ConcurrentHashMap.newKeySet();
                s.add(processName);
                return s;
            });
        }
        return dropIfRemovedMeanwhile(processName, meter);
    }

    /**
     * A callback's {@link #isRemoved} check can race {@link #onProcessRemoved} and register a
     * meter after the removal dropped them; re-checking here drops it again. The caller still
     * gets the meter and records into a detached one.
     */
    private <M extends Meter> M dropIfRemovedMeanwhile(String processName, M meter) {
        if (isRemoved(processName)) {
            owned.computeIfPresent(processName, (k, ids) -> {
                ids.remove(meter.getId());
                return ids.isEmpty() ? null : ids;
            });
            release(processName, meter.getId());
        }
        return meter;
    }
}
