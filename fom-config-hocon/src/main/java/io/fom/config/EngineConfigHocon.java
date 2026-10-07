package io.fom.config;

import com.cronutils.model.time.ExecutionTime;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import io.fom.EngineConfig;
import io.fom.ReinitStrategy;
import io.fom.SnapshotPolicy;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.regex.Matcher;

/**
 * Parses {@link EngineConfig} from a Typesafe {@link Config}, reading
 * {@code engine.graph.default.*} and {@code engine.system.*}.
 *
 * <p>{@code engine.system.log.rotate.cron} (Quartz, 6 fields) becomes a
 * {@link CronSnapshotPolicy}; {@code never} or blank disables rotation. The optional
 * {@code log.rotate.keep-history} is a number {@code >= 1}, or {@code all} (the default) for
 * {@link SnapshotPolicy#KEEP_ALL}.</p>
 *
 * <p>Both namespaces are strict: an unknown key under them, or a scalar where an object
 * belongs, is a {@link ConfigException} naming the full key. Keys outside them are not
 * checked.</p>
 */
public final class EngineConfigHocon {

    private EngineConfigHocon() {
    }

    /** A node of the owned-key schema; a leaf has no children. */
    private record Keys(SortedMap<String, Keys> children) {
        static final Keys LEAF = new Keys(new TreeMap<>());

        /** Alternating {@code name, child} arguments. */
        static Keys of(Object... nameChild) {
            var node = new TreeMap<String, Keys>();
            for (int i = 0; i < nameChild.length; i += 2) {
                node.put((String) nameChild[i], (Keys) nameChild[i + 1]);
            }
            return new Keys(Collections.unmodifiableSortedMap(node));
        }
    }

    private static final String DEFAULTS = "engine.graph.default.";
    private static final String SYSTEM = "engine.system.";
    private static final String INIT_TIMEOUT = DEFAULTS + "init.timeout";
    private static final String LOAD_TIMEOUT = DEFAULTS + "load.timeout";
    private static final String CLEANUP_TIMEOUT = DEFAULTS + "cleanup.timeout";
    private static final String MIN_BACKOFF = DEFAULTS + "init.min-backoff";
    private static final String MAX_BACKOFF = DEFAULTS + "init.max-backoff";
    private static final String MAX_LOAD_RETRIES = DEFAULTS + "load.max-retries";
    private static final String REINIT_STRATEGY = DEFAULTS + "reinit.strategy";
    private static final String REINIT_RETRY_MIN = DEFAULTS + "reinit.retry.min-backoff";
    private static final String REINIT_RETRY_MAX = DEFAULTS + "reinit.retry.max-backoff";
    private static final String QUERY_TIMEOUT = SYSTEM + "query-timeout";
    private static final String DEDUP_WINDOW = SYSTEM + "dedup-window";
    private static final String ROTATE = SYSTEM + "log.rotate";
    private static final String ROTATE_CRON = ROTATE + ".cron";
    private static final String ROTATE_KEEP_HISTORY = ROTATE + ".keep-history";

    /** Every key this parser reads; anything else under these objects is a misspelling. */
    private static final Map<String, Keys> OWNED_NAMESPACES = new TreeMap<>(Map.of(
            "engine.graph.default", Keys.of(
                    "init", Keys.of("timeout", Keys.LEAF, "min-backoff", Keys.LEAF, "max-backoff", Keys.LEAF),
                    "load", Keys.of("timeout", Keys.LEAF, "max-retries", Keys.LEAF),
                    "cleanup", Keys.of("timeout", Keys.LEAF),
                    "reinit", Keys.of("strategy", Keys.LEAF,
                            "retry", Keys.of("min-backoff", Keys.LEAF, "max-backoff", Keys.LEAF))),
            "engine.system", Keys.of(
                    "query-timeout", Keys.LEAF,
                    "dedup-window", Keys.LEAF,
                    "log", Keys.of("rotate", Keys.of("cron", Keys.LEAF, "keep-history", Keys.LEAF)))));

    /** {@link EngineConfig} component name → the HOCON key it is read from. */
    private static final Map<String, String> KEY_OF_COMPONENT = Map.ofEntries(
            Map.entry("initTimeout", INIT_TIMEOUT),
            Map.entry("loadTimeout", LOAD_TIMEOUT),
            Map.entry("cleanupTimeout", CLEANUP_TIMEOUT),
            Map.entry("queryTimeout", QUERY_TIMEOUT),
            Map.entry("dedupWindow", DEDUP_WINDOW),
            Map.entry("backoffMin", MIN_BACKOFF),
            Map.entry("backoffMax", MAX_BACKOFF),
            Map.entry("maxLoadRetries", MAX_LOAD_RETRIES),
            Map.entry("reinitRetryBackoffMin", REINIT_RETRY_MIN),
            Map.entry("reinitRetryBackoffMax", REINIT_RETRY_MAX));

    /**
     * Reads an {@link EngineConfig}. Every error starts with the full HOCON key
     * ({@code "engine.graph.default.init.min-backoff: must be > 0, was PT0S"}).
     *
     * @throws ConfigException for a malformed or mistyped value, or an unknown key
     * @throws IllegalArgumentException for a well-formed but invalid value
     */
    public static EngineConfig parse(Config raw) {
        Objects.requireNonNull(raw, "raw");
        rejectUnknownKeys(raw);
        Config cfg = raw.withFallback(defaults());

        Duration initTimeout = positiveDuration(cfg, INIT_TIMEOUT);
        Duration loadTimeout = positiveDuration(cfg, LOAD_TIMEOUT);
        Duration cleanupTimeout = positiveDuration(cfg, CLEANUP_TIMEOUT);
        Duration queryTimeout = positiveDuration(cfg, QUERY_TIMEOUT);
        Duration dedupWindow = positiveDuration(cfg, DEDUP_WINDOW);
        Duration minBackoff = positiveDuration(cfg, MIN_BACKOFF);
        Duration maxBackoff = positiveDuration(cfg, MAX_BACKOFF);
        if (maxBackoff.compareTo(minBackoff) < 0) {
            throw new IllegalArgumentException(MAX_BACKOFF + ": must be >= " + MIN_BACKOFF
                    + " (" + minBackoff + "), was " + maxBackoff);
        }
        int maxLoadRetries = read(cfg, MAX_LOAD_RETRIES, Config::getInt);
        if (maxLoadRetries < 1) {
            throw new IllegalArgumentException(MAX_LOAD_RETRIES + ": must be >= 1, was " + maxLoadRetries);
        }
        SnapshotPolicy snapshot = parseSnapshotPolicy(cfg);
        ReinitStrategy strategy = parseReinitStrategy(cfg);
        Duration reinitRetryMin = cfg.hasPath(REINIT_RETRY_MIN) ? nonNegativeDuration(cfg, REINIT_RETRY_MIN) : null;
        Duration reinitRetryMax = cfg.hasPath(REINIT_RETRY_MAX) ? positiveDuration(cfg, REINIT_RETRY_MAX) : null;
        if (reinitRetryMin != null && reinitRetryMax != null && reinitRetryMax.compareTo(reinitRetryMin) < 0) {
            throw new IllegalArgumentException(REINIT_RETRY_MAX + ": must be >= " + REINIT_RETRY_MIN
                    + " (" + reinitRetryMin + "), was " + reinitRetryMax);
        }

        try {
            return new EngineConfig(initTimeout, loadTimeout, cleanupTimeout, queryTimeout, dedupWindow,
                    minBackoff, maxBackoff, maxLoadRetries, snapshot, reinitRetryMin, reinitRetryMax, strategy);
        } catch (IllegalArgumentException e) {
            // A rule only EngineConfig checks; still name the HOCON keys.
            throw new IllegalArgumentException(inHoconKeys(e.getMessage()), e);
        }
    }

    /** {@link #parse(Config)} of {@code ConfigFactory.load()}. */
    public static EngineConfig parse() {
        return parse(ConfigFactory.load());
    }

    /** {@code keep-old} (the default) or {@code release-first}. */
    private static ReinitStrategy parseReinitStrategy(Config cfg) {
        String written = read(cfg, REINIT_STRATEGY, Config::getString);
        return switch (written.trim().toLowerCase(Locale.ROOT)) {
            case "keep-old" -> ReinitStrategy.KEEP_OLD;
            case "release-first" -> ReinitStrategy.RELEASE_FIRST;
            default -> throw new IllegalArgumentException(REINIT_STRATEGY
                    + ": must be keep-old or release-first, was " + written);
        };
    }

    private static Duration nonNegativeDuration(Config cfg, String key) {
        Duration d = read(cfg, key, Config::getDuration);
        if (d.isNegative()) {
            throw new IllegalArgumentException(key + ": must be >= 0, was " + d);
        }
        return d;
    }

    private static SnapshotPolicy parseSnapshotPolicy(Config cfg) {
        if (!read(cfg, ROTATE, Config::hasPath)) return SnapshotPolicy.Disabled.INSTANCE;
        // Only an explicit cron = never turns rotation off; a scalar or a missing cron is an error.
        ConfigValue rotate = cfg.getValue(ROTATE);
        String where = " (" + rotate.origin().description() + ")";
        if (rotate.valueType() != ConfigValueType.OBJECT) {
            throw new ConfigException.Generic(ROTATE + ": must be an object"
                    + " { cron = \"<quartz cron>\" | never, keep-history = <n> | all }, was "
                    + typeName(rotate)
                    + " " + rotate.render() + where);
        }
        if (!cfg.hasPath(ROTATE_CRON)) {
            throw new ConfigException.Generic(ROTATE_CRON + ": missing — " + ROTATE
                    + " is set, so a cron is required; use cron = never to disable rotation" + where);
        }
        String cron = read(cfg, ROTATE_CRON, Config::getString);
        if (cron.equalsIgnoreCase("never") || cron.isBlank()) {
            return SnapshotPolicy.Disabled.INSTANCE;
        }
        int keep = keepHistory(cfg);
        try {
            return new CronSnapshotPolicy(cron, keep);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(ROTATE_CRON + ": " + e.getMessage(), e);
        }
    }

    /** {@code keep-history}: absent or {@code all} keeps every archive; otherwise an int {@code >= 1}. */
    private static int keepHistory(Config cfg) {
        if (!cfg.hasPath(ROTATE_KEEP_HISTORY)) return SnapshotPolicy.KEEP_ALL;
        ConfigValue v = cfg.getValue(ROTATE_KEEP_HISTORY);
        if (v.valueType() == ConfigValueType.STRING && "all".equalsIgnoreCase(String.valueOf(v.unwrapped()).trim())) {
            return SnapshotPolicy.KEEP_ALL;
        }
        int keep = read(cfg, ROTATE_KEEP_HISTORY, Config::getInt);
        if (keep < 1) {
            throw new IllegalArgumentException(ROTATE_KEEP_HISTORY + ": must be >= 1 (or all, the default), was "
                    + keep);
        }
        return keep;
    }

    /**
     * Rejects unknown keys under the owned namespaces, and a scalar where one of their objects
     * (or a parent such as {@code engine.graph}) belongs — reported against that key itself.
     * Only the user's config is checked, not the built-in defaults.
     */
    private static void rejectUnknownKeys(Config raw) {
        for (var ns : OWNED_NAMESPACES.entrySet()) {
            ConfigValue value = raw.root();
            String path = null;
            for (String segment : ns.getKey().split("\\.")) {
                path = path == null ? segment : path + "." + segment;
                value = ((ConfigObject) value).get(segment);
                if (isAbsent(value)) break; // defaults apply
                requireObject(path, value);
            }
            if (!isAbsent(value)) checkKeys(ns.getKey(), value, ns.getValue());
        }
    }

    private static boolean isAbsent(ConfigValue value) {
        return value == null || value.valueType() == ConfigValueType.NULL;
    }

    private static void requireObject(String path, ConfigValue value) {
        if (value.valueType() != ConfigValueType.OBJECT) {
            throw new ConfigException.Generic(path + ": must be an object, was "
                    + typeName(value) + " " + value.render() + " (" + value.origin().description() + ")");
        }
    }

    private static String typeName(ConfigValue value) {
        return value.valueType().name().toLowerCase(Locale.ROOT);
    }

    private static void checkKeys(String path, ConfigValue value, Keys schema) {
        if (schema.children().isEmpty()) return;
        // A scalar log.rotate gets its own message in parseSnapshotPolicy.
        if (path.equals(ROTATE) && value.valueType() != ConfigValueType.OBJECT) return;
        requireObject(path, value);
        for (var entry : ((ConfigObject) value).entrySet()) {
            String key = entry.getKey();
            String full = path + "." + (isSimple(key) ? key : ConfigUtil.quoteString(key));
            Keys child = schema.children().get(key);
            if (child == null) {
                throw new ConfigException.Generic(full + ": unknown key '" + key + "' in " + path
                        + " (allowed: " + String.join(", ", schema.children().keySet()) + ")"
                        + " (" + entry.getValue().origin().description() + ")");
            }
            checkKeys(full, entry.getValue(), child);
        }
    }

    private static boolean isSimple(String key) {
        return !key.isEmpty() && key.chars().allMatch(c -> Character.isLetterOrDigit(c) || c == '-' || c == '_');
    }

    private static Duration positiveDuration(Config cfg, String key) {
        Duration d = read(cfg, key, Config::getDuration);
        if (d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(key + ": must be > 0, was " + d);
        }
        return d;
    }

    /** Reads {@code key}, re-throwing a failure with the full key in front of the message. */
    private static <T> T read(Config cfg, String key, BiFunction<Config, String, T> getter) {
        try {
            return getter.apply(cfg, key);
        } catch (ConfigException e) {
            throw new ConfigException.Generic(key + ": " + e.getMessage(), e);
        }
    }

    private static String inHoconKeys(String message) {
        String result = message == null ? "invalid engine configuration" : message;
        for (var entry : KEY_OF_COMPONENT.entrySet()) {
            result = result.replaceAll("\\b" + entry.getKey() + "\\b",
                    Matcher.quoteReplacement(entry.getValue()));
        }
        return result;
    }

    /**
     * Time from now until the next execution of a Quartz cron. A one-off measurement, not a
     * repeat period: use {@link CronSnapshotPolicy} for cron-driven scheduling.
     */
    public static Duration nextFireInterval(String quartzCron) {
        Objects.requireNonNull(quartzCron, "quartzCron");
        ExecutionTime exec = ExecutionTime.forCron(CronSnapshotPolicy.QUARTZ.parse(quartzCron));
        ZonedDateTime now = ZonedDateTime.now();
        // cron-utils keeps the input's sub-second part (12:00:00.300 -> 12:00:01.300).
        return exec.nextExecution(now.truncatedTo(ChronoUnit.SECONDS))
                .map(at -> Duration.between(now, at))
                .orElseThrow(() -> new IllegalArgumentException("Cron has no future execution: " + quartzCron));
    }

    private static Config defaults() {
        return ConfigFactory.parseString("""
                engine {
                  graph.default {
                    init {
                      timeout = 30s
                      min-backoff = 50ms
                      max-backoff = 5m
                    }
                    load {
                      timeout = 30s
                      max-retries = 1
                    }
                    cleanup.timeout = 30s
                    reinit.strategy = keep-old
                  }
                  system {
                    query-timeout = 10s
                    dedup-window = 100ms
                  }
                }
                """);
    }
}
