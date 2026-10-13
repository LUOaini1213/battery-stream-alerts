package bsa.core.rules;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

/**
 * All tunable numbers in one place. Defaults are the values used for the README results.
 *
 * <p>Where the defaults come from (see README "Thresholds"): physical-range limits are deliberately
 * wide sanity bounds, not operating limits. The over-temperature limit (40 degC) is a demo value for
 * lab data taken at 24 degC ambient; a datasheet discharge limit (typically about 60 degC) would raise
 * zero alerts on this data set. The sag rate and capacity numbers were chosen after looking at the
 * distribution of the NASA data, which is stated in the README rather than hidden.
 */
public class Thresholds implements Serializable {
    private static final long serialVersionUID = 1L;

    // --- event time ---
    /** Bounded out-of-orderness: watermark = max event time seen - this. */
    public long outOfOrdernessMs = 20_000;
    /** Tumbling window size for voltage/temperature rules. */
    public long windowMs = 120_000;
    /** A cycle's session window closes after this much event-time silence. */
    public long sessionGapMs = 300_000;
    /** How long (event time, behind the watermark) a sample key is remembered for duplicate detection. */
    public long dedupRetentionMs = 600_000;

    // --- physical plausibility (data quality) ---
    public double minVoltageV = 0.0;
    public double maxVoltageV = 5.0;
    public double maxAbsCurrentA = 10.0;
    public double minTemperatureC = -40.0;
    public double maxTemperatureC = 100.0;

    // --- window rules ---
    public double overTemperatureC = 40.0;
    /** Current below this (negative = discharge) counts as "under load". */
    public double loadCurrentA = -0.5;
    /** Least-squares dV/dt under load below this raises VOLTAGE_SAG (volts per minute). */
    public double sagVoltsPerMinute = -0.3;
    public int sagMinLoadSamples = 5;

    // --- cycle rules ---
    /** Time between two consecutive samples of a cycle above this is a gap. */
    public long maxSampleGapMs = 60_000;
    public int minCycleSamples = 20;
    public int baselineCycles = 3;
    public int rollingCycles = 5;
    /** CAPACITY_DROP when capacity < (1 - this) * rolling mean of the previous cycles. */
    public double capacityDropFraction = 0.03;
    /** CAPACITY_EOL when capacity / initial capacity falls below this. */
    public double endOfLifeSoh = 0.80;

    public Thresholds() {
    }

    /** Overrides defaults from environment-style keys, e.g. BSA_OVER_TEMPERATURE_C=45. */
    public static Thresholds fromEnv(Map<String, String> env) {
        Thresholds t = new Thresholds();
        t.outOfOrdernessMs = get(env, "BSA_OUT_OF_ORDERNESS_MS", Long::parseLong, t.outOfOrdernessMs);
        t.windowMs = get(env, "BSA_WINDOW_MS", Long::parseLong, t.windowMs);
        t.sessionGapMs = get(env, "BSA_SESSION_GAP_MS", Long::parseLong, t.sessionGapMs);
        t.dedupRetentionMs = get(env, "BSA_DEDUP_RETENTION_MS", Long::parseLong, t.dedupRetentionMs);
        t.overTemperatureC = get(env, "BSA_OVER_TEMPERATURE_C", Double::parseDouble, t.overTemperatureC);
        t.sagVoltsPerMinute = get(env, "BSA_SAG_V_PER_MIN", Double::parseDouble, t.sagVoltsPerMinute);
        t.capacityDropFraction = get(env, "BSA_CAPACITY_DROP_FRACTION", Double::parseDouble, t.capacityDropFraction);
        t.endOfLifeSoh = get(env, "BSA_EOL_SOH", Double::parseDouble, t.endOfLifeSoh);
        t.validate();
        return t;
    }

    private static <T> T get(Map<String, String> env, String key, Function<String, T> parse, T dflt) {
        String v = env.get(key);
        return v == null || v.isBlank() ? dflt : parse.apply(v.trim());
    }

    public void validate() {
        if (outOfOrdernessMs < 0) throw new IllegalArgumentException("outOfOrdernessMs must be >= 0");
        if (windowMs <= 0) throw new IllegalArgumentException("windowMs must be > 0");
        if (sessionGapMs <= maxSampleGapMs) {
            throw new IllegalArgumentException("sessionGapMs must exceed maxSampleGapMs, otherwise a gap splits a cycle");
        }
        if (dedupRetentionMs < outOfOrdernessMs) {
            throw new IllegalArgumentException("dedupRetentionMs must be >= outOfOrdernessMs");
        }
        if (baselineCycles < 1 || rollingCycles < 1) throw new IllegalArgumentException("cycle counts must be >= 1");
    }
}
