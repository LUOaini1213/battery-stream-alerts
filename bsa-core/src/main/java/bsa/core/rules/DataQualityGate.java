package bsa.core.rules;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.Severity;
import bsa.core.model.Telemetry;

/**
 * Decides, per incoming sample, whether it may enter the windows.
 *
 * <p>Order of checks matters and is fixed: out-of-range first (the values are unusable no matter
 * when they arrive), then late (once the watermark has passed, the windows that would need the
 * sample may already have fired, so it is dropped even if it would also be a duplicate), then
 * duplicate.
 */
public final class DataQualityGate {

    public enum Verdict { ACCEPT, OUT_OF_RANGE, LATE, DUPLICATE }

    private DataQualityGate() {
    }

    /**
     * @param currentWatermark the operator's current watermark (Long.MIN_VALUE before the first one)
     * @param seenBefore       whether (cell, cycle, seq) is already in the de-duplication state
     */
    public static Verdict classify(Telemetry t, long currentWatermark, boolean seenBefore, Thresholds th) {
        if (rangeViolation(t, th) != null) {
            return Verdict.OUT_OF_RANGE;
        }
        // Flink watermark contract: watermark W promises no more elements with timestamp <= W.
        if (t.eventTimeMs <= currentWatermark) {
            return Verdict.LATE;
        }
        if (seenBefore) {
            return Verdict.DUPLICATE;
        }
        return Verdict.ACCEPT;
    }

    /** Returns a human-readable reason if any value is NaN or outside the physical sanity range, else null. */
    public static String rangeViolation(Telemetry t, Thresholds th) {
        if (Double.isNaN(t.voltageV) || t.voltageV < th.minVoltageV || t.voltageV > th.maxVoltageV) {
            return "voltage " + t.voltageV + " V outside [" + th.minVoltageV + ", " + th.maxVoltageV + "]";
        }
        if (Double.isNaN(t.currentA) || Math.abs(t.currentA) > th.maxAbsCurrentA) {
            return "current " + t.currentA + " A outside +/-" + th.maxAbsCurrentA;
        }
        if (Double.isNaN(t.temperatureC) || t.temperatureC < th.minTemperatureC || t.temperatureC > th.maxTemperatureC) {
            return "temperature " + t.temperatureC + " C outside [" + th.minTemperatureC + ", " + th.maxTemperatureC + "]";
        }
        if (t.seq < 0 || t.cycle < 0) {
            return "negative cycle/seq";
        }
        return null;
    }

    /** Builds the data-quality alert for a rejected sample. Throws for ACCEPT. */
    public static Alert alertFor(Verdict v, Telemetry t, long currentWatermark, Thresholds th, long nowMs) {
        AlertType type;
        String detail;
        double value;
        double threshold;
        switch (v) {
            case OUT_OF_RANGE:
                type = AlertType.DQ_OUT_OF_RANGE;
                detail = rangeViolation(t, th);
                value = t.voltageV;
                threshold = th.maxVoltageV;
                break;
            case LATE:
                type = AlertType.DQ_LATE;
                value = (double) (currentWatermark - t.eventTimeMs);
                threshold = 0;
                detail = "arrived " + (currentWatermark - t.eventTimeMs) + " ms behind the watermark";
                break;
            case DUPLICATE:
                type = AlertType.DQ_DUPLICATE;
                value = 1;
                threshold = 0;
                detail = "sample " + t.sampleKey() + " received again";
                break;
            default:
                throw new IllegalArgumentException("no alert for " + v);
        }
        Alert a = Alert.of(t.cellId, type, Severity.INFO, t.sampleKey());
        a.cycle = t.cycle;
        a.eventTimeMs = t.eventTimeMs;
        a.windowStartMs = t.eventTimeMs;
        a.windowEndMs = t.eventTimeMs;
        a.value = value;
        a.threshold = threshold;
        a.detail = detail;
        a.maxIngestMs = t.ingestMs;
        a.emittedMs = nowMs;
        return a;
    }
}
