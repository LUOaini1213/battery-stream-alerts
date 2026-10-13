package bsa.core.rules;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.CycleSummary;
import bsa.core.model.Severity;
import bsa.core.model.Telemetry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Turns all accepted samples of one (cell, cycle) into a {@link CycleSummary} and gap alerts.
 *
 * <p>Runs when the cycle's session window closes, so it sees the whole cycle at once and can sort
 * it; arrival order does not matter.
 */
public final class CycleAnalyzer {

    public static final class Result {
        public final CycleSummary summary;
        public final List<Alert> gapAlerts;

        Result(CycleSummary summary, List<Alert> gapAlerts) {
            this.summary = summary;
            this.gapAlerts = gapAlerts;
        }
    }

    private CycleAnalyzer() {
    }

    public static Result analyze(String cellId, int cycle, Iterable<Telemetry> samples, Thresholds th, long nowMs) {
        List<Telemetry> list = new ArrayList<>();
        for (Telemetry t : samples) list.add(t);
        if (list.isEmpty()) throw new IllegalArgumentException("empty cycle");
        list.sort(Comparator.comparingInt((Telemetry t) -> t.seq).thenComparingLong(t -> t.eventTimeMs));

        CycleSummary s = new CycleSummary();
        s.cellId = cellId;
        s.cycle = cycle;
        s.startMs = Long.MAX_VALUE;
        s.endMs = Long.MIN_VALUE;
        s.minVoltageV = Double.POSITIVE_INFINITY;
        s.maxTemperatureC = Double.NEGATIVE_INFINITY;
        List<Alert> gaps = new ArrayList<>();

        Telemetry prev = null;
        double capacityAh = 0;
        for (Telemetry t : list) {
            if (prev != null && t.seq == prev.seq) {
                continue; // defensive: duplicates are normally removed upstream
            }
            s.samples++;
            s.startMs = Math.min(s.startMs, t.eventTimeMs);
            s.endMs = Math.max(s.endMs, t.eventTimeMs);
            s.minVoltageV = Math.min(s.minVoltageV, t.voltageV);
            s.maxTemperatureC = Math.max(s.maxTemperatureC, t.temperatureC);
            s.maxIngestMs = Math.max(s.maxIngestMs, t.ingestMs);

            if (prev == null) {
                if (t.seq > 0) {
                    s.missingSamples += t.seq;
                    gaps.add(gapAlert(cellId, cycle, -1, t, t.seq, 0, th, nowMs));
                }
            } else {
                long dt = t.eventTimeMs - prev.eventTimeMs;
                int missing = t.seq - prev.seq - 1;
                if (missing > 0 || dt > th.maxSampleGapMs) {
                    s.missingSamples += Math.max(0, missing);
                    gaps.add(gapAlert(cellId, cycle, prev.seq, t, missing, dt, th, nowMs));
                }
                // trapezoid rule on discharge current (positive while discharging)
                double hours = dt / 3_600_000.0;
                capacityAh += 0.5 * (discharge(prev) + discharge(t)) * hours;
            }
            prev = t;
        }
        s.capacityAh = capacityAh;
        return new Result(s, gaps);
    }

    static double discharge(Telemetry t) {
        return Math.max(0.0, -t.currentA);
    }

    private static Alert gapAlert(String cellId, int cycle, int afterSeq, Telemetry next,
                                  int missing, long dtMs, Thresholds th, long nowMs) {
        Alert a = Alert.of(cellId, AlertType.DQ_GAP, Severity.INFO, cycle + ":" + afterSeq);
        a.cycle = cycle;
        a.eventTimeMs = next.eventTimeMs;
        a.windowStartMs = next.eventTimeMs - Math.max(0, dtMs);
        a.windowEndMs = next.eventTimeMs;
        a.value = missing;
        a.threshold = th.maxSampleGapMs;
        a.detail = afterSeq < 0
                ? "cycle starts at seq " + next.seq + " (" + missing + " leading samples missing)"
                : missing + " samples missing after seq " + afterSeq + ", time step " + dtMs + " ms";
        a.maxIngestMs = next.ingestMs;
        a.emittedMs = nowMs;
        return a;
    }
}
