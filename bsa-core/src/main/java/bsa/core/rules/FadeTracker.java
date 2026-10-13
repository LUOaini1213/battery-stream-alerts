package bsa.core.rules;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.CycleSummary;
import bsa.core.model.Severity;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-cell capacity history, kept in Flink keyed state (one instance per cell).
 *
 * <ul>
 *   <li>initial capacity = mean of the first {@code baselineCycles} valid cycles;</li>
 *   <li>rolling baseline = mean of the previous {@code rollingCycles} valid cycles;</li>
 *   <li>CAPACITY_DROP if this cycle is more than {@code capacityDropFraction} below the rolling baseline;</li>
 *   <li>CAPACITY_EOL once, the first time capacity / initial falls below {@code endOfLifeSoh}.</li>
 * </ul>
 *
 * Cycles with fewer than {@code minCycleSamples} samples are reported but neither judged nor
 * allowed into the baselines, so one truncated cycle cannot poison the history.
 */
public class FadeTracker {
    /** Capacities of the first cycles, used for the initial baseline. */
    public double[] firstCapacities = new double[0];
    /** Ring buffer of the most recent capacities. */
    public double[] recent = new double[0];
    public int recentCount;
    public int recentNext;
    public boolean eolRaised;

    public FadeTracker() {
    }

    public double initialCapacity(Thresholds th) {
        if (firstCapacities.length < th.baselineCycles) return Double.NaN;
        return mean(firstCapacities, firstCapacities.length);
    }

    public double rollingBaseline(Thresholds th) {
        if (recentCount < th.rollingCycles) return Double.NaN;
        return mean(recent, recentCount);
    }

    /** Evaluates the rules for a closed cycle, fills the baseline fields of {@code s}, then updates the history. */
    public List<Alert> onCycle(CycleSummary s, Thresholds th, long nowMs) {
        List<Alert> out = new ArrayList<>(2);
        if (s.samples < th.minCycleSamples) {
            return out;
        }
        double initial = initialCapacity(th);
        double rolling = rollingBaseline(th);
        s.initialCapacityAh = initial;
        s.rollingBaselineAh = rolling;
        s.soh = Double.isNaN(initial) ? Double.NaN : s.capacityAh / initial;

        if (!Double.isNaN(rolling) && s.capacityAh < (1.0 - th.capacityDropFraction) * rolling) {
            Alert a = base(s, AlertType.CAPACITY_DROP, Severity.WARN, nowMs);
            a.value = s.capacityAh;
            a.threshold = (1.0 - th.capacityDropFraction) * rolling;
            a.detail = String.format("capacity %.4f Ah is %.1f%% below rolling mean %.4f Ah of last %d cycles",
                    s.capacityAh, 100.0 * (1.0 - s.capacityAh / rolling), rolling, th.rollingCycles);
            out.add(a);
        }
        if (!eolRaised && !Double.isNaN(s.soh) && s.soh < th.endOfLifeSoh) {
            Alert a = base(s, AlertType.CAPACITY_EOL, Severity.CRITICAL, nowMs);
            a.value = s.soh;
            a.threshold = th.endOfLifeSoh;
            a.detail = String.format("SOH %.3f < %.2f (capacity %.4f Ah, initial %.4f Ah)",
                    s.soh, th.endOfLifeSoh, s.capacityAh, initial);
            out.add(a);
            eolRaised = true;
        }
        remember(s.capacityAh, th);
        return out;
    }

    private void remember(double capacityAh, Thresholds th) {
        if (firstCapacities.length < th.baselineCycles) {
            double[] f = new double[firstCapacities.length + 1];
            System.arraycopy(firstCapacities, 0, f, 0, firstCapacities.length);
            f[firstCapacities.length] = capacityAh;
            firstCapacities = f;
        }
        if (recent.length != th.rollingCycles) {
            recent = new double[th.rollingCycles];
            recentCount = 0;
            recentNext = 0;
        }
        recent[recentNext] = capacityAh;
        recentNext = (recentNext + 1) % recent.length;
        recentCount = Math.min(recentCount + 1, recent.length);
    }

    private static double mean(double[] xs, int n) {
        double sum = 0;
        for (int i = 0; i < n; i++) sum += xs[i];
        return sum / n;
    }

    private static Alert base(CycleSummary s, AlertType type, Severity sev, long nowMs) {
        Alert a = Alert.of(s.cellId, type, sev, Integer.toString(s.cycle));
        a.cycle = s.cycle;
        a.eventTimeMs = s.endMs;
        a.windowStartMs = s.startMs;
        a.windowEndMs = s.endMs;
        a.maxIngestMs = s.maxIngestMs;
        a.emittedMs = nowMs;
        return a;
    }
}
