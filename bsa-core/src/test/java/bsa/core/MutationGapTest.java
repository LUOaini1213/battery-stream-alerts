package bsa.core;

import bsa.core.model.Alert;
import bsa.core.model.CycleSummary;
import bsa.core.model.Telemetry;
import bsa.core.rules.CycleAnalyzer;
import bsa.core.rules.DataQualityGate;
import bsa.core.rules.FadeTracker;
import bsa.core.rules.Thresholds;
import bsa.core.rules.WindowRules;
import bsa.core.rules.WindowStats;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests added after the first PIT run (see results/pit-summary.md): each one kills a mutant that
 * the original suite let survive. Mutants judged equivalent are listed there instead.
 */
class MutationGapTest {
    private final Thresholds th = new Thresholds();

    private static CycleSummary cycle(int n, double cap) {
        CycleSummary s = new CycleSummary();
        s.cellId = "C1";
        s.cycle = n;
        s.samples = 100;
        s.capacityAh = cap;
        return s;
    }

    @Test
    void initialCapacityNeedsAllBaselineCycles() {
        FadeTracker f = new FadeTracker();
        f.onCycle(cycle(1, 2.0), th, 0);
        f.onCycle(cycle(2, 2.0), th, 0);
        assertTrue(Double.isNaN(f.initialCapacity(th)), "two of three baseline cycles is not enough");
        CycleSummary third = cycle(3, 2.0);
        f.onCycle(third, th, 0);
        assertTrue(Double.isNaN(third.soh), "the third cycle is judged before it joins the baseline");
    }

    @Test
    void dropDetailStatesPercentageAndBaseline() {
        FadeTracker f = new FadeTracker();
        for (int n = 1; n <= 5; n++) f.onCycle(cycle(n, 2.0), th, 0);
        Alert a = f.onCycle(cycle(6, 1.9), th, 0).get(0);
        assertEquals("capacity 1.9000 Ah is 5.0% below rolling mean 2.0000 Ah of last 5 cycles", a.detail);
    }

    @Test
    void addReturnsTheAccumulator() {
        WindowStats w = new WindowStats();
        assertSame(w, w.add(Telemetry.sample("C1", 1, 0, 0, 3.7, -2, 25), th));
    }

    @Test
    void sagThresholdIsStrict() {
        WindowStats w = new WindowStats();
        for (int k = 0; k < 8; k++) {
            w.add(Telemetry.sample("C1", 1, k, k * 10_000L, 4.0 - 0.1 * k * k / 7.0, -2, 25), th);
        }
        Thresholds exact = new Thresholds();
        exact.sagVoltsPerMinute = w.slopeVoltsPerMinute(); // threshold equal to the measured slope
        assertTrue(WindowRules.evaluate("C1", 0, 120_000, w, exact, 0).isEmpty());
        exact.sagVoltsPerMinute = Math.nextUp(w.slopeVoltsPerMinute());
        assertEquals(1, WindowRules.evaluate("C1", 0, 120_000, w, exact, 0).size());
    }

    @Test
    void lateDetailAndNegativeSeqMessage() {
        Telemetry t = Telemetry.sample("C1", 1, 2, 1000, 3.7, -2, 25);
        Alert a = DataQualityGate.alertFor(DataQualityGate.Verdict.LATE, t, 4000, th, 0);
        assertEquals("arrived 3000 ms behind the watermark", a.detail);
        t.seq = -1;
        assertEquals("negative cycle/seq", DataQualityGate.rangeViolation(t, th));
    }

    @Test
    void cycleIsOrderedBySequenceNotByClock() {
        // seq 5 carries a timestamp slightly before seq 4 (clock jitter): no gap, no negative step
        List<Telemetry> xs = new ArrayList<>();
        for (int k = 0; k < 10; k++) xs.add(Telemetry.sample("C1", 1, k, 1_000_000L + k * 10_000L, 3.8, -2, 25));
        xs.get(5).eventTimeMs = xs.get(4).eventTimeMs - 1;
        CycleAnalyzer.Result r = CycleAnalyzer.analyze("C1", 1, xs, th, 0);
        assertTrue(r.gapAlerts.isEmpty(), r.gapAlerts.toString());
        assertEquals(0, r.summary.missingSamples);
    }

    @Test
    void thresholdValidationBoundaries() {
        Thresholds t = new Thresholds();
        t.sessionGapMs = t.maxSampleGapMs;
        assertThrows(IllegalArgumentException.class, t::validate);
        t.sessionGapMs = t.maxSampleGapMs + 1;
        t.validate();
        t.dedupRetentionMs = t.outOfOrdernessMs;
        t.validate();
        t.dedupRetentionMs = t.outOfOrdernessMs - 1;
        assertThrows(IllegalArgumentException.class, t::validate);
        Thresholds z = new Thresholds();
        z.outOfOrdernessMs = 0;
        z.windowMs = 1;
        z.baselineCycles = 1;
        z.rollingCycles = 1;
        z.validate();
        z.rollingCycles = 0;
        assertThrows(IllegalArgumentException.class, z::validate);
    }
}
