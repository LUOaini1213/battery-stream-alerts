package bsa.core;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.CycleSummary;
import bsa.core.model.Severity;
import bsa.core.rules.FadeTracker;
import bsa.core.rules.Thresholds;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FadeTrackerTest {
    private final Thresholds th = new Thresholds(); // baseline 3, rolling 5, drop 3 %, EOL 0.8

    private static CycleSummary cycle(int n, double cap) {
        CycleSummary s = new CycleSummary();
        s.cellId = "C1";
        s.cycle = n;
        s.samples = 100;
        s.capacityAh = cap;
        s.startMs = n * 1000L;
        s.endMs = n * 1000L + 500;
        s.maxIngestMs = n;
        return s;
    }

    @Test
    void baselinesAppearAfterEnoughCycles() {
        FadeTracker f = new FadeTracker();
        CycleSummary c1 = cycle(1, 2.0);
        f.onCycle(c1, th, 0);
        assertTrue(Double.isNaN(c1.initialCapacityAh));
        assertTrue(Double.isNaN(c1.soh));
        f.onCycle(cycle(2, 1.9), th, 0);
        f.onCycle(cycle(3, 1.8), th, 0);
        assertEquals(1.9, f.initialCapacity(th), 1e-12);
        assertTrue(Double.isNaN(f.rollingBaseline(th)));
        CycleSummary c4 = cycle(4, 1.9);
        f.onCycle(c4, th, 0);
        assertEquals(1.9, c4.initialCapacityAh, 1e-12);
        assertEquals(1.0, c4.soh, 1e-12);
        assertTrue(Double.isNaN(c4.rollingBaselineAh));
        f.onCycle(cycle(5, 1.9), th, 0);
        assertEquals((2.0 + 1.9 + 1.8 + 1.9 + 1.9) / 5, f.rollingBaseline(th), 1e-12);
    }

    @Test
    void rollingBaselineSlidesOverLastCycles() {
        FadeTracker f = new FadeTracker();
        for (int n = 1; n <= 8; n++) f.onCycle(cycle(n, n), th, 0); // capacities 1..8
        assertEquals((4 + 5 + 6 + 7 + 8) / 5.0, f.rollingBaseline(th), 1e-12);
        assertEquals(2.0, f.initialCapacity(th), 1e-12);
    }

    @Test
    void dropAlertBoundary() {
        FadeTracker f = new FadeTracker();
        for (int n = 1; n <= 5; n++) f.onCycle(cycle(n, 2.0), th, 0);
        double limit = (1 - th.capacityDropFraction) * 2.0;
        assertTrue(f.onCycle(cycle(6, limit), th, 0).isEmpty(), "exactly at the limit is not a drop");

        FadeTracker g = new FadeTracker();
        for (int n = 1; n <= 5; n++) g.onCycle(cycle(n, 2.0), th, 0);
        List<Alert> a = g.onCycle(cycle(6, limit - 1e-6), th, 77);
        assertEquals(1, a.size());
        Alert d = a.get(0);
        assertEquals(AlertType.CAPACITY_DROP, d.alertType);
        assertEquals(Severity.WARN, d.severity);
        assertEquals(limit, d.threshold, 1e-12);
        assertEquals(limit - 1e-6, d.value, 1e-12);
        assertEquals(6, d.cycle);
        assertEquals(6500, d.eventTimeMs);
        assertEquals(6000, d.windowStartMs);
        assertEquals(6, d.maxIngestMs);
        assertEquals(77, d.emittedMs);
        assertEquals(Alert.idFor("C1", AlertType.CAPACITY_DROP, "6"), d.alertId);
    }

    @Test
    void endOfLifeRaisedOnceAtFirstCrossing() {
        FadeTracker f = new FadeTracker();
        for (int n = 1; n <= 3; n++) f.onCycle(cycle(n, 2.0), th, 0);
        // 0.80 * 2.0 = 1.6 exactly: not below
        assertTrue(f.onCycle(cycle(4, 1.6), th, 0).stream().noneMatch(a -> a.alertType == AlertType.CAPACITY_EOL));
        List<Alert> a = f.onCycle(cycle(5, 1.59), th, 0);
        Alert eol = a.stream().filter(x -> x.alertType == AlertType.CAPACITY_EOL).findFirst().orElseThrow();
        assertEquals(Severity.CRITICAL, eol.severity);
        assertEquals(1.59 / 2.0, eol.value, 1e-12);
        assertEquals(th.endOfLifeSoh, eol.threshold);
        assertTrue(f.eolRaised);
        assertTrue(f.onCycle(cycle(6, 1.5), th, 0).stream().noneMatch(x -> x.alertType == AlertType.CAPACITY_EOL));
    }

    @Test
    void shortCyclesAreNeitherJudgedNorRemembered() {
        FadeTracker f = new FadeTracker();
        for (int n = 1; n <= 5; n++) f.onCycle(cycle(n, 2.0), th, 0);
        CycleSummary tiny = cycle(6, 0.1);
        tiny.samples = th.minCycleSamples - 1;
        assertTrue(f.onCycle(tiny, th, 0).isEmpty());
        assertTrue(Double.isNaN(tiny.soh));
        assertEquals(2.0, f.rollingBaseline(th), 1e-12);
        CycleSummary justEnough = cycle(7, 2.0);
        justEnough.samples = th.minCycleSamples;
        f.onCycle(justEnough, th, 0);
        assertEquals(1.0, justEnough.soh, 1e-12);
    }

    @Test
    void ringBufferResizesWhenConfigChanges() {
        FadeTracker f = new FadeTracker();
        for (int n = 1; n <= 6; n++) f.onCycle(cycle(n, 2.0), th, 0);
        Thresholds t3 = new Thresholds();
        t3.rollingCycles = 3;
        f.onCycle(cycle(7, 1.0), t3, 0);
        assertTrue(Double.isNaN(f.rollingBaseline(t3)), "history restarts after a resize");
        f.onCycle(cycle(8, 1.0), t3, 0);
        f.onCycle(cycle(9, 1.0), t3, 0);
        assertEquals(1.0, f.rollingBaseline(t3), 1e-12);
    }
}
