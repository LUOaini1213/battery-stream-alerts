package bsa.core;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.Telemetry;
import bsa.core.rules.Thresholds;
import bsa.core.rules.WindowRules;
import bsa.core.rules.WindowStats;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WindowStatsAndRulesTest {
    private final Thresholds th = new Thresholds(); // window 120 s, sag -0.3 V/min, over-temp 40 C

    private static final long W0 = 1_200_000L; // multiple of 120 000

    private static Telemetry s(long offsetMs, double v, double i, double temp) {
        Telemetry t = Telemetry.sample("C1", 1, (int) (offsetMs / 1000), W0 + offsetMs, v, i, temp);
        t.ingestMs = 500 + offsetMs;
        return t;
    }

    private WindowStats linear(double voltsPerMinute, int n, double temp) {
        WindowStats w = new WindowStats();
        for (int k = 0; k < n; k++) {
            long off = k * 10_000L;
            w.add(s(off, 4.0 + voltsPerMinute * off / 60_000.0, -2.0, temp), th);
        }
        return w;
    }

    @Test
    void windowStartIsFloorEvenForNegativeTimes() {
        assertEquals(240_000, WindowStats.windowStart(250_000, 120_000));
        assertEquals(240_000, WindowStats.windowStart(240_000, 120_000));
        assertEquals(-120_000, WindowStats.windowStart(-1, 120_000));
    }

    @Test
    void slopeOfExactLineIsRecovered() {
        WindowStats w = linear(-0.5, 12, 25);
        assertEquals(-0.5, w.slopeVoltsPerMinute(), 1e-9);
        assertEquals(12, w.count);
        assertEquals(12, w.loadCount);
        assertEquals(W0, w.anchorMs);
    }

    @Test
    void restingSamplesStayOutOfRegressionButCountForExtremes() {
        WindowStats w = linear(-0.5, 6, 25);
        w.add(s(65_000, 1.0, 0.0, 44.0), th);       // resting: not under load
        w.add(s(66_000, 1.0, th.loadCurrentA, 20.0), th); // exactly at the load limit: not under load
        assertEquals(8, w.count);
        assertEquals(6, w.loadCount);
        assertEquals(-0.5, w.slopeVoltsPerMinute(), 1e-9);
        assertEquals(44.0, w.maxTemperatureC);
        assertEquals(1.0, w.minVoltageV);
        assertEquals(500 + 66_000, w.maxIngestMs);
    }

    @Test
    void slopeUndefinedWithTooFewOrCoincidentPoints() {
        assertTrue(Double.isNaN(new WindowStats().slopeVoltsPerMinute()));
        assertTrue(Double.isNaN(linear(-1, 1, 25).slopeVoltsPerMinute()));
        WindowStats same = new WindowStats();
        same.add(s(1000, 3.9, -2, 25), th);
        same.add(s(1000, 3.8, -2, 25), th);
        assertTrue(Double.isNaN(same.slopeVoltsPerMinute()));
        assertEquals(-1.0, linear(-1, 2, 25).slopeVoltsPerMinute(), 1e-9);
    }

    @Test
    void mergeEqualsSequentialAdd() {
        WindowStats a = new WindowStats();
        WindowStats b = new WindowStats();
        WindowStats all = new WindowStats();
        for (int k = 0; k < 10; k++) {
            Telemetry t = s(k * 9_000L, 4.0 - 0.01 * k * k, -2.0, 25 + k);
            (k % 2 == 0 ? a : b).add(t, th);
            all.add(t, th);
        }
        WindowStats m = a.merge(b);
        assertEquals(all.count, m.count);
        assertEquals(all.loadCount, m.loadCount);
        assertEquals(all.slopeVoltsPerMinute(), m.slopeVoltsPerMinute(), 1e-9);
        assertEquals(all.maxTemperatureC, m.maxTemperatureC);
        assertEquals(all.minVoltageV, m.minVoltageV);
        assertEquals(all.maxIngestMs, m.maxIngestMs);
        assertSame(a, a.merge(new WindowStats()));
        WindowStats empty = new WindowStats();
        assertSame(b, empty.merge(b));
    }

    @Test
    void cannotMixWindows() {
        WindowStats w = linear(-0.1, 2, 25);
        assertThrows(IllegalStateException.class, () -> w.add(s(th.windowMs, 3.0, -2, 25), th));
        WindowStats other = new WindowStats();
        other.add(s(th.windowMs, 3.0, -2, 25), th);
        assertThrows(IllegalStateException.class, () -> w.merge(other));
    }

    @Test
    void overTemperatureIsStrictlyAbove() {
        assertTrue(WindowRules.evaluate("C1", W0, W0 + th.windowMs, linear(0, 6, th.overTemperatureC), th, 0).isEmpty());
        List<Alert> a = WindowRules.evaluate("C1", W0, W0 + th.windowMs, linear(0, 6, th.overTemperatureC + 0.01), th, 42);
        assertEquals(1, a.size());
        Alert t = a.get(0);
        assertEquals(AlertType.OVER_TEMPERATURE, t.alertType);
        assertEquals(th.overTemperatureC + 0.01, t.value, 1e-12);
        assertEquals(th.overTemperatureC, t.threshold);
        assertEquals(W0, t.windowStartMs);
        assertEquals(W0 + th.windowMs, t.windowEndMs);
        assertEquals(W0 + th.windowMs - 1, t.eventTimeMs);
        assertEquals(42, t.emittedMs);
        assertEquals(500 + 50_000, t.maxIngestMs);
    }

    @Test
    void sagNeedsSteepEnoughSlopeAndEnoughLoadedSamples() {
        long we = W0 + th.windowMs;
        assertTrue(WindowRules.evaluate("C1", W0, we, linear(-0.3, 12, 25), th, 0).isEmpty(), "slope equal to threshold");
        assertTrue(WindowRules.evaluate("C1", W0, we, linear(-0.31, th.sagMinLoadSamples - 1, 25), th, 0).isEmpty());
        List<Alert> a = WindowRules.evaluate("C1", W0, we, linear(-0.31, th.sagMinLoadSamples, 25), th, 0);
        assertEquals(1, a.size());
        assertEquals(AlertType.VOLTAGE_SAG, a.get(0).alertType);
        assertEquals(-0.31, a.get(0).value, 1e-9);
        assertEquals(th.sagVoltsPerMinute, a.get(0).threshold);
        assertTrue(WindowRules.evaluate("C1", W0, we, linear(0.5, 12, 25), th, 0).isEmpty(), "rising voltage");
    }

    @Test
    void bothRulesCanFireAndIdsAreDeterministicPerWindow() {
        long we = W0 + th.windowMs;
        List<Alert> a = WindowRules.evaluate("C1", W0, we, linear(-1.0, 12, 41), th, 0);
        assertEquals(2, a.size());
        List<Alert> again = WindowRules.evaluate("C1", W0, we, linear(-1.0, 12, 41), th, 999);
        assertEquals(a.get(0).alertId, again.get(0).alertId);
        assertEquals(a.get(1).alertId, again.get(1).alertId);
        assertNotEquals(a.get(0).alertId, a.get(1).alertId);
        assertEquals(Alert.idFor("C1", AlertType.VOLTAGE_SAG, Long.toString(W0)), a.get(1).alertId);
        assertTrue(WindowRules.evaluate("C1", W0, we, new WindowStats(), th, 0).isEmpty());
    }
}
