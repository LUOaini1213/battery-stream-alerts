package bsa.core;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.Telemetry;
import bsa.core.rules.CycleAnalyzer;
import bsa.core.rules.Thresholds;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CycleAnalyzerTest {
    private final Thresholds th = new Thresholds();

    /** n samples, 10 s apart, constant current. */
    private static List<Telemetry> constant(int n, double currentA) {
        List<Telemetry> out = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            Telemetry t = Telemetry.sample("C1", 4, k, 1_000_000L + k * 10_000L, 4.0 - 0.001 * k, currentA, 25 + 0.01 * k);
            t.ingestMs = 10 + k;
            out.add(t);
        }
        return out;
    }

    @Test
    void constantTwoAmpsForOneHourIsTwoAh() {
        CycleAnalyzer.Result r = CycleAnalyzer.analyze("C1", 4, constant(361, -2.0), th, 7);
        assertEquals(2.0, r.summary.capacityAh, 1e-9);
        assertEquals(361, r.summary.samples);
        assertEquals(0, r.summary.missingSamples);
        assertTrue(r.gapAlerts.isEmpty());
        assertEquals(1_000_000L, r.summary.startMs);
        assertEquals(1_000_000L + 360 * 10_000L, r.summary.endMs);
        assertEquals(4.0 - 0.36, r.summary.minVoltageV, 1e-9);
        assertEquals(25 + 3.6, r.summary.maxTemperatureC, 1e-9);
        assertEquals(10 + 360, r.summary.maxIngestMs);
        assertEquals("C1", r.summary.cellId);
        assertEquals(4, r.summary.cycle);
    }

    @Test
    void trapezoidRuleOnRamp() {
        List<Telemetry> xs = constant(2, 0);
        xs.get(0).currentA = 0.0;
        xs.get(1).currentA = -3.6; // 0 -> 3.6 A over 10 s: area = 0.5*3.6*10/3600 = 0.005 Ah
        assertEquals(0.005, CycleAnalyzer.analyze("C1", 4, xs, th, 0).summary.capacityAh, 1e-12);
    }

    @Test
    void chargingCurrentDoesNotCount() {
        assertEquals(0.0, CycleAnalyzer.analyze("C1", 4, constant(50, +1.5), th, 0).summary.capacityAh);
    }

    @Test
    void arrivalOrderDoesNotMatter() {
        List<Telemetry> xs = constant(100, -2.0);
        double sorted = CycleAnalyzer.analyze("C1", 4, xs, th, 0).summary.capacityAh;
        Collections.shuffle(xs, new java.util.Random(1));
        assertEquals(sorted, CycleAnalyzer.analyze("C1", 4, xs, th, 0).summary.capacityAh, 1e-12);
    }

    @Test
    void sequenceHoleIsOneGapAlertCountingMissingSamples() {
        List<Telemetry> xs = constant(30, -2.0);
        xs.remove(10);
        xs.remove(10); // seq 10 and 11 gone
        CycleAnalyzer.Result r = CycleAnalyzer.analyze("C1", 4, xs, th, 0);
        assertEquals(1, r.gapAlerts.size());
        Alert a = r.gapAlerts.get(0);
        assertEquals(AlertType.DQ_GAP, a.alertType);
        assertEquals(2.0, a.value);
        assertEquals(2, r.summary.missingSamples);
        assertEquals(28, r.summary.samples);
        assertEquals(Alert.idFor("C1", AlertType.DQ_GAP, "4:9"), a.alertId);
        assertEquals(xs.get(10).eventTimeMs, a.eventTimeMs); // the first sample after the hole (seq 12)
        assertEquals(12, xs.get(10).seq);
        assertEquals(30_000, a.windowEndMs - a.windowStartMs);
        // capacity is still interpolated across the hole
        assertEquals(2.0 * 290 / 3600.0, r.summary.capacityAh, 1e-9);
    }

    @Test
    void timeGapWithoutSequenceHole() {
        List<Telemetry> xs = constant(10, -2.0);
        for (int k = 5; k < 10; k++) xs.get(k).eventTimeMs += th.maxSampleGapMs; // step 5: 10 s + 60 s
        CycleAnalyzer.Result r = CycleAnalyzer.analyze("C1", 4, xs, th, 0);
        assertEquals(1, r.gapAlerts.size());
        assertEquals(0.0, r.gapAlerts.get(0).value);
        assertEquals(0, r.summary.missingSamples);
    }

    @Test
    void stepExactlyAtMaxGapIsFine() {
        List<Telemetry> xs = constant(3, -2.0);
        xs.get(1).eventTimeMs = xs.get(0).eventTimeMs + th.maxSampleGapMs;
        xs.get(2).eventTimeMs = xs.get(1).eventTimeMs + th.maxSampleGapMs;
        assertTrue(CycleAnalyzer.analyze("C1", 4, xs, th, 0).gapAlerts.isEmpty());
    }

    @Test
    void leadingHoleIsDetected() {
        List<Telemetry> xs = constant(10, -2.0).subList(3, 10);
        CycleAnalyzer.Result r = CycleAnalyzer.analyze("C1", 4, xs, th, 0);
        assertEquals(1, r.gapAlerts.size());
        assertEquals(3.0, r.gapAlerts.get(0).value);
        assertEquals(3, r.summary.missingSamples);
        assertEquals(Alert.idFor("C1", AlertType.DQ_GAP, "4:-1"), r.gapAlerts.get(0).alertId);
    }

    @Test
    void repeatedSequenceNumberIsIgnored() {
        List<Telemetry> xs = constant(10, -2.0);
        xs.add(xs.get(4).copy());
        CycleAnalyzer.Result r = CycleAnalyzer.analyze("C1", 4, xs, th, 0);
        assertEquals(10, r.summary.samples);
        assertEquals(2.0 * 90 / 3600.0, r.summary.capacityAh, 1e-12);
    }

    @Test
    void emptyCycleIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> CycleAnalyzer.analyze("C1", 1, List.of(), th, 0));
    }
}
