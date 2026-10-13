package bsa.core;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.Severity;
import bsa.core.model.Telemetry;
import bsa.core.rules.DataQualityGate;
import bsa.core.rules.DataQualityGate.Verdict;
import bsa.core.rules.Thresholds;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataQualityGateTest {
    private final Thresholds th = new Thresholds();

    private static Telemetry ok(long ts) {
        return Telemetry.sample("C1", 3, 7, ts, 3.7, -2.0, 30.0);
    }

    @Test
    void acceptsNormalSampleAheadOfWatermark() {
        assertEquals(Verdict.ACCEPT, DataQualityGate.classify(ok(1000), 999, false, th));
        assertEquals(Verdict.ACCEPT, DataQualityGate.classify(ok(1000), Long.MIN_VALUE, false, th));
    }

    @Test
    void sampleAtWatermarkIsLate() {
        // watermark W promises nothing with timestamp <= W will follow
        assertEquals(Verdict.LATE, DataQualityGate.classify(ok(1000), 1000, false, th));
        assertEquals(Verdict.LATE, DataQualityGate.classify(ok(1000), 5000, false, th));
    }

    @Test
    void duplicateDetectedOnlyWhenSeenBefore() {
        assertEquals(Verdict.DUPLICATE, DataQualityGate.classify(ok(1000), 0, true, th));
    }

    @Test
    void precedenceIsRangeThenLateThenDuplicate() {
        Telemetry bad = ok(1000);
        bad.voltageV = 9.0;
        assertEquals(Verdict.OUT_OF_RANGE, DataQualityGate.classify(bad, 5000, true, th));
        assertEquals(Verdict.LATE, DataQualityGate.classify(ok(1000), 5000, true, th));
    }

    @Test
    void rangeLimitsAreInclusive() {
        Telemetry t = ok(1);
        t.voltageV = th.maxVoltageV;
        t.currentA = -th.maxAbsCurrentA;
        t.temperatureC = th.maxTemperatureC;
        assertNull(DataQualityGate.rangeViolation(t, th));
        t.voltageV = th.minVoltageV;
        t.currentA = th.maxAbsCurrentA;
        t.temperatureC = th.minTemperatureC;
        assertNull(DataQualityGate.rangeViolation(t, th));
    }

    @Test
    void eachFieldOutOfRangeIsReported() {
        double eps = 1e-6;
        Telemetry t;
        t = ok(1); t.voltageV = th.maxVoltageV + eps; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.voltageV = th.minVoltageV - eps; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.voltageV = Double.NaN; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.currentA = th.maxAbsCurrentA + eps; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.currentA = -th.maxAbsCurrentA - eps; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.currentA = Double.NaN; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.temperatureC = th.maxTemperatureC + eps; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.temperatureC = th.minTemperatureC - eps; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.temperatureC = Double.NaN; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.seq = -1; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.cycle = -1; assertNotNull(DataQualityGate.rangeViolation(t, th));
        t = ok(1); t.seq = 0; t.cycle = 0; assertNull(DataQualityGate.rangeViolation(t, th));
    }

    @Test
    void rangeMessagesNameTheField() {
        Telemetry t = ok(1); t.currentA = 50;
        assertTrue(DataQualityGate.rangeViolation(t, th).startsWith("current"));
        t = ok(1); t.temperatureC = 500;
        assertTrue(DataQualityGate.rangeViolation(t, th).startsWith("temperature"));
        t = ok(1); t.voltageV = -1;
        assertTrue(DataQualityGate.rangeViolation(t, th).startsWith("voltage"));
    }

    @Test
    void lateAlertCarriesHowFarBehind() {
        Telemetry t = ok(1000);
        t.ingestMs = 77;
        Alert a = DataQualityGate.alertFor(Verdict.LATE, t, 4000, th, 99);
        assertEquals(AlertType.DQ_LATE, a.alertType);
        assertEquals(Severity.INFO, a.severity);
        assertEquals(3000.0, a.value);
        assertEquals("C1", a.cellId);
        assertEquals(3, a.cycle);
        assertEquals(1000, a.eventTimeMs);
        assertEquals(77, a.maxIngestMs);
        assertEquals(99, a.emittedMs);
        assertEquals(Alert.idFor("C1", AlertType.DQ_LATE, "3:7"), a.alertId);
    }

    @Test
    void duplicateAndRangeAlerts() {
        Alert d = DataQualityGate.alertFor(Verdict.DUPLICATE, ok(5), 0, th, 1);
        assertEquals(AlertType.DQ_DUPLICATE, d.alertType);
        assertTrue(d.detail.contains("3:7"));
        Telemetry bad = ok(5);
        bad.voltageV = 7.5;
        Alert r = DataQualityGate.alertFor(Verdict.OUT_OF_RANGE, bad, 0, th, 1);
        assertEquals(AlertType.DQ_OUT_OF_RANGE, r.alertType);
        assertEquals(7.5, r.value);
        assertTrue(r.detail.startsWith("voltage"));
        assertNotEquals(d.alertId, r.alertId);
    }

    @Test
    void acceptHasNoAlert() {
        assertThrows(IllegalArgumentException.class,
                () -> DataQualityGate.alertFor(Verdict.ACCEPT, ok(1), 0, th, 0));
    }
}
