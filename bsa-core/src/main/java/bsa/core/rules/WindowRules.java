package bsa.core.rules;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.Severity;

import java.util.ArrayList;
import java.util.List;

/** Rules evaluated once per cell per tumbling window, after the window closes. */
public final class WindowRules {

    private WindowRules() {
    }

    public static List<Alert> evaluate(String cellId, long windowStartMs, long windowEndMs,
                                       WindowStats s, Thresholds th, long nowMs) {
        List<Alert> out = new ArrayList<>(2);
        if (s.count == 0) {
            return out;
        }
        String disc = Long.toString(windowStartMs);

        if (s.maxTemperatureC > th.overTemperatureC) {
            Alert a = base(cellId, AlertType.OVER_TEMPERATURE, Severity.WARN, disc, windowStartMs, windowEndMs, s, nowMs);
            a.value = s.maxTemperatureC;
            a.threshold = th.overTemperatureC;
            a.detail = String.format("max %.2f C > %.2f C in window", s.maxTemperatureC, th.overTemperatureC);
            out.add(a);
        }

        double slope = s.slopeVoltsPerMinute();
        if (s.loadCount >= th.sagMinLoadSamples && slope < th.sagVoltsPerMinute) {
            Alert a = base(cellId, AlertType.VOLTAGE_SAG, Severity.WARN, disc, windowStartMs, windowEndMs, s, nowMs);
            a.value = slope;
            a.threshold = th.sagVoltsPerMinute;
            a.detail = String.format("dV/dt %.3f V/min < %.3f over %d loaded samples, min %.3f V",
                    slope, th.sagVoltsPerMinute, s.loadCount, s.minVoltageV);
            out.add(a);
        }
        return out;
    }

    private static Alert base(String cellId, AlertType type, Severity sev, String disc,
                              long ws, long we, WindowStats s, long nowMs) {
        Alert a = Alert.of(cellId, type, sev, disc);
        a.windowStartMs = ws;
        a.windowEndMs = we;
        a.eventTimeMs = we - 1;
        a.maxIngestMs = s.maxIngestMs;
        a.emittedMs = nowMs;
        return a;
    }
}
