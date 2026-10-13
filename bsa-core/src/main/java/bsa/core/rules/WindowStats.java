package bsa.core.rules;

import bsa.core.model.Telemetry;

/**
 * Incremental statistics for one cell in one tumbling window (the Flink AggregateFunction accumulator).
 *
 * <p>It never stores the samples: it keeps running sums so that a least-squares slope of voltage
 * against time can be computed at the end. x = minutes since the window start, y = volts, so the
 * slope is directly in volts per minute. Only samples under load enter the regression, because a
 * resting cell's voltage relaxes upward and would dilute the sag signal.
 */
public class WindowStats {
    public long anchorMs = Long.MIN_VALUE;
    public int count;
    public int loadCount;
    public double sumX;
    public double sumY;
    public double sumXX;
    public double sumXY;
    public double maxTemperatureC = Double.NEGATIVE_INFINITY;
    public double minVoltageV = Double.POSITIVE_INFINITY;
    public long maxIngestMs;

    public WindowStats() {
    }

    public static long windowStart(long eventTimeMs, long windowMs) {
        return eventTimeMs - Math.floorMod(eventTimeMs, windowMs);
    }

    public WindowStats add(Telemetry t, Thresholds th) {
        long start = windowStart(t.eventTimeMs, th.windowMs);
        if (anchorMs == Long.MIN_VALUE) {
            anchorMs = start;
        } else if (anchorMs != start) {
            throw new IllegalStateException("sample from another window: " + start + " vs " + anchorMs);
        }
        count++;
        maxTemperatureC = Math.max(maxTemperatureC, t.temperatureC);
        minVoltageV = Math.min(minVoltageV, t.voltageV);
        maxIngestMs = Math.max(maxIngestMs, t.ingestMs);
        if (t.currentA < th.loadCurrentA) {
            double x = (t.eventTimeMs - anchorMs) / 60_000.0;
            loadCount++;
            sumX += x;
            sumY += t.voltageV;
            sumXX += x * x;
            sumXY += x * t.voltageV;
        }
        return this;
    }

    /** Merges two accumulators of the same window (needed by Flink's AggregateFunction contract). */
    public WindowStats merge(WindowStats o) {
        if (o.count == 0) return this;
        if (count == 0) return o;
        if (anchorMs != o.anchorMs) throw new IllegalStateException("cannot merge different windows");
        count += o.count;
        loadCount += o.loadCount;
        sumX += o.sumX;
        sumY += o.sumY;
        sumXX += o.sumXX;
        sumXY += o.sumXY;
        maxTemperatureC = Math.max(maxTemperatureC, o.maxTemperatureC);
        minVoltageV = Math.min(minVoltageV, o.minVoltageV);
        maxIngestMs = Math.max(maxIngestMs, o.maxIngestMs);
        return this;
    }

    /** Least-squares dV/dt of the under-load samples in V/min, or NaN when it is undefined. */
    public double slopeVoltsPerMinute() {
        if (loadCount < 2) return Double.NaN;
        double denom = loadCount * sumXX - sumX * sumX;
        if (Math.abs(denom) < 1e-12) return Double.NaN;
        return (loadCount * sumXY - sumX * sumY) / denom;
    }
}
