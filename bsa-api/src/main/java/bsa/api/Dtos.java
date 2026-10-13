package bsa.api;

import java.time.Instant;
import java.util.Map;

/** Response shapes. Records serialise to JSON with Jackson; times are ISO-8601 UTC. */
public final class Dtos {
    private Dtos() {
    }

    public record CellSummary(
            String cellId,
            long samples,
            Instant firstEventTime,
            Instant lastEventTime,
            long cycles,
            Integer latestCycle,
            Double initialCapacityAh,
            Double latestCapacityAh,
            Double latestSoh,
            Map<String, Long> alertCounts) {
    }

    public record AlertView(
            String alertId,
            String cellId,
            String alertType,
            String severity,
            Instant eventTime,
            Integer cycle,
            double value,
            double threshold,
            String detail,
            Instant emittedAt) {
    }

    public record CycleView(
            int cycle,
            Instant startTime,
            Instant endTime,
            long samples,
            long missingSamples,
            double capacityAh,
            Double soh,
            Double rollingBaselineAh) {
    }
}
