package bsa.flink;

import bsa.core.model.Telemetry;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * Event-time setup.
 *
 * <p>Watermark = (largest event time seen so far) - outOfOrderness - 1 ms. A watermark W tells every
 * downstream operator "no record with timestamp <= W will arrive any more", which is what lets a
 * window [start, end) fire once W >= end - 1. A record that still shows up with timestamp <= W is
 * "late": the data-quality gate turns it into a DQ_LATE alert.
 *
 * <p>The generator is punctuated (it emits on every record) instead of the usual periodic one. At
 * the record rates here (a few thousand per second) that costs nothing, and it makes the job's
 * behaviour depend only on record order, not on wall-clock timing, which keeps tests deterministic.
 */
public final class TelemetryWatermarks {

    private TelemetryWatermarks() {
    }

    public static WatermarkStrategy<Telemetry> strategy(long outOfOrdernessMs) {
        return WatermarkStrategy.<Telemetry>forGenerator(ctx -> new Generator(outOfOrdernessMs))
                .withTimestampAssigner((t, previous) -> t.eventTimeMs);
    }

    /**
     * Same strategy plus watermark alignment across Kafka partitions: a partition whose watermark runs
     * more than {@code maxDriftMs} ahead of the slowest one is paused. Without it, one partition can
     * be consumed minutes (in event time) ahead of another and the late/on-time decision would depend
     * on consumer scheduling.
     *
     * <p>Cost: alignment re-computes the allowed watermark every {@code updateIntervalMs}, so event
     * time can advance at most about maxDrift per interval. The first demo run used 60 s / 200 ms and
     * the job crawled at ~130 records/s while the replayer was sending ~2200/s. The defaults are now
     * 600 s / 50 ms, which lets event time advance far faster than the 5000x demo replay needs.
     */
    public static WatermarkStrategy<Telemetry> alignedStrategy(long outOfOrdernessMs, long maxDriftMs, long updateIntervalMs) {
        return strategy(outOfOrdernessMs)
                .withWatermarkAlignment("bsa-telemetry", Duration.ofMillis(maxDriftMs), Duration.ofMillis(updateIntervalMs));
    }

    static final class Generator implements WatermarkGenerator<Telemetry> {
        private final long bound;
        private long maxTs;

        Generator(long bound) {
            this.bound = bound;
            this.maxTs = Long.MIN_VALUE + bound + 1;
        }

        @Override
        public void onEvent(Telemetry event, long eventTimestamp, WatermarkOutput output) {
            if (eventTimestamp > maxTs) {
                maxTs = eventTimestamp;
                output.emitWatermark(new Watermark(maxTs - bound - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTs - bound - 1));
        }
    }
}
