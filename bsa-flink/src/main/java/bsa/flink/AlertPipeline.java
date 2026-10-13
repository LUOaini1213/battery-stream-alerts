package bsa.flink;

import bsa.core.model.Alert;
import bsa.core.model.CycleSummary;
import bsa.core.model.Telemetry;
import bsa.core.rules.DataQualityGate;
import bsa.core.rules.Thresholds;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * The whole topology, independent of where data comes from or goes to. {@link StreamingJob} plugs
 * Kafka in and ClickHouse out; the MiniCluster test plugs in a list and collecting sinks.
 *
 * <pre>
 * input (timestamps + watermarks already assigned)
 *   -> drop heartbeats
 *   -> keyBy(cell) DataQualityFunction  --side--> DQ_OUT_OF_RANGE / DQ_LATE / DQ_DUPLICATE
 *        | accepted samples
 *        +-> keyBy(cell) tumbling 120 s window, incremental WindowStats -> OVER_TEMPERATURE / VOLTAGE_SAG
 *        +-> keyBy(cell#cycle) session window (5 min gap) -> CycleSummary --side--> DQ_GAP
 *               -> keyBy(cell) CapacityFade (keyed state) --side--> CAPACITY_DROP / CAPACITY_EOL
 * </pre>
 */
public final class AlertPipeline {
    /** Samples that slipped past the gate but were still too late for a window. Expected to stay empty. */
    public static final OutputTag<Telemetry> LATE_FOR_WINDOW = new OutputTag<>("late-for-window") { };
    public static final OutputTag<Telemetry> LATE_FOR_CYCLE = new OutputTag<>("late-for-cycle") { };

    public static final class Outputs {
        public final DataStream<Telemetry> accepted;
        public final DataStream<Alert> alerts;
        public final DataStream<CycleSummary> cycles;

        Outputs(DataStream<Telemetry> accepted, DataStream<Alert> alerts, DataStream<CycleSummary> cycles) {
            this.accepted = accepted;
            this.alerts = alerts;
            this.cycles = cycles;
        }
    }

    private AlertPipeline() {
    }

    public static Outputs build(DataStream<Telemetry> withWatermarks, Thresholds th) {
        // Same parallelism as the source so the filter is chained to it (no network hop). This also
        // means every data-quality subtask sees exactly one input channel per source subtask.
        DataStream<Telemetry> samples = withWatermarks
                .filter(t -> !t.isHeartbeat()).name("drop-heartbeats").uid("drop-heartbeats")
                .setParallelism(withWatermarks.getParallelism());

        SingleOutputStreamOperator<Telemetry> accepted = samples
                .keyBy(t -> t.cellId)
                .process(new DataQualityFunction(th)).name("data-quality-gate").uid("data-quality-gate");

        SingleOutputStreamOperator<Alert> windowAlerts = accepted
                .keyBy(t -> t.cellId)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(th.windowMs)))
                .sideOutputLateData(LATE_FOR_WINDOW)
                .aggregate(new WindowFunctions.StatsAggregate(th), new WindowFunctions.WindowAlerts(th))
                .name("window-rules").uid("window-rules");

        SingleOutputStreamOperator<CycleSummary> rawCycles = accepted
                .keyBy(t -> t.cellId + "#" + t.cycle)
                .window(EventTimeSessionWindows.withGap(Duration.ofMillis(th.sessionGapMs)))
                .sideOutputLateData(LATE_FOR_CYCLE)
                .process(new WindowFunctions.CycleWindow(th))
                .name("cycle-summary").uid("cycle-summary");

        SingleOutputStreamOperator<CycleSummary> cycles = rawCycles
                .keyBy(s -> s.cellId)
                .process(new WindowFunctions.CapacityFade(th))
                .name("capacity-fade").uid("capacity-fade");

        DataStream<Alert> lateForWindows = windowAlerts.getSideOutput(LATE_FOR_WINDOW)
                .union(rawCycles.getSideOutput(LATE_FOR_CYCLE))
                .map(t -> DataQualityGate.alertFor(DataQualityGate.Verdict.LATE, t, t.eventTimeMs, th,
                        System.currentTimeMillis()))
                .name("late-for-window-alerts").uid("late-for-window-alerts");

        DataStream<Alert> alerts = accepted.getSideOutput(DataQualityFunction.DQ_ALERTS)
                .union(windowAlerts,
                        rawCycles.getSideOutput(WindowFunctions.GAP_ALERTS),
                        cycles.getSideOutput(WindowFunctions.FADE_ALERTS),
                        lateForWindows);

        return new Outputs(accepted, alerts, cycles);
    }
}
