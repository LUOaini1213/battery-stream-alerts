package bsa.flink;

import bsa.core.model.Alert;
import bsa.core.model.CycleSummary;
import bsa.core.model.Telemetry;
import bsa.core.rules.CycleAnalyzer;
import bsa.core.rules.FadeTracker;
import bsa.core.rules.Thresholds;
import bsa.core.rules.WindowRules;
import bsa.core.rules.WindowStats;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Thin Flink wrappers around the plain-Java rules in bsa-core. */
public final class WindowFunctions {
    public static final OutputTag<Alert> GAP_ALERTS = new OutputTag<>("gap-alerts") { };
    public static final OutputTag<Alert> FADE_ALERTS = new OutputTag<>("fade-alerts") { };

    private WindowFunctions() {
    }

    /** Incremental aggregation: Flink keeps one small WindowStats per (cell, window), not the samples. */
    public static class StatsAggregate implements AggregateFunction<Telemetry, WindowStats, WindowStats> {
        private static final long serialVersionUID = 1L;
        private final Thresholds th;

        public StatsAggregate(Thresholds th) {
            this.th = th;
        }

        @Override
        public WindowStats createAccumulator() {
            return new WindowStats();
        }

        @Override
        public WindowStats add(Telemetry value, WindowStats acc) {
            return acc.add(value, th);
        }

        @Override
        public WindowStats getResult(WindowStats acc) {
            return acc;
        }

        @Override
        public WindowStats merge(WindowStats a, WindowStats b) {
            return a.merge(b);
        }
    }

    /** Receives the finished WindowStats plus the window bounds and applies the window rules. */
    public static class WindowAlerts extends ProcessWindowFunction<WindowStats, Alert, String, TimeWindow> {
        private static final long serialVersionUID = 1L;
        private final Thresholds th;

        public WindowAlerts(Thresholds th) {
            this.th = th;
        }

        @Override
        public void process(String cellId, Context ctx, Iterable<WindowStats> stats, Collector<Alert> out) {
            WindowStats s = stats.iterator().next();
            TimeWindow w = ctx.window();
            for (Alert a : WindowRules.evaluate(cellId, w.getStart(), w.getEnd(), s, th, System.currentTimeMillis())) {
                out.collect(a);
            }
        }
    }

    /**
     * One session window = one discharge cycle (key = cell#cycle). Needs all samples at once to sort
     * them and integrate current, so this is a full ProcessWindowFunction (about 300 samples).
     */
    public static class CycleWindow extends ProcessWindowFunction<Telemetry, CycleSummary, String, TimeWindow> {
        private static final long serialVersionUID = 1L;
        private final Thresholds th;

        public CycleWindow(Thresholds th) {
            this.th = th;
        }

        @Override
        public void process(String key, Context ctx, Iterable<Telemetry> samples, Collector<CycleSummary> out) {
            Telemetry first = samples.iterator().next();
            CycleAnalyzer.Result r = CycleAnalyzer.analyze(first.cellId, first.cycle, samples, th, System.currentTimeMillis());
            out.collect(r.summary);
            for (Alert a : r.gapAlerts) ctx.output(GAP_ALERTS, a);
        }
    }

    /**
     * Keyed by cell; keeps the capacity history in keyed state across cycles.
     *
     * <p>Ordering matters here (the rolling baseline must see cycle 5 before cycle 6), but records
     * coming from different parallel subtasks of the cycle window are NOT ordered by event time:
     * cycle 6 can overtake cycle 5 on another network channel. Only watermarks are coordinated
     * across channels. So each summary is parked in keyed state and an event-time timer is set at its
     * timestamp; when the watermark passes it, every summary up to that time has arrived and they are
     * processed in event-time order. (Found by the MiniCluster test, which failed without this.)
     */
    public static class CapacityFade extends KeyedProcessFunction<String, CycleSummary, CycleSummary> {
        private static final long serialVersionUID = 1L;
        private final Thresholds th;
        private transient ValueState<FadeTracker> tracker;
        private transient MapState<Long, List<CycleSummary>> pending;

        public CapacityFade(Thresholds th) {
            this.th = th;
        }

        @Override
        public void open(Configuration parameters) {
            tracker = getRuntimeContext().getState(new ValueStateDescriptor<>("fade-tracker", FadeTracker.class));
            pending = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "pending-cycles", Types.LONG,
                    Types.LIST(TypeInformation.of(CycleSummary.class))));
        }

        @Override
        public void processElement(CycleSummary s, Context ctx, Collector<CycleSummary> out) throws Exception {
            long ts = ctx.timestamp(); // session window result timestamp = window end - 1
            List<CycleSummary> list = pending.get(ts);
            if (list == null) list = new ArrayList<>();
            list.add(s);
            pending.put(ts, list);
            ctx.timerService().registerEventTimeTimer(ts);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<CycleSummary> out) throws Exception {
            List<Long> due = new ArrayList<>();
            for (Long ts : pending.keys()) if (ts <= timestamp) due.add(ts);
            Collections.sort(due);
            FadeTracker f = tracker.value();
            if (f == null) f = new FadeTracker();
            for (Long ts : due) {
                List<CycleSummary> batch = pending.get(ts);
                batch.sort(Comparator.comparingInt(c -> c.cycle));
                for (CycleSummary s : batch) {
                    for (Alert a : f.onCycle(s, th, System.currentTimeMillis())) ctx.output(FADE_ALERTS, a);
                    out.collect(s);
                }
                pending.remove(ts);
            }
            tracker.update(f);
        }
    }
}
