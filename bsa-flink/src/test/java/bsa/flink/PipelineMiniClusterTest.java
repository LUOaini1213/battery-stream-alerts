package bsa.flink;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.CycleSummary;
import bsa.core.model.Telemetry;
import bsa.core.rules.Thresholds;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the real topology on an embedded Flink MiniCluster (2 slots, parallelism 2) with the
 * synthetic fixture, then with injected duplicates, reordering, late and out-of-range samples.
 */
class PipelineMiniClusterTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    static final Map<String, Queue<Object>> SINKS = new ConcurrentHashMap<>();

    /** Collects into a static queue (the sink instance is serialised and shipped to the task). */
    static final class Collect<T> implements SinkFunction<T> {
        private static final long serialVersionUID = 1L;
        private final String name;

        Collect(String name) {
            this.name = name;
        }

        @Override
        public void invoke(T value, Context context) {
            SINKS.computeIfAbsent(name, k -> new ConcurrentLinkedQueue<>()).add(value);
        }
    }

    private final Thresholds th = new Thresholds();

    static final class Run {
        final List<Alert> alerts;
        final List<Telemetry> accepted;
        final List<CycleSummary> cycles;

        Run(List<Alert> alerts, List<Telemetry> accepted, List<CycleSummary> cycles) {
            this.alerts = alerts;
            this.accepted = accepted;
            this.cycles = cycles;
        }

        long count(AlertType t) {
            return alerts.stream().filter(a -> a.alertType == t).count();
        }

        TreeSet<String> nonDqPlusGaps() {
            return alerts.stream()
                    .filter(a -> !a.alertType.isDataQuality() || a.alertType == AlertType.DQ_GAP)
                    .map(TestData::key).collect(Collectors.toCollection(TreeSet::new));
        }
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private Run run(List<Telemetry> input) throws Exception {
        SINKS.clear();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // A single source subtask feeds the watermark generator, so record order (and with it the
        // watermark sequence) is exactly the order of the input list.
        DataStream<Telemetry> src = env.fromData(input)
                .assignTimestampsAndWatermarks(TelemetryWatermarks.strategy(th.outOfOrdernessMs))
                .setParallelism(1);
        AlertPipeline.Outputs out = AlertPipeline.build(src, th);
        out.alerts.addSink(new Collect<>("alerts"));
        out.accepted.addSink(new Collect<>("accepted"));
        out.cycles.addSink(new Collect<>("cycles"));
        env.execute("pipeline-test");
        return new Run(
                (List<Alert>) (List<?>) new ArrayList<>(SINKS.getOrDefault("alerts", new ConcurrentLinkedQueue<>())),
                (List<Telemetry>) (List<?>) new ArrayList<>(SINKS.getOrDefault("accepted", new ConcurrentLinkedQueue<>())),
                (List<CycleSummary>) (List<?>) new ArrayList<>(SINKS.getOrDefault("cycles", new ConcurrentLinkedQueue<>())));
    }

    @Test
    void cleanFixtureMatchesBatchOracleAndDesignedCounts() throws Exception {
        List<Telemetry> fixture = TestData.fixture();
        Run r = run(fixture);

        assertEquals(TestData.oracle(fixture, th), r.nonDqPlusGaps(), () -> "streaming result must equal batch result; cycles="
                + r.cycles.stream().sorted(java.util.Comparator.comparing((CycleSummary c) -> c.cellId).thenComparingInt(c -> c.cycle))
                .map(c -> c.cellId + "/" + c.cycle + "/" + c.samples + "/" + c.capacityAh + "/" + c.initialCapacityAh + "/" + c.soh)
                .collect(Collectors.joining(" ")));
        // counts the fixture was designed to produce (see scripts/make_synthetic_fixture.py)
        assertEquals(16, r.count(AlertType.VOLTAGE_SAG));
        assertEquals(6, r.count(AlertType.OVER_TEMPERATURE));
        assertEquals(4, r.count(AlertType.CAPACITY_DROP));
        assertEquals(1, r.count(AlertType.CAPACITY_EOL));
        assertTrue(r.alerts.stream().noneMatch(a -> a.alertType.isDataQuality()), r.alerts.toString());
        assertTrue(r.alerts.stream().filter(a -> a.alertType == AlertType.OVER_TEMPERATURE).allMatch(a -> a.cellId.equals("SYN-B")));
        Alert eol = r.alerts.stream().filter(a -> a.alertType == AlertType.CAPACITY_EOL).findFirst().orElseThrow();
        assertEquals("SYN-B", eol.cellId);
        assertEquals(8, eol.cycle);
        assertEquals(0.75, eol.value, 1e-9);

        assertEquals(720, r.accepted.size());
        assertEquals(16, r.cycles.size());
        CycleSummary a1 = r.cycles.stream().filter(c -> c.cellId.equals("SYN-A") && c.cycle == 1).findFirst().orElseThrow();
        assertEquals(2.0 * 435 / 3600.0, a1.capacityAh, 1e-9); // 0 A at seq 0, then 2 A for 435 s (trapezoid)
        CycleSummary b8 = r.cycles.stream().filter(c -> c.cellId.equals("SYN-B") && c.cycle == 8).findFirst().orElseThrow();
        assertEquals(0.75, b8.soh, 1e-9);

        // idempotency precondition: a second run over the same input yields the same alert ids
        Run again = run(fixture);
        assertEquals(ids(r), ids(again));
    }

    @Test
    void injectedFaultsAreDetectedExactly() throws Exception {
        List<Telemetry> fixture = TestData.fixture();
        List<Telemetry> stream = new ArrayList<>(fixture);

        // 3 duplicates, re-sent two records after the original (same event time: not late)
        for (String k : List.of("SYN-A,1,10", "SYN-B,4,20", "SYN-A,7,5")) {
            int i = indexOf(stream, k);
            stream.add(i + 3, stream.get(i).copy());
        }
        // 3 records delayed by two positions (= 10 s of event time, inside the 20 s bound): no alert
        for (String k : List.of("SYN-A,2,12", "SYN-B,3,30", "SYN-A,8,3")) {
            int i = indexOf(stream, k);
            stream.add(i + 2, stream.remove(i));
        }
        // 2 records delayed by 60 positions (= 300 s, far beyond the bound): DQ_LATE + a hole in their cycle
        List<Telemetry> late = new ArrayList<>();
        for (String k : List.of("SYN-B,2,15", "SYN-A,3,20")) {
            int i = indexOf(stream, k);
            Telemetry t = stream.remove(i);
            stream.add(i + 60, t);
            late.add(t);
        }
        // 2 physically impossible values: DQ_OUT_OF_RANGE + a hole in their cycle
        List<Telemetry> bad = new ArrayList<>();
        Telemetry nanV = stream.get(indexOf(stream, "SYN-A,5,30"));
        nanV.voltageV = Double.NaN;
        bad.add(nanV);
        Telemetry hot = stream.get(indexOf(stream, "SYN-B,6,8"));
        hot.temperatureC = 150;
        bad.add(hot);
        // heartbeats carry no data and must not change anything
        stream.add(100, Telemetry.heartbeat(stream.get(99).eventTimeMs));
        stream.add(400, Telemetry.heartbeat(stream.get(399).eventTimeMs));

        Run r = run(stream);

        assertEquals(3, r.count(AlertType.DQ_DUPLICATE), r.alerts.toString());
        assertEquals(2, r.count(AlertType.DQ_LATE));
        assertEquals(2, r.count(AlertType.DQ_OUT_OF_RANGE));
        assertEquals(4, r.count(AlertType.DQ_GAP), "every dropped sample leaves one hole");
        assertEquals(720 - 4, r.accepted.size());

        List<Telemetry> expectedAccepted = new ArrayList<>(fixture);
        expectedAccepted.removeIf(t -> late.stream().anyMatch(l -> same(l, t)) || bad.stream().anyMatch(b -> same(b, t)));
        assertEquals(TestData.oracle(expectedAccepted, th), r.nonDqPlusGaps());

        TreeMap<String, Long> lateBySample = new TreeMap<>();
        r.alerts.stream().filter(a -> a.alertType == AlertType.DQ_LATE).forEach(a -> lateBySample.put(a.cellId + "," + a.cycle, (long) a.value));
        assertEquals(List.of("SYN-A,3", "SYN-B,2"), new ArrayList<>(lateBySample.keySet()));
        assertTrue(lateBySample.values().stream().allMatch(v -> v > th.outOfOrdernessMs), lateBySample.toString());
    }

    private static boolean same(Telemetry a, Telemetry b) {
        return a.cellId.equals(b.cellId) && a.cycle == b.cycle && a.seq == b.seq;
    }

    private static int indexOf(List<Telemetry> xs, String cellCycleSeq) {
        String[] p = cellCycleSeq.split(",");
        for (int i = 0; i < xs.size(); i++) {
            Telemetry t = xs.get(i);
            if (!t.isHeartbeat() && t.cellId.equals(p[0]) && t.cycle == Integer.parseInt(p[1]) && t.seq == Integer.parseInt(p[2])) {
                return i;
            }
        }
        throw new IllegalArgumentException(cellCycleSeq);
    }

    private static TreeSet<String> ids(Run r) {
        return r.alerts.stream().map(a -> a.alertId).collect(Collectors.toCollection(TreeSet::new));
    }
}
