package bsa.flink;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.Telemetry;
import bsa.core.rules.Thresholds;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** Operator-level tests with Flink's test harness: explicit watermarks, timers and state snapshots. */
class DataQualityFunctionTest {
    private final Thresholds th = new Thresholds();

    private KeyedOneInputStreamOperatorTestHarness<String, Telemetry, Telemetry> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DataQualityFunction(th)), t -> t.cellId, Types.STRING);
    }

    private static Telemetry s(int seq, long ts) {
        return Telemetry.sample("C1", 1, seq, ts, 3.8, -2.0, 30.0);
    }

    private static void push(KeyedOneInputStreamOperatorTestHarness<String, Telemetry, Telemetry> h, Telemetry t) throws Exception {
        h.processElement(new StreamRecord<>(t, t.eventTimeMs));
    }

    private static List<AlertType> dq(KeyedOneInputStreamOperatorTestHarness<String, Telemetry, Telemetry> h) {
        var side = h.getSideOutput(DataQualityFunction.DQ_ALERTS);
        if (side == null) return List.of();
        return side.stream().map(r -> ((Alert) r.getValue()).alertType).collect(Collectors.toList());
    }

    @Test
    void acceptsLateAndDuplicateAreSeparated() throws Exception {
        var h = harness();
        h.open();
        push(h, s(0, 100_000));
        push(h, s(1, 110_000));
        push(h, s(1, 110_000));            // duplicate
        h.processWatermark(150_000);
        push(h, s(2, 150_000));            // exactly at watermark: late
        push(h, s(3, 150_001));            // just after: fine
        Telemetry bad = s(4, 160_000);
        bad.voltageV = -3;
        push(h, bad);
        assertEquals(3, h.extractOutputValues().size());
        assertEquals(List.of(AlertType.DQ_DUPLICATE, AlertType.DQ_LATE, AlertType.DQ_OUT_OF_RANGE), dq(h));
        h.close();
    }

    @Test
    void dedupStateSurvivesSnapshotAndRestore() throws Exception {
        var h = harness();
        h.open();
        push(h, s(0, 100_000));
        OperatorSubtaskState snap = h.snapshot(1L, 1L);
        h.close();

        var restored = harness();
        restored.initializeState(snap);
        restored.open();
        push(restored, s(0, 100_000));
        assertEquals(List.of(AlertType.DQ_DUPLICATE), dq(restored));
        assertTrue(restored.extractOutputValues().isEmpty());
        restored.close();
    }

    @Test
    void seenKeysArePrunedByEventTimeTimers() throws Exception {
        var h = harness();
        h.open();
        DataQualityFunction f = new DataQualityFunction(th);
        push(h, s(0, 100_000));   // cleanup bucket 720 000
        push(h, s(1, 110_000));   // same bucket: no extra timer
        push(h, s(2, 130_000));   // cleanup bucket 780 000
        assertEquals(1, h.numKeyedStateEntries(), "one MapState for the one cell");
        assertEquals(2, h.numEventTimeTimers(), "timers are bucketed per minute, not one per sample");
        h.processWatermark(f.cleanupTime(100_000) - 1);
        assertEquals(2, h.numEventTimeTimers(), "not due yet");
        h.processWatermark(f.cleanupTime(100_000));
        assertEquals(1, h.numEventTimeTimers());
        assertEquals(1, h.numKeyedStateEntries(), "sample 2 is still remembered");
        h.processWatermark(f.cleanupTime(130_000));
        assertEquals(0, h.numKeyedStateEntries(), "all keys forgotten once the watermark passed them + retention");
        h.close();
    }

    @Test
    void cleanupTimeRoundsUpToNextMinuteBucket() {
        DataQualityFunction f = new DataQualityFunction(th);
        assertEquals(720_000, f.cleanupTime(100_000));  // due 700 000 -> next minute edge
        assertEquals(720_000, f.cleanupTime(110_000));  // due 710 000
        assertEquals(780_000, f.cleanupTime(120_000));  // due 720 000 is itself an edge: next bucket
        assertEquals(780_000, f.cleanupTime(130_000));
    }
}
