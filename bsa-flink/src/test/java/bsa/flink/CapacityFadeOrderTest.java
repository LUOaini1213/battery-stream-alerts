package bsa.flink;

import bsa.core.model.CycleSummary;
import bsa.core.rules.Thresholds;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Added after hand mutation M14 survived: the MiniCluster test never had two cycles of one cell
 * pending when a single watermark passed both, so processing them newest-first went unnoticed.
 */
class CapacityFadeOrderTest {

    private static CycleSummary cycle(int n) {
        CycleSummary s = new CycleSummary();
        s.cellId = "C1";
        s.cycle = n;
        s.samples = 100;
        s.capacityAh = 2.0 - 0.01 * n;
        return s;
    }

    @Test
    void cyclesReleasedInEventTimeOrderWhenOneWatermarkPassesSeveral() throws Exception {
        var h = new KeyedOneInputStreamOperatorTestHarness<String, CycleSummary, CycleSummary>(
                new KeyedProcessOperator<>(new WindowFunctions.CapacityFade(new Thresholds())), s -> s.cellId, Types.STRING);
        h.open();
        for (int n : new int[]{4, 2, 6, 1, 5, 3}) {      // arrival order scrambled, as across network channels
            h.processElement(new StreamRecord<>(cycle(n), 1_000L * n));
        }
        assertTrue(h.extractOutputValues().isEmpty(), "nothing is released before the watermark");
        h.processWatermark(10_000L);                     // passes all six at once

        List<CycleSummary> out = h.extractOutputValues();
        assertEquals(List.of(1, 2, 3, 4, 5, 6), out.stream().map(c -> c.cycle).collect(Collectors.toList()));
        assertTrue(Double.isNaN(out.get(2).soh), "cycle 3 is judged before the 3-cycle baseline exists");
        assertEquals((2.0 - 0.04) / (2.0 - 0.02), out.get(3).soh, 1e-12, "cycle 4 sees the mean of cycles 1-3");
        h.close();
    }
}
