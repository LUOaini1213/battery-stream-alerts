package bsa.replayer;

import bsa.core.model.Telemetry;
import bsa.replayer.ReplayPlan.Delivery;
import bsa.replayer.ReplayPlan.Fault;
import bsa.replayer.ReplayPlan.FaultCounts;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ReplayPlanTest {

    private static List<Telemetry> fixture() throws Exception {
        return Replayer.readCsv(Path.of("..", "fixtures", "synthetic_telemetry.csv"), 0);
    }

    @Test
    void noFaultsKeepsEventTimeOrderAndAddsHeartbeats() throws Exception {
        List<Telemetry> xs = fixture();
        ReplayPlan p = ReplayPlan.build(xs, FaultCounts.parse("none"), 1, 120_000, 3_600_000);
        List<Delivery> data = p.deliveries.stream().filter(d -> !d.record.isHeartbeat()).collect(Collectors.toList());
        assertEquals(xs.size(), data.size());
        for (int i = 1; i < data.size(); i++) {
            assertTrue(data.get(i - 1).record.eventTimeMs <= data.get(i).record.eventTimeMs);
        }
        List<Delivery> hb = p.deliveries.stream().filter(d -> d.record.isHeartbeat()).collect(Collectors.toList());
        assertTrue(hb.size() > 10);
        Delivery last = p.deliveries.get(p.deliveries.size() - 1);
        assertTrue(last.record.isHeartbeat());
        assertEquals(xs.get(xs.size() - 1).eventTimeMs + 3_600_000, last.record.eventTimeMs);
        // heartbeats never run ahead of the data already delivered (except the final one)
        long max = Long.MIN_VALUE;
        for (Delivery d : p.deliveries.subList(0, p.deliveries.size() - 1)) {
            if (d.record.isHeartbeat()) assertTrue(d.record.eventTimeMs <= max);
            else max = Math.max(max, d.record.eventTimeMs);
        }
        assertEquals(Map.of("DQ_DUPLICATE", 0, "DQ_GAP", 0, "DQ_LATE", 0, "DQ_OUT_OF_RANGE", 0), p.expectedDqAlerts());
    }

    @Test
    void faultsAreDeterministicOnePerCycleAndShapedAsDocumented() throws Exception {
        List<Telemetry> xs = fixture();
        FaultCounts f = FaultCounts.parse("dup=2,reorder=2,late=2,oor=2,drop=2");
        ReplayPlan p1 = ReplayPlan.build(xs, f, 7, 0, 0);
        ReplayPlan p2 = ReplayPlan.build(fixture(), f, 7, 0, 0);
        assertEquals(p1.injected, p2.injected, "same seed, same plan");
        assertNotEquals(p1.injected, ReplayPlan.build(fixture(), f, 8, 0, 0).injected);

        Set<String> cycles = new HashSet<>();
        p1.injected.values().forEach(l -> l.forEach(s -> cycles.add(s.substring(0, s.lastIndexOf(':')))));
        assertEquals(10, cycles.size(), "one fault per (cell, cycle)");

        assertEquals(xs.size() + 2 - 2, p1.deliveries.size(), "+2 duplicate copies, -2 dropped");
        long dups = p1.deliveries.stream().filter(d -> d.fault == Fault.DUPLICATE).count();
        assertEquals(2, dups);
        for (Delivery d : p1.deliveries) {
            long delay = d.deliveryTimeMs - d.record.eventTimeMs;
            switch (d.fault) {
                case LATE: assertEquals(ReplayPlan.LATE_DELAY_MS, delay); break;
                case REORDER: assertEquals(ReplayPlan.REORDER_DELAY_MS, delay); break;
                case DUPLICATE: assertEquals(ReplayPlan.DUP_DELAY_MS, delay); break;
                case OUT_OF_RANGE: assertEquals(ReplayPlan.OUT_OF_RANGE_VOLTAGE, d.record.voltageV); break;
                default: assertEquals(0, delay);
            }
            if (d.fault != Fault.NONE) {
                assertTrue(d.record.seq >= 5 && d.record.seq <= 45 - 31, "fault not at cycle edges: " + d.record);
            }
        }
        assertEquals(Map.of("DQ_DUPLICATE", 2, "DQ_GAP", 6, "DQ_LATE", 2, "DQ_OUT_OF_RANGE", 2), p1.expectedDqAlerts());
    }

    /** Added after hand mutation M20 survived: the delays must actually move records in the sequence. */
    @Test
    void delayedRecordsAreReallyDeliveredLater() throws Exception {
        ReplayPlan p = ReplayPlan.build(fixture(), FaultCounts.parse("late=2,reorder=2,dup=2"), 7, 0, 0);
        List<Delivery> d = p.deliveries;
        for (int i = 1; i < d.size(); i++) {
            assertTrue(d.get(i - 1).deliveryTimeMs <= d.get(i).deliveryTimeMs, "delivery order at " + i);
        }
        for (int i = 0; i < d.size(); i++) {
            if (d.get(i).fault != Fault.LATE) continue;
            long t = d.get(i).record.eventTimeMs;
            long newerBefore = d.subList(0, i).stream().filter(x -> x.record.eventTimeMs > t + 60_000).count();
            assertTrue(newerBefore > 0, "a late record must be preceded by records more than 60 s newer (beyond the 20 s bound)");
        }
    }

    @Test
    void tooManyFaultsRejected() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ReplayPlan.build(fixture(), FaultCounts.parse("drop=17"), 1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> FaultCounts.parse("boom=1"));
    }

    @Test
    void pacerScalesAndCapsGaps() throws Exception {
        Replayer.Pacer p = new Replayer.Pacer(1000, 50);
        assertEquals(0, p.await(0));
        assertEquals(10, p.await(10_000));       // 10 s of event time at 1000x = 10 ms
        assertEquals(50, p.await(3_600_000));    // a 1 h idle gap is capped
        assertEquals(0, p.await(1_000));         // going backwards adds nothing
        assertEquals(0, new Replayer.Pacer(0, 50).await(123));
    }

    @Test
    void argsParsing() {
        assertEquals(Map.of("csv", "a.csv", "speedup", "0"), Replayer.parseArgs(new String[]{"--csv", "a.csv", "--speedup", "0"}));
        assertThrows(IllegalArgumentException.class, () -> Replayer.parseArgs(new String[]{"--csv"}));
        assertThrows(IllegalArgumentException.class, () -> Replayer.parseArgs(new String[]{"csv"}));
    }
}
