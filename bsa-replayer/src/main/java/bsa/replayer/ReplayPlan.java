package bsa.replayer;

import bsa.core.model.Telemetry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Pure, deterministic construction of the delivery order (no Kafka, no clock).
 *
 * <p>Every sample gets a delivery time = event time + delay. Normally delay = 0, so delivery is in
 * event-time order (ties broken by the original file order). Fault injection changes delays or
 * values for a few samples, chosen with a seeded RNG so the same seed always gives the same plan:
 * <ul>
 *   <li>duplicate: an extra copy delivered 1 s later (inside the 20 s out-of-orderness bound)</li>
 *   <li>reorder: delivered 5 s later (inside the bound: must NOT raise anything)</li>
 *   <li>late: delivered 30 min later (far beyond the bound: DQ_LATE, and a hole in its cycle)</li>
 *   <li>out-of-range: voltage set to 9.99 V (DQ_OUT_OF_RANGE, and a hole)</li>
 *   <li>drop: never delivered (a hole: DQ_GAP)</li>
 * </ul>
 * At most one fault per (cell, cycle), never in the first 5 or last 30 samples of a cycle, so each
 * dropped sample is a separate, detectable hole and a late sample still has later data behind it.
 */
public final class ReplayPlan {
    public static final long DUP_DELAY_MS = 1_000;
    public static final long REORDER_DELAY_MS = 5_000;
    public static final long LATE_DELAY_MS = 1_800_000;
    public static final double OUT_OF_RANGE_VOLTAGE = 9.99;

    public enum Fault { NONE, DUPLICATE, REORDER, LATE, OUT_OF_RANGE, DROP }

    /** One record to send: a sample, a duplicate copy, or a heartbeat. */
    public static final class Delivery {
        public final Telemetry record;
        public final long deliveryTimeMs;
        public final Fault fault;
        final long order;

        Delivery(Telemetry record, long deliveryTimeMs, Fault fault, long order) {
            this.record = record;
            this.deliveryTimeMs = deliveryTimeMs;
            this.fault = fault;
            this.order = order;
        }
    }

    public static final class FaultCounts {
        public int duplicate;
        public int reorder;
        public int late;
        public int outOfRange;
        public int drop;

        public static FaultCounts parse(String spec) {
            FaultCounts c = new FaultCounts();
            if (spec == null || spec.isBlank() || spec.equals("none")) return c;
            for (String part : spec.split(",")) {
                String[] kv = part.trim().split("=");
                int n = Integer.parseInt(kv[1].trim());
                switch (kv[0].trim()) {
                    case "dup": c.duplicate = n; break;
                    case "reorder": c.reorder = n; break;
                    case "late": c.late = n; break;
                    case "oor": c.outOfRange = n; break;
                    case "drop": c.drop = n; break;
                    default: throw new IllegalArgumentException("unknown fault " + kv[0]);
                }
            }
            return c;
        }

        public int total() {
            return duplicate + reorder + late + outOfRange + drop;
        }
    }

    public final List<Delivery> deliveries;
    public final Map<Fault, List<String>> injected;
    public final int samplesIn;

    private ReplayPlan(List<Delivery> deliveries, Map<Fault, List<String>> injected, int samplesIn) {
        this.deliveries = deliveries;
        this.injected = injected;
        this.samplesIn = samplesIn;
    }

    /**
     * @param samples     samples in file order (the converter writes them sorted by event time)
     * @param heartbeatMs insert a heartbeat whenever delivery time advanced this much (0 = never)
     * @param tailMs      a final heartbeat at (last delivery time + tailMs) closes every open window
     */
    public static ReplayPlan build(List<Telemetry> samples, FaultCounts faults, long seed, long heartbeatMs, long tailMs) {
        Map<String, List<Integer>> byCycle = new TreeMap<>();
        for (int i = 0; i < samples.size(); i++) {
            Telemetry t = samples.get(i);
            byCycle.computeIfAbsent(t.cellId + "#" + t.cycle, k -> new ArrayList<>()).add(i);
        }
        List<String> cycles = new ArrayList<>(byCycle.keySet());
        Collections.shuffle(cycles, new Random(seed));
        if (faults.total() > cycles.size()) {
            throw new IllegalArgumentException("more faults (" + faults.total() + ") than cycles (" + cycles.size() + ")");
        }
        List<Fault> wanted = new ArrayList<>();
        add(wanted, Fault.DUPLICATE, faults.duplicate);
        add(wanted, Fault.REORDER, faults.reorder);
        add(wanted, Fault.LATE, faults.late);
        add(wanted, Fault.OUT_OF_RANGE, faults.outOfRange);
        add(wanted, Fault.DROP, faults.drop);

        Random pick = new Random(seed * 31 + 7);
        Map<Integer, Fault> faultAt = new HashMap<>();
        Map<Fault, List<String>> injected = new TreeMap<>();
        int c = 0;
        for (Fault f : wanted) {
            List<Integer> idx;
            do {
                if (c >= cycles.size()) throw new IllegalArgumentException("not enough long cycles for " + wanted.size() + " faults");
                idx = byCycle.get(cycles.get(c++));
            } while (idx.size() < 40);
            idx = new ArrayList<>(idx);
            idx.sort(Comparator.comparingInt(i -> samples.get(i).seq));
            int at = idx.get(5 + pick.nextInt(idx.size() - 35));
            faultAt.put(at, f);
            injected.computeIfAbsent(f, k -> new ArrayList<>()).add(samples.get(at).cellId + ":" + samples.get(at).sampleKey());
        }

        List<Delivery> out = new ArrayList<>(samples.size() + faults.duplicate);
        long order = 0;
        for (int i = 0; i < samples.size(); i++) {
            Telemetry t = samples.get(i);
            Fault f = faultAt.getOrDefault(i, Fault.NONE);
            switch (f) {
                case DROP:
                    break;
                case DUPLICATE:
                    out.add(new Delivery(t, t.eventTimeMs, Fault.NONE, order++));
                    out.add(new Delivery(t.copy(), t.eventTimeMs + DUP_DELAY_MS, Fault.DUPLICATE, order++));
                    break;
                case REORDER:
                    out.add(new Delivery(t, t.eventTimeMs + REORDER_DELAY_MS, f, order++));
                    break;
                case LATE:
                    out.add(new Delivery(t, t.eventTimeMs + LATE_DELAY_MS, f, order++));
                    break;
                case OUT_OF_RANGE: {
                    Telemetry bad = t.copy();
                    bad.voltageV = OUT_OF_RANGE_VOLTAGE;
                    out.add(new Delivery(bad, t.eventTimeMs, f, order++));
                    break;
                }
                default:
                    out.add(new Delivery(t, t.eventTimeMs, Fault.NONE, order++));
            }
        }
        out.sort(Comparator.comparingLong((Delivery d) -> d.deliveryTimeMs).thenComparingLong(d -> d.order));

        List<Delivery> withHeartbeats = new ArrayList<>(out.size() + 1024);
        long lastHb = Long.MIN_VALUE;
        long maxEventTime = Long.MIN_VALUE;
        for (Delivery d : out) {
            withHeartbeats.add(d);
            maxEventTime = Math.max(maxEventTime, d.record.eventTimeMs);
            if (heartbeatMs > 0 && (lastHb == Long.MIN_VALUE || d.deliveryTimeMs - lastHb >= heartbeatMs)) {
                // a heartbeat never claims a time beyond the data already sent
                withHeartbeats.add(new Delivery(Telemetry.heartbeat(maxEventTime), d.deliveryTimeMs, Fault.NONE, order++));
                lastHb = d.deliveryTimeMs;
            }
        }
        if (tailMs > 0 && !out.isEmpty()) {
            long end = maxEventTime + tailMs;
            withHeartbeats.add(new Delivery(Telemetry.heartbeat(end), out.get(out.size() - 1).deliveryTimeMs, Fault.NONE, order));
        }
        return new ReplayPlan(withHeartbeats, injected, samples.size());
    }

    private static void add(List<Fault> list, Fault f, int n) {
        for (int i = 0; i < n; i++) list.add(f);
    }

    /** Data-quality alerts the job should raise for this plan. */
    public Map<String, Integer> expectedDqAlerts() {
        Map<String, Integer> m = new TreeMap<>();
        int late = injected.getOrDefault(Fault.LATE, List.of()).size();
        int oor = injected.getOrDefault(Fault.OUT_OF_RANGE, List.of()).size();
        int drop = injected.getOrDefault(Fault.DROP, List.of()).size();
        m.put("DQ_DUPLICATE", injected.getOrDefault(Fault.DUPLICATE, List.of()).size());
        m.put("DQ_LATE", late);
        m.put("DQ_OUT_OF_RANGE", oor);
        m.put("DQ_GAP", late + oor + drop);
        return m;
    }
}
