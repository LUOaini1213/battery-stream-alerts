package bsa.flink;

import bsa.core.codec.TelemetryCsv;
import bsa.core.model.Alert;
import bsa.core.model.CycleSummary;
import bsa.core.model.Telemetry;
import bsa.core.rules.CycleAnalyzer;
import bsa.core.rules.FadeTracker;
import bsa.core.rules.Thresholds;
import bsa.core.rules.WindowRules;
import bsa.core.rules.WindowStats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Fixture loading and a batch "oracle" that applies the same core rules without any streaming. */
final class TestData {
    private TestData() {
    }

    static List<Telemetry> fixture() throws IOException {
        List<String> lines = Files.readAllLines(Path.of("..", "fixtures", "synthetic_telemetry.csv"), StandardCharsets.UTF_8);
        List<Telemetry> out = new ArrayList<>();
        for (String l : lines.subList(1, lines.size())) out.add(TelemetryCsv.parse(l));
        return out;
    }

    /**
     * What the streaming job should produce for a set of accepted samples, computed in batch:
     * group by (cell, window) and (cell, cycle), then walk cycles in order for the fade rules.
     * Returns sorted "TYPE|cell|alertId" strings for non-DQ alerts plus DQ_GAP alerts.
     */
    static TreeSet<String> oracle(List<Telemetry> accepted, Thresholds th) {
        TreeSet<String> out = new TreeSet<>();
        Map<String, WindowStats> windows = new TreeMap<>();
        Map<String, List<Telemetry>> cycles = new TreeMap<>();
        for (Telemetry t : accepted) {
            long ws = WindowStats.windowStart(t.eventTimeMs, th.windowMs);
            windows.computeIfAbsent(t.cellId + "|" + ws, k -> new WindowStats()).add(t, th);
            cycles.computeIfAbsent(t.cellId + "|" + String.format("%06d", t.cycle), k -> new ArrayList<>()).add(t);
        }
        windows.forEach((k, s) -> {
            String cell = k.split("\\|")[0];
            long ws = Long.parseLong(k.split("\\|")[1]);
            for (Alert a : WindowRules.evaluate(cell, ws, ws + th.windowMs, s, th, 0)) out.add(key(a));
        });
        List<CycleSummary> summaries = new ArrayList<>();
        cycles.forEach((k, xs) -> {
            CycleAnalyzer.Result r = CycleAnalyzer.analyze(xs.get(0).cellId, xs.get(0).cycle, xs, th, 0);
            summaries.add(r.summary);
            for (Alert a : r.gapAlerts) out.add(key(a));
        });
        summaries.sort(Comparator.comparing((CycleSummary s) -> s.cellId).thenComparingLong(s -> s.endMs));
        Map<String, FadeTracker> trackers = new TreeMap<>();
        for (CycleSummary s : summaries) {
            for (Alert a : trackers.computeIfAbsent(s.cellId, c -> new FadeTracker()).onCycle(s, th, 0)) out.add(key(a));
        }
        return out;
    }

    static String key(Alert a) {
        return a.alertType + "|" + a.cellId + "|" + a.alertId;
    }
}
