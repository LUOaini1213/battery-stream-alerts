package bsa.core;

import bsa.core.codec.TelemetryCodec;
import bsa.core.codec.TelemetryCsv;
import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.Telemetry;
import bsa.core.rules.Thresholds;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CodecAndConfigTest {

    @Test
    void jsonRoundTripUsesSnakeCase() {
        Telemetry t = Telemetry.sample("B0005", 2, 3, 1207149941593L, 4.19, -2.0, 24.3);
        t.ingestMs = 42;
        String json = new String(TelemetryCodec.encode(t), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"cell_id\":\"B0005\""), json);
        assertTrue(json.contains("\"event_time_ms\":1207149941593"), json);
        assertFalse(json.contains("heartbeat\":"), "derived getters must not leak into the wire format: " + json);
        assertEquals(t, TelemetryCodec.decodeOrNull(json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void heartbeatRoundTrip() {
        Telemetry hb = TelemetryCodec.decodeOrNull(TelemetryCodec.encode(Telemetry.heartbeat(99)));
        assertNotNull(hb);
        assertTrue(hb.isHeartbeat());
        assertEquals(99, hb.eventTimeMs);
    }

    @Test
    void malformedRecordsDecodeToNull() {
        assertNull(TelemetryCodec.decodeOrNull(null));
        assertNull(TelemetryCodec.decodeOrNull("not json".getBytes()));
        assertNull(TelemetryCodec.decodeOrNull("{\"kind\":\"sample\",\"cycle\":1}".getBytes()));
        assertNull(TelemetryCodec.decodeOrNull("{\"kind\":null,\"cell_id\":\"x\"}".getBytes()));
        assertNotNull(TelemetryCodec.decodeOrNull("{\"kind\":\"sample\",\"cell_id\":\"x\",\"extra\":1}".getBytes()));
    }

    @Test
    void csvParsing() {
        Telemetry t = TelemetryCsv.parse("1207149941593,B0005,1,0,4.191492,-0.004902,24.3300");
        assertEquals("B0005", t.cellId);
        assertEquals(1, t.cycle);
        assertEquals(0, t.seq);
        assertEquals(1207149941593L, t.eventTimeMs);
        assertEquals(-0.004902, t.currentA);
        assertThrows(IllegalArgumentException.class, () -> TelemetryCsv.parse("1,2,3"));
    }

    @Test
    void syntheticFixtureParses() throws Exception {
        List<Telemetry> rows = new ArrayList<>();
        try (BufferedReader r = java.nio.file.Files.newBufferedReader(
                java.nio.file.Path.of("..", "fixtures", "synthetic_telemetry.csv"), StandardCharsets.UTF_8)) {
            assertEquals(TelemetryCsv.HEADER, r.readLine());
            for (String line = r.readLine(); line != null; line = r.readLine()) rows.add(TelemetryCsv.parse(line));
        }
        assertTrue(rows.size() > 50);
    }

    @Test
    void alertIdsAreDeterministicAndDistinct() {
        assertEquals(Alert.idFor("a", AlertType.DQ_GAP, "1"), Alert.idFor("a", AlertType.DQ_GAP, "1"));
        assertNotEquals(Alert.idFor("a", AlertType.DQ_GAP, "1"), Alert.idFor("a", AlertType.DQ_GAP, "2"));
        assertNotEquals(Alert.idFor("a", AlertType.DQ_GAP, "1"), Alert.idFor("b", AlertType.DQ_GAP, "1"));
        assertNotEquals(Alert.idFor("a", AlertType.DQ_GAP, "1"), Alert.idFor("a", AlertType.DQ_LATE, "1"));
        assertTrue(AlertType.DQ_LATE.isDataQuality());
        assertFalse(AlertType.VOLTAGE_SAG.isDataQuality());
    }

    @Test
    void thresholdsFromEnvAndValidation() {
        Thresholds t = Thresholds.fromEnv(Map.of("BSA_OVER_TEMPERATURE_C", "45.5", "BSA_WINDOW_MS", " 60000 ",
                "BSA_SAG_V_PER_MIN", ""));
        assertEquals(45.5, t.overTemperatureC);
        assertEquals(60_000, t.windowMs);
        assertEquals(-0.3, t.sagVoltsPerMinute);
        assertThrows(IllegalArgumentException.class, () -> Thresholds.fromEnv(Map.of("BSA_SESSION_GAP_MS", "1000")));
        assertThrows(IllegalArgumentException.class, () -> Thresholds.fromEnv(Map.of("BSA_WINDOW_MS", "0")));
        assertThrows(IllegalArgumentException.class, () -> Thresholds.fromEnv(Map.of("BSA_OUT_OF_ORDERNESS_MS", "-1")));
        assertThrows(IllegalArgumentException.class, () -> Thresholds.fromEnv(Map.of("BSA_DEDUP_RETENTION_MS", "10")));
        Thresholds bad = new Thresholds();
        bad.baselineCycles = 0;
        assertThrows(IllegalArgumentException.class, bad::validate);
    }
}
