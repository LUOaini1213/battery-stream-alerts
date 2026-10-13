package bsa.core.codec;

import bsa.core.model.Telemetry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * JSON wire format shared by the replayer (producer) and the Flink job (consumer), so both sides
 * always agree on field names. Example:
 * <pre>{"kind":"sample","cell_id":"B0005","cycle":1,"seq":0,"event_time_ms":1207149941593,
 *  "voltage_v":4.19,"current_a":-0.0049,"temperature_c":24.33,"ingest_ms":1760000000000}</pre>
 */
public final class TelemetryCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private TelemetryCodec() {
    }

    public static byte[] encode(Telemetry t) {
        try {
            return MAPPER.writeValueAsBytes(t);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Returns null for bytes that are not a valid record (the job counts and drops them). */
    public static Telemetry decodeOrNull(byte[] bytes) {
        if (bytes == null) return null;
        try {
            Telemetry t = MAPPER.readValue(bytes, Telemetry.class);
            if (t.kind == null) return null;
            if (!t.isHeartbeat() && (t.cellId == null || t.cellId.isEmpty())) return null;
            return t;
        } catch (IOException e) {
            return null;
        }
    }
}
