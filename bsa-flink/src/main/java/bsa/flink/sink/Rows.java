package bsa.flink.sink;

import bsa.core.model.Alert;
import bsa.core.model.CycleSummary;
import bsa.core.model.Telemetry;

import java.util.LinkedHashMap;
import java.util.Map;

/** Object -> ClickHouse column maps. Column names match clickhouse/init/01-schema.sh. */
public final class Rows {
    private Rows() {
    }

    public static final ClickHouseSink.RowMapper<Telemetry> TELEMETRY = t -> {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("cell_id", t.cellId);
        m.put("cycle", t.cycle);
        m.put("seq", t.seq);
        m.put("event_time_ms", t.eventTimeMs);
        m.put("voltage_v", t.voltageV);
        m.put("current_a", t.currentA);
        m.put("temperature_c", t.temperatureC);
        m.put("ingest_ms", t.ingestMs);
        return m;
    };

    public static final ClickHouseSink.RowMapper<Alert> ALERT = a -> {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("alert_id", a.alertId);
        m.put("cell_id", a.cellId);
        m.put("alert_type", a.alertType.name());
        m.put("severity", a.severity.name());
        m.put("event_time_ms", a.eventTimeMs);
        m.put("window_start_ms", a.windowStartMs);
        m.put("window_end_ms", a.windowEndMs);
        m.put("cycle", a.cycle);
        m.put("value", finite(a.value));
        m.put("threshold", finite(a.threshold));
        m.put("detail", a.detail == null ? "" : a.detail);
        m.put("max_ingest_ms", a.maxIngestMs);
        m.put("emitted_ms", a.emittedMs);
        return m;
    };

    public static final ClickHouseSink.RowMapper<CycleSummary> CYCLE = s -> {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("cell_id", s.cellId);
        m.put("cycle", s.cycle);
        m.put("start_ms", s.startMs);
        m.put("end_ms", s.endMs);
        m.put("samples", s.samples);
        m.put("missing_samples", s.missingSamples);
        m.put("capacity_ah", s.capacityAh);
        m.put("min_voltage_v", s.minVoltageV);
        m.put("max_temperature_c", s.maxTemperatureC);
        m.put("initial_capacity_ah", finite(s.initialCapacityAh));
        m.put("rolling_baseline_ah", finite(s.rollingBaselineAh));
        m.put("soh", finite(s.soh));
        m.put("max_ingest_ms", s.maxIngestMs);
        return m;
    };

    /** JSON has no NaN/Infinity; such values become NULL (Nullable columns) or 0. */
    static Double finite(double v) {
        return Double.isFinite(v) ? v : null;
    }
}
