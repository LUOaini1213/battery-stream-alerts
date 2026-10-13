package bsa.core.codec;

import bsa.core.model.Telemetry;

/**
 * Reads the converter's CSV layout:
 * {@code event_time_ms,cell_id,cycle,seq,voltage_v,current_a,temperature_c}.
 */
public final class TelemetryCsv {
    public static final String HEADER = "event_time_ms,cell_id,cycle,seq,voltage_v,current_a,temperature_c";

    private TelemetryCsv() {
    }

    public static Telemetry parse(String line) {
        String[] p = line.split(",", -1);
        if (p.length != 7) {
            throw new IllegalArgumentException("expected 7 columns, got " + p.length + ": " + line);
        }
        return Telemetry.sample(p[1].trim(), Integer.parseInt(p[2].trim()), Integer.parseInt(p[3].trim()),
                Long.parseLong(p[0].trim()), Double.parseDouble(p[4].trim()), Double.parseDouble(p[5].trim()),
                Double.parseDouble(p[6].trim()));
    }
}
