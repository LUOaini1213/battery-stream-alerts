package bsa.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

/**
 * One telemetry record as it travels on the Kafka topic.
 *
 * <p>Public fields + a no-arg constructor make this a Flink POJO, so Flink serialises it with its
 * POJO serializer instead of falling back to Kryo.
 *
 * <p>{@code kind = "heartbeat"} records carry no measurement. The replayer sends them to every
 * partition so that event time (and therefore the watermark) advances on all partitions even when
 * a partition has no cell data. The job drops them right after watermark assignment.
 */
public class Telemetry {
    public static final String KIND_SAMPLE = "sample";
    public static final String KIND_HEARTBEAT = "heartbeat";

    public String kind = KIND_SAMPLE;
    public String cellId;
    /** Discharge-cycle counter as reported by the (simulated) BMS. */
    public int cycle;
    /** Sample index inside the cycle, starting at 0. (cellId, cycle, seq) identifies a sample. */
    public int seq;
    /** When the measurement was taken (event time), epoch millis. */
    public long eventTimeMs;
    public double voltageV;
    public double currentA;
    public double temperatureC;
    /** Wall-clock millis when the replayer produced the record; used only for latency stats. */
    public long ingestMs;

    public Telemetry() {
    }

    public static Telemetry sample(String cellId, int cycle, int seq, long eventTimeMs,
                                   double voltageV, double currentA, double temperatureC) {
        Telemetry t = new Telemetry();
        t.cellId = cellId;
        t.cycle = cycle;
        t.seq = seq;
        t.eventTimeMs = eventTimeMs;
        t.voltageV = voltageV;
        t.currentA = currentA;
        t.temperatureC = temperatureC;
        return t;
    }

    public static Telemetry heartbeat(long eventTimeMs) {
        Telemetry t = new Telemetry();
        t.kind = KIND_HEARTBEAT;
        t.cellId = "";
        t.eventTimeMs = eventTimeMs;
        return t;
    }

    @JsonIgnore
    public boolean isHeartbeat() {
        return KIND_HEARTBEAT.equals(kind);
    }

    /** Identity of a measurement inside one cell; duplicates share this key. */
    @JsonIgnore
    public String sampleKey() {
        return cycle + ":" + seq;
    }

    public Telemetry copy() {
        Telemetry t = new Telemetry();
        t.kind = kind;
        t.cellId = cellId;
        t.cycle = cycle;
        t.seq = seq;
        t.eventTimeMs = eventTimeMs;
        t.voltageV = voltageV;
        t.currentA = currentA;
        t.temperatureC = temperatureC;
        t.ingestMs = ingestMs;
        return t;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Telemetry)) return false;
        Telemetry t = (Telemetry) o;
        return cycle == t.cycle && seq == t.seq && eventTimeMs == t.eventTimeMs
                && Double.compare(voltageV, t.voltageV) == 0
                && Double.compare(currentA, t.currentA) == 0
                && Double.compare(temperatureC, t.temperatureC) == 0
                && ingestMs == t.ingestMs
                && Objects.equals(kind, t.kind) && Objects.equals(cellId, t.cellId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, cellId, cycle, seq, eventTimeMs);
    }

    @Override
    public String toString() {
        return kind + "{" + cellId + " c" + cycle + " s" + seq + " t=" + eventTimeMs
                + " V=" + voltageV + " I=" + currentA + " T=" + temperatureC + "}";
    }
}
