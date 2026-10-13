package bsa.core.model;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * An alert produced by the streaming job.
 *
 * <p>{@link #alertId} is deterministic: it is derived only from (cell, type, discriminator), where
 * the discriminator is the window start, the cycle, or the sample key. Re-processing the same input
 * therefore yields the same id, which is what makes the ClickHouse sink idempotent.
 */
public class Alert {
    public String alertId;
    public String cellId;
    public AlertType alertType;
    public Severity severity;
    /** Event time the alert refers to (window end - 1, cycle end, or the sample time). */
    public long eventTimeMs;
    public long windowStartMs;
    public long windowEndMs;
    /** Cycle number, or -1 when the alert is not tied to one cycle. */
    public int cycle = -1;
    /** Measured value that triggered the rule (unit depends on the type). */
    public double value;
    public double threshold;
    public String detail;
    /** Latest replayer wall-clock time among the inputs that produced this alert. */
    public long maxIngestMs;
    /** Wall-clock time the job emitted the alert. */
    public long emittedMs;

    public Alert() {
    }

    public static String idFor(String cellId, AlertType type, String discriminator) {
        String raw = cellId + "|" + type.name() + "|" + discriminator;
        return UUID.nameUUIDFromBytes(raw.getBytes(StandardCharsets.UTF_8)).toString();
    }

    public static Alert of(String cellId, AlertType type, Severity severity, String discriminator) {
        Alert a = new Alert();
        a.alertId = idFor(cellId, type, discriminator);
        a.cellId = cellId;
        a.alertType = type;
        a.severity = severity;
        return a;
    }

    @Override
    public String toString() {
        return alertType + "{" + cellId + " c" + cycle + " t=" + eventTimeMs + " v=" + value
                + " th=" + threshold + " " + detail + "}";
    }
}
