package bsa.core.model;

/** What the job knows about one discharge cycle once its session window has closed. */
public class CycleSummary {
    public String cellId;
    public int cycle;
    public long startMs;
    public long endMs;
    public int samples;
    /** Samples missing according to sequence numbers (holes inside the cycle). */
    public int missingSamples;
    /** Discharged charge: integral of discharge current over time, trapezoid rule, in Ah. */
    public double capacityAh;
    public double minVoltageV;
    public double maxTemperatureC;
    /** Set by the fade tracker: mean of the first N cycles of this cell (NaN until known). */
    public double initialCapacityAh = Double.NaN;
    /** Set by the fade tracker: mean of the previous M cycles (NaN until known). */
    public double rollingBaselineAh = Double.NaN;
    /** capacityAh / initialCapacityAh (NaN until known). */
    public double soh = Double.NaN;
    public long maxIngestMs;

    public CycleSummary() {
    }

    @Override
    public String toString() {
        return "Cycle{" + cellId + " c" + cycle + " n=" + samples + " cap=" + capacityAh + " soh=" + soh + "}";
    }
}
