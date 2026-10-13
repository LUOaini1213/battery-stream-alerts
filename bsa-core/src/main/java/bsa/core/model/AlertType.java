package bsa.core.model;

/** Every alert the job can raise. DQ_* are data-quality alerts about the feed itself. */
public enum AlertType {
    /** Max temperature in a window above the configured limit. */
    OVER_TEMPERATURE,
    /** Voltage falling faster than the configured rate while the cell is under load. */
    VOLTAGE_SAG,
    /** Cycle capacity clearly below the rolling mean of the previous cycles. */
    CAPACITY_DROP,
    /** State of health (capacity / initial capacity) crossed the end-of-life line. Raised once per cell. */
    CAPACITY_EOL,
    /** Missing samples (sequence hole) or a time gap inside a cycle. */
    DQ_GAP,
    /** Physically implausible value or NaN. The sample is dropped. */
    DQ_OUT_OF_RANGE,
    /** Same (cell, cycle, seq) seen again. The copy is dropped. */
    DQ_DUPLICATE,
    /** Arrived after the watermark had passed its event time. The sample is dropped. */
    DQ_LATE;

    public boolean isDataQuality() {
        return name().startsWith("DQ_");
    }
}
