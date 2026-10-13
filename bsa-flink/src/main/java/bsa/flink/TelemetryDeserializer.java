package bsa.flink;

import bsa.core.codec.TelemetryCodec;
import bsa.core.model.Telemetry;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;

/** Kafka value bytes -> Telemetry. Unparseable records are counted and skipped (returning null skips). */
public class TelemetryDeserializer implements DeserializationSchema<Telemetry> {
    private static final long serialVersionUID = 1L;
    private transient Counter malformed;

    @Override
    public void open(InitializationContext context) {
        malformed = context.getMetricGroup().counter("malformedRecords");
    }

    @Override
    public Telemetry deserialize(byte[] message) {
        Telemetry t = TelemetryCodec.decodeOrNull(message);
        if (t == null && malformed != null) malformed.inc();
        return t;
    }

    @Override
    public boolean isEndOfStream(Telemetry nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Telemetry> getProducedType() {
        return TypeInformation.of(Telemetry.class);
    }
}
