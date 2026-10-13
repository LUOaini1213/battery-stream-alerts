package bsa.flink;

import bsa.core.model.Telemetry;
import bsa.core.rules.Thresholds;
import bsa.flink.sink.ClickHouseConfig;
import bsa.flink.sink.ClickHouseSink;
import bsa.flink.sink.Rows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/**
 * Entry point. Runs the pipeline in an embedded Flink MiniCluster inside this JVM (local
 * environment): same DataStream API, same operators, state and checkpoints as on a cluster, minus a
 * separate JobManager/TaskManager deployment. That keeps the demo inside a ~1 GB container.
 *
 * <p>All settings come from environment variables (see docker-compose.yml).
 */
public final class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    private StreamingJob() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> env = System.getenv();
        Thresholds th = Thresholds.fromEnv(env);
        String bootstrap = env.getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = env.getOrDefault("KAFKA_TOPIC", "bms.telemetry");
        String group = env.getOrDefault("KAFKA_GROUP", "bsa-alert-job");
        int parallelism = Integer.parseInt(env.getOrDefault("FLINK_PARALLELISM", "2"));
        long checkpointMs = Long.parseLong(env.getOrDefault("CHECKPOINT_INTERVAL_MS", "10000"));
        long maxDriftMs = Long.parseLong(env.getOrDefault("WATERMARK_MAX_DRIFT_MS", "600000"));
        long alignUpdateMs = Long.parseLong(env.getOrDefault("WATERMARK_ALIGN_UPDATE_MS", "50"));
        String checkpointDir = env.getOrDefault("CHECKPOINT_DIR", "/tmp/bsa-checkpoints");
        ClickHouseConfig ch = new ClickHouseConfig(
                env.getOrDefault("CLICKHOUSE_URL", "http://localhost:8123"),
                env.getOrDefault("CLICKHOUSE_DB", "bsa"),
                env.getOrDefault("CLICKHOUSE_USER", "bsa_writer"),
                env.getOrDefault("CLICKHOUSE_PASSWORD", ""),
                Integer.parseInt(env.getOrDefault("CLICKHOUSE_BATCH_SIZE", "5000")),
                Long.parseLong(env.getOrDefault("CLICKHOUSE_FLUSH_MS", "1000")),
                3);

        Configuration conf = new Configuration();
        conf.setString("state.backend.type", "hashmap");
        conf.setString("state.checkpoints.dir", Path.of(checkpointDir).toUri().toString());
        conf.setString("state.checkpoints.num-retained", "2");
        conf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
        conf.setString("restart-strategy.type", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "10");
        conf.setString("restart-strategy.fixed-delay.delay", "5 s");
        conf.setString("taskmanager.memory.managed.size", "64m");
        conf.setString("taskmanager.numberOfTaskSlots", Integer.toString(parallelism));
        Optional<Path> resume = CheckpointLocator.latest(Path.of(checkpointDir));
        if (resume.isPresent() && !"false".equalsIgnoreCase(env.get("RESUME_FROM_CHECKPOINT"))) {
            LOG.info("Restoring from retained checkpoint {}", resume.get());
            conf.setString("execution.savepoint.path", resume.get().toUri().toString());
        } else {
            LOG.info("No retained checkpoint found under {}, starting fresh", checkpointDir);
        }

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        sEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        sEnv.setParallelism(parallelism);
        sEnv.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
        sEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointMs / 2);

        // Starting offsets only matter without restored state: committed group offsets, else earliest.
        KafkaSource<Telemetry> source = KafkaSource.<Telemetry>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new TelemetryDeserializer())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();

        DataStream<Telemetry> input = sEnv.fromSource(source,
                        TelemetryWatermarks.alignedStrategy(th.outOfOrdernessMs, maxDriftMs, alignUpdateMs), "kafka-telemetry")
                .uid("kafka-telemetry");

        AlertPipeline.Outputs out = AlertPipeline.build(input, th);
        out.accepted.sinkTo(new ClickHouseSink<>(ch, "telemetry", Rows.TELEMETRY)).name("ch-telemetry").uid("ch-telemetry");
        out.alerts.sinkTo(new ClickHouseSink<>(ch, "alerts", Rows.ALERT)).name("ch-alerts").uid("ch-alerts");
        out.cycles.sinkTo(new ClickHouseSink<>(ch, "cycles", Rows.CYCLE)).name("ch-cycles").uid("ch-cycles");

        LOG.info("Starting job: kafka={} topic={} parallelism={} checkpoint={}ms clickhouse={}",
                bootstrap, topic, parallelism, checkpointMs, ch);
        sEnv.execute("battery-stream-alerts");
    }
}
