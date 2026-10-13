package bsa.replayer;

import bsa.core.codec.TelemetryCodec;
import bsa.core.codec.TelemetryCsv;
import bsa.core.model.Telemetry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes the converted CSV to Kafka as if it were live BMS telemetry.
 *
 * <pre>
 * java -jar bsa-replayer.jar --csv /data/telemetry.csv --bootstrap redpanda:9092 --topic bms.telemetry
 *      [--partitions 4] [--speedup 2000] [--max-sleep-ms 50] [--heartbeat-ms 60000]
 *      [--faults dup=40,reorder=40,late=20,oor=20,drop=20] [--seed 42] [--limit N] [--report out.json]
 * </pre>
 *
 * Message key = cell id, so all samples of a cell land in one partition and keep their order.
 * Heartbeats are sent to every partition explicitly. --speedup 2000 means 2000 s of event time per
 * wall-clock second; long idle stretches between cycles are capped at --max-sleep-ms each.
 * --speedup 0 sends as fast as the broker accepts.
 */
public final class Replayer {

    private Replayer() {
    }

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = parseArgs(argv);
        Path csv = Path.of(req(a, "csv"));
        String bootstrap = a.getOrDefault("bootstrap", "localhost:9092");
        String topic = a.getOrDefault("topic", "bms.telemetry");
        int partitions = Integer.parseInt(a.getOrDefault("partitions", "4"));
        double speedup = Double.parseDouble(a.getOrDefault("speedup", "2000"));
        long maxSleepMs = Long.parseLong(a.getOrDefault("max-sleep-ms", "50"));
        long heartbeatMs = Long.parseLong(a.getOrDefault("heartbeat-ms", "60000"));
        long tailMs = Long.parseLong(a.getOrDefault("tail-ms", "3600000"));
        long seed = Long.parseLong(a.getOrDefault("seed", "42"));
        int limit = Integer.parseInt(a.getOrDefault("limit", "0"));
        ReplayPlan.FaultCounts faults = ReplayPlan.FaultCounts.parse(a.getOrDefault("faults", "none"));

        List<Telemetry> samples = readCsv(csv, limit);
        ReplayPlan plan = ReplayPlan.build(samples, faults, seed, heartbeatMs, tailMs);
        System.out.printf("plan: %d samples, %d deliveries, faults=%s%n", samples.size(), plan.deliveries.size(), plan.injected.keySet());

        ensureTopic(bootstrap, topic, partitions);

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        // Idempotent producer: broker drops retried duplicates, and ordering per partition is kept.
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        AtomicLong acked = new AtomicLong();
        AtomicReference<Exception> sendError = new AtomicReference<>();
        long sentSamples = 0;
        long sentHeartbeats = 0;
        long wallStart = System.currentTimeMillis();
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(p)) {
            Pacer pacer = new Pacer(speedup, maxSleepMs);
            for (ReplayPlan.Delivery d : plan.deliveries) {
                pacer.await(d.deliveryTimeMs);
                if (sendError.get() != null) throw sendError.get();
                Telemetry t = d.record.copy();
                t.ingestMs = System.currentTimeMillis();
                byte[] value = TelemetryCodec.encode(t);
                if (t.isHeartbeat()) {
                    for (int part = 0; part < partitions; part++) {
                        producer.send(new ProducerRecord<>(topic, part, null, value), (m, e) -> {
                            if (e != null) sendError.compareAndSet(null, e);
                            else acked.incrementAndGet();
                        });
                    }
                    sentHeartbeats++;
                } else {
                    producer.send(new ProducerRecord<>(topic, t.cellId, value), (m, e) -> {
                        if (e != null) sendError.compareAndSet(null, e);
                        else acked.incrementAndGet();
                    });
                    sentSamples++;
                }
            }
            producer.flush();
        }
        if (sendError.get() != null) throw sendError.get();
        long wallMs = System.currentTimeMillis() - wallStart;

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("finished_at", Instant.now().toString());
        report.put("csv_rows", samples.size());
        report.put("sample_records_sent", sentSamples);
        report.put("heartbeats_sent", sentHeartbeats);
        report.put("heartbeat_records_sent", sentHeartbeats * partitions);
        report.put("records_acked", acked.get());
        report.put("partitions", partitions);
        report.put("speedup", speedup);
        report.put("wall_ms", wallMs);
        report.put("records_per_s", wallMs == 0 ? 0 : Math.round(1000.0 * (sentSamples + sentHeartbeats * partitions) / wallMs));
        report.put("seed", seed);
        Map<String, Integer> injectedCounts = new LinkedHashMap<>();
        plan.injected.forEach((k, v) -> injectedCounts.put(k.name(), v.size()));
        report.put("injected", injectedCounts);
        report.put("expected_dq_alerts", plan.expectedDqAlerts());
        report.put("injected_samples", plan.injected);
        String json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(report);
        if (a.containsKey("report")) Files.writeString(Path.of(a.get("report")), json);
        System.out.println(json);
    }

    static List<Telemetry> readCsv(Path csv, int limit) throws Exception {
        List<Telemetry> out = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
            String header = r.readLine();
            if (!TelemetryCsv.HEADER.equals(header)) throw new IllegalArgumentException("unexpected header: " + header);
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                if (line.isBlank()) continue;
                out.add(TelemetryCsv.parse(line));
                if (limit > 0 && out.size() >= limit) break;
            }
        }
        return out;
    }

    static void ensureTopic(String bootstrap, String topic, int partitions) throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(p)) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
                System.out.printf("created topic %s with %d partitions%n", topic, partitions);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) throw e;
                int existing = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions().size();
                if (existing != partitions) {
                    throw new IllegalStateException("topic " + topic + " has " + existing + " partitions, expected " + partitions);
                }
            }
        }
    }

    static Map<String, String> parseArgs(String[] argv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < argv.length; i++) {
            if (!argv[i].startsWith("--")) throw new IllegalArgumentException("unexpected argument " + argv[i]);
            String k = argv[i].substring(2);
            if (i + 1 >= argv.length) throw new IllegalArgumentException("missing value for --" + k);
            m.put(k, argv[++i]);
        }
        return m;
    }

    private static String req(Map<String, String> a, String k) {
        String v = a.get(k);
        if (v == null) throw new IllegalArgumentException("--" + k + " is required");
        return v;
    }

    /** Maps event-time progress to wall-clock sleeps; each gap is capped at maxSleepMs. */
    static final class Pacer {
        private final double speedup;
        private final long maxSleepMs;
        private long lastEventMs = Long.MIN_VALUE;
        private long wallTarget;

        Pacer(double speedup, long maxSleepMs) {
            this.speedup = speedup;
            this.maxSleepMs = maxSleepMs;
        }

        /** Returns the wall-clock delay (ms) this event added; sleeps unless speedup is 0. */
        long await(long eventMs) throws InterruptedException {
            if (speedup <= 0) return 0;
            if (lastEventMs == Long.MIN_VALUE) {
                lastEventMs = eventMs;
                wallTarget = System.nanoTime();
                return 0;
            }
            long gap = Math.max(0, eventMs - lastEventMs);
            lastEventMs = Math.max(lastEventMs, eventMs);
            long add = Math.min((long) (gap * 1_000_000.0 / speedup), maxSleepMs * 1_000_000L);
            wallTarget += add;
            long sleepNs = wallTarget - System.nanoTime();
            if (sleepNs > 0) Thread.sleep(sleepNs / 1_000_000L, (int) (sleepNs % 1_000_000L));
            return add / 1_000_000L;
        }
    }
}
