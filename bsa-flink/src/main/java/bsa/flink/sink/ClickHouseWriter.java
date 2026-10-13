package bsa.flink.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Buffers rows and POSTs them as one JSONEachRow batch when (a) the batch is full, (b) the flush
 * interval elapsed, or (c) Flink asks for a flush before a checkpoint. All three run on the task's
 * mailbox thread, so no locking is needed.
 */
public class ClickHouseWriter<T> implements SinkWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseWriter.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ClickHouseConfig cfg;
    private final String table;
    private final ClickHouseSink.RowMapper<T> mapper;
    private final ProcessingTimeService timers;
    private final HttpClient http;
    private final URI insertUri;
    private final List<String> buffer = new ArrayList<>();
    private final Counter rowsOut;
    private final Counter retries;
    private boolean timerArmed;

    public ClickHouseWriter(ClickHouseConfig cfg, String table, ClickHouseSink.RowMapper<T> mapper,
                            ProcessingTimeService timers, SinkWriterMetricGroup metrics) {
        this.cfg = cfg;
        this.table = table;
        this.mapper = mapper;
        this.timers = timers;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        String query = "INSERT INTO " + cfg.database + "." + table + " FORMAT JSONEachRow";
        this.insertUri = URI.create(cfg.url + "/?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8));
        this.rowsOut = metrics == null ? new org.apache.flink.metrics.SimpleCounter() : metrics.counter("clickhouseRows_" + table);
        this.retries = metrics == null ? new org.apache.flink.metrics.SimpleCounter() : metrics.counter("clickhouseRetries_" + table);
    }

    @Override
    public void write(T element, Context context) throws IOException {
        buffer.add(JSON.writeValueAsString(mapper.apply(element)));
        if (buffer.size() >= cfg.batchSize) {
            send();
        } else {
            armTimer();
        }
    }

    private void armTimer() {
        if (timerArmed || timers == null || cfg.flushIntervalMs <= 0) return;
        timerArmed = true;
        timers.registerTimer(timers.getCurrentProcessingTime() + cfg.flushIntervalMs, ts -> {
            timerArmed = false;
            send();
        });
    }

    /** Called by Flink before a checkpoint (endOfInput=false) and at the end of a bounded input (true). */
    @Override
    public void flush(boolean endOfInput) throws IOException {
        send();
    }

    int buffered() {
        return buffer.size();
    }

    private void send() throws IOException {
        if (buffer.isEmpty()) return;
        String body = String.join("\n", buffer) + "\n";
        HttpRequest req = HttpRequest.newBuilder(insertUri)
                .timeout(Duration.ofSeconds(30))
                .header("X-ClickHouse-User", cfg.user)
                .header("X-ClickHouse-Key", cfg.password)
                .header("Content-Type", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        IOException last = null;
        for (int attempt = 0; attempt <= cfg.maxRetries; attempt++) {
            if (attempt > 0) {
                retries.inc();
                sleep(200L << Math.min(attempt, 5));
            }
            try {
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    rowsOut.inc(buffer.size());
                    buffer.clear();
                    return;
                }
                last = new IOException("ClickHouse " + table + " HTTP " + resp.statusCode() + ": " + abbreviate(resp.body()));
                if (resp.statusCode() >= 400 && resp.statusCode() < 500) break; // bad request/auth: retrying will not help
            } catch (IOException e) {
                last = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while writing to ClickHouse", e);
            }
            LOG.warn("insert into {} failed (attempt {}): {}", table, attempt + 1, last.getMessage());
        }
        // Failing the task makes Flink restart from the last checkpoint; rows are re-sent (at-least-once).
        throw last;
    }

    private static String abbreviate(String s) {
        return s == null ? "" : (s.length() > 300 ? s.substring(0, 300) + "..." : s.trim());
    }

    private static void sleep(long ms) throws IOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        // Rows still buffered here were not covered by a checkpoint; they will be replayed after restart.
        buffer.clear();
    }
}
