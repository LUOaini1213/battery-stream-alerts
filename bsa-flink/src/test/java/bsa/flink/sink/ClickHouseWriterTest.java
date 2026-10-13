package bsa.flink.sink;

import bsa.core.model.Alert;
import bsa.core.model.AlertType;
import bsa.core.model.CycleSummary;
import bsa.core.model.Severity;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The writer against a fake ClickHouse HTTP endpoint (JDK built-in server, no Docker). */
class ClickHouseWriterTest {
    private HttpServer server;
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private final List<String> queries = new CopyOnWriteArrayList<>();
    private final List<String> users = new CopyOnWriteArrayList<>();
    private final AtomicInteger failuresLeft = new AtomicInteger();
    private volatile int failureStatus = 500;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            byte[] body = ex.getRequestBody().readAllBytes();
            int status = failuresLeft.getAndDecrement() > 0 ? failureStatus : 200;
            if (status == 200) {
                bodies.add(new String(body, StandardCharsets.UTF_8));
                queries.add(URLDecoder.decode(ex.getRequestURI().getRawQuery(), StandardCharsets.UTF_8));
                users.add(ex.getRequestHeaders().getFirst("X-ClickHouse-User") + ":" + ex.getRequestHeaders().getFirst("X-ClickHouse-Key"));
            }
            byte[] resp = (status == 200 ? "" : "Code: 999. boom").getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(status, resp.length == 0 ? -1 : resp.length);
            if (resp.length > 0) ex.getResponseBody().write(resp);
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private ClickHouseWriter<Map<String, Object>> writer(int batch, int retries) {
        ClickHouseConfig cfg = new ClickHouseConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/",
                "bsa", "w", "secret", batch, 0, retries);
        return new ClickHouseWriter<>(cfg, "alerts", m -> m, null, null);
    }

    @Test
    void batchesBySizeAndFlushesRestOnCheckpoint() throws Exception {
        var w = writer(3, 0);
        for (int i = 0; i < 7; i++) w.write(Map.of("i", i), null);
        assertEquals(2, bodies.size(), "two full batches of 3");
        assertEquals(1, w.buffered());
        w.flush(false); // what Flink calls before a checkpoint barrier
        assertEquals(3, bodies.size());
        assertEquals(0, w.buffered());
        assertEquals("{\"i\":0}\n{\"i\":1}\n{\"i\":2}\n", bodies.get(0));
        assertEquals("query=INSERT INTO bsa.alerts FORMAT JSONEachRow", queries.get(0));
        assertEquals("w:secret", users.get(0));
        w.flush(true);
        assertEquals(3, bodies.size(), "empty flush sends nothing");
    }

    @Test
    void retriesServerErrorsThenSucceeds() throws Exception {
        failuresLeft.set(2);
        var w = writer(1, 3);
        w.write(Map.of("a", 1), null);
        assertEquals(1, bodies.size());
        assertEquals(0, w.buffered());
    }

    @Test
    void givesUpAfterRetriesAndKeepsRowsForReplay() throws Exception {
        failuresLeft.set(10);
        var w = writer(1, 1);
        IOException e = assertThrows(IOException.class, () -> w.write(Map.of("a", 1), null));
        assertTrue(e.getMessage().contains("HTTP 500"), e.getMessage());
        assertEquals(1, w.buffered());
    }

    @Test
    void clientErrorsAreNotRetried() throws Exception {
        failureStatus = 403;
        failuresLeft.set(1);
        var w = writer(1, 5);
        assertThrows(IOException.class, () -> w.write(Map.of("a", 1), null));
        assertEquals(0, failuresLeft.get(), "exactly one attempt");
    }

    @Test
    void rowMappersUseSchemaColumnsAndNullForNaN() {
        Alert a = Alert.of("B0005", AlertType.VOLTAGE_SAG, Severity.WARN, "1");
        a.value = Double.NaN;
        Map<String, Object> row = Rows.ALERT.apply(a);
        assertEquals("VOLTAGE_SAG", row.get("alert_type"));
        assertNull(row.get("value"));
        assertEquals("", row.get("detail"));
        CycleSummary s = new CycleSummary();
        s.cellId = "B0005";
        s.capacityAh = 1.8;
        Map<String, Object> c = Rows.CYCLE.apply(s);
        assertNull(c.get("soh"));
        assertEquals(1.8, c.get("capacity_ah"));
    }
}
