package bsa.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

/**
 * Minimal ClickHouse sink over the HTTP interface ({@code INSERT ... FORMAT JSONEachRow}).
 *
 * <p>Delivery guarantee: <b>at-least-once</b>. {@link ClickHouseWriter#flush(boolean)} is called by
 * Flink before every checkpoint barrier is passed on, so everything that came before a completed
 * checkpoint is in ClickHouse. After a failure Flink rewinds to the last checkpoint and re-sends
 * what came after it, so some rows can be written twice. That is made harmless (effectively-once
 * results) on the table side: every row has a deterministic key and the tables are
 * ReplacingMergeTree, which collapses rows with the same sorting key. See README "Delivery semantics".
 */
public class ClickHouseSink<T> implements Sink<T> {
    private static final long serialVersionUID = 1L;

    /** Serializable row mapper: object -> column map, serialised as one JSONEachRow line. */
    public interface RowMapper<T> extends Function<T, Map<String, Object>>, Serializable {
    }

    private final ClickHouseConfig config;
    private final String table;
    private final RowMapper<T> mapper;

    public ClickHouseSink(ClickHouseConfig config, String table, RowMapper<T> mapper) {
        this.config = config;
        this.table = table;
        this.mapper = mapper;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return new ClickHouseWriter<>(config, table, mapper, context.getProcessingTimeService(),
                context.metricGroup());
    }

    @Override
    @Deprecated
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        throw new UnsupportedOperationException("Flink 1.20 calls createWriter(WriterInitContext)");
    }
}
