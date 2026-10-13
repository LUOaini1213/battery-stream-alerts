package bsa.flink.sink;

import java.io.Serializable;

/** Connection settings for the HTTP interface. The password is never logged. */
public class ClickHouseConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String url;       // e.g. http://clickhouse:8123
    public final String database;
    public final String user;
    public final String password;
    public final int batchSize;
    public final long flushIntervalMs;
    public final int maxRetries;

    public ClickHouseConfig(String url, String database, String user, String password,
                            int batchSize, long flushIntervalMs, int maxRetries) {
        this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        this.database = database;
        this.user = user;
        this.password = password;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.maxRetries = maxRetries;
    }

    @Override
    public String toString() {
        return "ClickHouseConfig{" + url + "/" + database + " user=" + user + " batch=" + batchSize
                + " flushMs=" + flushIntervalMs + "}";
    }
}
