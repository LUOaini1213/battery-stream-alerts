package bsa.api;

import bsa.api.Dtos.AlertView;
import bsa.api.Dtos.CellSummary;
import bsa.api.Dtos.CycleView;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * All SQL lives here. Every user-supplied value is a named bind parameter (:cellId, :sinceMs, ...);
 * nothing from the request is concatenated into SQL text.
 *
 * <p>FINAL: the tables are ReplacingMergeTree, which removes rows with the same sorting key only
 * when parts merge in the background. FINAL applies that de-duplication at query time, so a row
 * written twice by an at-least-once retry is still counted once. Fine at this data size; for large
 * tables one would pre-aggregate or use argMax per key instead.
 */
@Repository
public class BatteryRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public BatteryRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String ALERT_COLUMNS =
            "toString(alert_id) AS alert_id, cell_id, alert_type, severity, event_time_ms, cycle, value, threshold, detail, emitted_ms";

    private static final RowMapper<AlertView> ALERT_ROW = (rs, n) -> new AlertView(
            rs.getString("alert_id"), rs.getString("cell_id"), rs.getString("alert_type"), rs.getString("severity"),
            Instant.ofEpochMilli(rs.getLong("event_time_ms")),
            rs.getInt("cycle") < 0 ? null : rs.getInt("cycle"),
            rs.getDouble("value"), rs.getDouble("threshold"), rs.getString("detail"),
            Instant.ofEpochMilli(rs.getLong("emitted_ms")));

    public List<CellSummary> cells() {
        Map<String, long[]> telemetry = new TreeMap<>();
        jdbc.query("SELECT cell_id, count() AS n, min(event_time_ms) AS first_ms, max(event_time_ms) AS last_ms "
                        + "FROM bsa.telemetry FINAL GROUP BY cell_id",
                rs -> {
                    telemetry.put(rs.getString("cell_id"),
                            new long[]{rs.getLong("n"), rs.getLong("first_ms"), rs.getLong("last_ms")});
                });
        Map<String, Object[]> cycles = new TreeMap<>();
        jdbc.query("SELECT cell_id, count() AS n, max(cycle) AS latest_cycle, "
                        + "argMax(capacity_ah, cycle) AS latest_cap, argMax(soh, cycle) AS latest_soh, "
                        + "argMax(initial_capacity_ah, cycle) AS initial_cap "
                        + "FROM bsa.cycles FINAL GROUP BY cell_id",
                rs -> {
                    cycles.put(rs.getString("cell_id"), new Object[]{rs.getLong("n"), rs.getInt("latest_cycle"),
                            nullableDouble(rs, "initial_cap"), rs.getDouble("latest_cap"), nullableDouble(rs, "latest_soh")});
                });
        Map<String, Map<String, Long>> counts = new TreeMap<>();
        jdbc.query("SELECT cell_id, alert_type, count() AS n FROM bsa.alerts FINAL GROUP BY cell_id, alert_type",
                rs -> {
                    counts.computeIfAbsent(rs.getString("cell_id"), k -> new TreeMap<>())
                            .put(rs.getString("alert_type"), rs.getLong("n"));
                });
        List<CellSummary> out = new ArrayList<>();
        for (Map.Entry<String, long[]> e : telemetry.entrySet()) {
            Object[] c = cycles.get(e.getKey());
            long[] t = e.getValue();
            out.add(new CellSummary(e.getKey(), t[0], Instant.ofEpochMilli(t[1]), Instant.ofEpochMilli(t[2]),
                    c == null ? 0 : (Long) c[0],
                    c == null ? null : (Integer) c[1],
                    c == null ? null : (Double) c[2],
                    c == null ? null : (Double) c[3],
                    c == null ? null : (Double) c[4],
                    counts.getOrDefault(e.getKey(), Map.of())));
        }
        return out;
    }

    public Optional<CellSummary> cell(String cellId) {
        return cells().stream().filter(c -> c.cellId().equals(cellId)).findFirst();
    }

    public boolean cellExists(String cellId) {
        Long n = jdbc.queryForObject("SELECT count() FROM bsa.telemetry WHERE cell_id = :cellId",
                new MapSqlParameterSource("cellId", cellId), Long.class);
        return n != null && n > 0;
    }

    public List<AlertView> alertsForCell(String cellId, String type, Instant since, int limit) {
        MapSqlParameterSource p = new MapSqlParameterSource()
                .addValue("cellId", cellId)
                .addValue("type", type)
                .addValue("sinceMs", since == null ? 0L : since.toEpochMilli())
                .addValue("limit", limit);
        return jdbc.query("SELECT " + ALERT_COLUMNS + " FROM bsa.alerts FINAL "
                + "WHERE cell_id = :cellId AND event_time_ms >= :sinceMs "
                + "AND (:type IS NULL OR alert_type = :type) "
                + "ORDER BY event_time_ms, alert_type, alert_id LIMIT :limit", p, ALERT_ROW);
    }

    public List<AlertView> alertsSince(Instant since, Instant until, String type, int limit) {
        MapSqlParameterSource p = new MapSqlParameterSource()
                .addValue("sinceMs", since.toEpochMilli())
                .addValue("untilMs", until == null ? Long.MAX_VALUE : until.toEpochMilli())
                .addValue("type", type)
                .addValue("limit", limit);
        return jdbc.query("SELECT " + ALERT_COLUMNS + " FROM bsa.alerts FINAL "
                + "WHERE event_time_ms >= :sinceMs AND event_time_ms < :untilMs "
                + "AND (:type IS NULL OR alert_type = :type) "
                + "ORDER BY event_time_ms, cell_id, alert_type, alert_id LIMIT :limit", p, ALERT_ROW);
    }

    public Map<String, Long> alertCountsByType() {
        Map<String, Long> m = new TreeMap<>();
        jdbc.query("SELECT alert_type, count() AS n FROM bsa.alerts FINAL GROUP BY alert_type",
                rs -> {
                    m.put(rs.getString("alert_type"), rs.getLong("n"));
                });
        return m;
    }

    public List<CycleView> cycles(String cellId, int limit) {
        MapSqlParameterSource p = new MapSqlParameterSource().addValue("cellId", cellId).addValue("limit", limit);
        return jdbc.query("SELECT cycle, start_ms, end_ms, samples, missing_samples, capacity_ah, soh, rolling_baseline_ah "
                        + "FROM bsa.cycles FINAL WHERE cell_id = :cellId ORDER BY cycle LIMIT :limit", p,
                (rs, n) -> new CycleView(rs.getInt("cycle"), Instant.ofEpochMilli(rs.getLong("start_ms")),
                        Instant.ofEpochMilli(rs.getLong("end_ms")), rs.getLong("samples"), rs.getLong("missing_samples"),
                        rs.getDouble("capacity_ah"), nullableDouble(rs, "soh"), nullableDouble(rs, "rolling_baseline_ah")));
    }

    private static Double nullableDouble(ResultSet rs, String col) throws SQLException {
        double v = rs.getDouble(col);
        return rs.wasNull() || Double.isNaN(v) ? null : v;
    }
}
