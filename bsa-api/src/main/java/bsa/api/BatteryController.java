package bsa.api;

import bsa.api.Dtos.AlertView;
import bsa.api.Dtos.CellSummary;
import bsa.api.Dtos.CycleView;
import bsa.core.model.AlertType;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Read-only endpoints. Validation happens before any SQL runs:
 * cell ids must match a short whitelist pattern, alert types must be a known enum value,
 * limits are bounded, and times must be ISO-8601 instants. Violations return 400 problem+json.
 */
@RestController
@RequestMapping("/api/v1")
public class BatteryController {
    static final String CELL_ID = "^[A-Za-z0-9_-]{1,32}$";

    private final BatteryRepository repo;

    public BatteryController(BatteryRepository repo) {
        this.repo = repo;
    }

    @Operation(summary = "List cells with sample counts, latest capacity/SOH and alert counts by type")
    @GetMapping("/cells")
    public List<CellSummary> cells() {
        return repo.cells();
    }

    @Operation(summary = "One cell")
    @GetMapping("/cells/{cellId}")
    public CellSummary cell(@PathVariable @Pattern(regexp = CELL_ID) String cellId) {
        return repo.cell(cellId).orElseThrow(() -> notFound(cellId));
    }

    @Operation(summary = "Alerts of one cell, oldest first")
    @GetMapping("/cells/{cellId}/alerts")
    public List<AlertView> cellAlerts(
            @PathVariable @Pattern(regexp = CELL_ID) String cellId,
            @Parameter(description = "Filter by alert type") @RequestParam(required = false) AlertType type,
            @Parameter(description = "Only alerts with event time >= this ISO-8601 instant")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant since,
            @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int limit) {
        if (!repo.cellExists(cellId)) throw notFound(cellId);
        return repo.alertsForCell(cellId, type == null ? null : type.name(), since, limit);
    }

    @Operation(summary = "Per-cycle capacity, SOH and rolling baseline of one cell")
    @GetMapping("/cells/{cellId}/cycles")
    public List<CycleView> cycles(@PathVariable @Pattern(regexp = CELL_ID) String cellId,
                                  @RequestParam(defaultValue = "500") @Min(1) @Max(1000) int limit) {
        if (!repo.cellExists(cellId)) throw notFound(cellId);
        return repo.cycles(cellId, limit);
    }

    @Operation(summary = "Alerts of all cells with event time in [since, until)")
    @GetMapping("/alerts")
    public List<AlertView> alerts(
            @Parameter(description = "Required ISO-8601 instant, e.g. 2008-04-02T00:00:00Z", required = true)
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant since,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant until,
            @RequestParam(required = false) AlertType type,
            @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int limit) {
        if (until != null && !until.isAfter(since)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "until must be after since");
        }
        return repo.alertsSince(since, until, type == null ? null : type.name(), limit);
    }

    @Operation(summary = "Alert counts by type over all cells")
    @GetMapping("/alerts/summary")
    public Map<String, Long> summary() {
        return repo.alertCountsByType();
    }

    private static ResponseStatusException notFound(String cellId) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "unknown cell " + cellId);
    }
}
