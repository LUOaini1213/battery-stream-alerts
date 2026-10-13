package bsa.api;

import bsa.api.Dtos.AlertView;
import bsa.api.Dtos.CellSummary;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Web-layer slice: controller + validation + error mapping, repository mocked (no database). */
@WebMvcTest(BatteryController.class)
class BatteryControllerTest {
    @Autowired
    MockMvc mvc;

    @MockBean
    BatteryRepository repo;

    private static final AlertView ALERT = new AlertView("id-1", "B0005", "VOLTAGE_SAG", "WARN",
            Instant.parse("2008-04-02T16:00:00Z"), 1, -0.5, -0.3, "dV/dt", Instant.parse("2026-10-13T00:00:00Z"));

    @Test
    void listsCells() throws Exception {
        when(repo.cells()).thenReturn(List.of(new CellSummary("B0005", 10, Instant.EPOCH, Instant.EPOCH, 2, 2,
                1.85, 1.80, 0.97, Map.of("VOLTAGE_SAG", 3L))));
        mvc.perform(get("/api/v1/cells"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].cellId").value("B0005"))
                .andExpect(jsonPath("$[0].alertCounts.VOLTAGE_SAG").value(3))
                .andExpect(jsonPath("$[0].firstEventTime").value("1970-01-01T00:00:00Z"));
    }

    @Test
    void unknownCellIs404() throws Exception {
        when(repo.cell("B0099")).thenReturn(Optional.empty());
        mvc.perform(get("/api/v1/cells/B0099")).andExpect(status().isNotFound());
        when(repo.cellExists("B0099")).thenReturn(false);
        mvc.perform(get("/api/v1/cells/B0099/alerts")).andExpect(status().isNotFound());
        mvc.perform(get("/api/v1/cells/B0099/cycles")).andExpect(status().isNotFound());
    }

    @Test
    void cellAlertsPassValidatedParametersThrough() throws Exception {
        when(repo.cellExists("B0005")).thenReturn(true);
        when(repo.alertsForCell(eq("B0005"), eq("VOLTAGE_SAG"), eq(Instant.parse("2008-04-02T00:00:00Z")), eq(5)))
                .thenReturn(List.of(ALERT));
        mvc.perform(get("/api/v1/cells/B0005/alerts").param("type", "VOLTAGE_SAG")
                        .param("since", "2008-04-02T00:00:00Z").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].alertType").value("VOLTAGE_SAG"))
                .andExpect(jsonPath("$[0].eventTime").value("2008-04-02T16:00:00Z"));
    }

    @Test
    void defaultsApplyWhenOptionalParametersMissing() throws Exception {
        when(repo.cellExists("B0005")).thenReturn(true);
        when(repo.alertsForCell(eq("B0005"), isNull(), isNull(), eq(100))).thenReturn(List.of());
        mvc.perform(get("/api/v1/cells/B0005/alerts")).andExpect(status().isOk()).andExpect(content().json("[]"));
        verify(repo).alertsForCell("B0005", null, null, 100);
    }

    @Test
    void rejectsBadCellIdsBeforeTouchingTheDatabase() throws Exception {
        mvc.perform(get("/api/v1/cells/{id}/alerts", "B0005' OR 1=1 --"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"));
        mvc.perform(get("/api/v1/cells/{id}/alerts", "x".repeat(33))).andExpect(status().isBadRequest());
        verifyNoInteractions(repo);
    }

    @Test
    void rejectsBadQueryParameters() throws Exception {
        mvc.perform(get("/api/v1/cells/B0005/alerts").param("limit", "0")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/cells/B0005/alerts").param("limit", "1001")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/cells/B0005/alerts").param("type", "NOT_A_TYPE")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/cells/B0005/alerts").param("since", "yesterday")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/cells/B0005/cycles").param("limit", "-1")).andExpect(status().isBadRequest());
        verify(repo, never()).alertsForCell(anyString(), any(), any(), anyInt());
    }

    @Test
    void alertsSinceIsRequiredAndOrdered() throws Exception {
        mvc.perform(get("/api/v1/alerts")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/alerts").param("since", "2008-05-01T00:00:00Z").param("until", "2008-04-01T00:00:00Z"))
                .andExpect(status().isBadRequest());
        when(repo.alertsSince(Instant.parse("2008-04-01T00:00:00Z"), null, "DQ_LATE", 100)).thenReturn(List.of(ALERT));
        mvc.perform(get("/api/v1/alerts").param("since", "2008-04-01T00:00:00Z").param("type", "DQ_LATE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1));
    }

    @Test
    void databaseErrorsBecome503WithoutDetailsLeaking() throws Exception {
        when(repo.cells()).thenThrow(new DataAccessResourceFailureException("Connection refused: secret-host:8123"));
        mvc.perform(get("/api/v1/cells"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.detail").value("database unavailable"));
    }
}
