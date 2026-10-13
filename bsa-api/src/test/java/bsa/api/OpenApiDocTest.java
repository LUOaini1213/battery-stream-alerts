package bsa.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Full application context (repository mocked): the OpenAPI document lists every endpoint. */
@SpringBootTest
@AutoConfigureMockMvc
class OpenApiDocTest {
    @Autowired
    MockMvc mvc;

    @MockBean
    BatteryRepository repo;

    @Test
    void openApiDocumentDescribesEndpoints() throws Exception {
        mvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.info.title").value("battery-stream-alerts API"))
                .andExpect(jsonPath("$.paths['/api/v1/cells']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/cells/{cellId}/alerts']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/alerts'].get.parameters[0].name").value("since"))
                .andExpect(jsonPath("$.paths['/api/v1/alerts'].get.parameters[0].required").value(true))
                .andExpect(jsonPath("$.paths['/api/v1/alerts/summary']").exists());
    }
}
