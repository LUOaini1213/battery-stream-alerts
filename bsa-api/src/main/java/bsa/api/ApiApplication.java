package bsa.api;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@OpenAPIDefinition(info = @Info(title = "battery-stream-alerts API", version = "0.1.0",
        description = "Read-only access to replayed battery telemetry, cycle summaries and alerts stored in ClickHouse. "
                + "Times are event times of the replayed lab data (2008), not wall-clock times."))
public class ApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }
}
