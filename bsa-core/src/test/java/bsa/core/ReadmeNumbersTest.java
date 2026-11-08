package bsa.core;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pins every number quoted in README.md to the results file it came from.
 *
 * <p>A README table row like
 * {@code | Samples replayed | 185721 | <!-- e2e-clean.json:/replay/sample_records_sent --> |}
 * must show exactly the value at that JSON pointer in {@code results/e2e-clean.json}. Editing a
 * number by hand, or re-running the demo without updating the README, fails this test.
 */
class ReadmeNumbersTest {
    /** The value is the table cell directly before the pin comment, whatever the table width. */
    private static final Pattern PIN = Pattern.compile(
            "\\|[ \\t]*([^|\\n]+?)[ \\t]*\\|[ \\t]*<!--[ \\t]*([\\w.-]+\\.json):(/[^\\n]*?)[ \\t]*-->");
    /** Every pin comment in the README, matched by PIN or not; the two counts must agree. */
    private static final Pattern ANY_PIN_COMMENT = Pattern.compile("<!--[ \\t]*[\\w.-]+\\.json:");

    @Test
    void readmeNumbersMatchResultFiles() throws Exception {
        Path root = Path.of("..");
        String readme = Files.readString(root.resolve("README.md"), StandardCharsets.UTF_8);
        ObjectMapper om = new ObjectMapper();
        Matcher m = PIN.matcher(readme);
        List<String> problems = new ArrayList<>();
        int pins = 0;
        while (m.find()) {
            pins++;
            String shown = m.group(1).replace(",", "").replace("`", "").trim();
            JsonNode doc = om.readTree(root.resolve("results").resolve(m.group(2)).toFile());
            JsonNode v = doc.at(JsonPointer.compile(m.group(3)));
            if (v.isMissingNode()) {
                problems.add(m.group(2) + m.group(3) + " does not exist");
            } else if (!v.asText().equals(shown)) {
                problems.add(m.group(2) + m.group(3) + ": README shows '" + shown + "' but file has '" + v.asText() + "'");
            }
        }
        Matcher all = ANY_PIN_COMMENT.matcher(readme);
        int comments = 0;
        while (all.find()) {
            comments++;
        }
        assertTrue(pins > 0, "no pinned numbers found in README.md");
        assertEquals(comments, pins, "README.md has " + comments + " '<!-- file.json:/pointer -->' comments but only "
                + pins + " sit directly after a table cell, so the rest are not checked");
        assertTrue(problems.isEmpty(), String.join("\n", problems));
    }

    /** The checksummed NASA download must convert to the same data the end-to-end runs replayed. */
    @Test
    void nasaInputTotalsMatchTheReplayedData() throws Exception {
        Path results = Path.of("..").resolve("results");
        ObjectMapper om = new ObjectMapper();
        JsonNode inputs = om.readTree(results.resolve("nasa-inputs.json").toFile());
        JsonNode clean = om.readTree(results.resolve("e2e-clean.json").toFile());
        assertEquals(clean.at("/replay/csv_rows").asLong(), inputs.at("/converter/total_samples").asLong(),
                "nasa-inputs.json total_samples vs e2e-clean.json csv_rows");
        assertEquals(clean.at("/rows/cycles/final").asLong(), inputs.at("/converter/discharge_cycles").asLong(),
                "nasa-inputs.json discharge_cycles vs e2e-clean.json cycles");
        long perCell = 0;
        for (JsonNode cell : inputs.at("/converter/per_cell")) {
            perCell += cell.get("samples").asLong();
        }
        assertEquals(inputs.at("/converter/total_samples").asLong(), perCell, "per-cell samples must add up");
        assertEquals(4, inputs.at("/mat_sha256").size(), "one checksum per .mat file");
        inputs.at("/mat_sha256").forEach(h -> assertTrue(h.asText().matches("[0-9a-f]{64}"), "bad sha256 " + h));
    }
}
