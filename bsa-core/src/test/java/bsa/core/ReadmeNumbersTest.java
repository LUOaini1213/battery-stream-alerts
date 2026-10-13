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
    private static final Pattern PIN = Pattern.compile(
            "^\\|[^|]*\\|\\s*([^|]+?)\\s*\\|\\s*<!--\\s*([\\w.-]+\\.json):(/.*?)\\s*-->", Pattern.MULTILINE);

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
        assertTrue(pins >= 20, "expected at least 20 pinned numbers in README.md, found " + pins);
        assertTrue(problems.isEmpty(), String.join("\n", problems));
    }
}
