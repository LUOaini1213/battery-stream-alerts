package bsa.flink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointLocatorTest {

    @Test
    void picksNewestCompletedCheckpoint(@TempDir Path root) throws Exception {
        assertTrue(CheckpointLocator.latest(root).isEmpty());
        assertTrue(CheckpointLocator.latest(root.resolve("missing")).isEmpty());

        Path old = Files.createDirectories(root.resolve("job1/chk-7"));
        Files.writeString(old.resolve("_metadata"), "x");
        Files.setLastModifiedTime(old.resolve("_metadata"), FileTime.fromMillis(1_000_000));
        Path newer = Files.createDirectories(root.resolve("job2/chk-3"));
        Files.writeString(newer.resolve("_metadata"), "x");
        Files.setLastModifiedTime(newer.resolve("_metadata"), FileTime.fromMillis(2_000_000));
        Files.createDirectories(root.resolve("job2/chk-4")); // in progress: no _metadata yet
        Files.createDirectories(root.resolve("job2/shared"));

        assertEquals(newer, CheckpointLocator.latest(root).orElseThrow());
        assertEquals(3, CheckpointLocator.checkpointNumber(newer));
        assertEquals(-1, CheckpointLocator.checkpointNumber(root.resolve("chk-x")));
    }
}
