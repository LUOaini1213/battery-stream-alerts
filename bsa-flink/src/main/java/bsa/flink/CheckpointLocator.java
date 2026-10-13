package bsa.flink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Finds the newest completed checkpoint under the checkpoint directory.
 *
 * <p>Why: the job runs in an embedded MiniCluster. Inside one JVM, Flink's restart strategy already
 * restores from the latest checkpoint after a task failure. But if the whole container dies, the
 * MiniCluster's memory is gone; the only thing left is the retained checkpoint on the volume. On
 * start-up we look for it and, if present, pass it as the restore path, which is what a real
 * cluster's JobManager high-availability setup would do for us.
 *
 * <p>A checkpoint is complete when its {@code chk-N/_metadata} file exists.
 */
public final class CheckpointLocator {

    private CheckpointLocator() {
    }

    public static Optional<Path> latest(Path root) throws IOException {
        if (root == null || !Files.isDirectory(root)) return Optional.empty();
        try (Stream<Path> s = Files.walk(root, 3)) {
            return s.filter(p -> p.getFileName().toString().equals("_metadata"))
                    .map(Path::getParent)
                    .filter(p -> p.getFileName().toString().startsWith("chk-"))
                    .max(Comparator.comparingLong(CheckpointLocator::mtime)
                            .thenComparingLong(CheckpointLocator::checkpointNumber));
        }
    }

    static long checkpointNumber(Path chkDir) {
        try {
            return Long.parseLong(chkDir.getFileName().toString().substring(4));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static long mtime(Path chkDir) {
        try {
            return Files.getLastModifiedTime(chkDir.resolve("_metadata")).toMillis();
        } catch (IOException e) {
            return Long.MIN_VALUE;
        }
    }
}
