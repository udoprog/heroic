package com.spotify.heroic.metric.filesystem;

import com.spotify.heroic.common.Duration;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.filesystem.wal.FileWalConfig;
import com.spotify.heroic.test.AbstractMetricBackendIT;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class FilesystemBackendIT extends AbstractMetricBackendIT {
    public static final Duration WIDTH = Duration.of(2, TimeUnit.HOURS);

    private final Path storagePath = Paths.get("storage_" + UUID.randomUUID());

    @Override
    protected Optional<Long> period() {
        return Optional.of(WIDTH.toMilliseconds());
    }

    @Override
    protected void setupSupport() {
        super.setupSupport();

        this.eventSupport = true;
        this.maxBatchSize = Optional.of(10000);
    }

    @Override
    protected Optional<MetricModule> setupModule() {
        return Optional.of(FilesystemMetricModule
            .builder()
            .wal(new FileWalConfig().flushDuration(Duration.of(0, TimeUnit.MILLISECONDS)))
            .flushInterval(Duration.of(0, TimeUnit.MILLISECONDS))
            .memory(false)
            .segmentWidth(WIDTH)
            .compression(Compression.GORILLA)
            .storagePath(storagePath)
            .build());
    }

    @Override
    protected void postShutdown() throws Exception {
        // remove temporary storage directory
        Files.walkFileTree(storagePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(
                Path file, BasicFileAttributes attrs
            ) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
