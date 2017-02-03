package com.spotify.heroic.metric.filesystem.wal;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.metric.filesystem.FilesystemBackend;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@JsonTypeName("file")
@NoArgsConstructor
@AllArgsConstructor
public class FileWalConfig implements WalConfig {
    public static final Duration DEFAULT_FLUSH_DURATION = Duration.of(100, TimeUnit.MILLISECONDS);
    public static final long DEFAULT_MAX_TRANSACTIONS_PER_FLUSH = 100;

    private Optional<Duration> flushDuration = empty();
    private Optional<Long> maxTransactionsPerFlush = empty();

    @Override
    public Wal newWriteAheadLog(
        final FilesystemBackend backend
    ) {
        final Duration flushDuration = this.flushDuration.orElse(DEFAULT_FLUSH_DURATION);
        final long maxTransactionsPerFlush =
            this.maxTransactionsPerFlush.orElse(DEFAULT_MAX_TRANSACTIONS_PER_FLUSH);

        final Path walPath = backend.getStoragePath().resolve("wal");

        return new FileWal(backend.getAsync(), backend.getFiles(),
            backend.getSerializer(), backend.getScheduler(), walPath, flushDuration,
            maxTransactionsPerFlush);
    }

    /**
     * The duration at which the log should be flushed.
     *
     * If zero, disabled flushing and data is immediately commited to the log.
     */
    public FileWalConfig flushDuration(final Duration flushDuration) {
        this.flushDuration = of(flushDuration);
        return this;
    }
}
