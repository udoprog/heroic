package com.spotify.heroic.metric.filesystem.wal;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.dagger.PrimaryComponent;
import eu.toolchain.serializer.Serializer;
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
    public static final boolean DEFAULT_WAIT_FOR_NEXT_SYNC = false;

    private Optional<Duration> flushDuration = empty();
    private Optional<Boolean> waitForNextSync = empty();

    @Override
    public <T> Wal<T> newWriteAheadLog(
        final WalReceiver<T> receiver, final Serializer<T> serializer,
        final PrimaryComponent primary, final Dependencies dependencies
    ) {
        final Duration flushDuration = this.flushDuration.orElse(DEFAULT_FLUSH_DURATION);
        final boolean waitForNextSync = this.waitForNextSync.orElse(DEFAULT_WAIT_FOR_NEXT_SYNC);

        final Path rootPath = dependencies.storagePath().resolve("wal");

        return new FileWal<>(receiver, serializer, primary.async(), primary.serializer(),
            primary.scheduler(), dependencies.files(), rootPath, flushDuration, waitForNextSync);
    }

    /**
     * The duration at which the log should be flushed.
     * <p>
     * If zero, disabled flushing and data is immediately commited to the log.
     *
     * @param flushDuration flush duration
     * @return this config
     */
    public FileWalConfig flushDuration(final Duration flushDuration) {
        this.flushDuration = of(flushDuration);
        return this;
    }

    /**
     * Force writes to wait for next sync before they are resolved.
     *
     * @param waitForNextSync wait for next sync
     * @return this config
     */
    public FileWalConfig waitForNextSync(final boolean waitForNextSync) {
        this.waitForNextSync = of(waitForNextSync);
        return this;
    }
}
