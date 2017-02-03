package com.spotify.heroic.metric.filesystem.wal;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.metric.filesystem.FilesystemBackend;

@JsonTypeName("disabled")
public class DisabledWalConfig implements WalConfig {
    @Override
    public Wal newWriteAheadLog(final FilesystemBackend backend) {
        return new DisabledWal(backend.getAsync());
    }
}
