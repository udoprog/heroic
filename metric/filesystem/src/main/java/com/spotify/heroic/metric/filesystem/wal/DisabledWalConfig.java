package com.spotify.heroic.metric.filesystem.wal;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.dagger.PrimaryComponent;
import eu.toolchain.serializer.Serializer;

@JsonTypeName("disabled")
public class DisabledWalConfig implements WalConfig {
    @Override
    public <T> Wal<T> newWriteAheadLog(
        final WalReceiver<T> receiver, final Serializer<T> valueSerializer,
        final PrimaryComponent primary, final Dependencies dependencies
    ) {
        return new DisabledWal<>(primary.async(), receiver);
    }
}
