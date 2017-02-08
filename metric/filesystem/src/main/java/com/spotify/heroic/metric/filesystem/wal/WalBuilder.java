package com.spotify.heroic.metric.filesystem.wal;

public interface WalBuilder<T> {
    Wal<T> build(WalReceiver<T> receiver);
}
