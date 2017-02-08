package com.spotify.heroic.metric.filesystem.wal;

import com.spotify.heroic.metric.filesystem.io.SegmentIterable;

public interface WalReceiver<T> {
    void receive(SegmentIterable<WalEntry<T>> transactions) throws Exception;
}
