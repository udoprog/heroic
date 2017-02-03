package com.spotify.heroic.metric.filesystem.wal;

import java.nio.file.Path;
import lombok.Data;

@Data
public class WalPath implements Comparable<WalPath> {
    private final Path path;
    private final long txId;

    @Override
    public int compareTo(final WalPath o) {
        return Long.compare(txId, o.txId);
    }
}
