package com.spotify.heroic.metric.filesystem.wal;

import lombok.Data;

/**
 * Contents of the WAL ID file.
 */
@Data
public class WalEntry<T> {
    private final long txId;
    private final T value;
    private final int checksum;
}
