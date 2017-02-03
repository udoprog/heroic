package com.spotify.heroic.metric.filesystem.wal;

import lombok.Data;

/**
 * A single record to append to the WAL log.
 */
@Data
class WalBytesEntry {
    private final long txId;
    private final byte[] bytes;
    private final int checksum;
}
