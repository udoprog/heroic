package com.spotify.heroic.metric.filesystem.wal;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

/**
 * Contents of the WAL ID file.
 */
@AutoSerialize
@Data
public class TxIdFile {
    private final long committedId;
    private final long checksum;

    public long calculateChecksum() {
        return ~committedId;
    }
}
