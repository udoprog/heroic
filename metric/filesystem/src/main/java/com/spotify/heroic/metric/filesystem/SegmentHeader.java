package com.spotify.heroic.metric.filesystem;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class SegmentHeader {
    private final long txId;
}
