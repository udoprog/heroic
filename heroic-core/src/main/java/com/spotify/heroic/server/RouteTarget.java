package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.MimeType;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.rs.RsMapping;
import java.util.List;
import lombok.Data;

@Data
class RouteTarget {
    private final RsMapping<AsyncFuture<?>> mapping;
    private final List<MimeType> produces;
}
