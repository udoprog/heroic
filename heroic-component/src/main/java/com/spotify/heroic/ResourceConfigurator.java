package com.spotify.heroic;

import com.spotify.heroic.server.RequestFilter;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.rs.RsMapping;
import eu.toolchain.rs.RsRoutesProvider;

public interface ResourceConfigurator {
    void addRequestFilter(RequestFilter responseFilter);

    void addSimpleRewrite(String fromPrefix, String toPrefix);

    void addRoutes(RsRoutesProvider<? extends RsMapping<? extends AsyncFuture<?>>> routes);
}
