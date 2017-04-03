package com.spotify.heroic;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.rs.RsMapping;
import eu.toolchain.rs.RsRoutesProvider;
import javax.ws.rs.container.ContainerResponseFilter;

public interface ResourceConfigurator {
    void addResponseFilter(ContainerResponseFilter responseFilter);

    void addSimpleRewrite(String fromPrefix, String toPrefix);

    void addRoutes(RsRoutesProvider<? extends RsMapping<? extends AsyncFuture<?>>> routes);
}
