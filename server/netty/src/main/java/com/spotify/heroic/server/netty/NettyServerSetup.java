package com.spotify.heroic.server.netty;

import com.spotify.heroic.server.ServerHandle;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.ServerSetup;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.net.InetSocketAddress;

public class NettyServerSetup implements ServerSetup {
    private final AsyncFramework async;

    public NettyServerSetup(final AsyncFramework async) {
        this.async = async;
    }

    @Override
    public AsyncFuture<ServerHandle> bind(
        final InetSocketAddress bind, final ServerInstance serverInstance
    ) {
        return async.resolved(async::resolved);
    }
}
