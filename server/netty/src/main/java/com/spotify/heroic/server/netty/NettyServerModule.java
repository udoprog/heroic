package com.spotify.heroic.server.netty;

import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.server.ServerModule;
import com.spotify.heroic.server.ServerSetup;

public class NettyServerModule implements ServerModule {
    @Override
    public ServerSetup module(final PrimaryComponent primary) {
        return new NettyServerSetup(primary.async());
    }
}
