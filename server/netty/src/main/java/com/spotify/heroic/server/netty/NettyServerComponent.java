package com.spotify.heroic.server.netty;

import com.spotify.heroic.dagger.PrimaryComponent;
import dagger.Component;

@Component(modules = NettyServerConfig.class, dependencies = PrimaryComponent.class)
public interface NettyServerComponent {
    NettyServerSetup serverSetup();
}
