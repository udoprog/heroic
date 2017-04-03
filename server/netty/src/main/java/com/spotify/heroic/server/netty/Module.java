package com.spotify.heroic.server.netty;

import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.dagger.LoadingComponent;

public class Module implements HeroicModule {
    @Override
    public Runnable setup(final LoadingComponent loading) {
        return () -> {
            loading
                .heroicConfigurationContext()
                .registerType("netty", NettyServerConfig.Builder.class);
        };
    }
}
