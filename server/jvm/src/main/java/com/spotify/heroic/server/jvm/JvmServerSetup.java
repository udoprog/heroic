package com.spotify.heroic.server.jvm;

import com.spotify.heroic.server.ServerHandle;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.ServerSetup;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.net.InetSocketAddress;

public class JvmServerSetup implements ServerSetup {
    private final AsyncFramework async;
    private final JvmServerEnvironment.Instance environment;

    public JvmServerSetup(
        final AsyncFramework async, final JvmServerEnvironment.Instance environment
    ) {
        this.async = async;
        this.environment = environment;
    }

    @Override
    public AsyncFuture<ServerHandle> bind(
        final String defaultHost, final Integer defaultPort, final ServerInstance serverInstance
    ) {
        final InetSocketAddress address =
            InetSocketAddress.createUnresolved(defaultHost, defaultPort);

        final Runnable unbind = environment.bind(address, serverInstance);

        return async.resolved(() -> {
            unbind.run();
            return async.resolved();
        });
    }
}
