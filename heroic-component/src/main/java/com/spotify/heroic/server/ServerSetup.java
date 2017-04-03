package com.spotify.heroic.server;

import eu.toolchain.async.AsyncFuture;

public interface ServerSetup {
    AsyncFuture<ServerHandle> bind(
        String defaultHost, Integer defaultPort, ServerInstance serverInstance
    );
}
