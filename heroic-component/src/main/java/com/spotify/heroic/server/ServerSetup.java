package com.spotify.heroic.server;

import eu.toolchain.async.AsyncFuture;
import java.net.InetSocketAddress;

public interface ServerSetup {
    AsyncFuture<ServerHandle> bind(InetSocketAddress address, ServerInstance serverInstance);
}
