package com.spotify.heroic.server;

import eu.toolchain.async.AsyncFuture;

public interface ServerHandle {
    AsyncFuture<Void> stop();
}
