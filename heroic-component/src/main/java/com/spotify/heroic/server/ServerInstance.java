package com.spotify.heroic.server;

public interface ServerInstance {
    BodyWriter call(ServerRequest request, OnceObserver<ServerResponse> response);
}
