package com.spotify.heroic.server;

public interface ServerRequest {
    String method();

    String path();

    Headers headers();

    ServerRequest withHeaders(Headers headers);
}
