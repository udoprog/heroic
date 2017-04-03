package com.spotify.heroic.server;

import java.util.function.BiFunction;

public interface RequestFilter {
    BodyWriter filter(
        ServerRequest request, OnceObserver<ServerResponse> response,
        BiFunction<ServerRequest, OnceObserver<ServerResponse>, BodyWriter> next
    );
}
