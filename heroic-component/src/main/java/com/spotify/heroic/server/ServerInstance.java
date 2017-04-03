package com.spotify.heroic.server;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface ServerInstance {
    OnceObservable<Observer<ByteBuffer>> call(
        final String method, final String path, final Headers headers,
        final Consumer<OnceObservable<ServerResponse>> response
    );
}
