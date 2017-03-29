package com.spotify.heroic.elasticsearch.client;

import eu.toolchain.async.AsyncFuture;

public interface Client {
    AsyncFuture<Void> putTemplate(final PutTemplate request);

    void close();
}
