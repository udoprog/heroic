package com.spotify.heroic.server;

public interface Observer<I> {
    void observe(I result);

    void abort(Throwable reason);

    void end();
}
