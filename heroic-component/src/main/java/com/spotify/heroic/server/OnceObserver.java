package com.spotify.heroic.server;

public interface OnceObserver<T> {
    void observe(T result);

    void fail(Throwable error);
}
