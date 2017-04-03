package com.spotify.heroic.server;

public interface OnceObservable<T> {
    void observe(OnceObserver<T> observer);
}
