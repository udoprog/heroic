package com.spotify.heroic.server;

public interface Observable<I> {
    void observe(Observer<? super I> observer);
}
